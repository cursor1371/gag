// WebSocket Hub：节点注册/注销、生命周期管理、多路复用消息路由。
//
// Hub 是所有 WS 节点连接的管理中心，负责：
//   - 节点 WS 连接的建立、注册和断开
//   - 元数据提取（channel_id, node_id, node_label）
//   - 多路复用消息路由（通过请求 ID 将响应投递到正确的 Pending channel）
//   - 协议级心跳（Gateway 主动 WS ping）
//   - 应用级心跳（回复 Node 的 application-level ping）
//   - 节点冷却与退避（Node + Model 复合维度）
//   - 节点/渠道状态管理与查询
//   - 定时清理（离线节点 TTL、排空超时、空渠道回收、过期冷却条目）

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
)

// UpstreamResponse 表示上游 HTTP 响应的完整内容。
type UpstreamResponse struct {
	Status  int               `json:"status"`
	Headers map[string]string `json:"headers"`
	Body    string            `json:"body"`
}

// WSPayload 是 WebSocket 消息的通用载荷结构。
type WSPayload struct {
	Status       int               `json:"status"`
	Headers      map[string]string `json:"headers"`
	Body         string            `json:"body"`
	Data         string            `json:"data"`
	Code         json.RawMessage   `json:"code"`
	Message      string            `json:"message"`
	HTTPResponse *UpstreamResponse `json:"http_response"`
}

// WSMessage 是 WebSocket 消息的完整结构。
type WSMessage struct {
	ID      string    `json:"id"`
	Type    string    `json:"type"`
	Payload WSPayload `json:"payload"`
	size    int64     // 原始消息字节数（用于背压控制）
}

// CooldownEntry 保存单个 (Node, Model) 维度的冷却状态。
//
// 键约定：
//   - 具体模型名（如 "gemini-2.5-flash"）：该模型的自动/手动冷却
//   - ""：无模型路径请求的自动冷却
//   - "*"：通配冷却，阻断所有模型（仅手动冷却使用）
type CooldownEntry struct {
	Since  time.Time // 冷却开始时间
	Until  time.Time // 冷却结束时间
	Reason string    // 冷却原因（如 "http_429"、"manual"）
	Manual bool      // 是否为手动冷却
	Count  int       // 连续冷却次数（指数退避因子）
}

// Node 表示一个 WebSocket 代理节点连接。
type Node struct {
	// --- 不可变字段（连接建立时设置） ---
	id, label, channelID string
	tokenID              int
	tokenValue           string
	remote               string
	conn                 *websocket.Conn
	ctx                  context.Context
	cancel               context.CancelFunc
	writeGate            chan struct{} // 写入互斥（容量 1 的 channel 实现 mutex）
	done                 chan struct{} // 关闭时通知所有等待者

	// --- 以下字段受 Hub.mu 保护 ---
	online     bool
	disabled   bool              // 手动禁用（不断连但不调度）
	draining   bool              // 被新连接替换，等待 in-flight 请求完成
	drainUntil time.Time         // 排空超时时间
	connected  time.Time         // 连接建立时间
	offlineAt  time.Time         // 断开时间
	lastActive time.Time         // 最后活跃时间
	cooldowns  map[string]*CooldownEntry // model → 冷却状态
	pending    map[string]*Pending // 请求 ID → 进行中的请求
	queued     int64             // 所有 pending channel 中排队消息的总字节数
	total      int64             // 累计请求总数
	success    int64             // 累计成功数
	failures   int64             // 累计失败数
	latencySum int64             // 累计延迟总和（毫秒）
}

// Channel 表示一个节点渠道分组。
type Channel struct {
	id      string
	enabled bool
	emptyAt time.Time // 渠道变空的时间（用于 TTL 清理）
}

// Hub 是所有 WS 节点连接的管理中心。
type Hub struct {
	app      *App
	mu       sync.Mutex
	nodes    map[string]*Node    // node_id → 最新的 Node（逻辑主节点）
	all      map[*Node]bool      // 所有 Node 实例（包含 draining 旧连接）
	channels map[string]*Channel // channel_id → Channel
	queue    []*waiter           // FIFO 等待队列
	lastID   string              // round_robin 轮询的上次选择 ID
	stopping bool                // 是否正在停止调度
}

// NewHub 创建一个新的 Hub 实例。
func NewHub(a *App) *Hub {
	return &Hub{
		app:      a,
		nodes:    make(map[string]*Node),
		all:      make(map[*Node]bool),
		channels: make(map[string]*Channel),
	}
}

// metadata 按 Header → Query → Cookie 三级回退提取节点元数据。
func metadata(r *http.Request, header, query, fallback string) string {
	v := r.Header.Get(header)
	if v == "" {
		v = r.URL.Query().Get(query)
	}
	if v == "" {
		if cookie, err := r.Cookie(query); err == nil {
			v = cookie.Value
		}
	}
	if v == "" {
		v = fallback
	}
	return v
}

// WebSocket 处理 WS 节点接入请求（GET /v1/ws）。
// 流程：验证 Token → 提取元数据 → 升级 WS 连接 → 注册节点 → 消息循环。
func (a *App) WebSocket(w http.ResponseWriter, r *http.Request) {
	// 验证 WS Token
	value := r.URL.Query().Get("auth_token")
	token, exists := a.cfg.Load().tokenByValue[value]
	if !exists || !token.Enabled || value == "" {
		geminiError(w, http.StatusUnauthorized, "WS Token 无效或已禁用")
		return
	}

	// 提取节点元数据（Header → Query → Cookie 三级回退）
	id := metadata(r, "X-Node-ID", "node_id", "aistudio-"+randomHex(4))
	channel := metadata(r, "X-Channel-ID", "channel_id", "default")
	label := metadata(r, "X-Node-Label", "node_label", id)

	// 验证元数据格式
	for _, v := range []string{id, channel, label} {
		if len(v) > 256 || strings.ContainsAny(v, "\r\n\x00") {
			geminiError(w, http.StatusBadRequest, "节点元数据无效")
			return
		}
	}

	// 升级为 WebSocket 连接（禁用压缩，跨域由 Token 鉴权保证安全）
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		CompressionMode:    websocket.CompressionDisabled,
		InsecureSkipVerify: true,
	})
	if err != nil {
		return
	}
	conn.SetReadLimit(128 << 20) // 128 MiB 消息上限

	ctx, cancel := context.WithCancel(a.ctx)
	n := &Node{
		id: id, label: label, channelID: channel,
		tokenID: token.ID, tokenValue: value, remote: r.RemoteAddr,
		conn: conn, ctx: ctx, cancel: cancel,
		writeGate: make(chan struct{}, 1),
		done:      make(chan struct{}),
		online:    true,
		connected: time.Now(),
		pending:   make(map[string]*Pending),
		cooldowns: make(map[string]*CooldownEntry),
	}

	// 记录连接被拒绝的原因
	if code, reason := a.hub.Register(n); code != 0 {
		slog.Warn("节点连接被拒绝", "node_id", id, "channel_id", channel,
			"remote_addr", r.RemoteAddr, "close_code", code, "reason", reason)
		_ = conn.Close(websocket.StatusCode(code), reason)
		cancel()
		return
	}

	// 节点连接日志包含完整元数据
	slog.Info("节点已连接", "node_id", id, "channel_id", channel,
		"label", label, "remote_addr", r.RemoteAddr, "token_id", token.ID)
	defer a.hub.Disconnect(n, websocket.StatusNormalClosure, "连接关闭")

	// 启动心跳 goroutine
	go a.hub.ping(n)
	// 进入消息接收循环
	a.hub.read(n)
}

// Register 将新节点注册到 Hub（必须持有 Hub.mu）。
// 返回 (0, "") 表示成功，否则返回 WS 关闭码和原因。
func (h *Hub) Register(n *Node) (int, string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	// 再次验证 Token（防止注册前 Token 被删除）
	c := h.app.cfg.Load()
	token, exists := c.tokenByID[n.tokenID]
	if !exists || !token.Enabled || token.Value != n.tokenValue {
		return 4001, "unauthorized"
	}
	if h.stopping || h.app.stopping.Load() {
		return 1001, "going_away"
	}

	// 同 ID 替换不受连接上限限制
	isReplacement := false
	if old := h.nodes[n.id]; old != nil && old.online {
		isReplacement = true
	}

	// 非替换场景下检查连接数上限
	if !isReplacement {
		online := 0
		for node := range h.all {
			if node.online {
				online++
			}
		}
		if c.Node.MaxConnections > 0 && online >= c.Node.MaxConnections {
			return 4029, "too_many_connections"
		}
	}

	// 处理同 ID 旧连接替换
	if old := h.nodes[n.id]; old != nil && old.online {
		// 从旧节点继承逻辑管理状态（禁用、冷却）
		n.disabled = old.disabled
		now := time.Now()
		for model, cd := range old.cooldowns {
			if now.Before(cd.Until) {
				entry := *cd // 深拷贝
				n.cooldowns[model] = &entry
			}
		}

		// 记录节点替换事件（仅在继承了禁用状态时输出该字段）
		replaceArgs := []any{"node_id", n.id,
			"old_remote", old.remote, "new_remote", n.remote,
			"old_active_requests", len(old.pending)}
		if n.disabled {
			replaceArgs = append(replaceArgs, "inherited_disabled", true)
		}
		slog.Info("节点被新连接替换", replaceArgs...)

		// 旧节点进入排空状态
		old.draining = true
		old.drainUntil = time.Now().Add(dur(c.Timeout.Total))
		if len(old.pending) == 0 {
			h.disconnectLocked(old, websocket.StatusNormalClosure, "replaced")
		}
	}

	// 注册新节点
	h.nodes[n.id] = n
	h.all[n] = true

	// 自动创建渠道（如不存在）
	if h.channels[n.channelID] == nil {
		h.channels[n.channelID] = &Channel{id: n.channelID, enabled: true}
		// 记录渠道自动创建
		slog.Info("渠道自动创建", "channel_id", n.channelID, "created_by_node", n.id)
	}
	h.channels[n.channelID].emptyAt = time.Time{}

	// 新节点可能可以服务等待队列中的请求
	h.dispatchLocked()
	return 0, ""
}

// disconnectLocked 断开节点连接（必须持有 Hub.mu）。
// 关闭 done channel 通知所有等待者，异步关闭 WS 连接。
func (h *Hub) disconnectLocked(n *Node, code websocket.StatusCode, reason string) {
	if !n.online {
		return
	}
	n.online = false
	n.offlineAt = time.Now()
	close(n.done) // 通知所有 select 等待 n.done 的 goroutine

	// 异步关闭 WS 连接（避免阻塞 Hub 锁）
	go func() {
		_ = n.conn.Close(code, reason)
		n.cancel()
		_ = n.conn.CloseNow()
	}()
}

// Disconnect 断开节点连接并触发队列调度。
func (h *Hub) Disconnect(n *Node, code websocket.StatusCode, reason string) {
	h.mu.Lock()
	wasOnline := n.online
	activeReqs := len(n.pending) // 在持有锁时捕获，避免与 Finish 的并发 map 访问竞争
	h.disconnectLocked(n, code, reason)
	h.dispatchLocked()
	h.mu.Unlock()
	if wasOnline {
		// 节点断开日志包含渠道、存活时长、活跃请求数
		slog.Info("节点已断开", "node_id", n.id, "channel_id", n.channelID,
			"reason", reason, "remote_addr", n.remote,
			"uptime_seconds", int64(n.offlineAt.Sub(n.connected).Seconds()),
			"active_requests", activeReqs)
	}
}

// read 是节点的消息接收循环。
// 持续读取 WS 消息并路由到对应请求的 Pending channel。
func (h *Hub) read(n *Node) {
	for {
		kind, data, err := n.conn.Read(n.ctx)
		if err != nil {
			return // 连接断开，由 defer Disconnect 处理
		}
		if kind != websocket.MessageText {
			slog.Warn("忽略非文本 WS 消息", "node_id", n.id)
			continue
		}

		var msg WSMessage
		if err := json.Unmarshal(data, &msg); err != nil {
			slog.Warn("忽略畸形 WS JSON", "node_id", n.id)
			continue
		}

		// 处理应用级心跳
		if msg.Type == "ping" {
			// 异步发送 pong，避免大请求写入期间阻塞读循环
			go func() {
				ctx, cancel := context.WithTimeout(n.ctx, 10*time.Second)
				defer cancel()
				_ = n.writeJSON(ctx, map[string]string{"type": "pong"})
			}()
			continue
		}

		// 验证消息类型
		switch msg.Type {
		case "http_response", "stream_start", "stream_chunk", "stream_end", "error":
		default:
			slog.Warn("忽略未知 WS 消息类型", "node_id", n.id, "type", msg.Type)
			continue
		}

		// 路由消息到对应请求的 Pending channel
		msg.size = int64(len(data))
		h.mu.Lock()
		p := n.pending[msg.ID]
		if p == nil {
			h.mu.Unlock()
			// 请求已完成（客户端取消/超时），节点仍在发送尾部数据，属于正常情况
			slog.Debug("忽略未知请求 ID 的响应", "node_id", n.id, "request_id", msg.ID)
			continue
		}

		// 节点级背压：单连接排队字节上限 128 MiB
		if n.queued+msg.size > 128<<20 {
			h.disconnectLocked(n, websocket.StatusPolicyViolation, "response_backpressure")
			h.mu.Unlock()
			return
		}

		// 请求级 channel 溢出时丢弃消息而非断开整个节点
		select {
		case p.ch <- msg:
			p.queued += msg.size
			n.queued += msg.size
		default:
			slog.Warn("请求邮箱溢出，消息被丢弃",
				"node_id", n.id, "request_id", msg.ID)
		}
		h.mu.Unlock()
	}
}

// writeJSON 向节点发送 JSON 消息。
// 使用 writeGate channel 实现写入互斥（每次只有一个写操作）。
func (n *Node) writeJSON(ctx context.Context, value any) error {
	select {
	case n.writeGate <- struct{}{}:
		defer func() { <-n.writeGate }()
	case <-ctx.Done():
		return ctx.Err()
	case <-n.done:
		return errors.New("节点已断开")
	}

	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return n.conn.Write(ctx, websocket.MessageText, data)
}

// ping 是节点的 WS 协议级心跳 goroutine。
// 定期发送 WS ping 帧，超时未收到 pong 则判定节点失联并断开。
func (h *Hub) ping(n *Node) {
	c := h.app.cfg.Load()
	ticker := time.NewTicker(dur(c.Node.WSPingInterval))
	defer ticker.Stop()

	for {
		select {
		case <-n.ctx.Done():
			return
		case <-n.done:
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(n.ctx, dur(c.Node.WSPongTimeout))
			err := n.conn.Ping(ctx)
			cancel()
			if err != nil {
				h.Disconnect(n, websocket.StatusGoingAway, "pong_timeout")
				return
			}
		}
	}
}

// Consumed 标记一条消息已被代理引擎消费，减少背压计数。
func (h *Hub) Consumed(lease *Lease, msg WSMessage) {
	h.mu.Lock()
	defer h.mu.Unlock()
	lease.Pending.queued -= msg.size
	lease.Node.queued -= msg.size
}

// coolingStatus 判断给定的 HTTP 状态码是否应触发节点冷却。
func coolingStatus(status int) bool {
	switch status {
	case 401, 403, 429, 500, 502, 503, 504:
		return true
	default:
		return false
	}
}

// retryStatus 判断给定的 HTTP 状态码是否允许重试。
// 所有 5xx 可重试；4xx 中仅冷却状态码（401/403/429）可重试。
func retryStatus(status int) bool {
	if status >= 400 && status < 500 {
		return coolingStatus(status)
	}
	return status >= 500
}

// Finish 标记一个代理请求完成，释放节点容量并触发队列调度。
// 根据结果更新节点统计、冷却状态。model 为请求的目标模型名。
func (h *Hub) Finish(lease *Lease, status int, requestErr error, latency time.Duration, model string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	n, p := lease.Node, lease.Pending
	if n.pending[p.id] != p {
		return // 已被其他路径释放
	}
	delete(n.pending, p.id)
	n.queued -= p.queued
	n.total++
	n.latencySum += latency.Milliseconds()
	n.lastActive = time.Now()

	// 更新成功/失败计数
	if requestErr == nil && status >= 200 && status < 400 {
		n.success++
		// 成功请求重置当前模型的冷却退避计数
		if entry := n.cooldowns[model]; entry != nil {
			entry.Count = 0
		}
	} else {
		n.failures++
	}

	// 冷却逻辑（指数退避，Node + Model 复合维度）
	// 手动冷却期间不被同 model 的自动冷却覆盖
	if coolingStatus(status) && n.online {
		entry := n.cooldowns[model]
		if entry != nil && entry.Manual && time.Now().Before(entry.Until) {
			// 手动冷却保护：不覆盖
		} else {
			c := h.app.cfg.Load()
			if entry == nil {
				entry = &CooldownEntry{}
				n.cooldowns[model] = entry
			}
			entry.Count++
			base := float64(dur(c.Cooldown.BaseDuration))
			maximum := float64(dur(c.Cooldown.MaxDuration))
			d := math.Min(base*math.Pow(c.Cooldown.Multiplier, float64(entry.Count-1)), maximum)
			entry.Since = time.Now()
			entry.Until = entry.Since.Add(time.Duration(d))
			entry.Reason = fmt.Sprintf("http_%d", status)
			entry.Manual = false
			// 记录自动冷却触发
			slog.Warn("节点进入自动冷却", "node_id", n.id, "channel_id", n.channelID,
				"model", model, "reason", entry.Reason, "duration", time.Duration(d).String(),
				"cooldown_count", entry.Count)
		}
	}

	// 排空完成：旧连接无待处理请求时断开
	if n.draining && len(n.pending) == 0 {
		h.disconnectLocked(n, websocket.StatusNormalClosure, "drained")
	}

	// 容量释放后尝试分配等待队列中的请求
	h.dispatchLocked()
}

// Reconcile 在配置热重载后协调节点状态。
// 检查 Token 是否仍然有效，无效时将节点标记为 draining。
func (h *Hub) Reconcile() {
	h.mu.Lock()
	defer h.mu.Unlock()
	c := h.app.cfg.Load()
	for n := range h.all {
		if !n.online {
			continue
		}
		token, exists := c.tokenByID[n.tokenID]
		if !exists || token.Value != n.tokenValue {
			// 记录 Token 变更导致节点状态变化
			slog.Info("节点因 Token 变更进入排空状态", "node_id", n.id,
				"channel_id", n.channelID, "token_id", n.tokenID,
				"active_requests", len(n.pending))
			n.draining = true
			n.drainUntil = time.Now().Add(dur(c.Timeout.Total))
			if len(n.pending) == 0 {
				h.disconnectLocked(n, websocket.StatusNormalClosure, "token_deleted")
			}
		}
	}
	h.dispatchLocked()
}

// StopScheduling 停止接受新的请求调度。
func (h *Hub) StopScheduling() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.stopping = true
	h.dispatchLocked()
}

// CloseAll 关闭所有节点连接。
func (h *Hub) CloseAll() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.stopping = true
	h.dispatchLocked()
	for n := range h.all {
		h.disconnectLocked(n, websocket.StatusGoingAway, "gateway_shutdown")
	}
}

// Run 是 Hub 的后台维护循环，每 250ms 执行一次：
//   - 排空超时检测（draining 节点超时强制断开）
//   - 过期冷却条目清理
//   - 离线节点 TTL 清理
//   - 空渠道 TTL 清理
//   - 冷却到期后触发队列调度
func (h *Hub) Run() {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-h.app.ctx.Done():
			return
		case now := <-ticker.C:
			h.mu.Lock()
			ttl := dur(h.app.cfg.Load().Node.OfflineTTL)

			// 检查排空超时、过期冷却清理和离线节点清理
			for n := range h.all {
				if n.online && n.draining && !now.Before(n.drainUntil) {
					// 记录排空超时强制断开
					slog.Warn("节点排空超时，强制断开", "node_id", n.id, "channel_id", n.channelID)
					h.disconnectLocked(n, websocket.StatusNormalClosure, "drain_timeout")
				}
				// 清理在线节点的已过期冷却条目，防止 map 膨胀
				if n.online {
					for model, cd := range n.cooldowns {
						if !now.Before(cd.Until) {
							delete(n.cooldowns, model)
						}
					}
				}
				if !n.online && len(n.pending) == 0 &&
					(h.nodes[n.id] != n || now.Sub(n.offlineAt) >= ttl) {
					// 记录离线节点 TTL 清理
					slog.Debug("离线节点记录已清理", "node_id", n.id, "channel_id", n.channelID)
					delete(h.all, n)
					if h.nodes[n.id] == n {
						delete(h.nodes, n.id)
					}
				}
			}

			// 空渠道 TTL 清理
			for id, ch := range h.channels {
				hasNode := false
				for n := range h.all {
					if n.channelID == id {
						hasNode = true
						break
					}
				}
				if hasNode {
					ch.emptyAt = time.Time{}
				} else if ch.emptyAt.IsZero() {
					ch.emptyAt = now
				} else if now.Sub(ch.emptyAt) >= ttl {
					// 记录空渠道 TTL 清理
					slog.Debug("空渠道记录已清理", "channel_id", id)
					delete(h.channels, id)
				}
			}

			// 冷却到期的节点可能现在可以服务请求
			h.dispatchLocked()
			h.mu.Unlock()
		}
	}
}

// ====== 以下为管理 API 查询方法 ======

// stateLocked 返回节点的当前显示状态字符串。
func (h *Hub) stateLocked(n *Node) string {
	switch {
	case !n.online:
		return "offline"
	case n.draining:
		return "draining"
	case n.disabled:
		return "disabled"
	}
	token, ok := h.app.cfg.Load().tokenByID[n.tokenID]
	if !ok || !token.Enabled || token.Value != n.tokenValue {
		return "disabled"
	}
	now := time.Now()
	for _, cd := range n.cooldowns {
		if now.Before(cd.Until) {
			return "cooling"
		}
	}
	if len(n.pending) > 0 {
		return "busy"
	}
	return "idle"
}

// nodeViewLocked 构建节点的管理 API 视图。
func (h *Hub) nodeViewLocked(n *Node) map[string]any {
	ids := make([]string, 0, len(n.pending))
	for id := range n.pending {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	now := time.Now()
	var cooldown any
	activeCooldowns := map[string]any{}
	cooldownCount := 0
	for model, cd := range n.cooldowns {
		if now.Before(cd.Until) {
			activeCooldowns[model] = map[string]any{
				"since": cd.Since, "until": cd.Until,
				"duration_seconds": cd.Until.Sub(cd.Since).Seconds(),
				"reason": cd.Reason, "manual": cd.Manual,
				"count": cd.Count,
			}
			cooldownCount++
		}
	}
	if len(activeCooldowns) > 0 {
		cooldown = activeCooldowns
	}

	uptime := time.Since(n.connected)
	if !n.online {
		uptime = n.offlineAt.Sub(n.connected)
	}

	avg := int64(0)
	if n.total > 0 {
		avg = n.latencySum / n.total
	}

	return map[string]any{
		"id": n.id, "label": n.label, "channel_id": n.channelID,
		"token_id": n.tokenID, "status": h.stateLocked(n),
		"active_requests": len(n.pending),
		"max_concurrency": h.app.cfg.Load().Node.MaxConcurrency,
		"connected_at": n.connected, "uptime_seconds": int64(uptime.Seconds()),
		"remote_addr": n.remote, "cooldown": cooldown,
		"active_request_ids": ids,
		"stats": map[string]any{
			"total": n.total, "success": n.success, "error": n.failures,
			"avg_latency_ms": avg, "cooldown_count": cooldownCount,
			"last_active_at": n.lastActive,
		},
	}
}

// Nodes 返回节点列表或单个节点详情。
func (h *Hub) Nodes(id string) ([]map[string]any, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if id != "" {
		n := h.nodes[id]
		if n == nil {
			return nil, false
		}
		return []map[string]any{h.nodeViewLocked(n)}, true
	}
	result := make([]map[string]any, 0, len(h.all))
	for n := range h.all {
		result = append(result, h.nodeViewLocked(n))
	}
	sort.Slice(result, func(i, j int) bool {
		return fmt.Sprint(result[i]["id"]) < fmt.Sprint(result[j]["id"])
	})
	return result, true
}

// ChangeNode 修改节点状态（启用/禁用/手动冷却）。
// model 参数仅在 cooldown 操作时生效：
//   - 激活：model 非空时冷却指定模型，model 空时使用通配 "*" 阻断所有模型
//   - 解除：model 非空时删除指定模型冷却，model 空时清除所有冷却条目
func (h *Hub) ChangeNode(id, action string, activate bool, duration time.Duration, model string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := h.nodes[id]
	if n == nil {
		return errors.New("节点未找到")
	}
	switch action {
	case "enable":
		n.disabled = false
	case "disable":
		n.disabled = true
	case "cooldown":
		if activate {
			key := model
			if key == "" {
				key = "*"
			}
			entry := n.cooldowns[key]
			if entry == nil {
				entry = &CooldownEntry{}
				n.cooldowns[key] = entry
			}
			entry.Since = time.Now()
			entry.Until = entry.Since.Add(duration)
			entry.Reason = "manual"
			entry.Manual = true
		} else {
			if model != "" {
				delete(n.cooldowns, model)
			} else {
				// 清空所有冷却条目
				n.cooldowns = make(map[string]*CooldownEntry)
			}
		}
	}
	h.dispatchLocked()
	return nil
}

// Channels 返回渠道列表或单个渠道详情。
func (h *Hub) Channels(id string) ([]map[string]any, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if id != "" && h.channels[id] == nil {
		return nil, false
	}
	result := []map[string]any{}
	for channelID, ch := range h.channels {
		if id != "" && id != channelID {
			continue
		}
		counts := map[string]int{
			"total": 0, "online": 0, "idle": 0, "busy": 0,
			"cooling": 0, "disabled": 0, "draining": 0, "offline": 0,
		}
		nodes := []map[string]any{}
		for n := range h.all {
			if n.channelID != channelID {
				continue
			}
			counts["total"]++
			counts[h.stateLocked(n)]++
			if n.online {
				counts["online"]++
			}
			if id != "" {
				nodes = append(nodes, h.nodeViewLocked(n))
			}
		}
		v := map[string]any{"id": channelID, "enabled": ch.enabled, "nodes": counts}
		if id != "" {
			v["node_list"] = nodes
		}
		result = append(result, v)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i]["id"].(string) < result[j]["id"].(string)
	})
	return result, true
}

// ChangeChannel 修改渠道的启用/禁用状态。
func (h *Hub) ChangeChannel(id string, enabled bool) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	ch := h.channels[id]
	if ch == nil {
		return errors.New("渠道未找到")
	}
	ch.enabled = enabled
	h.dispatchLocked()
	return nil
}

// CountNodesByToken 返回使用指定 Token ID 的在线节点数量。
// 用于 Token 启用/禁用时记录受影响的节点数。
func (h *Hub) CountNodesByToken(tokenID int) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	count := 0
	for n := range h.all {
		if n.online && n.tokenID == tokenID {
			count++
		}
	}
	return count
}

// Summary 返回节点/渠道/请求的汇总统计，供 GET /admin/status 使用。
func (h *Hub) Summary() (map[string]int, map[string]int, map[string]int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	nodes := map[string]int{
		"total": len(h.all), "online": 0, "idle": 0, "busy": 0,
		"cooling": 0, "disabled": 0, "draining": 0, "offline": 0,
	}
	active := 0
	for n := range h.all {
		nodes[h.stateLocked(n)]++
		active += len(n.pending)
		if n.online {
			nodes["online"]++
		}
	}
	channels := map[string]int{"total": len(h.channels), "enabled": 0, "disabled": 0}
	for _, ch := range h.channels {
		if ch.enabled {
			channels["enabled"]++
		} else {
			channels["disabled"]++
		}
	}
	requests := map[string]int{
		"active": active, "queued": len(h.queue),
		"queue_capacity": h.app.cfg.Load().Request.QueueMaxSize,
	}
	return nodes, channels, requests
}
