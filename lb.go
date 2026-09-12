// 负载均衡器：实现 round_robin（最少连接优先）和 fill_first（填充优先）两种策略。
//
// 核心概念：
//   - Pending：一个正在执行的代理请求，通过 channel 接收节点响应
//   - Lease：节点执行租约，包含被选中的 Node 和对应的 Pending
//   - waiter：排队等待空闲节点的请求
//
// 调度流程：
//  1. Acquire 尝试直接选择空闲节点
//  2. 无空闲节点时，请求进入 FIFO 等待队列
//  3. 节点释放容量时（Finish/dispatch），唤醒可服务的等待者
//
// 冷却检查在 Node + Model 复合维度上执行：
//   - 通配冷却 "*" 阻断所有模型
//   - 模型冷却仅阻断特定模型的请求
//   - dispatchLocked 单遍扫描消除队首阻塞

package main

import (
	"context"
	"errors"
	"sort"
	"time"
)

// 调度错误定义
var (
	ErrNoNodes   = errors.New("无可用节点")
	ErrQueueFull = errors.New("请求队列已满")
	ErrWait      = errors.New("等待空闲节点超时")
	ErrStopping  = errors.New("网关正在关闭")
)

// Pending 表示一个正在等待节点响应的代理请求。
type Pending struct {
	id     string         // 请求 ID（与 WS 消息的 id 字段对应）
	ch     chan WSMessage // 接收节点响应消息的 channel
	queued int64          // 当前 channel 中排队消息的字节数
}

// Lease 是节点执行租约，包含被选中的 Node 和对应的 Pending。
type Lease struct {
	Node    *Node
	Pending *Pending
}

// waiter 表示一个正在等待空闲节点的请求。
type waiter struct {
	id    string          // 请求 ID
	model string          // 请求的目标模型名
	tried map[string]bool // 已尝试过的节点 ID 集合（重试时排除）
	ready chan struct{}   // 关闭时表示分配完成
	lease *Lease          // 分配结果
	err   error           // 分配失败的错误
	ctx   context.Context // 带超时的上下文
}

// eligibleLocked 检查节点是否有资格接受指定模型的新请求调度（必须持有 Hub.mu）。
// 条件：在线、未排空、未禁用、未被尝试过、目标模型未在冷却、Token 有效、渠道启用、有剩余容量。
// 冷却检查为双层：通配冷却 "*" 阻断所有模型，模型冷却仅阻断匹配的模型。
func (h *Hub) eligibleLocked(n *Node, tried map[string]bool, model string) bool {
	if !n.online || n.draining || n.disabled || tried[n.id] {
		return false
	}
	// 双层冷却检查：通配冷却 + 目标模型冷却
	now := time.Now()
	if cd := n.cooldowns["*"]; cd != nil && now.Before(cd.Until) {
		return false
	}
	if cd := n.cooldowns[model]; cd != nil && now.Before(cd.Until) {
		return false
	}
	c := h.app.cfg.Load()
	token, exists := c.tokenByID[n.tokenID]
	if !exists || !token.Enabled || token.Value != n.tokenValue {
		return false
	}
	ch := h.channels[n.channelID]
	return ch != nil && ch.enabled && len(n.pending) < c.Node.MaxConcurrency
}

// selectLocked 从所有节点中选择一个最合适的节点（必须持有 Hub.mu）。
//
// 策略行为：
//   - round_robin：最少连接优先 — 优先选择 activeRequests 最少的节点，相同负载时轮询
//   - fill_first：填充优先 — 按 ID 字典序选择第一个有容量的节点
func (h *Hub) selectLocked(tried map[string]bool, model string) *Node {
	candidates := make([]*Node, 0, len(h.nodes))
	for _, n := range h.nodes {
		if h.eligibleLocked(n, tried, model) {
			candidates = append(candidates, n)
		}
	}
	if len(candidates) == 0 {
		return nil
	}
	// 按 ID 字典序排序，确保确定性
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].id < candidates[j].id
	})

	// fill_first：直接选择 ID 最小的候选节点
	if h.app.cfg.Load().LB.Strategy == "fill_first" {
		return candidates[0]
	}

	// round_robin：找到最小负载值
	minLoad := len(candidates[0].pending)
	for _, n := range candidates {
		minLoad = min(minLoad, len(n.pending))
	}

	// 在最小负载节点中进行轮询
	var first *Node
	for _, n := range candidates {
		if len(n.pending) != minLoad {
			continue
		}
		if first == nil {
			first = n
		}
		// 选择 ID 大于上次选择的第一个节点（实现轮询）
		if n.id > h.lastID {
			h.lastID = n.id
			return n
		}
	}
	// 所有最小负载节点 ID 都 <= lastID，从头开始
	h.lastID = first.id
	return first
}

// reserveLocked 在节点上预留请求容量（必须持有 Hub.mu）。
// channel 缓冲区设为 64，以容纳正常 SSE 突发而不导致消息丢弃。
func (h *Hub) reserveLocked(n *Node, id string) *Lease {
	p := &Pending{id: id, ch: make(chan WSMessage, 64)}
	n.pending[id] = p
	n.lastActive = time.Now()
	return &Lease{Node: n, Pending: p}
}

// dispatchLocked 尝试将等待队列中的请求分配给空闲节点（必须持有 Hub.mu）。
// 网关关闭时，向所有等待者返回 ErrStopping。
// 使用单遍扫描策略：对每个等待者独立尝试分配，避免模型级冷却导致的队首阻塞。
func (h *Hub) dispatchLocked() {
	if h.stopping {
		for _, w := range h.queue {
			w.err = ErrStopping
			close(w.ready)
		}
		h.queue = nil
		return
	}
	kept := h.queue[:0]
	for _, w := range h.queue {
		// 跳过已取消的等待者
		if w.ctx.Err() != nil {
			w.err = w.ctx.Err()
			close(w.ready)
			continue
		}
		n := h.selectLocked(w.tried, w.model)
		if n != nil {
			w.lease = h.reserveLocked(n, w.id)
			close(w.ready)
		} else {
			kept = append(kept, w)
		}
	}
	h.queue = kept
}

// Acquire 获取一个节点执行租约。
//
// 流程：
//  1. 尝试直接分配空闲节点
//  2. 无空闲且 wait=true 时，进入等待队列
//  3. 等待超时（node_wait_timeout）后返回 ErrWait
//
// tried 集合用于重试时排除已失败的节点。
// model 为请求的目标模型名，用于模型级冷却检查。
func (h *Hub) Acquire(ctx context.Context, id string, tried map[string]bool, wait bool, model string) (*Lease, error) {
	c := h.app.cfg.Load()
	waitCtx, cancel := context.WithTimeout(ctx, dur(c.Request.NodeWaitTimeout))
	defer cancel()

	h.mu.Lock()
	if h.stopping {
		h.mu.Unlock()
		return nil, ErrStopping
	}

	// 队列为空时尝试直接分配
	if len(h.queue) == 0 {
		if n := h.selectLocked(tried, model); n != nil {
			lease := h.reserveLocked(n, id)
			h.mu.Unlock()
			return lease, nil
		}
	}

	// 不允许等待，立即返回
	if !wait {
		h.mu.Unlock()
		return nil, ErrNoNodes
	}

	// 零在线节点时立即拒绝
	hasOnline := false
	for n := range h.all { // 遍历实际很快（h.all 只含已注册节点）
		if n.online && !n.draining {
			hasOnline = true
			break
		}
	}
	if !hasOnline {
		h.mu.Unlock()
		return nil, ErrNoNodes
	}

	// 队列已满，立即拒绝
	if len(h.queue) >= c.Request.QueueMaxSize {
		h.mu.Unlock()
		return nil, ErrQueueFull
	}

	// 加入等待队列
	w := &waiter{id: id, model: model, tried: tried, ready: make(chan struct{}), ctx: waitCtx}
	h.queue = append(h.queue, w)
	h.mu.Unlock()

	// 等待分配或超时
	select {
	case <-w.ready:
		return w.lease, w.err
	case <-waitCtx.Done():
		h.mu.Lock()
		// 检查是否在超时期间被分配了
		if w.lease != nil {
			lease := w.lease
			h.mu.Unlock()
			return lease, nil
		}
		// 从队列中移除
		for i, queued := range h.queue {
			if queued == w {
				h.queue = append(h.queue[:i], h.queue[i+1:]...)
				break
			}
		}
		h.dispatchLocked()
		h.mu.Unlock()

		if ctx.Err() != nil {
			return nil, ctx.Err() // 客户端取消
		}
		return nil, ErrWait // 等待超时
	}
}
