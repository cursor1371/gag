// 请求代理引擎：HTTP → WS 转发、重试、超时控制。
//
// 核心流程：
//  1. 将请求体缓存到临时文件（支持重试时重放）
//  2. 构造上游 URL 并剥离鉴权字段
//  3. 通过 LB 选择节点，发送 WS http_request 消息
//  4. 接收节点响应（流式/非流式），写入 HTTP 响应
//  5. 失败时按重试策略选择新节点重试
//
// WS 发送阶段使用流式 JSON 编码，避免额外构造完整 body 字符串副本。
// 请求规范化命中目标字段时，仍会读取完整正文并按需重新编码。

package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/coder/websocket"
)

// ErrInvalidUTF8 在请求体包含无效 UTF-8 字节时返回。
// WS 协议的 Text 帧要求 UTF-8 编码，因此必须验证。
var ErrInvalidUTF8 = errors.New("请求体必须是有效的 UTF-8 编码（WebSocket JSON 协议要求）")

// utf8Chunks 以有界内存将 Reader 内容分块发送给 consume 函数。
// 保证每个 chunk 都是完整的 UTF-8 序列，不会在多字节字符中间断开。
func utf8Chunks(ctx context.Context, r io.Reader, consume func([]byte) error) error {
	buffer := make([]byte, 32*1024+utf8.UTFMax) // 32KB + 最大 UTF-8 字符长度
	carry := 0                                  // 上次遗留的不完整字节数
	emptyReads := 0                             // 连续空读计数（防止无进展死循环）

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		n, readErr := r.Read(buffer[carry : 32*1024])
		if n == 0 && readErr == nil {
			emptyReads++
			if emptyReads >= 100 {
				return io.ErrNoProgress
			}
			continue
		}
		emptyReads = 0
		total := carry + n
		end := 0

		// 扫描完整的 UTF-8 字符，跳过末尾可能不完整的字符
		for end < total {
			_, width := utf8.DecodeRune(buffer[end:total])
			if width == 1 && buffer[end] >= utf8.RuneSelf {
				// 无效 UTF-8 或不完整
				if !utf8.FullRune(buffer[end:total]) && readErr == nil {
					break // 可能只是跨 chunk 边界，等下次读取
				}
				return ErrInvalidUTF8
			}
			end += width
		}
		if end > 0 {
			if err := consume(buffer[:end]); err != nil {
				return err
			}
		}
		carry = copy(buffer, buffer[end:total])

		if readErr != nil {
			if readErr != io.EOF {
				return readErr
			}
			if carry != 0 {
				return ErrInvalidUTF8 // EOF 但有不完整字符
			}
			return nil
		}
	}
}

// spoolBody 将请求体缓存到临时文件，同时验证 UTF-8 编码。
// 使用临时文件而非内存缓冲区，以支持大请求体（最大 64MB）的重试重放。
func spoolBody(ctx context.Context, body io.Reader) (*os.File, error) {
	file, err := os.CreateTemp("", "gag-request-*")
	if err != nil {
		return nil, err
	}
	if err := utf8Chunks(ctx, body, func(data []byte) error {
		_, err := file.Write(data)
		return err
	}); err != nil {
		name := file.Name()
		file.Close()
		os.Remove(name)
		return nil, err
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		name := file.Name()
		file.Close()
		os.Remove(name)
		return nil, err
	}
	return file, nil
}

// multiScan 在文件中单次顺序扫描，同时检测多个字节模式。
// 使用 32KB 缓冲区并保留重叠区域，避免跨块边界漏匹配。
// 成功时返回各模式的命中状态，并将文件位置复位到开头。
func multiScan(file *os.File, patterns [][]byte) ([]bool, error) {
	found := make([]bool, len(patterns))

	maxLen := 0
	for _, p := range patterns {
		if len(p) > maxLen {
			maxLen = len(p)
		}
	}
	overlap := 0
	if maxLen > 1 {
		overlap = maxLen - 1
	}

	const chunkSize = 32 * 1024
	buf := make([]byte, chunkSize+overlap)
	carry := 0

	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	for {
		n, readErr := file.Read(buf[carry:])
		if readErr != nil && readErr != io.EOF {
			return nil, readErr
		}
		total := carry + n

		allFound := true
		for i, p := range patterns {
			if !found[i] {
				if bytes.Contains(buf[:total], p) {
					found[i] = true
				} else {
					allFound = false
				}
			}
		}
		if allFound || readErr == io.EOF {
			break
		}

		// 保留末尾 overlap 字节用于下次跨块边界匹配。
		if overlap > 0 && total > overlap {
			copy(buf, buf[total-overlap:total])
			carry = overlap
		} else {
			carry = total
		}
	}

	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	return found, nil
}

// normalizeSchema 只规范化 Schema 节点的字符串 type，递归 properties/items/anyOf。
// 非对象和异常子结构原样保留；不进入 example、default、enum 等业务数据。
// 无变化时返回原 RawMessage；error 仅表示内存重编码失败。
func normalizeSchema(raw json.RawMessage) (json.RawMessage, bool, error) {
	var schema map[string]json.RawMessage
	if json.Unmarshal(raw, &schema) != nil || schema == nil {
		return raw, false, nil
	}

	changed := false
	var kind string
	if json.Unmarshal(schema["type"], &kind) == nil {
		if upper := strings.ToUpper(kind); upper != kind {
			encoded, err := json.Marshal(upper)
			if err != nil {
				return raw, false, err
			}
			schema["type"] = encoded
			changed = true
		}
	}

	if propertiesRaw, ok := schema["properties"]; ok {
		var properties map[string]json.RawMessage
		if json.Unmarshal(propertiesRaw, &properties) == nil {
			propertiesChanged := false
			for name, child := range properties {
				normalized, modified, err := normalizeSchema(child)
				if err != nil {
					return raw, false, err
				}
				if modified {
					properties[name] = normalized
					propertiesChanged = true
				}
			}
			if propertiesChanged {
				encoded, err := json.Marshal(properties)
				if err != nil {
					return raw, false, err
				}
				schema["properties"] = encoded
				changed = true
			}
		}
	}

	if items, ok := schema["items"]; ok {
		normalized, modified, err := normalizeSchema(items)
		if err != nil {
			return raw, false, err
		}
		if modified {
			schema["items"] = normalized
			changed = true
		}
	}

	if anyOfRaw, ok := schema["anyOf"]; ok {
		var branches []json.RawMessage
		if json.Unmarshal(anyOfRaw, &branches) == nil {
			branchesChanged := false
			for i, child := range branches {
				normalized, modified, err := normalizeSchema(child)
				if err != nil {
					return raw, false, err
				}
				if modified {
					branches[i] = normalized
					branchesChanged = true
				}
			}
			if branchesChanged {
				encoded, err := json.Marshal(branches)
				if err != nil {
					return raw, false, err
				}
				schema["anyOf"] = encoded
				changed = true
			}
		}
	}

	if !changed {
		return raw, false, nil
	}
	encoded, err := json.Marshal(schema)
	if err != nil {
		return raw, false, err
	}
	return encoded, true, nil
}

// normalizeSchemaField 仅在目标键缺失时改名，然后规范化目标 Schema。
// 目标键即使为 null 也不覆盖；两键并存时保留来源字段及其原值。
func normalizeSchemaField(fields map[string]json.RawMessage, target, source string) (bool, error) {
	raw, exists := fields[target]
	renamed := false
	if !exists {
		raw, exists = fields[source]
		if !exists {
			return false, nil
		}
		renamed = true
	}

	normalized, modified, err := normalizeSchema(raw)
	if err != nil {
		return false, err
	}
	if renamed || modified {
		fields[target] = normalized
		if renamed {
			delete(fields, source)
		}
		return true, nil
	}
	return false, nil
}

// normalizeRequestBody 在节点选择前统一规范化普通与 PG 请求，不增加路径分支。
// 先单次扫描目标字面量，再按需解析；仅有变换时写回一次完整正文。
// 三处 *JsonSchema 收敛为有类型 Schema，删除 functionCallingConfig，
// 并保留原有 thinkingLevel 处理。非法 JSON、异常子结构和编码失败不本地拒绝。
// error 仅用于文件操作失败；成功返回时文件位置始终在开头。
func normalizeRequestBody(file *os.File) (err error) {
	patterns := [][]byte{
		[]byte(`"thinkingLevel"`),
		[]byte(`"functionDeclarations"`),
		[]byte(`"responseSchema"`),
		[]byte(`"responseJsonSchema"`),
		[]byte(`"functionCallingConfig"`),
	}
	hits, err := multiScan(file, patterns)
	if err != nil {
		return fmt.Errorf("扫描请求体: %w", err)
	}
	needThinking := hits[0]
	needTools := hits[1]
	needSchema := hits[2] || hits[3]
	needToolConfig := hits[4]
	if !needThinking && !needTools && !needSchema && !needToolConfig {
		return nil
	}

	// 包括未修改和内存编码失败的分支，均复位后使用原正文。
	// 若文件已写入失败，则保留错误并停止转发，不能当作原正文回退。
	defer func() {
		if _, seekErr := file.Seek(0, io.SeekStart); seekErr != nil && err == nil {
			err = fmt.Errorf("复位请求体: %w", seekErr)
		}
	}()

	data, err := io.ReadAll(file)
	if err != nil {
		return fmt.Errorf("读取请求体: %w", err)
	}
	var top map[string]json.RawMessage
	if json.Unmarshal(data, &top) != nil || top == nil {
		return nil
	}
	changed := false

	if needThinking || needSchema {
		var gc map[string]json.RawMessage
		if json.Unmarshal(top["generationConfig"], &gc) == nil && gc != nil {
			gcChanged := false
			if needThinking {
				var tc map[string]json.RawMessage
				if json.Unmarshal(gc["thinkingConfig"], &tc) == nil {
					var level string
					if json.Unmarshal(tc["thinkingLevel"], &level) == nil {
						if newLevel := mapThinkingLevel(level); newLevel != level {
							tc["thinkingLevel"], err = json.Marshal(newLevel)
							if err != nil {
								return nil
							}
							gc["thinkingConfig"], err = json.Marshal(tc)
							if err != nil {
								return nil
							}
							gcChanged = true
						}
					}
				}
			}

			if needSchema {
				modified, schemaErr := normalizeSchemaField(gc, "responseSchema", "responseJsonSchema")
				if schemaErr != nil {
					return nil
				}
				gcChanged = gcChanged || modified
			}
			if gcChanged {
				top["generationConfig"], err = json.Marshal(gc)
				if err != nil {
					return nil
				}
				changed = true
			}
		}
	}

	if needTools {
		var tools []json.RawMessage
		if json.Unmarshal(top["tools"], &tools) == nil {
			toolsChanged := false
			for ti, toolRaw := range tools {
				var tool map[string]json.RawMessage
				if json.Unmarshal(toolRaw, &tool) != nil {
					continue
				}
				var decls []json.RawMessage
				if json.Unmarshal(tool["functionDeclarations"], &decls) != nil {
					continue
				}
				declsChanged := false
				for di, declRaw := range decls {
					var decl map[string]json.RawMessage
					if json.Unmarshal(declRaw, &decl) != nil || decl == nil {
						continue
					}
					declChanged := false
					for _, names := range [][2]string{
						{"parameters", "parametersJsonSchema"},
						{"response", "responseJsonSchema"},
					} {
						modified, schemaErr := normalizeSchemaField(decl, names[0], names[1])
						if schemaErr != nil {
							return nil
						}
						declChanged = declChanged || modified
					}
					if declChanged {
						decls[di], err = json.Marshal(decl)
						if err != nil {
							return nil
						}
						declsChanged = true
					}
				}
				if declsChanged {
					tool["functionDeclarations"], err = json.Marshal(decls)
					if err != nil {
						return nil
					}
					tools[ti], err = json.Marshal(tool)
					if err != nil {
						return nil
					}
					toolsChanged = true
				}
			}
			if toolsChanged {
				top["tools"], err = json.Marshal(tools)
				if err != nil {
					return nil
				}
				changed = true
			}
		}
	}

	if needToolConfig {
		var tc map[string]json.RawMessage
		if json.Unmarshal(top["toolConfig"], &tc) == nil {
			if _, exists := tc["functionCallingConfig"]; exists {
				delete(tc, "functionCallingConfig")
				top["toolConfig"], err = json.Marshal(tc)
				if err != nil {
					return nil
				}
				changed = true
			}
		}
	}

	if !changed {
		return nil
	}
	newData, err := json.Marshal(top)
	if err != nil {
		return nil
	}

	// 全部内存变换完成后再写文件；任一步失败都不再转发。
	if _, err = file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("定位请求体写入位置: %w", err)
	}
	n, err := file.Write(newData)
	if err != nil {
		return fmt.Errorf("写回请求体: %w", err)
	}
	if n != len(newData) {
		return fmt.Errorf("写回请求体: %w", io.ErrShortWrite)
	}
	if err = file.Truncate(int64(len(newData))); err != nil {
		return fmt.Errorf("截断请求体: %w", err)
	}
	return nil
}

// mapThinkingLevel 将 thinkingLevel 值映射为 Gemini API 要求的大写枚举。
func mapThinkingLevel(level string) string {
	switch strings.ToLower(level) {
	case "minimal":
		return "MINIMAL"
	case "low":
		return "LOW"
	case "medium":
		return "MEDIUM"
	case "high":
		return "HIGH"
	case "xhigh", "max":
		// Gemini API 无 XHIGH/MAX 枚举，回退到 HIGH
		return "HIGH"
	default:
		// 未知值统一转大写兜底
		return strings.ToUpper(strings.TrimSpace(level))
	}
}

// cleanHeaders 清理 HTTP Header，移除 hop-by-hop 头部和敏感字段。
// request=true 时额外移除 Authorization、API Key 和 Cookie。
// request=false（响应）时移除 Content-Encoding（浏览器 fetch 已解压）。
func cleanHeaders(original http.Header, request bool) http.Header {
	h := original.Clone()
	// 处理 Connection 头部指定的 hop-by-hop 头
	for _, value := range h.Values("Connection") {
		for _, token := range strings.Split(value, ",") {
			h.Del(strings.TrimSpace(token))
		}
	}
	// 移除标准 hop-by-hop 头
	for _, name := range []string{
		"Connection", "Proxy-Connection", "Keep-Alive",
		"Proxy-Authenticate", "Proxy-Authorization",
		"TE", "Trailer", "Transfer-Encoding", "Upgrade",
		"Content-Length", "Host",
	} {
		h.Del(name)
	}
	if request {
		h.Del("Authorization")
		h.Del("X-Goog-Api-Key")
		h.Del("Cookie") // 防止 Gateway/浏览器 Cookie 泄露到上游
	} else {
		h.Del("Content-Encoding") // 浏览器 fetch 已解压
	}
	return h
}

// upstreamURL 构造上游 Gemini API 的完整 URL。
// 将配置的 base_url 与请求路径拼接，并剥离 key/auth_token 查询参数。
func upstreamURL(base string, r *http.Request) (string, error) {
	u, err := url.Parse(base)
	if err != nil {
		return "", err
	}
	path := strings.TrimRight(u.EscapedPath(), "/") + r.URL.EscapedPath()
	decoded, err := url.PathUnescape(path)
	if err != nil {
		return "", err
	}
	u.Path, u.RawPath = decoded, path
	// 剥离鉴权相关的查询参数
	query := r.URL.Query()
	for name := range query {
		switch strings.ToLower(name) {
		case "key", "auth_token":
			query.Del(name)
		}
	}
	u.RawQuery = query.Encode()
	return u.String(), nil
}

// sendRequest 向节点发送 WS http_request 消息。
// 使用流式 JSON 编码将请求体嵌入 WS 消息，避免完整 body 副本。
// writeGate 确保同一时刻只有一个写操作。
func sendRequest(ctx context.Context, n *Node, id string, r *http.Request, target string, body io.Reader) error {
	select {
	case n.writeGate <- struct{}{}:
		defer func() { <-n.writeGate }()
	case <-ctx.Done():
		return ctx.Err()
	case <-n.done:
		return errors.New("节点已断开")
	}

	// 构造请求头（清理后扁平化为 map[string]string）
	headers := cleanHeaders(r.Header, true)
	headers.Set("X-Request-ID", id)
	flat := make(map[string]string, len(headers))
	for name, values := range headers {
		flat[name] = strings.Join(values, ", ")
	}

	// 构造 JSON 信封（不含 body）
	envelope := struct {
		ID      string `json:"id"`
		Type    string `json:"type"`
		Payload struct {
			Method  string            `json:"method"`
			URL     string            `json:"url"`
			Headers map[string]string `json:"headers"`
		} `json:"payload"`
	}{ID: id, Type: "http_request"}
	envelope.Payload.Method = r.Method
	envelope.Payload.URL = target
	envelope.Payload.Headers = flat
	prefix, err := json.Marshal(envelope)
	if err != nil {
		return err
	}

	// 流式写入 WS 消息：JSON 前缀 + body（流式转义）+ JSON 后缀
	writer, err := n.conn.Writer(ctx, websocket.MessageText)
	if err != nil {
		return err
	}
	buffer := bufio.NewWriterSize(writer, 32*1024)
	writeErr := func() error {
		// 写入 JSON 前缀（去掉末尾的 "}}"）
		if _, err := buffer.Write(prefix[:len(prefix)-2]); err != nil {
			return err
		}
		if _, err := buffer.WriteString(`,"body":"`); err != nil {
			return err
		}
		// 流式写入 body（每 32KB 一次 JSON 字符串转义）
		if err := utf8Chunks(ctx, body, func(chunk []byte) error {
			encoded, err := json.Marshal(string(chunk))
			if err != nil {
				return err
			}
			// 去掉 json.Marshal 添加的外层引号
			_, err = buffer.Write(encoded[1 : len(encoded)-1])
			return err
		}); err != nil {
			return err
		}
		if _, err := buffer.WriteString(`"}}`); err != nil {
			return err
		}
		return buffer.Flush()
	}()
	if writeErr != nil {
		// 部分 JSON 消息无法恢复，必须关闭连接
		_ = n.conn.CloseNow()
		return writeErr
	}
	return writer.Close()
}

// validStatus 检查 HTTP 状态码是否在有效范围内。
func validStatus(status int) bool {
	return status >= 200 && status <= 599
}

// responseHeaders 将上游响应头写入客户端响应。
func responseHeaders(w http.ResponseWriter, headers map[string]string, id string) {
	raw := make(http.Header)
	for name, value := range headers {
		if strings.ContainsAny(name+value, "\r\n\x00") {
			continue // 跳过包含非法字符的头部
		}
		raw.Set(name, value)
	}
	for name, values := range cleanHeaders(raw, false) {
		w.Header()[name] = values
	}
	setCORS(w.Header())
	w.Header().Set("X-Request-ID", id)
}

// attemptResult 是单次代理尝试的结果。
type attemptResult struct {
	response  UpstreamResponse // 上游响应（状态码、头部、体）
	committed bool             // 是否已向客户端提交 HTTP 响应头
	complete  bool             // 上游响应是否完整接收
	err       error            // 错误信息
}

// receive 接收节点的响应消息，处理流式/非流式输出。
//
// 消息流程：
//   - stream_start → stream_chunk(s) → stream_end：流式响应（Node 始终使用此路径）
//   - http_response：完整响应（回退路径）
//   - error：错误响应
//
// 使用 complete 标志区分"收到了响应头"和"收到了完整响应"。
// 非流式响应缓冲设有上限（= MaxBodySize）。
func (a *App) receive(ctx context.Context, w http.ResponseWriter, lease *Lease, stream bool, c *Config) attemptResult {
	var result attemptResult
	var body bytes.Buffer
	started := false
	n := lease.Node
	// 非流式响应缓冲上限
	bufferLimit := size(c.Request.MaxBodySize)

	// 超时管理
	var timeout <-chan time.Time
	var timer *time.Timer
	if stream {
		timer = time.NewTimer(dur(c.Timeout.StreamFirstByte))
		timeout = timer.C
		defer timer.Stop()
	}
	resetIdle := func() {
		if timer != nil {
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(dur(c.Timeout.StreamIdle))
		}
	}
	writeChunk := func(data string) error {
		_ = http.NewResponseController(w).SetWriteDeadline(time.Now().Add(dur(c.Timeout.StreamIdle)))
		if _, err := io.WriteString(w, data); err != nil {
			return err
		}
		return http.NewResponseController(w).Flush()
	}

	for {
		select {
		case <-ctx.Done():
			result.err = ctx.Err()
			return result
		case <-n.done:
			result.err = errors.New("节点已断开")
			return result
		case <-timeout:
			result.err = context.DeadlineExceeded
			return result
		case msg := <-lease.Pending.ch:
			a.hub.Consumed(lease, msg)
			switch msg.Type {
			case "stream_start":
				if started || !validStatus(msg.Payload.Status) {
					result.err = errors.New("无效的 stream_start")
					return result
				}
				started = true
				result.response.Status = msg.Payload.Status
				result.response.Headers = msg.Payload.Headers
				resetIdle()

				// 流式成功响应：立即提交 HTTP 头部
				// 错误响应延迟提交，以便重试
				if stream && msg.Payload.Status < 400 {
					responseHeaders(w, msg.Payload.Headers, lease.Pending.id)
					_ = http.NewResponseController(w).SetWriteDeadline(time.Now().Add(dur(c.Timeout.StreamIdle)))
					w.WriteHeader(msg.Payload.Status)
					result.committed = true
					if err := http.NewResponseController(w).Flush(); err != nil {
						result.err = err
						return result
					}
				}

			case "stream_chunk":
				if !started {
					result.err = errors.New("stream_chunk 在 stream_start 之前")
					return result
				}
				resetIdle()
				if result.committed {
					// 流式：逐 chunk 写入并 flush
					if err := writeChunk(msg.Payload.Data); err != nil {
						result.err = err
						return result
					}
				} else {
					// 非流式：缓冲并检查上限
					if int64(body.Len())+int64(len(msg.Payload.Data)) > bufferLimit {
						result.err = errors.New("上游响应超出缓冲限制")
						return result
					}
					_, _ = body.WriteString(msg.Payload.Data)
				}

			case "stream_end":
				if !started {
					result.err = errors.New("stream_end 在 stream_start 之前")
					return result
				}
				// 标记响应已完整接收
				result.response.Body = body.String()
				result.complete = true
				return result

			case "http_response":
				// 完整响应回退路径
				if started || !validStatus(msg.Payload.Status) {
					result.err = errors.New("无效的 http_response")
					return result
				}
				result.response = UpstreamResponse{
					Status: msg.Payload.Status, Headers: msg.Payload.Headers, Body: msg.Payload.Body,
				}
				// 单消息完整响应
				result.complete = true
				return result

			case "error":
				// 节点报告的错误
				if msg.Payload.HTTPResponse != nil && validStatus(msg.Payload.HTTPResponse.Status) {
					result.response = *msg.Payload.HTTPResponse
					result.complete = true // 附带的 http_response 是完整错误体
				}
				message := msg.Payload.Message
				if message == "" {
					message = "上游节点错误"
				}
				result.err = errors.New(message)
				return result
			}
		}
	}
}

// Proxy 是通配代理的入口处理器。
// 处理所有 /v1/* 和 /v1beta/* 的非本地端点请求。
//
// 重试策略：
//   - 最大尝试次数 = 可用节点数
//   - tried 集合防止重复尝试同一节点
//   - 已提交流式输出后不可重试
//   - 非冷却 4xx（400/404）不可重试
//
// 请求级错误不断开整个节点（仅 WS 写入失败断开）。
// 502 响应附带最后错误的诊断信息。
func (a *App) Proxy(w http.ResponseWriter, r *http.Request) {
	c := a.cfg.Load()
	rec := requestRecord(r)
	ctx, cancel := context.WithTimeout(r.Context(), dur(c.Timeout.Total))
	defer cancel()
	defer http.NewResponseController(w).SetWriteDeadline(time.Time{})

	// 1. 缓存请求体到临时文件
	_ = http.NewResponseController(w).SetReadDeadline(time.Now().Add(dur(c.Timeout.Total)))
	body, err := spoolBody(ctx, r.Body)
	_ = http.NewResponseController(w).SetReadDeadline(time.Time{})
	if err != nil {
		rec.Error = err.Error()
		var tooLarge *http.MaxBytesError
		switch {
		case errors.Is(err, context.DeadlineExceeded):
			geminiError(w, 504, "请求体读取超时")
		case errors.As(err, &tooLarge), errors.Is(err, ErrInvalidUTF8):
			geminiError(w, 400, err.Error())
		default:
			geminiError(w, 400, "无法读取请求体")
		}
		return
	}
	defer func() {
		name := body.Name()
		body.Close()
		os.Remove(name)
	}()

	// 统一规范化一次，节点重试复用结果；文件错误不进入节点调度。
	if r.Method != http.MethodGet {
		if err := normalizeRequestBody(body); err != nil {
			rec.Error = err.Error()
			geminiError(w, http.StatusInternalServerError, "请求体规范化文件操作失败")
			return
		}
	}

	// 2. 构造上游 URL
	target, err := upstreamURL(c.Upstream.BaseURL, r)
	if err != nil {
		rec.Error = err.Error()
		geminiError(w, 400, "无效的请求 URL")
		return
	}

	// 3. 重试循环
	tried := map[string]bool{}
	var last attemptResult
	attempts := 0

	// 快照重试预算，防止连接动态变化导致无界重试
	a.hub.mu.Lock()
	budget := max(1, len(a.hub.nodes))
	a.hub.mu.Unlock()

	for attempts < budget {
		// 3a. 获取节点执行租约（首次允许排队等待，重试时不排队）
		lease, acquireErr := a.hub.Acquire(ctx, rec.RequestID, tried, attempts == 0, rec.Model)
		if acquireErr != nil {
			if attempts == 0 {
				rec.Error = acquireErr.Error()
				if errors.Is(acquireErr, context.DeadlineExceeded) {
					geminiError(w, 504, acquireErr.Error())
				} else if errors.Is(acquireErr, context.Canceled) {
					rec.Status = 499 // 客户端断连
				} else {
					geminiError(w, 503, acquireErr.Error())
				}
				return
			}
			break // 重试时获取失败，使用最后结果
		}

		n := lease.Node
		tried[n.id] = true
		rec.NodeID, rec.ChannelID, rec.TokenID = n.id, n.channelID, n.tokenID
		rec.Retries = attempts
		attempts++
		attemptStart := time.Now()

		// 记录请求分配信息
		if attempts == 1 {
			slog.Debug("请求分配到节点",
				"request_id", rec.RequestID, "node_id", n.id,
				"model", rec.Model, "stream", rec.Stream)
		} else {
			// 记录重试信息（仅在有实际状态码时输出 prev_status）
			retryArgs := []any{"request_id", rec.RequestID, "node_id", n.id,
				"attempt", attempts}
			if last.response.Status > 0 {
				retryArgs = append(retryArgs, "prev_status", last.response.Status)
			}
			if last.err != nil {
				retryArgs = append(retryArgs, "prev_error", last.err)
			}
			slog.Warn("请求重试", retryArgs...)
		}

		// 3b. 发送请求并接收响应
		// 区分 WS 写入失败（连接级）和请求级错误
		sendFailed := false
		if _, err := body.Seek(0, io.SeekStart); err != nil {
			last = attemptResult{err: err}
		} else if err := sendRequest(ctx, n, rec.RequestID, r, target, body); err != nil {
			last = attemptResult{err: err}
			sendFailed = true
		} else {
			last = a.receive(ctx, w, lease, rec.Stream, c)
		}

		// 仅 WS 写入失败断开节点；请求级错误保持连接
		if sendFailed {
			a.hub.Disconnect(n, websocket.StatusGoingAway, "send_error")
		}

		// 3c. 完成本次尝试
		a.hub.Finish(lease, last.response.Status, last.err, time.Since(attemptStart), rec.Model)

		// 3d. 已提交流式输出：不可重试
		if last.committed {
			if last.err != nil {
				rec.Error = last.err.Error()
				panic(http.ErrAbortHandler) // 静默关闭连接
			}
			return
		}

		// 3e. 检查是否应该重试
		if ctx.Err() != nil {
			last.err = ctx.Err()
			break
		}
		if last.response.Status >= 400 && !retryStatus(last.response.Status) {
			break // 非冷却 4xx 不可重试
		}
		if last.err == nil && !retryStatus(last.response.Status) {
			break // 成功或不可重试状态
		}
	}

	// 4. 返回最终结果
	if last.err != nil {
		rec.Error = last.err.Error()
	}
	if last.response.Status >= 400 {
		if rec.Error == "" {
			rec.Error = fmt.Sprintf("上游 HTTP %d", last.response.Status)
		}
	}

	// 客户端已断连
	if r.Context().Err() != nil {
		rec.Status = 499
		return
	}

	// 上游超时
	if errors.Is(last.err, context.DeadlineExceeded) {
		geminiError(w, 504, "上游请求超时")
		return
	}

	// 仅在上游响应完整接收时才转发给客户端
	// 不完整的响应（如 stream_start(200) 后断连）不能作为成功返回
	if last.complete && validStatus(last.response.Status) {
		responseHeaders(w, last.response.Headers, rec.RequestID)
		_ = http.NewResponseController(w).SetWriteDeadline(time.Now().Add(dur(c.Timeout.StreamIdle)))
		w.WriteHeader(last.response.Status)
		if _, err := io.WriteString(w, last.response.Body); err != nil {
			rec.Error = err.Error()
		}
		return
	}

	// 502 响应附带最后错误的诊断信息
	errMsg := "所有上游尝试均失败"
	if last.err != nil {
		errMsg = fmt.Sprintf("所有上游尝试均失败: %s", last.err.Error())
	}
	geminiError(w, 502, errMsg)
}
