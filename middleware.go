// HTTP 中间件：CORS、请求 ID 生成、Body 大小限制、响应包装与访问日志记录。
// 还包含通用的 JSON 响应和错误格式化工具函数。

package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"
)

// recordContextKey 是将 RequestRecord 存入 context 的键类型。
type recordContextKey struct{}

// RequestRecord 记录单个 API 请求的完整信息，供访问日志和统计使用。
type RequestRecord struct {
	RequestID string    `json:"request_id"`
	Method    string    `json:"method"`
	Path      string    `json:"path"`
	Model     string    `json:"model"`
	Stream    bool      `json:"stream"`
	Status    int       `json:"status"`
	LatencyMS int64     `json:"latency_ms"`
	TTFBMS    int64     `json:"ttfb_ms"`       // 首字节时间（Time To First Byte）
	KeyID     int       `json:"key_id"`
	TokenID   int       `json:"token_id,omitempty"`
	NodeID    string    `json:"node_id"`
	ChannelID string    `json:"channel_id"`
	BytesIn   int64     `json:"bytes_in"`
	BytesOut  int64     `json:"bytes_out"`
	Retries   int       `json:"retries"`
	ClientIP  string    `json:"client_ip"`
	Error     string    `json:"error,omitempty"`
	Completed time.Time `json:"completed_at"`
	Authenticated bool  `json:"-"` // 内部字段，标记请求是否已通过 API Key 鉴权
}

// requestRecord 从请求上下文中提取 RequestRecord。
func requestRecord(r *http.Request) *RequestRecord {
	v, _ := r.Context().Value(recordContextKey{}).(*RequestRecord)
	return v
}

// countBody 包装 ReadCloser 以统计读取的总字节数。
type countBody struct {
	io.ReadCloser
	n int64
}

func (b *countBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	b.n += int64(n)
	return n, err
}

// responseWriter 包装 http.ResponseWriter 以记录响应状态码、字节数和首字节时间。
type responseWriter struct {
	http.ResponseWriter
	status int
	bytes  int64
	start  time.Time
	ttfb   int64
}

func (w *responseWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

func (w *responseWriter) WriteHeader(status int) {
	// 1xx 信息性状态码直接透传，不记录为最终状态
	if status >= 100 && status < 200 {
		w.ResponseWriter.WriteHeader(status)
		return
	}
	if w.status != 0 {
		return // 防止重复写入状态码
	}
	w.status = status
	w.ttfb = time.Since(w.start).Milliseconds()
	w.ResponseWriter.WriteHeader(status)
}

func (w *responseWriter) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	n, err := w.ResponseWriter.Write(p)
	w.bytes += int64(n)
	return n, err
}

func (w *responseWriter) Flush() {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	_ = http.NewResponseController(w.ResponseWriter).Flush()
}

func (w *responseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return http.NewResponseController(w.ResponseWriter).Hijack()
}

// setCORS 设置 CORS 响应头。
// 显式列出 Authorization 等常见请求头，因为通配符 "*" 在部分浏览器中
// 不覆盖 Authorization，导致跨域 Bearer 鉴权预检失败。
func setCORS(h http.Header) {
	h.Set("Access-Control-Allow-Origin", "*")
	h.Set("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS, HEAD")
	h.Set("Access-Control-Allow-Headers", "Authorization, Content-Type, X-Goog-Api-Key, X-Channel-ID, X-Node-ID, X-Node-Label")
	h.Set("Access-Control-Expose-Headers", "X-Request-ID")
	h.Set("Access-Control-Max-Age", "86400")
}

// Middleware 是全局 HTTP 中间件，负责：
//  1. 生成唯一请求 ID（X-Request-ID）
//  2. 设置 CORS 响应头
//  3. 处理 OPTIONS 预检请求
//  4. 优雅关闭期间拒绝新请求
//  5. 限制请求体大小
//  6. 记录访问日志和统计数据
func (a *App) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rid := "req_" + randomHex(8) // 生成 16 字符的请求 ID
		setCORS(w.Header())
		w.Header().Set("X-Request-ID", rid)

		// OPTIONS 预检请求直接返回 204
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		// 优雅关闭期间拒绝新请求（健康检查除外）
		if a.stopping.Load() && r.URL.Path != "/health" {
			geminiError(w, http.StatusServiceUnavailable, "网关正在关闭")
			return
		}

		// 提取客户端 IP
		ip, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil {
			ip = r.RemoteAddr
		}

		// 构建请求记录（存入 context，贯穿整个请求生命周期）
		rec := &RequestRecord{
			RequestID: rid,
			Method:    r.Method,
			Path:      r.URL.Path,
			Model:     modelFromPath(r.URL.Path),
			Stream:    strings.Contains(r.URL.Path, "streamGenerateContent"),
			ClientIP:  ip,
		}
		r = r.WithContext(context.WithValue(r.Context(), recordContextKey{}, rec))

		// 包装 ResponseWriter 以收集响应指标
		wrapped := &responseWriter{ResponseWriter: w, start: start}
		limit := size(a.cfg.Load().Request.MaxBodySize)
		body := &countBody{ReadCloser: http.MaxBytesReader(wrapped, r.Body, limit)}
		r.Body = body

		// 判断是否为 API 请求（用于决定是否记录访问日志）
		apiRequest := (strings.HasPrefix(r.URL.Path, "/v1/") ||
			strings.HasPrefix(r.URL.Path, "/v1beta/")) &&
			r.URL.Path != "/v1/ws"

		// defer 记录访问日志和统计数据
		defer func() {
			if !apiRequest {
				return
			}
			if wrapped.status != 0 {
				rec.Status = wrapped.status
			} else if rec.Status == 0 {
				rec.Status = 200
			}
			rec.Completed = time.Now().UTC()
			rec.LatencyMS = time.Since(start).Milliseconds()
			rec.TTFBMS = wrapped.ttfb
			rec.BytesIn = body.n
			rec.BytesOut = wrapped.bytes

			a.stats.Add(*rec)
			fields := objectMap(rec)
			fields["key_id"] = fmt.Sprint(rec.KeyID)
			a.logs.Emit("access", "INFO",
				fmt.Sprintf("%s %s %d %dms", rec.Method, rec.Path, rec.Status, rec.LatencyMS),
				fields)
		}()

		// 提前检查 Content-Length 以快速拒绝明确超限的请求
		if r.ContentLength > limit {
			geminiError(wrapped, http.StatusBadRequest, "请求体超出大小限制")
			return
		}
		next.ServeHTTP(wrapped, r)
	})
}

// modelFromPath 从请求路径中提取模型名。
// 例如 "/v1beta/models/gemini-2.5-flash:generateContent" → "gemini-2.5-flash"
func modelFromPath(path string) string {
	const marker = "/models/"
	i := strings.Index(path, marker)
	if i < 0 {
		return ""
	}
	s := path[i+len(marker):]
	if j := strings.IndexAny(s, ":/"); j >= 0 {
		s = s[:j]
	}
	return s
}

// writeJSON 将值序列化为 JSON 并写入响应。
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// geminiError 返回 Gemini API 兼容的错误响应格式。
func geminiError(w http.ResponseWriter, status int, message string) {
	names := map[int]string{
		400: "INVALID_ARGUMENT",
		401: "UNAUTHENTICATED",
		403: "PERMISSION_DENIED",
		404: "NOT_FOUND",
		413: "RESOURCE_EXHAUSTED",
		500: "INTERNAL",
		502: "INTERNAL",
		503: "UNAVAILABLE",
		504: "DEADLINE_EXCEEDED",
	}
	name := names[status]
	if name == "" {
		name = "UNKNOWN"
	}
	writeJSON(w, status, map[string]any{
		"error": map[string]any{
			"code": status, "message": message, "status": name,
		},
	})
}

// adminError 返回管理 API 的错误响应格式。
func adminError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, map[string]any{
		"error": map[string]any{"code": code, "message": message},
	})
}

// decodeJSON 从请求体中解码 JSON 值，限制最大 1 MiB，拒绝未知字段和多余内容。
func decodeJSON(w http.ResponseWriter, r *http.Request, value any) error {
	d := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	d.DisallowUnknownFields()
	if err := d.Decode(value); err != nil {
		return err
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return fmt.Errorf("期望恰好一个 JSON 值")
	}
	return nil
}

// objectMap 将结构体转换为 map[string]any（通过 JSON 序列化/反序列化）。
func objectMap(v any) map[string]any {
	data, _ := json.Marshal(v)
	var result map[string]any
	_ = json.Unmarshal(data, &result)
	return result
}

// statusMatches 检查 HTTP 状态码是否匹配过滤表达式。
// 支持精确匹配（"429"）和范围匹配（"5xx"、"4xx"）。
func statusMatches(status int, filter string) bool {
	if filter == "" {
		return true
	}
	if len(filter) == 3 && strings.EqualFold(filter[1:], "xx") {
		return status/100 == int(filter[0]-'0')
	}
	return fmt.Sprint(status) == filter
}
