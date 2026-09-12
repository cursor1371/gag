// 时间窗口统计引擎：四窗口多维度聚合统计和请求历史缓冲区。
//
// 时间窗口：最近 60 分钟（1m 粒度）、24 小时（1h）、7 天（1h）、30 天（24h）
// 统计维度：全局、按模型、按 Key、按 Token、按渠道、按节点
//
// 所有统计数据纯内存聚合，进程重启后重置。
// 延迟分布使用直方图桶近似计算分位数（P50/P95/P99）。

package main

import (
	"errors"
	"fmt"
	"math"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Ring 是泛型环形缓冲区，支持固定容量的 FIFO 推入和值读取。
type Ring[T any] struct {
	data []T
	next int
	full bool
}

// Resize 调整缓冲区容量，保留尾部数据。
func (r *Ring[T]) Resize(capacity int) {
	values := r.Values()
	if len(values) > capacity {
		values = values[len(values)-capacity:]
	}
	r.data = make([]T, capacity)
	r.next, r.full = 0, false
	for _, value := range values {
		r.Push(value)
	}
}

// Push 向缓冲区推入一个值，满时覆盖最旧的值。
func (r *Ring[T]) Push(v T) {
	if len(r.data) == 0 {
		return
	}
	r.data[r.next] = v
	r.next++
	if r.next == len(r.data) {
		r.next = 0
		r.full = true
	}
}

// Values 返回缓冲区中所有值的副本，按时间顺序（最旧到最新）。
func (r *Ring[T]) Values() []T {
	if !r.full {
		return append([]T{}, r.data[:r.next]...)
	}
	result := make([]T, 0, len(r.data))
	result = append(result, r.data[r.next:]...)
	result = append(result, r.data[:r.next]...)
	return result
}

// Metric 是单个统计桶的聚合指标。
type Metric struct {
	Total       int64    `json:"total"`
	Success     int64    `json:"success"`
	Error       int64    `json:"error"`
	LatencySum  int64    `json:"latency_sum"`
	LatencyMin  int64    `json:"latency_min"`
	LatencyMax  int64    `json:"latency_max"`
	TTFBSum     int64    `json:"ttfb_sum"`
	BytesIn     int64    `json:"bytes_in"`
	BytesOut    int64    `json:"bytes_out"`
	StreamCount int64    `json:"stream_count"`
	RetryCount  int64    `json:"retry_count"`
	Hist        [9]int64 `json:"latency_hist"` // 延迟分布直方图
}

// histogramBounds 定义延迟直方图的桶上界（毫秒）。
// 最后一个 0 表示 ≥300s 的桶。
var histogramBounds = [9]int64{100, 500, 1000, 5000, 10000, 30000, 60000, 300000, 0}

// Add 将一条请求记录累加到指标中。
func (m *Metric) Add(r RequestRecord) {
	m.Total++
	if r.Status >= 200 && r.Status < 400 && r.Error == "" {
		m.Success++
	} else {
		m.Error++
	}
	m.LatencySum += r.LatencyMS
	if m.Total == 1 || r.LatencyMS < m.LatencyMin {
		m.LatencyMin = r.LatencyMS
	}
	m.LatencyMax = max(m.LatencyMax, r.LatencyMS)
	m.TTFBSum += r.TTFBMS
	m.BytesIn += r.BytesIn
	m.BytesOut += r.BytesOut
	if r.Stream {
		m.StreamCount++
	}
	m.RetryCount += int64(r.Retries)
	// 分配到直方图桶
	for i, upper := range histogramBounds {
		if upper == 0 || r.LatencyMS < upper {
			m.Hist[i]++
			break
		}
	}
}

// Merge 将另一个 Metric 合并到当前 Metric。
func (m *Metric) Merge(other Metric) {
	if other.Total == 0 {
		return
	}
	if m.Total == 0 || other.LatencyMin < m.LatencyMin {
		m.LatencyMin = other.LatencyMin
	}
	m.LatencyMax = max(m.LatencyMax, other.LatencyMax)
	m.Total += other.Total
	m.Success += other.Success
	m.Error += other.Error
	m.LatencySum += other.LatencySum
	m.TTFBSum += other.TTFBSum
	m.BytesIn += other.BytesIn
	m.BytesOut += other.BytesOut
	m.StreamCount += other.StreamCount
	m.RetryCount += other.RetryCount
	for i := range m.Hist {
		m.Hist[i] += other.Hist[i]
	}
}

// percentile 从直方图近似计算给定分位数的延迟值。
func (m Metric) percentile(q float64) int64 {
	if m.Total == 0 {
		return 0
	}
	target := int64(math.Ceil(float64(m.Total) * q))
	var sum int64
	for i, count := range m.Hist {
		sum += count
		if sum >= target {
			if histogramBounds[i] == 0 {
				return m.LatencyMax
			}
			return histogramBounds[i]
		}
	}
	return m.LatencyMax
}

// Summary 将 Metric 转换为可序列化的摘要 map。
func (m Metric) Summary() map[string]any {
	latency, ttfb := int64(0), int64(0)
	if m.Total > 0 {
		latency = m.LatencySum / m.Total
		ttfb = m.TTFBSum / m.Total
	}
	return map[string]any{
		"total": m.Total, "success": m.Success, "error": m.Error,
		"avg_latency_ms": latency, "min_latency_ms": m.LatencyMin,
		"max_latency_ms": m.LatencyMax,
		"p50_ms": m.percentile(.50), "p95_ms": m.percentile(.95), "p99_ms": m.percentile(.99),
		"avg_ttfb_ms": ttfb, "bytes_in": m.BytesIn, "bytes_out": m.BytesOut,
		"stream_count": m.StreamCount, "retry_count": m.RetryCount,
	}
}

// Bucket 是带时间戳（epoch）的统计桶。
type Bucket struct {
	Epoch int64 // 桶所属的时间 epoch（= unix_timestamp / granularity_seconds）
	Metric
}

// windowSpec 定义一个时间窗口的参数。
type windowSpec struct {
	Name        string // 窗口名称（如 "1h"）
	Granularity string // 粒度描述（如 "1m"）
	Seconds     int64  // 粒度秒数
	Count       int    // 桶数量
}

// statWindows 定义四个时间窗口。
var statWindows = []windowSpec{
	{"1h", "1m", 60, 60},
	{"24h", "1h", 3600, 24},
	{"7d", "1h", 3600, 168},
	{"30d", "24h", 86400, 30},
}

// Series 是单个统计实体的完整时间序列数据。
type Series struct {
	Windows [4][]Bucket // 四个时间窗口的桶数组
	Total   Metric      // 累计总量
	Last    time.Time   // 最后更新时间
}

func newSeries() *Series {
	s := &Series{}
	for i, spec := range statWindows {
		s.Windows[i] = make([]Bucket, spec.Count)
	}
	return s
}

// Stats 是统计引擎的核心结构。
type Stats struct {
	mu      sync.Mutex
	dims    map[string]map[string]*Series // 维度名 → (实体 ID → Series)
	history Ring[RequestRecord]           // 已完成请求历史环形缓冲区
}

// NewStats 创建统计引擎实例。
func NewStats(historySize int) *Stats {
	s := &Stats{}
	s.resetLocked()
	s.history.Resize(historySize)
	return s
}

func (s *Stats) resetLocked() {
	s.dims = make(map[string]map[string]*Series)
	for _, dim := range []string{"global", "model", "key", "token", "channel", "node"} {
		s.dims[dim] = make(map[string]*Series)
	}
}

// Resize 调整请求历史缓冲区容量。
func (s *Stats) Resize(n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.history.Resize(n)
}

// Reset 重置所有统计数据和请求历史。
func (s *Stats) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	capacity := len(s.history.data)
	s.resetLocked()
	s.history = Ring[RequestRecord]{}
	s.history.Resize(capacity)
}

// Add 记录一条已完成的请求，更新所有相关维度的时间窗口统计。
func (s *Stats) Add(r RequestRecord) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.history.Push(r)

	// 定义各维度的 ID 映射
	dimensions := map[string]string{
		"global":  "",
		"model":   r.Model,
		"key":     strconv.Itoa(r.KeyID),
		"channel": r.ChannelID,
		"node":    r.NodeID,
	}
	// 仅在请求经过代理时记录 Token 维度（非代理请求的 TokenID=0 不应创建统计条目）
	if r.NodeID != "" {
		dimensions["token"] = strconv.Itoa(r.TokenID)
	}
	for dim, id := range dimensions {
		// 空标识跳过（NodeID、ChannelID、Model 为空时）
		if dim != "global" && id == "" {
			continue
		}
		// 未认证请求仅记入全局维度，防止未鉴权的随机模型路径请求创建高基数时间序列
		if !r.Authenticated && dim != "global" {
			continue
		}

		items := s.dims[dim]
		series := items[id]
		if series == nil {
			// 基数限制：每个维度最多 2048 个独立实体
			if len(items) >= 2048 {
				// 先尝试清理 30 天未活跃的实体
				for key, candidate := range items {
					if r.Completed.Sub(candidate.Last) > 30*24*time.Hour {
						delete(items, key)
					}
				}
			}
			if len(items) >= 2048 {
				id = "__other__" // 超出基数限制归入 __other__
			}
			series = items[id]
			if series == nil {
				series = newSeries()
				items[id] = series
			}
		}
		series.Last = r.Completed
		series.Total.Add(r)

		// 更新四个时间窗口的桶
		for i, spec := range statWindows {
			epoch := r.Completed.Unix() / spec.Seconds
			index := int(epoch % int64(spec.Count))
			bucket := &series.Windows[i][index]
			if bucket.Epoch != epoch {
				*bucket = Bucket{Epoch: epoch} // 新 epoch，重置桶
			}
			bucket.Add(r)
		}
	}
}

// Counters 返回指定维度和 ID 的累计统计计数。
func (s *Stats) Counters(dim, id string) map[string]int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := Metric{}
	if item := s.dims[dim][id]; item != nil {
		m = item.Total
	}
	return map[string]int64{
		"total_requests": m.Total, "success_count": m.Success, "error_count": m.Error,
	}
}

// csvDefault 将逗号分隔的字符串拆分为切片，空值使用 fallback。
func csvDefault(value, fallback string) []string {
	if value == "" {
		value = fallback
	}
	return strings.Split(value, ",")
}

// queryLimit 解析查询参数中的 limit 值。
func queryLimit(q url.Values, fallback, maximum int) (int, error) {
	if q.Get("limit") == "" {
		return fallback, nil
	}
	n, err := strconv.Atoi(q.Get("limit"))
	if err != nil || n < 1 || n > maximum {
		return 0, fmt.Errorf("limit 必须在 1 到 %d 之间", maximum)
	}
	return n, nil
}

// validateBool 验证查询参数是否为有效的布尔值。
func validateBool(q url.Values, name string) error {
	v := q.Get(name)
	if v != "" && v != "true" && v != "false" {
		return fmt.Errorf("%s 必须是 true 或 false", name)
	}
	return nil
}

// Query 处理 GET /admin/stats 的统计查询。
// 支持按窗口、维度、实体 ID 过滤，可选是否包含逐桶时间序列。
func (s *Stats) Query(q url.Values) (map[string]any, error) {
	// 验证并解析窗口参数
	wantedWindows := map[string]bool{}
	for _, name := range csvDefault(q.Get("window"), "1h,24h,7d,30d") {
		found := false
		for _, spec := range statWindows {
			if spec.Name == name {
				found = true
			}
		}
		if !found {
			return nil, errors.New("无效的统计窗口")
		}
		wantedWindows[name] = true
	}

	// 验证维度参数
	dimensions := csvDefault(q.Get("dimension"), "global")
	for _, dim := range dimensions {
		switch dim {
		case "global", "model", "key", "channel", "node":
		default:
			return nil, errors.New("无效的统计维度")
		}
	}
	if err := validateBool(q, "include_buckets"); err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	result := map[string]any{}
	now := time.Now().Unix()

	for _, dim := range dimensions {
		group := map[string]any{}
		// 确定过滤参数名
		filterName := dim + "_id"
		if dim == "model" {
			filterName = "model"
		}
		for id, series := range s.dims[dim] {
			if q.Get(filterName) != "" && q.Get(filterName) != id {
				continue
			}
			windows := map[string]any{}
			for i, spec := range statWindows {
				if !wantedWindows[spec.Name] {
					continue
				}
				epoch := now / spec.Seconds
				var summary Metric
				buckets := make([]map[string]any, 0, spec.Count)
				for step := spec.Count - 1; step >= 0; step-- {
					target := epoch - int64(step)
					m := Metric{}
					bucket := series.Windows[i][int(target%int64(spec.Count))]
					if bucket.Epoch == target {
						m = bucket.Metric
					}
					summary.Merge(m)
					if q.Get("include_buckets") != "false" {
						view := m.Summary()
						view["time"] = time.Unix(target*spec.Seconds, 0).UTC()
						buckets = append(buckets, view)
					}
				}
				view := map[string]any{
					"granularity": spec.Granularity, "summary": summary.Summary(),
				}
				if q.Get("include_buckets") != "false" {
					view["buckets"] = buckets
				}
				windows[spec.Name] = view
			}
			group[id] = windows
		}
		if dim == "global" {
			if v, exists := group[""]; exists {
				result["global"] = v
			} else {
				result["global"] = map[string]any{}
			}
		} else {
			result["by_"+dim] = group
		}
	}
	return result, nil
}

// Requests 处理 GET /admin/requests 的已完成请求历史查询。
func (s *Stats) Requests(q url.Values) (map[string]any, error) {
	limit, err := queryLimit(q, 50, 200)
	if err != nil {
		return nil, err
	}
	for _, name := range []string{"stream", "error"} {
		if err := validateBool(q, name); err != nil {
			return nil, err
		}
	}
	minLatency := int64(0)
	if value := q.Get("min_latency"); value != "" {
		minLatency, err = strconv.ParseInt(value, 10, 64)
		if err != nil || minLatency < 0 {
			return nil, errors.New("min_latency 必须是非负整数")
		}
	}
	if q.Get("order") != "" && q.Get("order") != "asc" && q.Get("order") != "desc" {
		return nil, errors.New("无效的排序方式")
	}

	s.mu.Lock()
	history := s.history.Values()
	s.mu.Unlock()

	// 按时间排序
	sort.SliceStable(history, func(i, j int) bool {
		if q.Get("order") == "asc" {
			return history[i].Completed.Before(history[j].Completed)
		}
		return history[i].Completed.After(history[j].Completed)
	})

	// 过滤
	result := []RequestRecord{}
	for _, r := range history {
		if !statusMatches(r.Status, q.Get("status")) ||
			(q.Get("model") != "" && q.Get("model") != r.Model) ||
			(q.Get("key_id") != "" && q.Get("key_id") != strconv.Itoa(r.KeyID)) ||
			(q.Get("node_id") != "" && q.Get("node_id") != r.NodeID) ||
			(q.Get("stream") != "" && q.Get("stream") != strconv.FormatBool(r.Stream)) ||
			(q.Get("error") != "" && q.Get("error") != strconv.FormatBool(r.Error != "" || r.Status >= 400)) ||
			r.LatencyMS < minLatency {
			continue
		}
		result = append(result, r)
		if len(result) == limit {
			break
		}
	}
	return map[string]any{"requests": result, "returned": len(result)}, nil
}
