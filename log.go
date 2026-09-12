// 双流日志系统：应用日志 + 访问日志。
//
// 功能特性：
//   - 双流分离：应用日志（系统事件/错误/管理操作）和访问日志（每个 API 请求一条）
//   - 双输出：stdout（可配置 text/json 格式）+ 文件（固定 JSONL 格式）
//   - 内存环形缓冲区：每个日志源独立缓冲，供管理 API 查询
//   - 文件反向扫描：支持日志文件历史回溯查询
//   - 游标分页：基于时间戳+日志 ID 的分页游标
//   - slog.Handler 实现：桥接标准库 slog 到双流日志系统

package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// LogEntry 是日志系统的基本条目结构。
type LogEntry struct {
	ID        string         `json:"id"`        // 唯一 ID（用于去重和分页游标）
	Timestamp time.Time      `json:"timestamp"` // UTC 时间戳
	Level     string         `json:"level"`     // 日志级别：DEBUG/INFO/WARN/ERROR
	Source    string         `json:"source"`    // 来源：app/access
	Message   string         `json:"message"`   // 日志消息
	Fields    map[string]any `json:"fields"`    // 结构化字段
}

// Logs 管理双流日志系统的所有状态。
type Logs struct {
	mu      sync.Mutex
	config  LogConfig
	buffers map[string]*Ring[LogEntry] // 每个日志源的内存环形缓冲区
	files   map[string]*os.File       // 每个日志源的文件句柄
	closed  bool
}

// NewLogs 创建日志系统实例。
func NewLogs() *Logs {
	return &Logs{
		buffers: map[string]*Ring[LogEntry]{"app": {}, "access": {}},
		files:   make(map[string]*os.File),
	}
}

// Configure 根据配置更新日志系统（打开/关闭文件、调整缓冲区大小）。
func (l *Logs) Configure(config LogConfig) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	// 打开新的日志文件
	next := make(map[string]*os.File)
	for source, fc := range map[string]FileLogConfig{
		"app": config.File.App, "access": config.File.Access,
	} {
		if !fc.Enabled {
			continue
		}
		// 自动创建目录
		if err := os.MkdirAll(filepath.Dir(fc.Path), 0700); err != nil {
			for _, f := range next {
				f.Close()
			}
			return err
		}
		f, err := os.OpenFile(fc.Path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
		if err != nil {
			for _, opened := range next {
				opened.Close()
			}
			return err
		}
		next[source] = f
	}

	// 关闭旧文件
	for _, f := range l.files {
		_ = f.Close()
	}
	l.files = next
	l.config = config
	// 调整缓冲区大小
	for _, ring := range l.buffers {
		ring.Resize(config.BufferSize)
	}
	return nil
}

// levelNumber 将日志级别字符串转换为数值（与 slog.Level 对齐）。
func levelNumber(level string) int {
	switch strings.ToUpper(level) {
	case "DEBUG":
		return -4
	case "WARN":
		return 4
	case "ERROR":
		return 8
	default:
		return 0 // INFO
	}
}

// Enabled 检查给定级别是否应该被记录。
func (l *Logs) Enabled(level slog.Level) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return int(level) >= levelNumber(l.config.Level)
}

// Emit 向指定日志源写入一条日志。
// 同时写入内存缓冲区、stdout 和文件（如已启用）。
func (l *Logs) Emit(source, level, message string, fields map[string]any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed || (source == "app" && levelNumber(level) < levelNumber(l.config.Level)) {
		return
	}
	// 限制消息长度防止单条日志过大
	if len(message) > 16384 {
		message = message[:16384]
	}
	entry := LogEntry{
		ID: randomHex(8), Timestamp: time.Now().UTC(),
		Level: strings.ToUpper(level), Source: source, Message: message, Fields: fields,
	}
	if entry.Fields == nil {
		entry.Fields = map[string]any{}
	}
	data, err := json.Marshal(entry)
	if err != nil {
		fmt.Fprintln(os.Stderr, "日志序列化失败:", err)
		return
	}

	// 写入内存缓冲区
	l.buffers[source].Push(entry)

	// 写入 stdout
	if l.config.Format == "json" {
		_, _ = os.Stdout.Write(append(data, '\n'))
	} else {
		// 空 fields 不输出尾部内容，保持日志简洁
		if len(fields) > 0 {
			fieldData, _ := json.Marshal(fields)
			fmt.Fprintf(os.Stdout, "%s %-5s [%s] %s %s\n",
				entry.Timestamp.Format(time.RFC3339Nano), entry.Level, source, message, fieldData)
		} else {
			fmt.Fprintf(os.Stdout, "%s %-5s [%s] %s\n",
				entry.Timestamp.Format(time.RFC3339Nano), entry.Level, source, message)
		}
	}

	// 写入文件（固定 JSONL 格式）
	if file := l.files[source]; file != nil {
		if _, err := file.Write(append(data, '\n')); err != nil {
			fmt.Fprintln(os.Stderr, "日志文件写入失败:", err)
		}
	}
}

// Close 关闭日志系统，同步并关闭所有文件。
func (l *Logs) Close() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return
	}
	l.closed = true
	for _, f := range l.files {
		_ = f.Sync()
		_ = f.Close()
	}
}

// LogHandler 实现 slog.Handler 接口，将标准库 slog 桥接到双流日志系统。
type LogHandler struct {
	logs   *Logs
	attrs  []slog.Attr
	groups []string
}

func (h *LogHandler) Enabled(_ context.Context, level slog.Level) bool {
	return h.logs.Enabled(level)
}

// addLogAttr 递归地将 slog.Attr 添加到 fields map 中。
func addLogAttr(fields map[string]any, prefix string, attr slog.Attr) {
	attr.Value = attr.Value.Resolve()
	if attr.Equal(slog.Attr{}) {
		return
	}
	key := prefix + attr.Key
	if attr.Value.Kind() == slog.KindGroup {
		if attr.Key != "" {
			key += "."
		}
		for _, child := range attr.Value.Group() {
			addLogAttr(fields, key, child)
		}
		return
	}
	fields[key] = attr.Value.Any()
}

func (h *LogHandler) Handle(_ context.Context, record slog.Record) error {
	fields := map[string]any{}
	for _, attr := range h.attrs {
		addLogAttr(fields, "", attr)
	}
	prefix := strings.Join(h.groups, ".")
	if prefix != "" {
		prefix += "."
	}
	record.Attrs(func(attr slog.Attr) bool {
		addLogAttr(fields, prefix, attr)
		return true
	})
	h.logs.Emit("app", record.Level.String(), record.Message, fields)
	return nil
}

func (h *LogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	n := *h
	n.attrs = append([]slog.Attr{}, h.attrs...)
	if len(h.groups) > 0 {
		wrapped := attrs
		for i := len(h.groups) - 1; i >= 0; i-- {
			wrapped = []slog.Attr{{Key: h.groups[i], Value: slog.GroupValue(wrapped...)}}
		}
		n.attrs = append(n.attrs, wrapped...)
	} else {
		n.attrs = append(n.attrs, attrs...)
	}
	return &n
}

func (h *LogHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	n := *h
	n.groups = append(append([]string{}, h.groups...), name)
	return &n
}

// logCursor 是日志分页游标的内部表示。
type logCursor struct {
	Time time.Time `json:"t"`
	ID   string    `json:"id"`
}

// ScanInfo 包含日志文件扫描的统计信息。
type ScanInfo struct {
	BufferHits       int   `json:"buffer_hits"`
	FileHits         int   `json:"file_hits"`
	FileLinesScanned int   `json:"file_lines_scanned"`
	FileBytesScanned int64 `json:"file_bytes_scanned"`
	ScanLimitReached bool  `json:"scan_limit_reached"`
}

// logBefore 判断日志条目 a 是否在 b 之前（按时间戳+ID 排序）。
func logBefore(a, b LogEntry) bool {
	if a.Timestamp.Equal(b.Timestamp) {
		return a.ID < b.ID
	}
	return a.Timestamp.Before(b.Timestamp)
}

// reverseLines 从文件尾部反向读取并逐行回调。
// 用于日志文件的历史回溯查询。
func reverseLines(path string, maxLines int, maxBytes int64, info *ScanInfo, consume func([]byte)) error {
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer file.Close()
	stat, err := file.Stat()
	if err != nil {
		return err
	}

	position := stat.Size()
	var remainder []byte
	discardLongLine := false

	for position > 0 {
		remaining := maxBytes - info.FileBytesScanned
		if remaining <= 0 || info.FileLinesScanned >= maxLines {
			info.ScanLimitReached = true
			return nil
		}
		n := min(int64(64*1024), position, remaining)
		position -= n
		block := make([]byte, n)
		read, err := file.ReadAt(block, position)
		if err != nil && err != io.EOF {
			return err
		}
		block = block[:read]
		info.FileBytesScanned += int64(read)
		block = append(block, remainder...)
		end := len(block)
		remainder = nil

		for end > 0 {
			index := bytes.LastIndexByte(block[:end], '\n')
			if index < 0 {
				if end > 1<<20 {
					discardLongLine = true
					remainder = nil
				} else {
					remainder = append([]byte{}, block[:end]...)
				}
				break
			}
			line := block[index+1 : end]
			if len(line) > 0 {
				if info.FileLinesScanned >= maxLines {
					info.ScanLimitReached = true
					return nil
				}
				info.FileLinesScanned++
				if !discardLongLine {
					consume(line)
				}
				discardLongLine = false
			}
			end = index
		}
	}
	if len(remainder) > 0 && !discardLongLine {
		if info.FileLinesScanned >= maxLines {
			info.ScanLimitReached = true
			return nil
		}
		info.FileLinesScanned++
		consume(remainder)
	}
	return nil
}

// Query 处理 GET /admin/logs 的日志查询。
// 支持从内存缓冲区和日志文件中联合查询，多条件过滤、游标分页。
func (l *Logs) Query(q url.Values) (map[string]any, error) {
	limit, err := queryLimit(q, 100, 1000)
	if err != nil {
		return nil, err
	}
	source := q.Get("source")
	if source != "" && source != "app" && source != "access" {
		return nil, errors.New("source 必须是 app 或 access")
	}
	order := q.Get("order")
	if order != "" && order != "asc" && order != "desc" {
		return nil, errors.New("order 必须是 asc 或 desc")
	}
	if err := validateBool(q, "file"); err != nil {
		return nil, err
	}

	// 解析时间范围
	var since, until time.Time
	if q.Get("since") != "" {
		since, err = time.Parse(time.RFC3339Nano, q.Get("since"))
		if err != nil {
			return nil, errors.New("无效的 since 时间戳")
		}
	}
	if q.Get("until") != "" {
		until, err = time.Parse(time.RFC3339Nano, q.Get("until"))
		if err != nil {
			return nil, errors.New("无效的 until 时间戳")
		}
	}

	// 解析分页游标
	var cursor *logCursor
	if encoded := q.Get("cursor"); encoded != "" {
		data, err := base64.RawURLEncoding.DecodeString(encoded)
		if err != nil {
			return nil, errors.New("无效的游标")
		}
		cursor = &logCursor{}
		if err := json.Unmarshal(data, cursor); err != nil || cursor.Time.IsZero() || cursor.ID == "" {
			return nil, errors.New("无效的游标")
		}
	}

	// 解析日志级别过滤
	levels := map[string]bool{}
	if q.Get("level") != "" {
		for _, level := range strings.Split(strings.ToUpper(q.Get("level")), ",") {
			switch level {
			case "DEBUG", "INFO", "WARN", "ERROR":
				levels[level] = true
			default:
				return nil, errors.New("无效的日志级别")
			}
		}
	}

	// 构建过滤器
	matches := func(entry LogEntry) bool {
		if source != "" && entry.Source != source {
			return false
		}
		if len(levels) > 0 && !levels[entry.Level] {
			return false
		}
		if !since.IsZero() && entry.Timestamp.Before(since) {
			return false
		}
		if !until.IsZero() && !entry.Timestamp.Before(until) {
			return false
		}
		if !strings.Contains(strings.ToLower(entry.Message), strings.ToLower(q.Get("search"))) {
			return false
		}
		for _, name := range []string{"node_id", "channel_id", "key_id", "model", "request_id"} {
			if q.Get(name) != "" && fmt.Sprint(entry.Fields[name]) != q.Get(name) {
				return false
			}
		}
		if filter := q.Get("status_code"); filter != "" {
			status, _ := strconv.Atoi(fmt.Sprint(entry.Fields["status"]))
			if !statusMatches(status, filter) {
				return false
			}
		}
		if cursor != nil {
			point := LogEntry{Timestamp: cursor.Time, ID: cursor.ID}
			if order == "asc" {
				if !logBefore(point, entry) {
					return false
				}
			} else if !logBefore(entry, point) {
				return false
			}
		}
		return true
	}

	// 从内存缓冲区收集候选
	l.mu.Lock()
	config := l.config
	memory := []LogEntry{}
	for name, ring := range l.buffers {
		if source == "" || source == name {
			memory = append(memory, ring.Values()...)
		}
	}
	l.mu.Unlock()

	// 排序和插入式收集
	entries := make([]LogEntry, 0, limit+1)
	seen := map[string]bool{}
	info := &ScanInfo{}
	less := func(a, b LogEntry) bool {
		if order == "asc" {
			return logBefore(a, b)
		}
		return logBefore(b, a)
	}
	add := func(entry LogEntry, file bool) {
		if entry.ID == "" || seen[entry.ID] || !matches(entry) {
			return
		}
		index := sort.Search(len(entries), func(i int) bool {
			return !less(entries[i], entry)
		})
		if index >= limit+1 {
			return
		}
		entries = append(entries, LogEntry{})
		copy(entries[index+1:], entries[index:])
		entries[index] = entry
		seen[entry.ID] = true
		if len(entries) > limit+1 {
			delete(seen, entries[len(entries)-1].ID)
			entries = entries[:limit+1]
		}
		if file {
			info.FileHits++
		} else {
			info.BufferHits++
		}
	}

	// 先从内存缓冲区收集
	for _, entry := range memory {
		add(entry, false)
	}

	// 再从日志文件扫描补充
	if q.Get("file") != "false" {
		for _, item := range []struct {
			source string
			config FileLogConfig
		}{
			{"app", config.File.App},
			{"access", config.File.Access},
		} {
			if !item.config.Enabled || (source != "" && source != item.source) {
				continue
			}
			if err := reverseLines(item.config.Path,
				config.Query.MaxScanLines, size(config.Query.MaxScanBytes), info,
				func(line []byte) {
					var entry LogEntry
					if json.Unmarshal(line, &entry) == nil {
						add(entry, true)
					}
				}); err != nil {
				return nil, err
			}
		}
	}

	// 构建响应
	hasMore := len(entries) > limit
	if hasMore {
		entries = entries[:limit]
	}
	next := ""
	if len(entries) > 0 && hasMore {
		last := entries[len(entries)-1]
		data, _ := json.Marshal(logCursor{Time: last.Timestamp, ID: last.ID})
		next = base64.RawURLEncoding.EncodeToString(data)
	}
	return map[string]any{
		"entries": entries, "returned": len(entries),
		"has_more": hasMore, "next_cursor": next, "scan_info": info,
	}, nil
}
