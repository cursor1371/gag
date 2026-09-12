// 配置管理：ENV/YAML 加载、验证、热重载、原子回写与文件监听。
//
// 配置分为两层：
//   - 环境变量：私密信息和启动参数（ADMIN_KEY, HOST, PORT 等），不可热重载
//   - YAML 文件：所有运行时参数，支持热重载，缺失时自动创建默认文件
//
// 运行时配置通过 atomic.Pointer[Config] 存储，代理请求路径可零锁读取。

package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"

	"github.com/fsnotify/fsnotify"
	"gopkg.in/yaml.v3"
)

// Credential 表示一个 API Key 或 WS Token 条目。
type Credential struct {
	ID        int       `yaml:"id" json:"id"`
	Value     string    `yaml:"value" json:"value"`
	Note      string    `yaml:"note" json:"note"`
	Enabled   bool      `yaml:"enabled" json:"enabled"`
	CreatedAt time.Time `yaml:"created_at" json:"created_at"`
}

// CredentialList 管理一组凭证及其自增 ID 种子。
type CredentialList struct {
	NextID int          `yaml:"next_id" json:"next_id"`
	Items  []Credential `yaml:"items" json:"items"`
}

// FileLogConfig 定义单个日志文件的开关和路径。
type FileLogConfig struct {
	Enabled bool   `yaml:"enabled" json:"enabled"`
	Path    string `yaml:"path" json:"path"`
}

// LogConfig 包含日志系统的完整配置。
type LogConfig struct {
	Level      string `yaml:"level" json:"level"`           // 日志级别：debug/info/warn/error
	Format     string `yaml:"format" json:"format"`         // stdout 格式：text/json（文件始终 JSONL）
	BufferSize int    `yaml:"buffer_size" json:"buffer_size"` // 内存环形缓冲区容量（每个日志源独立）
	File       struct {
		App    FileLogConfig `yaml:"app" json:"app"`       // 应用日志文件配置
		Access FileLogConfig `yaml:"access" json:"access"` // 访问日志文件配置
	} `yaml:"file" json:"file"`
	Query struct {
		MaxScanLines int    `yaml:"max_scan_lines" json:"max_scan_lines"` // 单次查询最大扫描行数
		MaxScanBytes string `yaml:"max_scan_bytes" json:"max_scan_bytes"` // 单次查询最大扫描字节数
	} `yaml:"query" json:"query"`
}

// Config 是运行时配置的完整结构体，对应 config.yaml。
type Config struct {
	Upstream struct {
		BaseURL string `yaml:"base_url" json:"base_url"` // 上游 Gemini API 基础 URL
	} `yaml:"upstream" json:"upstream"`

	LB struct {
		Strategy string `yaml:"strategy" json:"strategy"` // 负载均衡策略：round_robin/fill_first
	} `yaml:"lb" json:"lb"`

	Node struct {
		MaxConcurrency int    `yaml:"max_concurrency" json:"max_concurrency"` // 单节点最大并发请求数
		MaxConnections int    `yaml:"max_connections" json:"max_connections"` // 最大 WS 连接总数，0 不限制
		WSPingInterval string `yaml:"ws_ping_interval" json:"ws_ping_interval"` // WS 心跳间隔（不可热重载）
		WSPongTimeout  string `yaml:"ws_pong_timeout" json:"ws_pong_timeout"`   // WS pong 超时（不可热重载）
		OfflineTTL     string `yaml:"offline_ttl" json:"offline_ttl"`           // 离线节点记录保留时长
	} `yaml:"node" json:"node"`

	Request struct {
		MaxBodySize     string `yaml:"max_body_size" json:"max_body_size"`         // 请求体大小限制
		NodeWaitTimeout string `yaml:"node_wait_timeout" json:"node_wait_timeout"` // 等待空闲节点超时
		QueueMaxSize    int    `yaml:"queue_max_size" json:"queue_max_size"`       // 等待队列最大深度
	} `yaml:"request" json:"request"`

	Timeout struct {
		StreamFirstByte string `yaml:"stream_first_byte" json:"stream_first_byte"` // 流式首字响应超时
		StreamIdle      string `yaml:"stream_idle" json:"stream_idle"`             // 流式空闲间隔超时
		Total           string `yaml:"total" json:"total"`                         // 总请求超时
	} `yaml:"timeout" json:"timeout"`

	Cooldown struct {
		BaseDuration string  `yaml:"base_duration" json:"base_duration"` // 基础冷却时间
		MaxDuration  string  `yaml:"max_duration" json:"max_duration"`   // 最大冷却时间
		Multiplier   float64 `yaml:"multiplier" json:"multiplier"`       // 退避倍数
	} `yaml:"cooldown" json:"cooldown"`

	Server struct {
		DrainTimeout string `yaml:"drain_timeout" json:"drain_timeout"` // 优雅关闭排空超时
	} `yaml:"server" json:"server"`

	Log LogConfig `yaml:"log" json:"log"` // 日志配置

	Stats struct {
		RequestHistorySize int `yaml:"request_history_size" json:"request_history_size"` // 已完成请求历史缓冲区大小
	} `yaml:"stats" json:"stats"`

	Keys   CredentialList `yaml:"keys" json:"keys"`     // API Key 列表
	Tokens CredentialList `yaml:"tokens" json:"tokens"` // WS Token 列表

	// 以下为运行时内部索引，不序列化到 YAML
	keyByValue   map[string]Credential // Key 值 → Credential 快速查找
	tokenByValue map[string]Credential // Token 值 → Credential 快速查找
	tokenByID    map[int]Credential    // Token ID → Credential 快速查找
}

// defaultYAML 是配置文件不存在时自动创建的默认内容。
const defaultYAML = `upstream:
  base_url: "https://generativelanguage.googleapis.com"
lb:
  strategy: "round_robin"
node:
  max_concurrency: 5
  max_connections: 0
  ws_ping_interval: "30s"
  ws_pong_timeout: "10s"
  offline_ttl: "30m"
request:
  max_body_size: "64MB"
  node_wait_timeout: "30s"
  queue_max_size: 100
timeout:
  stream_first_byte: "60s"
  stream_idle: "30s"
  total: "600s"
cooldown:
  base_duration: "30s"
  max_duration: "300s"
  multiplier: 2
server:
  drain_timeout: "60s"
log:
  level: "info"
  format: "text"
  buffer_size: 1000
  file:
    app:
      enabled: true
      path: "./logs/app.log"
    access:
      enabled: true
      path: "./logs/access.log"
  query:
    max_scan_lines: 50000
    max_scan_bytes: "50MB"
stats:
  request_history_size: 1000
keys:
  next_id: 1
  items: []
tokens:
  next_id: 1
  items: []
`

// dur 将字符串解析为 time.Duration，解析失败返回 0。
func dur(s string) time.Duration {
	d, _ := time.ParseDuration(s)
	return d
}

// parseSize 将 "64MB"、"1GiB" 等人类可读的大小字符串解析为字节数。
func parseSize(s string) (int64, error) {
	s = strings.ToUpper(strings.TrimSpace(s))
	multiplier := int64(1)
	for _, suffix := range []struct {
		name string
		size int64
	}{
		{"GIB", 1 << 30}, {"GB", 1 << 30},
		{"MIB", 1 << 20}, {"MB", 1 << 20},
		{"KIB", 1 << 10}, {"KB", 1 << 10},
		{"B", 1},
	} {
		if strings.HasSuffix(s, suffix.name) {
			s = strings.TrimSpace(strings.TrimSuffix(s, suffix.name))
			multiplier = suffix.size
			break
		}
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n <= 0 || n > math.MaxInt64/multiplier {
		return 0, fmt.Errorf("invalid size")
	}
	return n * multiplier, nil
}

// size 是 parseSize 的便捷版本，解析失败返回 0。
func size(s string) int64 {
	n, _ := parseSize(s)
	return n
}

// parseConfig 将 YAML 字节解析为 Config，先填充默认值再叠加用户配置。
func parseConfig(data []byte) (*Config, error) {
	var c Config
	// 先加载默认值
	if err := yaml.Unmarshal([]byte(defaultYAML), &c); err != nil {
		return nil, err
	}
	// 叠加用户配置（严格模式：拒绝未知字段）
	d := yaml.NewDecoder(bytes.NewReader(data))
	d.KnownFields(true)
	if err := d.Decode(&c); err != nil {
		return nil, fmt.Errorf("YAML 解码错误: %w", err)
	}
	// 确保只有一个 YAML 文档
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return nil, errors.New("YAML 文件中只允许一个文档")
	}
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return &c, nil
}

// Validate 对配置执行全面的语义校验，并构建内部索引。
func (c *Config) Validate() error {
	// --- 上游 URL 校验 ---
	u, err := url.Parse(c.Upstream.BaseURL)
	if err != nil || u.Host == "" ||
		(u.Scheme != "http" && u.Scheme != "https") ||
		u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return errors.New("upstream.base_url 必须是不含凭证、查询参数或片段的 http(s) URL")
	}

	// --- LB 策略校验 ---
	if c.LB.Strategy != "round_robin" && c.LB.Strategy != "fill_first" {
		return errors.New("lb.strategy 必须是 round_robin 或 fill_first")
	}

	// --- 时间类参数校验 ---
	for name, value := range map[string]string{
		"node.ws_ping_interval":     c.Node.WSPingInterval,
		"node.ws_pong_timeout":      c.Node.WSPongTimeout,
		"node.offline_ttl":          c.Node.OfflineTTL,
		"request.node_wait_timeout": c.Request.NodeWaitTimeout,
		"timeout.stream_first_byte": c.Timeout.StreamFirstByte,
		"timeout.stream_idle":       c.Timeout.StreamIdle,
		"timeout.total":             c.Timeout.Total,
		"cooldown.base_duration":    c.Cooldown.BaseDuration,
		"cooldown.max_duration":     c.Cooldown.MaxDuration,
		"server.drain_timeout":      c.Server.DrainTimeout,
	} {
		d, err := time.ParseDuration(value)
		if err != nil || d <= 0 {
			return fmt.Errorf("%s 必须是正数时间值", name)
		}
	}

	// --- 数值范围校验 ---
	if c.Node.MaxConcurrency < 1 || c.Node.MaxConcurrency > 10000 {
		return errors.New("node.max_concurrency 必须在 1 到 10000 之间")
	}
	if c.Node.MaxConnections < 0 {
		return errors.New("node.max_connections 不能为负数")
	}
	if c.Request.QueueMaxSize < 0 || c.Request.QueueMaxSize > 100000 {
		return errors.New("request.queue_max_size 必须在 0 到 100000 之间")
	}
	if c.Log.BufferSize < 1 || c.Log.BufferSize > 100000 ||
		c.Stats.RequestHistorySize < 1 || c.Stats.RequestHistorySize > 100000 {
		return errors.New("缓冲区容量必须在 1 到 100000 之间")
	}
	if c.Log.Query.MaxScanLines < 1 || c.Log.Query.MaxScanLines > 1000000 {
		return errors.New("log.query.max_scan_lines 必须在 1 到 1000000 之间")
	}
	if _, err := parseSize(c.Request.MaxBodySize); err != nil {
		return errors.New("无效的 request.max_body_size")
	}
	if _, err := parseSize(c.Log.Query.MaxScanBytes); err != nil {
		return errors.New("无效的 log.query.max_scan_bytes")
	}
	if c.Cooldown.Multiplier < 1 || math.IsNaN(c.Cooldown.Multiplier) ||
		math.IsInf(c.Cooldown.Multiplier, 0) {
		return errors.New("cooldown.multiplier 必须是 >= 1 的有限数")
	}
	if dur(c.Cooldown.MaxDuration) < dur(c.Cooldown.BaseDuration) {
		return errors.New("cooldown.max_duration 必须 >= base_duration")
	}

	// --- 日志配置校验 ---
	switch c.Log.Level {
	case "debug", "info", "warn", "error":
	default:
		return errors.New("无效的 log.level")
	}
	if c.Log.Format != "text" && c.Log.Format != "json" {
		return errors.New("无效的 log.format")
	}
	for _, f := range []FileLogConfig{c.Log.File.App, c.Log.File.Access} {
		if f.Enabled && f.Path == "" {
			return errors.New("已启用的日志文件必须指定路径")
		}
	}

	// --- 凭证列表校验 ---
	validateCredentials := func(list *CredentialList) error {
		ids := make(map[int]bool)
		values := make(map[string]bool)
		maxID := 0
		for _, item := range list.Items {
			if item.ID < 1 || ids[item.ID] {
				return errors.New("凭证 ID 必须为正整数且唯一")
			}
			if len(item.Value) < 1 || len(item.Value) > 256 ||
				strings.IndexFunc(item.Value, unicode.IsSpace) >= 0 {
				return errors.New("凭证值必须为 1-256 字节且不含空白字符")
			}
			if values[item.Value] {
				return errors.New("凭证值必须唯一")
			}
			if item.CreatedAt.IsZero() {
				return errors.New("凭证 created_at 不能为空")
			}
			ids[item.ID], values[item.Value] = true, true
			maxID = max(maxID, item.ID)
		}
		if list.NextID <= maxID || list.NextID < 1 {
			return errors.New("凭证 next_id 必须大于所有现有 ID")
		}
		if list.Items == nil {
			list.Items = []Credential{}
		}
		return nil
	}

	if err := validateCredentials(&c.Keys); err != nil {
		return fmt.Errorf("keys: %w", err)
	}
	if err := validateCredentials(&c.Tokens); err != nil {
		return fmt.Errorf("tokens: %w", err)
	}

	// --- 构建内部索引 ---
	c.keyByValue = make(map[string]Credential, len(c.Keys.Items))
	c.tokenByValue = make(map[string]Credential, len(c.Tokens.Items))
	c.tokenByID = make(map[int]Credential, len(c.Tokens.Items))
	for _, v := range c.Keys.Items {
		c.keyByValue[v.Value] = v
	}
	for _, v := range c.Tokens.Items {
		c.tokenByValue[v.Value], c.tokenByID[v.ID] = v, v
	}
	return nil
}

// cloneConfig 创建配置的浅拷贝，确保凭证切片独立。
func cloneConfig(c *Config) *Config {
	n := *c
	n.Keys.Items = append([]Credential{}, c.Keys.Items...)
	n.Tokens.Items = append([]Credential{}, c.Tokens.Items...)
	return &n
}

// configDiff 比较两份配置的所有可热重载字段，仅返回发生变化的字段及其新旧值。
// 不比较 ws_ping_interval/ws_pong_timeout（不可热重载）和 Key/Token 具体值（敏感信息）。
func configDiff(old, new *Config) map[string]any {
	changes := map[string]any{}
	check := func(name string, oldVal, newVal any) {
		if fmt.Sprint(oldVal) != fmt.Sprint(newVal) {
			changes[name] = map[string]any{"from": oldVal, "to": newVal}
		}
	}

	check("upstream.base_url", old.Upstream.BaseURL, new.Upstream.BaseURL)
	check("lb.strategy", old.LB.Strategy, new.LB.Strategy)
	check("node.max_concurrency", old.Node.MaxConcurrency, new.Node.MaxConcurrency)
	check("node.max_connections", old.Node.MaxConnections, new.Node.MaxConnections)
	check("node.offline_ttl", old.Node.OfflineTTL, new.Node.OfflineTTL)
	check("request.max_body_size", old.Request.MaxBodySize, new.Request.MaxBodySize)
	check("request.node_wait_timeout", old.Request.NodeWaitTimeout, new.Request.NodeWaitTimeout)
	check("request.queue_max_size", old.Request.QueueMaxSize, new.Request.QueueMaxSize)
	check("timeout.stream_first_byte", old.Timeout.StreamFirstByte, new.Timeout.StreamFirstByte)
	check("timeout.stream_idle", old.Timeout.StreamIdle, new.Timeout.StreamIdle)
	check("timeout.total", old.Timeout.Total, new.Timeout.Total)
	check("cooldown.base_duration", old.Cooldown.BaseDuration, new.Cooldown.BaseDuration)
	check("cooldown.max_duration", old.Cooldown.MaxDuration, new.Cooldown.MaxDuration)
	check("cooldown.multiplier", old.Cooldown.Multiplier, new.Cooldown.Multiplier)
	check("server.drain_timeout", old.Server.DrainTimeout, new.Server.DrainTimeout)
	check("log.level", old.Log.Level, new.Log.Level)
	check("log.format", old.Log.Format, new.Log.Format)
	check("log.buffer_size", old.Log.BufferSize, new.Log.BufferSize)
	check("log.file.app.enabled", old.Log.File.App.Enabled, new.Log.File.App.Enabled)
	check("log.file.app.path", old.Log.File.App.Path, new.Log.File.App.Path)
	check("log.file.access.enabled", old.Log.File.Access.Enabled, new.Log.File.Access.Enabled)
	check("log.file.access.path", old.Log.File.Access.Path, new.Log.File.Access.Path)
	check("log.query.max_scan_lines", old.Log.Query.MaxScanLines, new.Log.Query.MaxScanLines)
	check("log.query.max_scan_bytes", old.Log.Query.MaxScanBytes, new.Log.Query.MaxScanBytes)
	check("stats.request_history_size", old.Stats.RequestHistorySize, new.Stats.RequestHistorySize)
	check("keys_count", len(old.Keys.Items), len(new.Keys.Items))
	check("tokens_count", len(old.Tokens.Items), len(new.Tokens.Items))

	return changes
}

// ConfigManager 管理配置的加载、存储、热重载和原子回写。
type ConfigManager struct {
	path     string                 // 配置文件绝对路径
	mu       sync.Mutex             // 保护写操作（重载、回写）
	value    atomic.Pointer[Config] // 运行时配置快照（无锁读取）
	hash     [32]byte               // 当前文件内容的 SHA-256 哈希（用于防止重复加载）
	created  bool                   // 配置文件是否为本次启动自动创建
	apply    func(*Config) error    // 配置发布前回调（如重新配置日志系统）
	after    func(*Config)          // 配置发布后回调（如调整统计窗口）
	envKey   *Credential            // 环境变量注入的 API Key（ID=0），nil 表示未设置
	envToken *Credential            // 环境变量注入的 WS Token（ID=0），nil 表示未设置
}

// OpenConfig 加载配置文件，文件不存在时自动创建默认配置。
func OpenConfig(path string) (*ConfigManager, error) {
	path, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	data, err := os.ReadFile(path)
	created := false
	if errors.Is(err, os.ErrNotExist) {
		data = []byte(defaultYAML)
		err = atomicWrite(path, data)
		created = true
	}
	if err != nil {
		return nil, err
	}
	c, err := parseConfig(data)
	if err != nil {
		return nil, err
	}
	m := &ConfigManager{path: path, hash: sha256.Sum256(data), created: created}
	m.value.Store(c)
	return m, nil
}

// Load 返回当前有效配置的不可变快照（无锁读取）。
func (m *ConfigManager) Load() *Config {
	return m.value.Load()
}

// publish 将新配置发布为运行时有效配置。
// WS 心跳参数不可热重载，始终保持进程启动时的值。
func (m *ConfigManager) publish(c *Config) error {
	old := m.Load()
	c.Node.WSPingInterval = old.Node.WSPingInterval
	c.Node.WSPongTimeout = old.Node.WSPongTimeout
	if m.apply != nil {
		if err := m.apply(c); err != nil {
			return err
		}
	}
	m.injectEnv(c)
	m.value.Store(c)
	if m.after != nil {
		m.after(c)
	}
	return nil
}

// SetEnvCredentials 设置环境变量注入的凭证（ID=0），在启动时调用一次。
// 环境变量凭证不写入 YAML，不可通过管理 API 删除或修改，仅支持启用/禁用。
func (m *ConfigManager) SetEnvCredentials(apiKey, wsToken string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now().UTC()
	if apiKey != "" {
		m.envKey = &Credential{
			ID: 0, Value: apiKey, Note: "ENV_API_KEY",
			Enabled: true, CreatedAt: now,
		}
	}
	if wsToken != "" {
		m.envToken = &Credential{
			ID: 0, Value: wsToken, Note: "ENV_WS_TOKEN",
			Enabled: true, CreatedAt: now,
		}
	}

	// 注入到当前配置的查找表中
	c := cloneConfig(m.value.Load())
	_ = c.Validate()
	m.injectEnv(c)
	m.value.Store(c)
}

// injectEnv 将环境变量凭证注入到配置的查找表中。
// 在 publish 和 SetEnvCredentials/SetEnvEnabled 中调用，确保每次配置更新后环境变量凭证仍然有效。
func (m *ConfigManager) injectEnv(c *Config) {
	if m.envKey != nil {
		c.keyByValue[m.envKey.Value] = *m.envKey
	}
	if m.envToken != nil {
		c.tokenByValue[m.envToken.Value] = *m.envToken
		c.tokenByID[0] = *m.envToken
	}
}

// EnvCredential 返回指定类型的环境变量凭证副本（如果存在）。
// 线程安全，通过 mu 保护对 envKey/envToken 的 Enabled 字段读取。
func (m *ConfigManager) EnvCredential(kind string) *Credential {
	m.mu.Lock()
	defer m.mu.Unlock()
	if kind == "keys" && m.envKey != nil {
		cred := *m.envKey
		return &cred
	}
	if kind == "tokens" && m.envToken != nil {
		cred := *m.envToken
		return &cred
	}
	return nil
}

// SetEnvEnabled 切换环境变量凭证的启用/禁用状态。
// 更新后重建配置快照并触发 after 回调（节点状态重新评估）。
func (m *ConfigManager) SetEnvEnabled(kind string, enabled bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if kind == "keys" && m.envKey != nil {
		m.envKey.Enabled = enabled
	} else if kind == "tokens" && m.envToken != nil {
		m.envToken.Enabled = enabled
	}

	// 重建配置快照以反映环境变量凭证的新状态
	c := cloneConfig(m.value.Load())
	_ = c.Validate()
	m.injectEnv(c)
	m.value.Store(c)

	// 触发节点状态重新评估（Token 启用/禁用影响调度资格）
	if m.after != nil {
		m.after(c)
	}
}

// Reload 从磁盘重新加载配置文件，文件内容未变化时跳过。
func (m *ConfigManager) Reload() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	data, err := os.ReadFile(m.path)
	if err != nil {
		return err
	}
	hash := sha256.Sum256(data)
	if hash == m.hash {
		return nil // 内容未变化，跳过
	}
	c, err := parseConfig(data)
	if err != nil {
		return err
	}
	if err := m.publish(c); err != nil {
		return err
	}
	m.hash = hash
	return nil
}

// Raw 返回配置文件的原始内容。
func (m *ConfigManager) Raw() ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return os.ReadFile(m.path)
}

// Replace 用新的 YAML 内容替换配置文件。
// 流程：验证语法 → 备份旧文件为 .bak → 写入新文件 → 发布新配置。
func (m *ConfigManager) Replace(data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	c, err := parseConfig(data)
	if err != nil {
		return err
	}
	oldData, err := os.ReadFile(m.path)
	if err != nil {
		return err
	}
	// 备份旧文件
	if err := atomicWrite(m.path+".bak", oldData); err != nil {
		return err
	}
	if err := atomicWrite(m.path, data); err != nil {
		return err
	}
	if err := m.publish(c); err != nil {
		// 发布失败时回滚文件
		rollback := atomicWrite(m.path, oldData)
		return errors.Join(err, rollback)
	}
	m.hash = sha256.Sum256(data)
	return nil
}

// Update 以当前配置为基础执行修改函数，然后验证、持久化并发布。
// 主要用于管理 API 的凭证增删改操作。
func (m *ConfigManager) Update(fn func(*Config) error) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	c := cloneConfig(m.Load())
	if err := fn(c); err != nil {
		return err
	}
	if err := c.Validate(); err != nil {
		return err
	}
	data, err := yaml.Marshal(c)
	if err != nil {
		return err
	}
	oldData, err := os.ReadFile(m.path)
	if err != nil {
		return err
	}
	if err := atomicWrite(m.path, data); err != nil {
		return err
	}
	if err := m.publish(c); err != nil {
		return errors.Join(err, atomicWrite(m.path, oldData))
	}
	m.hash = sha256.Sum256(data)
	return nil
}

// atomicWrite 使用 "写临时文件 → fsync → rename" 模式安全写入文件。
// 防止写入中途断电或崩溃导致文件损坏。
func atomicWrite(path string, data []byte) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	f, err := os.CreateTemp(dir, ".gag-*")
	if err != nil {
		return err
	}
	name := f.Name()
	defer os.Remove(name)

	if err := f.Chmod(0600); err != nil {
		f.Close()
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(name, path); err != nil {
		return err
	}
	// 同步父目录以确保目录项更新持久化
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
}

// Watch 启动文件系统监听，监控配置文件和模型文件的变更并触发热重载。
// 使用 500ms 防抖机制避免编辑器保存时的多次触发。
func (a *App) Watch() (*fsnotify.Watcher, error) {
	w, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}

	configPath, _ := filepath.Abs(a.cfg.path)
	modelsPath, _ := filepath.Abs(a.models.path)
	// 监听文件所在的目录（而非文件本身），以支持原子替换（rename）
	dirs := map[string]bool{
		filepath.Dir(configPath): true,
		filepath.Dir(modelsPath): true,
	}
	for dir := range dirs {
		if err := os.MkdirAll(dir, 0700); err != nil {
			w.Close()
			return nil, err
		}
		if err := w.Add(dir); err != nil {
			w.Close()
			return nil, err
		}
	}

	go func(ctx context.Context) {
		var timer *time.Timer
		var timerC <-chan time.Time
		defer func() {
			if timer != nil {
				timer.Stop()
			}
		}()

		for {
			select {
			case <-ctx.Done():
				return
			case ev, ok := <-w.Events:
				if !ok {
					return
				}
				// 只关注目标文件的写入、创建、重命名和删除事件
				name, _ := filepath.Abs(ev.Name)
				if name != configPath && name != modelsPath {
					continue
				}
				if ev.Op&(fsnotify.Write|fsnotify.Create|fsnotify.Rename|fsnotify.Remove) == 0 {
					continue
				}
				// 500ms 防抖：重置计时器
				if timer == nil {
					timer = time.NewTimer(500 * time.Millisecond)
				} else {
					if !timer.Stop() {
						select {
						case <-timer.C:
						default:
						}
					}
					timer.Reset(500 * time.Millisecond)
				}
				timerC = timer.C
			case <-timerC:
				// 防抖窗口结束，执行重载
				timerC = nil
				// 记录重载前的配置和模型指针，用于判断是否有实际变更
				oldCfg := a.cfg.Load()
				oldModels := a.models.value.Load()
				if err := a.Reload(); err != nil {
					a.logs.Emit("app", "ERROR", "热重载失败", map[string]any{"error": err.Error()})
				} else {
					newCfg := a.cfg.Load()
					newModels := a.models.value.Load()
					// 仅在配置或模型实际变更时才输出日志，避免管理 API 写入后 fsnotify 触发的冗余日志
					configChanged := newCfg != oldCfg
					modelsChanged := newModels != oldModels
					if configChanged || modelsChanged {
						fields := map[string]any{}
						if configChanged {
							changes := configDiff(oldCfg, newCfg)
							if len(changes) > 0 {
								fields["config_changes"] = changes
							} else {
								fields["config_changes"] = "格式或注释变更"
							}
						}
						if modelsChanged {
							oldCount := 0
							if oldModels != nil {
								oldCount = len(oldModels.items)
							}
							newCount := 0
							if newModels != nil {
								newCount = len(newModels.items)
							}
							if oldCount != newCount {
								fields["models_count"] = map[string]any{"from": oldCount, "to": newCount}
							} else {
								fields["models_content_updated"] = true
							}
						}
						a.logs.Emit("app", "INFO", "热重载完成", fields)
					}
				}
			case err, ok := <-w.Errors:
				if !ok {
					return
				}
				a.logs.Emit("app", "ERROR", "文件监听错误", map[string]any{"error": err.Error()})
			}
		}
	}(a.ctx)

	return w, nil
}
