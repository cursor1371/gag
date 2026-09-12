// 模型列表加载器：从 models.json 文件加载模型定义。
//
// 使用 json.RawMessage 保留原始 JSON 内容，不解析具体字段，
// 确保 thinking、batchGenerateContent 等扩展字段透传无损。
// 支持热重载（与 YAML 共用 fsnotify watcher）。

package main

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync/atomic"
)

// modelSnapshot 是模型列表的不可变快照。
type modelSnapshot struct {
	raw   json.RawMessage            // 完整的 JSON 内容（原样返回给客户端）
	items map[string]json.RawMessage // name → 单个模型的 JSON（用于详情查询）
}

// Models 管理模型列表的加载和访问。
type Models struct {
	path  string                        // 模型文件路径
	value atomic.Pointer[modelSnapshot] // 当前模型快照（无锁读取）
	hash  [32]byte                      // 当前文件内容的 SHA-256（用于跳过无变化的重载）
}

// Reload 从磁盘重新加载模型文件。
//
// 文件不存在时的行为：
//   - 首次加载：使用空模型列表，输出 WARN 日志
//   - 后续重载：保留上一个有效快照，输出 WARN 日志
func (m *Models) Reload() error {
	data, err := os.ReadFile(m.path)
	if errors.Is(err, os.ErrNotExist) {
		if m.value.Load() == nil {
			// 首次加载，使用空列表
			m.value.Store(&modelSnapshot{
				raw:   json.RawMessage(`{"models":[]}`),
				items: map[string]json.RawMessage{},
			})
			slog.Warn("未找到模型文件，将提供空模型列表", "path", m.path)
		} else {
			slog.Warn("模型文件缺失，将沿用先前的模型快照", "path", m.path)

		}
		return nil
	}
	if err != nil {
		return err
	}

	// 文件内容未变化时跳过重载
	hash := sha256.Sum256(data)
	if hash == m.hash {
		return nil
	}

	// 限制文件大小防止过大文件
	if len(data) > 16<<20 {
		return errors.New("模型文件超过 16 MiB 限制")
	}

	// 解析 JSON 结构
	var root struct {
		Models []json.RawMessage `json:"models"`
	}
	if err := json.Unmarshal(data, &root); err != nil {
		return fmt.Errorf("模型 JSON 解析错误: %w", err)
	}
	if root.Models == nil {
		return errors.New("模型 JSON 必须包含 models 数组")
	}

	// 构建快照
	snapshot := &modelSnapshot{
		raw:   append(json.RawMessage(nil), data...),
		items: make(map[string]json.RawMessage),
	}
	for _, raw := range root.Models {
		var item struct {
			Name string `json:"name"`
		}
		if err := json.Unmarshal(raw, &item); err != nil {
			return err
		}
		if item.Name == "" {
			return errors.New("模型 name 字段不能为空")
		}
		if _, exists := snapshot.items[item.Name]; exists {
			return fmt.Errorf("重复的模型名: %s", item.Name)
		}
		snapshot.items[item.Name] = raw
	}
	m.value.Store(snapshot)
	m.hash = hash
	return nil
}
