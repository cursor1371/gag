// 管理 API 处理器：凭证 CRUD、节点/渠道控制、系统状态、配置管理、日志/统计查询。
//
// 所有端点均需 Admin Key Bearer 鉴权（中间件 AdminAuth 保护）。
// 端点设计遵循 RESTful 风格：
//   - POST   创建资源
//   - GET    查询资源
//   - PATCH  部分更新（仅 note 字段）
//   - DELETE 删除资源
//   - POST   /{id}/enable|disable|cooldown 状态控制

package main

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
)

var errCredentialNotFound = errors.New("凭证未找到")

// RegisterAdmin 注册所有管理 API 路由。
func (a *App) RegisterAdmin(r chi.Router) {
	// ---- 凭证管理（Key / Token 对称路由）----
	for _, kind := range []string{"keys", "tokens"} {
		kind := kind
		r.Route("/"+kind, func(r chi.Router) {
			r.Get("/", func(w http.ResponseWriter, r *http.Request) {
				a.Credentials(w, r, kind, "list")
			})
			r.Post("/", func(w http.ResponseWriter, r *http.Request) {
				a.Credentials(w, r, kind, "create")
			})
			r.Get("/{id}", func(w http.ResponseWriter, r *http.Request) {
				a.Credentials(w, r, kind, "get")
			})
			r.Patch("/{id}", func(w http.ResponseWriter, r *http.Request) {
				a.Credentials(w, r, kind, "patch")
			})
			r.Delete("/{id}", func(w http.ResponseWriter, r *http.Request) {
				a.Credentials(w, r, kind, "delete")
			})
			for _, action := range []string{"enable", "disable"} {
				action := action
				r.Post("/{id}/"+action, func(w http.ResponseWriter, r *http.Request) {
					a.Credentials(w, r, kind, action)
				})
			}
		})
	}

	// ---- 节点管理 ----
	r.Get("/nodes", a.AdminNodes)
	r.Get("/nodes/{id}", a.AdminNodes)
	for _, action := range []string{"enable", "disable", "cooldown"} {
		action := action
		r.Post("/nodes/{id}/"+action, func(w http.ResponseWriter, r *http.Request) {
			a.AdminChangeNode(w, r, action)
		})
	}

	// ---- 渠道管理 ----
	r.Get("/channels", a.AdminChannels)
	r.Get("/channels/{id}", a.AdminChannels)
	for _, action := range []string{"enable", "disable"} {
		action := action
		r.Post("/channels/{id}/"+action, func(w http.ResponseWriter, r *http.Request) {
			id := chi.URLParam(r, "id")
			if err := a.hub.ChangeChannel(id, action == "enable"); err != nil {
				adminError(w, 404, "NOT_FOUND", err.Error())
				return
			}
			// 渠道状态变更日志包含下属节点数
			channels, _ := a.hub.Channels(id)
			nodeCount := 0
			if len(channels) > 0 {
				if nodes, ok := channels[0]["nodes"].(map[string]int); ok {
					nodeCount = nodes["total"]
				}
			}
			slog.Info("渠道已更新", "channel_id", id, "action", action, "node_count", nodeCount)
			a.AdminChannels(w, r)
		})
	}

	// ---- 系统管理 ----
	r.Get("/status", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, a.Status())
	})
	r.Get("/config", func(w http.ResponseWriter, r *http.Request) {
		c := cloneConfig(a.cfg.Load())
		// 脱敏显示凭证值
		for i := range c.Keys.Items {
			c.Keys.Items[i].Value = "***"
		}
		for i := range c.Tokens.Items {
			c.Tokens.Items[i].Value = "***"
		}
		writeJSON(w, 200, c)
	})
	r.Post("/config/reload", func(w http.ResponseWriter, r *http.Request) {
		// 记录重载前的状态，用于输出变更差异
		oldCfg := a.cfg.Load()
		oldModels := a.models.value.Load()

		if err := a.Reload(); err != nil {
			slog.Warn("手动配置重载失败", "error", err)
			adminError(w, 500, "RELOAD_FAILED", err.Error())
			return
		}

		newCfg := a.cfg.Load()
		newModels := a.models.value.Load()
		configChanged := newCfg != oldCfg
		modelsChanged := newModels != oldModels

		if configChanged || modelsChanged {
			args := []any{}
			if configChanged {
				changes := configDiff(oldCfg, newCfg)
				if len(changes) > 0 {
					args = append(args, "config_changes", changes)
				} else {
					args = append(args, "config_changes", "格式或注释变更")
				}
			}
			if modelsChanged {
				oldCount := 0
				if oldModels != nil {
					oldCount = len(oldModels.items)
				}
				newCount := 0
				if snap := a.models.value.Load(); snap != nil {
					newCount = len(snap.items)
				}
				if oldCount != newCount {
					args = append(args, "models_count_from", oldCount, "models_count_to", newCount)
				} else {
					args = append(args, "models_content_updated", true)
				}
			}
			slog.Info("手动配置重载完成", args...)
		} else {
			slog.Info("手动配置重载完成（无变更）")
		}
		writeJSON(w, 200, map[string]string{"message": "配置重载成功"})
	})
	r.Get("/config/yaml", func(w http.ResponseWriter, r *http.Request) {
		data, err := a.cfg.Raw()
		if err != nil {
			adminError(w, 500, "READ_FAILED", err.Error())
			return
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		_, _ = w.Write(data)
	})
	r.Put("/config/yaml", func(w http.ResponseWriter, r *http.Request) {
		data, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 4<<20))
		if err != nil {
			adminError(w, 400, "INVALID_YAML", err.Error())
			return
		}
		// 先验证 YAML 语法和语义
		preview, err := parseConfig(data)
		if err != nil {
			adminError(w, 400, "INVALID_YAML", err.Error())
			return
		}
		// 计算配置变更差异，在 Replace 前输出，确保日志因果顺序：
		// 正在应用 → Reconcile 影响（节点排空等）→ 完成/失败
		current := a.cfg.Load()
		changes := configDiff(current, preview)
		if len(changes) > 0 {
			slog.Info("正在应用管理 API 配置更新", "changes", changes)
		} else {
			slog.Info("正在应用管理 API 配置更新（无字段变更）")
		}
		if err := a.cfg.Replace(data); err != nil {
			slog.Warn("管理 API 配置更新失败，已回滚", "error", err)
			adminError(w, 500, "UPDATE_FAILED", err.Error())
			return
		}
		slog.Info("管理 API 配置更新完成")
		writeJSON(w, 200, map[string]string{"message": "配置更新并重载成功"})
	})

	// ---- 可观测性 ----
	r.Get("/logs", func(w http.ResponseWriter, r *http.Request) {
		result, err := a.logs.Query(r.URL.Query())
		adminQueryResult(w, result, err)
	})
	r.Get("/requests", func(w http.ResponseWriter, r *http.Request) {
		result, err := a.stats.Requests(r.URL.Query())
		adminQueryResult(w, result, err)
	})
	r.Get("/stats", func(w http.ResponseWriter, r *http.Request) {
		result, err := a.stats.Query(r.URL.Query())
		adminQueryResult(w, result, err)
	})
	r.Post("/stats/reset", func(w http.ResponseWriter, r *http.Request) {
		a.stats.Reset()
		// 同时重置节点的内存计数器
		a.hub.mu.Lock()
		for n := range a.hub.all {
			n.total, n.success, n.failures, n.latencySum = 0, 0, 0, 0
		}
		a.hub.mu.Unlock()
		// 统计重置日志包含请求信息
		rid := r.Context().Value(recordContextKey{})
		reqID := ""
		if rec, ok := rid.(*RequestRecord); ok && rec != nil {
			reqID = rec.RequestID
		}
		slog.Info("统计数据已重置", "request_id", reqID)
		writeJSON(w, 200, map[string]string{"message": "统计数据已重置"})
	})
}

// adminQueryResult 是查询类端点的通用响应函数。
func adminQueryResult(w http.ResponseWriter, result any, err error) {
	if err != nil {
		adminError(w, 400, "INVALID_QUERY", err.Error())
		return
	}
	writeJSON(w, 200, result)
}

// credentialView 构建凭证的管理 API 视图（包含使用统计）。
func (a *App) credentialView(kind string, c Credential) map[string]any {
	v := objectMap(c)
	dim := "key"
	if kind == "tokens" {
		dim = "token"
	}
	v["stats"] = a.stats.Counters(dim, strconv.Itoa(c.ID))
	return v
}

// Credentials 是凭证管理的统一处理器，根据 action 参数执行不同操作。
func (a *App) Credentials(w http.ResponseWriter, r *http.Request, kind, action string) {
	w.Header().Set("Cache-Control", "no-store")
	list := a.cfg.Load().Keys
	if kind == "tokens" {
		list = a.cfg.Load().Tokens
	}
	// 解析路径参数中的 ID
	idParam := chi.URLParam(r, "id")
	id := 0
	if idParam != "" {
		var err error
		id, err = strconv.Atoi(idParam)
		if err != nil || id < 0 {
			adminError(w, 400, "INVALID_ID", "ID 必须为非负整数")
			return
		}
	}

	// 环境变量凭证操作 (ID=0)
	if idParam != "" && id == 0 {
		envCred := a.cfg.EnvCredential(kind)
		if envCred == nil {
			adminError(w, 404, "NOT_FOUND", "环境变量凭证未配置")
			return
		}
		switch action {
		case "get":
			v := a.credentialView(kind, *envCred)
			v["source"] = "env"
			writeJSON(w, 200, v)
		case "enable", "disable":
			enabled := action == "enable"
			a.cfg.SetEnvEnabled(kind, enabled)
			updated := a.cfg.EnvCredential(kind)
			logFields := []any{"kind", kind, "id", 0, "action", action, "enabled", enabled}
			if kind == "tokens" {
				affected := a.hub.CountNodesByToken(0)
				if affected > 0 {
					logFields = append(logFields, "affected_nodes", affected)
				}
			}
			slog.Info("凭证已更新", logFields...)
			v := a.credentialView(kind, *updated)
			v["source"] = "env"
			writeJSON(w, 200, v)
		default:
			adminError(w, 403, "ENV_CREDENTIAL", "环境变量凭证不可修改或删除")
		}
		return
	}

	// ---- 只读操作 ----
	if action == "list" {
		items := make([]map[string]any, 0, len(list.Items)+1)
		// 如果存在环境变量凭证，添加到列表头部
		if envCred := a.cfg.EnvCredential(kind); envCred != nil {
			v := a.credentialView(kind, *envCred)
			v["source"] = "env"
			items = append(items, v)
		}
		for _, item := range list.Items {
			items = append(items, a.credentialView(kind, item))
		}
		writeJSON(w, 200, map[string]any{kind: items, "total": len(items)})
		return
	}
	if action == "get" {
		for _, item := range list.Items {
			if item.ID == id {
				writeJSON(w, 200, a.credentialView(kind, item))
				return
			}
		}
		adminError(w, 404, "NOT_FOUND", "凭证未找到")
		return
	}

	// ---- 解析请求体（创建和修改需要）----
	var input struct {
		Value string  `json:"value"`
		Note  *string `json:"note"`
	}
	if action == "create" || action == "patch" {
		if err := decodeJSON(w, r, &input); err != nil {
			adminError(w, 400, "INVALID_BODY", err.Error())
			return
		}
		if action == "patch" && (input.Note == nil || input.Value != "") {
			adminError(w, 400, "INVALID_BODY", "PATCH 仅接受 note 字段")
			return
		}
		if input.Note != nil && len(*input.Note) > 4096 {
			adminError(w, 400, "INVALID_BODY", "note 不能超过 4096 字节")
			return
		}
	}

	// 创建时检查用户指定的值是否与环境变量凭证冲突
	if action == "create" && input.Value != "" {
		if envCred := a.cfg.EnvCredential(kind); envCred != nil && input.Value == envCred.Value {
			adminError(w, 409, "CONFLICT", "凭证值与环境变量凭证冲突")
			return
		}
	}

	// ---- 写操作（通过 ConfigManager.Update 原子执行）----
	var changed Credential
	err := a.cfg.Update(func(config *Config) error {
		items := &config.Keys
		if kind == "tokens" {
			items = &config.Tokens
		}
		if action == "create" {
			value := input.Value
			if value == "" {
				if kind == "tokens" {
					value = "gag-t-" + randomBase62(24)
				} else {
					value = "gag-k-" + randomBase62(32)
				}
			}
			note := ""
			if input.Note != nil {
				note = *input.Note
			}
			changed = Credential{
				ID: items.NextID, Value: value, Note: note,
				Enabled: true, CreatedAt: time.Now().UTC(),
			}
			items.NextID++
			items.Items = append(items.Items, changed)
			return nil
		}
		for i := range items.Items {
			if items.Items[i].ID != id {
				continue
			}
			switch action {
			case "patch":
				items.Items[i].Note = *input.Note
			case "enable":
				items.Items[i].Enabled = true
			case "disable":
				items.Items[i].Enabled = false
			case "delete":
				changed = items.Items[i]
				items.Items = append(items.Items[:i], items.Items[i+1:]...)
				return nil
			}
			changed = items.Items[i]
			return nil
		}
		return errCredentialNotFound
	})
	if err != nil {
		if errors.Is(err, errCredentialNotFound) {
			adminError(w, 404, "NOT_FOUND", err.Error())
		} else {
			adminError(w, 400, "UPDATE_FAILED", err.Error())
		}
		return
	}

	// 凭证操作日志：值使用密文显示，真实值通过管理 API 获取
	logFields := []any{"kind", kind, "id", changed.ID, "action", action}
	switch action {
	case "create":
		logFields = append(logFields, "value", maskValue(changed.Value), "note", changed.Note)
	case "patch":
		logFields = append(logFields, "note", changed.Note)
	case "enable", "disable":
		logFields = append(logFields, "enabled", changed.Enabled)
		// Token 启用/禁用时记录受影响的在线节点数
		if kind == "tokens" {
			affected := a.hub.CountNodesByToken(changed.ID)
			if affected > 0 {
				logFields = append(logFields, "affected_nodes", affected)
			}
		}
	case "delete":
		logFields = append(logFields, "value", maskValue(changed.Value), "note", changed.Note)
	}
	slog.Info("凭证已更新", logFields...)
	if action == "delete" {
		w.WriteHeader(204)
		return
	}
	status := 200
	if action == "create" {
		status = 201
	}
	writeJSON(w, status, a.credentialView(kind, changed))
}

// AdminNodes 处理节点列表和详情查询。
func (a *App) AdminNodes(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	nodes, found := a.hub.Nodes(id)
	if !found {
		adminError(w, 404, "NOT_FOUND", "节点未找到")
		return
	}
	if id != "" {
		writeJSON(w, 200, nodes[0])
		return
	}
	writeJSON(w, 200, map[string]any{"nodes": nodes, "total": len(nodes)})
}

// AdminChangeNode 处理节点状态变更（启用/禁用/冷却控制）。
// 冷却操作支持可选的 model 字段：
//   - 激活时：model 非空冷却指定模型，省略则通配阻断所有模型
//   - 解除时：model 非空解除指定模型冷却，省略则清除所有冷却条目
func (a *App) AdminChangeNode(w http.ResponseWriter, r *http.Request, action string) {
	id := chi.URLParam(r, "id")
	activate := false
	duration := dur(a.cfg.Load().Cooldown.BaseDuration)
	model := ""

	if action == "cooldown" {
		var input struct {
			Activate *bool  `json:"activate"`
			Duration string `json:"duration"`
			Model    string `json:"model"`
		}
		if err := decodeJSON(w, r, &input); err != nil || input.Activate == nil {
			adminError(w, 400, "INVALID_BODY", "activate 字段必填")
			return
		}
		activate = *input.Activate
		model = input.Model
		if input.Duration != "" {
			var err error
			duration, err = time.ParseDuration(input.Duration)
			if err != nil || duration <= 0 {
				adminError(w, 400, "INVALID_DURATION", "duration 必须是正数时间值")
				return
			}
		}
	}
	if err := a.hub.ChangeNode(id, action, activate, duration, model); err != nil {
		adminError(w, 404, "NOT_FOUND", err.Error())
		return
	}
	// 冷却控制日志区分激活和解除，包含目标模型
	if action == "cooldown" {
		logModel := model
		if logModel == "" {
			logModel = "*"
		}
		if activate {
			slog.Info("节点手动冷却已激活", "node_id", id, "model", logModel, "duration", fmt.Sprint(duration))
		} else {
			slog.Info("节点手动冷却已解除", "node_id", id, "model", logModel)
		}
	} else {
		slog.Info("节点已更新", "node_id", id, "action", action)
	}
	a.AdminNodes(w, r)
}

// AdminChannels 处理渠道列表和详情查询。
func (a *App) AdminChannels(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	channels, found := a.hub.Channels(id)
	if !found {
		adminError(w, 404, "NOT_FOUND", "渠道未找到")
		return
	}
	if id != "" {
		writeJSON(w, 200, channels[0])
		return
	}
	writeJSON(w, 200, map[string]any{"channels": channels, "total": len(channels)})
}
