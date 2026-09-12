// GAG — Gemini API Gateway
// 项目入口：负责应用初始化、路由组装、信号处理与优雅关闭。

package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
)

// 编译时注入的版本信息，通过 -ldflags 设置。
var (
	version   = "dev"
	commit    = "unknown"
	buildTime = "unknown"
)

// App 是网关的核心应用结构体，持有所有全局共享状态。
type App struct {
	cfg      *ConfigManager // 配置管理器（atomic 快照 + 文件热重载）
	models   *Models        // 模型列表加载器
	logs     *Logs          // 双流日志系统（应用日志 + 访问日志）
	stats    *Stats         // 时间窗口统计引擎
	hub      *Hub           // WebSocket 节点管理中心（Hub）
	adminKey string         // 管理 API 鉴权密钥（来自环境变量）
	started  time.Time      // 应用启动时间
	ctx      context.Context
	cancel   context.CancelFunc
	stopping atomic.Bool // 标记是否正在执行优雅关闭
}

// env 读取环境变量，若未设置则返回 fallback 默认值。
func env(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}

// NewApp 创建并初始化应用实例。
// 依次加载配置文件、初始化日志系统、创建统计引擎和 Hub。
func NewApp(configPath, modelsPath, adminKey string) (*App, error) {
	// 1. 加载或创建配置文件
	cm, err := OpenConfig(configPath)
	if err != nil {
		return nil, err
	}

	// 2. 初始化日志系统
	logs := NewLogs()
	if err := logs.Configure(cm.Load().Log); err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	a := &App{
		cfg:      cm,
		models:   &Models{path: modelsPath},
		logs:     logs,
		stats:    NewStats(cm.Load().Stats.RequestHistorySize),
		adminKey: adminKey,
		started:  time.Now(),
		ctx:      ctx,
		cancel:   cancel,
	}

	// 3. 将日志系统注册为 slog 全局 handler
	slog.SetDefault(slog.New(&LogHandler{logs: logs}))
	a.hub = NewHub(a)

	// 4. 注册配置热重载回调
	cm.apply = func(c *Config) error {
		// 配置变更时重新配置日志系统
		if err := logs.Configure(c.Log); err != nil {
			return err
		}
		return nil
	}
	cm.after = func(c *Config) {
		// 配置变更后调整统计窗口大小并协调节点状态
		a.stats.Resize(c.Stats.RequestHistorySize)
		a.hub.Reconcile()
	}

	// 5. 加载模型文件（缺失时使用空列表并输出 WARN）
	if err := a.models.Reload(); err != nil {
		a.cancel()
		a.logs.Close()
		return nil, err
	}

	// 记录配置文件加载状态
	if cm.created {
		slog.Info("配置文件不存在，已自动创建默认配置", "path", cm.path)
	} else {
		slog.Info("配置文件已加载", "path", cm.path)
	}

	// 记录模型加载结果
	snap := a.models.value.Load()
	if snap != nil {
		slog.Info("模型文件已加载", "path", modelsPath, "models_count", len(snap.items))
	}

	if adminKey == "" {
		slog.Warn("ADMIN_KEY 未设置，管理 API 已禁用")
	}

	return a, nil
}

// Router 构建并返回完整的 HTTP 路由器。
// 路由分组：公开端点 → WS 节点接入 → 管理 API → Gemini API 代理。
func (a *App) Router() http.Handler {
	r := chi.NewRouter()
	r.Use(a.Middleware) // 全局中间件：CORS、请求 ID、Body 限制、访问日志

	// 健康检查端点，无鉴权
	r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})

	// WebSocket 节点接入端点（WS Token 鉴权在 handler 内处理）
	r.Get("/v1/ws", a.WebSocket)

	// 管理 API 路由组（Admin Key Bearer 鉴权）
	r.Route("/admin", func(r chi.Router) {
		r.Use(a.AdminAuth)
		a.RegisterAdmin(r)
	})

	// Gemini API 路由组（API Key 鉴权）
	// 同时注册 /v1 和 /v1beta 双版本路径
	for _, prefix := range []string{"/v1", "/v1beta"} {
		r.Route(prefix, func(r chi.Router) {
			r.Use(a.APIAuth)
			r.Get("/models", a.ModelList)
			r.Get("/models/{model}", a.ModelDetail)
			r.HandleFunc("/*", a.Proxy) // 通配代理：所有其他路径转发至上游
		})
	}

	return r
}

// Reload 同时重载配置文件和模型文件。
func (a *App) Reload() error {
	configErr := a.cfg.Reload()
	modelErr := a.models.Reload()
	return errors.Join(configErr, modelErr)
}

// Close 执行应用关闭清理：标记停止状态、取消上下文、关闭所有节点连接、关闭日志文件。
func (a *App) Close() {
	a.stopping.Store(true)
	a.cancel()
	a.hub.CloseAll()
	a.logs.Close()
}

// Status 返回网关当前运行状态的完整快照，供 GET /admin/status 使用。
func (a *App) Status() map[string]any {
	c := a.cfg.Load()
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)

	nodes, channels, requests := a.hub.Summary()

	// 统计凭证启用/禁用数量
	count := func(items []Credential) map[string]int {
		n := 0
		for _, item := range items {
			if item.Enabled {
				n++
			}
		}
		return map[string]int{
			"total": len(items), "enabled": n, "disabled": len(items) - n,
		}
	}

	return map[string]any{
		"version":        version,
		"commit":         commit,
		"build_time":     buildTime,
		"uptime_seconds": int64(time.Since(a.started).Seconds()),
		"go_version":     runtime.Version(),
		"lb_strategy":    c.LB.Strategy,
		"nodes":          nodes,
		"channels":       channels,
		"requests":       requests,
		"keys":           count(c.Keys.Items),
		"tokens":         count(c.Tokens.Items),
		"memory": map[string]any{
			"alloc_mb":   float64(mem.Alloc) / 1024 / 1024,
			"sys_mb":     float64(mem.Sys) / 1024 / 1024,
			"gc_count":   mem.NumGC,
			"goroutines": runtime.NumGoroutine(),
		},
	}
}

// run 是应用的主运行函数，包含完整的启动、监听和优雅关闭流程。
func run() error {
	// 1. 创建应用实例
	a, err := NewApp(
		env("CONFIG_PATH", "./config.yaml"),
		env("MODELS_PATH", "./models.json"),
		os.Getenv("ADMIN_KEY"),
	)
	if err != nil {
		return err
	}
	defer a.Close()

	// 2. 启动文件监听（配置和模型文件热重载）
	watcher, err := a.Watch()
	if err != nil {
		return err
	}
	defer watcher.Close()

	// 3. 环境变量凭证注入
	apiKeyEnv := os.Getenv("API_KEY")
	wsTokenEnv := os.Getenv("WS_TOKEN")
	if apiKeyEnv != "" || wsTokenEnv != "" {
		a.cfg.SetEnvCredentials(apiKeyEnv, wsTokenEnv)
	}
	if apiKeyEnv != "" {
		slog.Info("环境变量 API Key 已注入", "value", maskValue(apiKeyEnv))
	}
	if wsTokenEnv != "" {
		slog.Info("环境变量 WS Token 已注入", "value", maskValue(wsTokenEnv))
	}

	// 4. 启动 Hub 后台循环（节点清理、冷却恢复、队列调度）
	go a.hub.Run()

	// 5. 配置并启动 HTTP 服务器
	server := &http.Server{
		Addr:              net.JoinHostPort(env("HOST", "0.0.0.0"), env("PORT", "8848")),
		Handler:           a.Router(),
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       90 * time.Second,
		MaxHeaderBytes:    1 << 20, // 1 MiB
		BaseContext: func(net.Listener) context.Context {
			return a.ctx
		},
	}

	errCh := make(chan error, 1)
	go func() {
		// 输出完整启动摘要
		c := a.cfg.Load()
		slog.Info("网关开始监听",
			"address", server.Addr,
			"version", version,
			"config_path", a.cfg.path,
			"models_path", a.models.path,
			"lb_strategy", c.LB.Strategy,
			"log_level", c.Log.Level,
			"keys_count", len(c.Keys.Items),
			"tokens_count", len(c.Tokens.Items),
			"admin_api", a.adminKey != "",
			"env_api_key", apiKeyEnv != "",
			"env_ws_token", wsTokenEnv != "",
		)
		errCh <- server.ListenAndServe()
	}()

	// 6. 等待终止信号（SIGINT / SIGTERM）
	sigCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	select {
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-sigCtx.Done():
	}

	// 7. 优雅关闭流程
	slog.Info("开始优雅关闭")
	a.stopping.Store(true)
	a.hub.StopScheduling() // 停止接受新的请求调度

	// 等待 in-flight 请求完成，超时后强制关闭
	ctx, cancel := context.WithTimeout(context.Background(), dur(a.cfg.Load().Server.DrainTimeout))
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		slog.Warn("排空超时，强制中断剩余请求", "error", err)
		a.cancel()
		a.hub.CloseAll()
		_ = server.Close()
	}

	a.hub.CloseAll()
	slog.Info("关闭完成")
	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "gag:", err)
		os.Exit(1)
	}
}
