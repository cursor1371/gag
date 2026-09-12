// Gemini API HTTP 处理器：本地模型列表/详情查询。
// 通配代理入口位于 proxy.go。

package main

import (
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
)

// ModelList 处理 GET /v1beta/models 和 GET /v1/models。
// 直接返回 models.json 文件的原始 JSON 内容，不做任何解析或修改。
func (a *App) ModelList(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_, _ = w.Write(a.models.value.Load().raw)
}

// ModelDetail 处理 GET /v1beta/models/{model} 和 GET /v1/models/{model}。
// 从 models.json 快照中查找指定模型并返回。
// 注意：包含冒号的路径（如 models/gemini:generateContent）不是模型查询，
// 而是 RPC 调用，需要转发到代理引擎处理。
func (a *App) ModelDetail(w http.ResponseWriter, r *http.Request) {
	model := chi.URLParam(r, "model")
	// 包含冒号的路径是 RPC 操作（如 :generateContent），转发到代理
	if strings.Contains(model, ":") {
		a.Proxy(w, r)
		return
	}
	// 在 models.json 中查找 "models/{model}"
	raw, found := a.models.value.Load().items["models/"+model]
	if !found {
		geminiError(w, http.StatusNotFound, "模型未找到")
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_, _ = w.Write(raw)
}
