// 鉴权模块：实现三层鉴权体系。
//
//   - Admin Key：管理 API 鉴权（来自环境变量 ADMIN_KEY）
//   - API Key：外部客户端 Gemini API 鉴权（YAML 持久化，管理 API 增删）
//   - WS Token：代理节点 WebSocket 接入鉴权（YAML 持久化，管理 API 增删）

package main

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"net/http"
	"strings"
)

// randomHex 生成 n 字节的十六进制随机字符串（输出长度 = 2n）。
func randomHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic(fmt.Errorf("crypto/rand: %w", err))
	}
	return hex.EncodeToString(b)
}

// randomBase62 生成指定长度的 base62 随机字符串。
// 使用拒绝采样避免取模偏差（modulo bias）。
func randomBase62(n int) string {
	const alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	result := make([]byte, 0, n)
	var buffer [64]byte
	for len(result) < n {
		if _, err := rand.Read(buffer[:]); err != nil {
			panic(fmt.Errorf("crypto/rand: %w", err))
		}
		for _, b := range buffer {
			if b >= 248 { // 拒绝 248-255 以消除偏差（248 = 62*4）
				continue
			}
			result = append(result, alphabet[int(b)%62])
			if len(result) == n {
				break
			}
		}
	}
	return string(result)
}

// maskValue 将凭证值转为密文显示，用于日志输出。
// 规则：长度≤4 全部替换为 *；否则保留首2尾2字符，中间用 * 填充。
func maskValue(s string) string {
	if len(s) <= 4 {
		return strings.Repeat("*", len(s))
	}
	return s[:2] + strings.Repeat("*", len(s)-4) + s[len(s)-2:]
}

// bearer 从 Authorization 请求头中提取 Bearer Token 值。
func bearer(r *http.Request) string {
	fields := strings.Fields(r.Header.Get("Authorization"))
	if len(fields) == 2 && strings.EqualFold(fields[0], "Bearer") {
		return fields[1]
	}
	return ""
}

// AdminAuth 是管理 API 的鉴权中间件。
// 验证请求的 Authorization: Bearer <admin_key> 头部。
// ADMIN_KEY 未设置时，所有管理 API 返回 403。
func (a *App) AdminAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if a.adminKey == "" {
			adminError(w, http.StatusForbidden, "ADMIN_DISABLED", "ADMIN_KEY 未配置")
			return
		}
		key := bearer(r)
		// 使用常量时间比较防止时序攻击
		if subtle.ConstantTimeCompare([]byte(key), []byte(a.adminKey)) != 1 {
			adminError(w, http.StatusUnauthorized, "UNAUTHORIZED", "管理密钥无效")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// APIAuth 是 Gemini API 端点的鉴权中间件。
// 按优先级从以下位置提取 API Key：
//  1. Header: x-goog-api-key
//  2. Header: Authorization: Bearer
//  3. Query: ?key=
//
// 空 Key 列表时拒绝所有请求（返回 401），迫使管理员先创建 Key。
func (a *App) APIAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 三级回退提取 API Key
		key := r.Header.Get("x-goog-api-key")
		if key == "" {
			key = bearer(r)
		}
		if key == "" {
			key = r.URL.Query().Get("key")
		}

		item, found := a.cfg.Load().keyByValue[key]
		if !found || key == "" {
			geminiError(w, http.StatusUnauthorized, "API Key 无效或缺失")
			return
		}
		if !item.Enabled {
			geminiError(w, http.StatusForbidden, "API Key 已禁用")
			return
		}
		// 将 Key ID 和认证状态记录到请求记录中，供统计使用
		if record := requestRecord(r); record != nil {
			record.KeyID = item.ID
			record.Authenticated = true
		}
		next.ServeHTTP(w, r)
	})
}
