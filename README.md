# GAG — Gemini API Gateway

Go 实现的 Gemini API 反向代理网关，通过 WebSocket 连接后端Google AIStudio节点，对外提供标准 HTTP API。

## 特性

- **WebSocket 多路复用**：单节点连接承载多个并发请求
- **负载均衡**：最少连接优先轮询（round_robin）和填充优先（fill_first）两种策略
- **自动重试**：失败请求自动切换节点，已提交流式输出除外
- **智能冷却**：Node + Model 复合维度的指数退避，模型级故障隔离
- **热重载**：配置文件和模型列表变更自动生效，无需重启
- **凭证管理**：API Key 和 WS Token 支持环境变量注入与管理 API 动态增删
- **可观测性**：双流日志、时间窗口统计、请求历史查询

## 快速开始

### Docker（推荐）

```bash
docker run -d -p 8848:8848 \
  -e ADMIN_KEY='your-admin-secret' \
  -e API_KEY='your-api-key' \
  -e WS_TOKEN='your-ws-token' \
  -v gag-data:/data \
  ghcr.io/your-org/gag:latest
```

### 从源码构建

```bash
go build -o gag .
export ADMIN_KEY='your-admin-secret'
export API_KEY='your-api-key'
export WS_TOKEN='your-ws-token'
./gag
```

### 验证

节点接入：

```
ws://localhost:8848/v1/ws?auth_token=your-ws-token
```

API 请求：

```bash
curl -H "x-goog-api-key: your-api-key" \
     http://localhost:8848/v1beta/models/gemini-2.5-flash:generateContent \
     -d '{"contents":[{"parts":[{"text":"你好"}]}]}'
```

## 环境变量

| 变量 | 默认值 | 说明 |
|---|---|---|
| `ADMIN_KEY` | — | 管理 API 密钥，为空则禁用管理 API |
| `API_KEY` | — | 快捷 API Key，注入为 ID=0 的默认凭证 |
| `WS_TOKEN` | — | 快捷 WS Token，注入为 ID=0 的默认凭证 |
| `HOST` | `0.0.0.0` | 监听地址 |
| `PORT` | `8848` | 监听端口 |
| `CONFIG_PATH` | `./config.yaml` | 配置文件路径 |
| `MODELS_PATH` | `./models.json` | 模型列表文件路径 |

`API_KEY` 和 `WS_TOKEN` 提供零配置快速启动能力，注入为 ID=0 的特殊凭证，与配置文件中的凭证（ID≥1）共存。不写入配置文件，不随热重载变化。通过管理 API 可查看（标记 `source: "env"`）、启用/禁用，但不可删除或修改。

## Docker

### 构建

```bash
docker build -t gag:latest \
  --build-arg VERSION=$(git describe --tags --always) \
  --build-arg COMMIT=$(git rev-parse HEAD) \
  --build-arg BUILD_TIME=$(date -u +%Y-%m-%dT%H:%M:%SZ) \
  .
```

### 运行

```bash
docker run -d -p 8848:8848 \
  -e ADMIN_KEY='your-admin-secret' \
  -e API_KEY='your-api-key' \
  -e WS_TOKEN='your-ws-token' \
  -v gag-data:/data \
  gag:latest
```

数据目录 `/data` 包含配置文件、模型列表和日志，建议挂载持久卷。

## 配置

首次启动时自动创建 `config.yaml`，包含所有参数的默认值和完整注释。详见 [config.example.yaml](config.example.yaml)。

配置分为两层：

- **环境变量**：私密信息和启动参数，不可热重载
- **YAML 文件**：所有运行时参数，支持热重载（fsnotify 监听 + 500ms 防抖）

也可通过管理 API 在线修改：`GET/PUT /admin/config/yaml`、`POST /admin/config/reload`。

## 管理 API

所有管理端点需 `Authorization: Bearer <ADMIN_KEY>` 鉴权。

| 分类 | 端点 | 说明 |
|---|---|---|
| 凭证 | `/admin/keys`、`/admin/tokens` | CRUD + enable/disable |
| 节点 | `/admin/nodes` | 查询 + enable/disable/cooldown |
| 渠道 | `/admin/channels` | 查询 + enable/disable |
| 系统 | `/admin/status` | 运行状态快照 |
| 配置 | `/admin/config`、`/admin/config/yaml` | 查看/修改/重载 |
| 日志 | `/admin/logs` | 多条件过滤 + 游标分页 |
| 统计 | `/admin/stats`、`/admin/requests` | 多维度聚合 + 请求历史 |

### 示例

```bash
# 创建 API Key
curl -X POST -H "Authorization: Bearer $ADMIN_KEY" \
  http://localhost:8848/admin/keys -d '{"note":"LobeChat"}'

# 创建 WS Token
curl -X POST -H "Authorization: Bearer $ADMIN_KEY" \
  http://localhost:8848/admin/tokens -d '{"note":"AIStudio 节点组"}'

# 查看网关状态
curl -H "Authorization: Bearer $ADMIN_KEY" \
  http://localhost:8848/admin/status

```

## 架构概览

```
客户端 ──HTTP──▶ GAG ◀──WebSocket──▶ 节点 (AIStudio)
                 │                      │
                 ├─ API Key 鉴权        ├─ WS Token 鉴权
                 ├─ 负载均衡            ├─ 多路复用
                 ├─ 自动重试            ├─ 协议级心跳
                 ├─ 模型级冷却          └─ 应用级心跳
                 └─ 请求体规范化
```

## License

[MIT](./LICENSE)