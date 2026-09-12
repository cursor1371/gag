# ===========================================================================
# GAG (Gemini API Gateway) — Multi-stage Docker Build
# ===========================================================================

# ---- Build Stage ----
FROM golang:1.23-alpine AS builder

ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_TIME=unknown

WORKDIR /src

# 依赖层（go.mod/go.sum 变更时才重建，利用 Docker 层缓存）
COPY go.mod go.sum ./
RUN go mod download

# 编译层
COPY *.go ./
RUN CGO_ENABLED=0 GOFLAGS="-trimpath" go build \
    -ldflags="-s -w \
      -X main.version=${VERSION} \
      -X main.commit=${COMMIT} \
      -X 'main.buildTime=${BUILD_TIME}'" \
    -o /gag .

# ---- Runtime Stage ----
FROM alpine:3.21

# ca-certificates: 为可能的 TLS 上游连接预留
# tzdata:          支持 TZ 环境变量时区配置
RUN apk add --no-cache ca-certificates tzdata \
    && addgroup -S gag && adduser -S -G gag -H -h /data gag \
    && mkdir -p /data/logs \
    && chown -R gag:gag /data

COPY --from=builder /gag /usr/local/bin/gag
COPY models.json /data/models.json

# 运行时默认环境变量
ENV HOST=0.0.0.0 \
    PORT=8848 \
    CONFIG_PATH=/data/config.yaml \
    MODELS_PATH=/data/models.json

EXPOSE 8848

WORKDIR /data
USER gag

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD wget -qO /dev/null http://localhost:${PORT}/health

ENTRYPOINT ["gag"]
