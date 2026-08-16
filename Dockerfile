# syntax=docker/dockerfile:1

# ============================================================
# 阶段 1：构建后端（前端是原生 JS，无需构建步骤）
# ============================================================
FROM golang:1.26-alpine AS go-builder
WORKDIR /src

# 先拷依赖清单，命中缓存
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -o /out/server ./cmd

# ============================================================
# 阶段 2：运行时（二进制 + 前端源码，镜像很小）
# ============================================================
FROM alpine:3.20
WORKDIR /app

# 非 root 用户运行
RUN addgroup -S app && adduser -S -G app app

# 二进制放到 /app/server；前端源码放到 /app/web（和 main.go 里的相对路径一致）
COPY --from=go-builder /out/server /app/server
COPY web/ /app/web/

# 数据目录交给 app 用户：named volume 首次挂载时会继承镜像内的目录所有权
RUN mkdir -p /data && chown -R app:app /data /app

USER app

EXPOSE 8001 9001

# busybox wget 探测 HTTP 端口；各节点 HTTP_PORT 不同，由 compose 注入环境变量
HEALTHCHECK --interval=5s --timeout=3s --start-period=10s --retries=3 \
  CMD wget -q -O- "http://127.0.0.1:${HTTP_PORT:-8001}/ping" >/dev/null 2>&1 || exit 1

ENTRYPOINT ["/app/server"]
