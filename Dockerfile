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

# 二进制放到 /app/server；前端源码放到 /app/web（和 main.go 里的相对路径一致）
COPY --from=go-builder /out/server /app/server
COPY web/ /app/web/

EXPOSE 8001 9001

ENTRYPOINT ["/app/server"]
