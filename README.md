# IM System

基于 Raft 共识协议的分布式即时通讯系统。后端 Go + Gin，前端原生 JS（无构建步骤），一致性由 `internal/raft` 从零实现。

## 目录

- `cmd/main.go` — 入口：HTTP 服务 + Raft 节点装配
- `internal/raft` — Raft 共识库（选举 / 日志复制 / 持久化）
- `internal/chat` — 聊天状态机（消费已提交命令）
- `web` — 原生 JS 前端（`index.html` + `app.js` + `style.css`，由后端直接托管）

## 本地运行

### 单节点

```bash
go run ./cmd -id node-1 -http :8001 -raft :9001
```

前端是纯静态文件，后端启动后直接托管，无需 `npm install` / 构建。

浏览器打开 http://localhost:8001

### 多节点（本机，三个终端）

```bash
go run ./cmd -id node-1 -http :8001 -raft :9001 -peers "node-2@localhost:9002:8002,node-3@localhost:9003:8003"
go run ./cmd -id node-2 -http :8002 -raft :9002 -peers "node-1@localhost:9001:8001,node-3@localhost:9003:8003"
go run ./cmd -id node-3 -http :8003 -raft :9003 -peers "node-1@localhost:9001:8001,node-2@localhost:9002:8002"
```

### peer 地址格式

`id@host:raftPort:httpPort`，多个用逗号分隔。`host` 是节点可达的主机名 / IP：本机用 `localhost`，容器环境用服务名（如 `node-2`）。

## Docker 运行

前置：安装 Docker Desktop（Mac/Windows）或 Docker Engine + Compose 插件（Linux）。

```bash
docker compose up --build
```

- 浏览器打开 http://localhost:8001（映射到 node-1）。三个节点 HTTP 端口分别映射到本机 8001 / 8002 / 8003。
- 任意节点都能收发消息：写请求会自动转发给 Leader，历史（`/api/messages/history`）在所有节点一致。

**故障转移演示**：`docker ps` 找到当前 leader（或看日志），`docker stop <leader容器>`，集群会在几百毫秒内选出新 leader，浏览器仍可正常收发。

**重置**（清空数据重新选举）：`docker compose down -v`

## 已知取舍

- 在线用户列表不通过 Raft 复制，而是 `GET /users` 时实时**扇出聚合**各节点（内部用 `?local=1` 取单节点）。所以聊天内容强一致，在线列表是「读时一致」。任一节点挂掉时，其在线用户会暂时从全局列表消失（优雅降级）。
