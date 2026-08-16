# IM System

基于 Raft 共识协议的分布式即时通讯系统。后端 Go + Gin，前端原生 JS（无构建步骤），一致性由 `internal/raft` 从零实现。

## 目录

- `cmd/main.go` — 入口：HTTP 服务 + Raft 节点装配
- `internal/raft` — Raft 共识库（选举 / 日志复制 / 持久化）
- `internal/chat` — 聊天状态机（消费已提交命令）
- `web` — 原生 JS 前端（`index.html` + `app.js` + `style.css`，由后端直接托管）

## 认证模型

系统无账号密码，采用「名字 + 会话令牌」：

1. `POST /connect` 提交名字，服务端校验后返回一个随机会话令牌；
2. 之后所有 API 请求携带 `Authorization: Bearer <token>`（SSE 用 `?token=`）；
3. 发送者身份由令牌决定，客户端提交的 `from` 字段会被忽略（防冒充）；
4. 同名重连会使旧令牌立即失效（last-connect-wins）。

注意：这是演示级的身份模型——知道对方名字的人仍可冒充其登录。生产环境应接入真实账号体系（密码/OAuth）。

## 安全加固

- **节点间 RPC 认证**：`/raft/vote`、`/raft/append` 需要共享密钥的 HMAC-SHA256 签名（`X-Raft-Sig`）+ 时间戳（`X-Raft-Ts`，±60s 防重放），并校验对端必须是集群成员。
- **接口防护**：请求体 ≤ 1 MiB、按 IP 令牌桶限流（写 5 rps / 读 30 rps）、SSE 每 IP 最多 10 条并发连接、HTTP 服务器完整超时（防 Slowloris）。
- **持久化**：meta/log 原子写 + fsync；日志按 JSONL 追加（O(1)），损坏时快速失败而不是静默清空。
- **可选 TLS**：`-tls-cert` / `-tls-key` 同时启用后，HTTP 与 Raft RPC 端口都走 HTTPS（节点间客户端信任该证书，适配自签名）。未提供证书时走明文 HTTP（局域网演示）。

## 本地运行

### 单节点

```bash
go run ./cmd -id node-1 -http :8001 -raft :9001
```

前端是纯静态文件，后端启动后直接托管，无需 `npm install` / 构建。

浏览器打开 http://localhost:8001

### 多节点（本机，三个终端）

多节点必须配置共享密钥 `-secret`（所有节点一致）：

```bash
go run ./cmd -id node-1 -http :8001 -raft :9001 -secret s3cret \
  -peers "node-2@localhost:9002:8002,node-3@localhost:9003:8003"
go run ./cmd -id node-2 -http :8002 -raft :9002 -secret s3cret \
  -peers "node-1@localhost:9001:8001,node-3@localhost:9003:8003"
go run ./cmd -id node-3 -http :8003 -raft :9003 -secret s3cret \
  -peers "node-1@localhost:9001:8001,node-2@localhost:9002:8002"
```

### 启动参数

| 参数 | 默认 | 说明 |
| --- | --- | --- |
| `-id` | `node-1` | 节点 ID |
| `-http` | `:8001` | 浏览器 HTTP 地址 |
| `-raft` | `:9001` | 节点间 RPC 地址 |
| `-peers` | 空 | 逗号分隔的 peer 列表（多节点必填） |
| `-secret` | 空 | 节点间 RPC 共享密钥（`-peers` 非空时必填） |
| `-data` | `./data/<id>` | 数据目录 |
| `-tls-cert` / `-tls-key` | 空 | 同时提供则启用 HTTPS（HTTP + Raft RPC） |
| `-debug` | false | 打开 gin debug 模式（默认 release） |

### peer 地址格式

`id@host:raftPort:httpPort`，多个用逗号分隔。`host` 是节点可达的主机名 / IP：本机用 `localhost`，容器环境用服务名（如 `node-2`）。

## Docker 运行

前置：安装 Docker Desktop（Mac/Windows）或 Docker Engine + Compose 插件（Linux）。

```bash
# 生产/共享网络部署请务必换成随机密钥：
export RAFT_SECRET="$(openssl rand -hex 32)"
docker compose up --build
```

- 浏览器打开 http://localhost:8001（映射到 node-1）。三个节点 HTTP 端口分别映射到本机 8001 / 8002 / 8003。
- 任意节点都能收发消息：写请求会自动转发给 Leader，历史（`/api/messages/history`）在所有节点一致。
- 容器以非 root 用户运行，带健康检查（探测 `/ping`）与 `restart: unless-stopped`。
- 若未设置 `RAFT_SECRET`，compose 使用 `change-me-dev-secret`（仅限本机演示）。

**故障转移演示**：`docker ps` 找到当前 leader（或看日志），`docker stop <leader容器>`，集群会在几百毫秒内选出新 leader，浏览器仍可正常收发。

**重置**（清空数据重新选举）：`docker compose down -v`。注意：从旧版（root 运行）升级时也必须 `down -v` 重建数据卷，否则非 root 用户对旧卷无写权限。

## 接口速览

| 方法 | 路径 | 认证 | 说明 |
| --- | --- | --- | --- |
| POST | `/connect` | 无 | 登录，返回 `{token}` |
| POST | `/api/messages` | Bearer | 发消息（等待提交确认后返回） |
| GET | `/api/messages/history` | Bearer | 历史（只含自己可见的消息，`?limit=` 默认 500） |
| GET | `/stream/:name` | `?token=` | SSE 实时推送 |
| GET | `/users` | Bearer | 全局在线用户（各节点扇出聚合） |

## 已知取舍

- 在线用户列表不通过 Raft 复制，而是 `GET /users` 时实时**扇出聚合**各节点（内部用 `?local=1` 取单节点，需集群密钥）。所以聊天内容强一致，在线列表是「读时一致」。任一节点挂掉时，其在线用户会暂时从全局列表消失（优雅降级）。
- 同名用户在各节点的身份不全局唯一：两个节点上可以各有一个「alice」。私聊按名字路由，在线列表去重后显示为一个。需要全局唯一身份请接入账号体系。
- 消息历史暂无分页游标（只有 `limit`）；Raft 日志无快照/压缩，长时间运行会持续增长。
- 未配置 TLS 时全部流量为明文 HTTP，仅适合可信局域网；公网部署请使用 `-tls-cert` / `-tls-key` 或前置反向代理。
