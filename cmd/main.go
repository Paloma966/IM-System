package main

import (
	"bytes"
	crand "crypto/rand"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/gin-gonic/gin"

	"IMSystem/internal/chat"
	"IMSystem/internal/raft"
)

// ConnectRequest 连接（登录）请求
type ConnectRequest struct {
	Name string `json:"name"`
}

// 会话与在线状态（"本节点本地"状态：在线列表不做 Raft 复制，聊天内容才复制）。
// sessions 是 token → name 的映射；nameToken 记录每个名字当前的有效 token，
// 同名重连时旧 token 立即失效（last-connect-wins，防止旧连接继续冒充）。
var (
	users       = make(map[string]bool)
	subscribers = make(map[string]chan string)
	sessions    = make(map[string]string)
	nameToken   = make(map[string]string)
	mu          sync.RWMutex
)

// leaderHTTPClient 转发写请求到 Leader 的客户端（带超时，main 里按 TLS 配置构造）
var leaderHTTPClient *http.Client

const (
	// maxNameLen 用户名最大长度（rune）
	maxNameLen = 32
	// maxTextLen 单条消息文本最大长度（rune）
	maxTextLen = 4096
	// historyDefaultLimit 历史接口默认返回条数上限
	historyDefaultLimit = 500
	// historyMaxLimit 历史接口允许的最大条数
	historyMaxLimit = 1000
)

// requireAuth 校验请求携带的会话令牌，返回对应的用户名。
// 令牌可从 Authorization: Bearer <t>、X-Auth-Token 头或 ?token= 查询参数获取
// （EventSource 无法自定义请求头，只能走查询参数）。
func requireAuth(c *gin.Context) (string, bool) {
	token := bearerToken(c)
	if token == "" {
		c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "missing token"})
		return "", false
	}
	mu.RLock()
	name, ok := sessions[token]
	mu.RUnlock()
	if !ok || name == "" {
		c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid token"})
		return "", false
	}
	return name, true
}

// bearerToken 从请求中提取会话令牌
func bearerToken(c *gin.Context) string {
	if auth := c.GetHeader("Authorization"); auth != "" {
		if t, ok := strings.CutPrefix(auth, "Bearer "); ok {
			return strings.TrimSpace(t)
		}
	}
	if t := c.GetHeader("X-Auth-Token"); t != "" {
		return t
	}
	return c.Query("token")
}

// randomToken 生成 32 字节的随机会话令牌（hex 编码）
func randomToken() (string, error) {
	b := make([]byte, 32)
	if _, err := crand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

// main 装配 Raft 节点、聊天状态机与 HTTP 服务
func main() {
	id := flag.String("id", "node-1", "node id")
	httpAddr := flag.String("http", ":8001", "http addr for browser")
	raftAddr := flag.String("raft", ":9001", "raft rpc addr")
	peersStr := flag.String("peers", "", "comma-separated peers: id@host:raftPort:httpPort (e.g. node-2@node-2:9002:8002)")
	dataDir := flag.String("data", "", "data dir (default ./data/<id>)")
	secret := flag.String("secret", "", "shared secret for node-to-node RPC auth (required when -peers is set)")
	debug := flag.Bool("debug", false, "enable gin debug mode (default: release mode)")
	tlsCert := flag.String("tls-cert", "", "TLS certificate file (with -tls-key enables HTTPS for HTTP and Raft RPC)")
	tlsKey := flag.String("tls-key", "", "TLS private key file")
	flag.Parse()

	if !*debug {
		gin.SetMode(gin.ReleaseMode)
	}

	// TLS 证书与私钥必须成对提供
	if (*tlsCert == "") != (*tlsKey == "") {
		log.Fatal("both -tls-cert and -tls-key must be set to enable TLS")
	}

	// 解析 peers: "node-2@node-2:9002:8002,node-3@node-3:9003:8003" → []raft.Peer
	// 格式：id@host:raftPort:httpPort，host 是节点可达的主机名/IP（容器环境用服务名）
	var peers []raft.Peer
	for s := range strings.SplitSeq(*peersStr, ",") {
		if s == "" {
			continue
		}
		id, rest, ok := strings.Cut(s, "@")
		if !ok {
			log.Fatalf("bad peer %q (want id@host:raftPort:httpPort)", s)
		}
		hp := strings.Split(rest, ":")
		if len(hp) != 3 {
			log.Fatalf("bad peer %q (want id@host:raftPort:httpPort)", s)
		}
		peers = append(peers, raft.Peer{
			ID:       id,
			RaftAddr: hp[0] + ":" + hp[1],
			HTTPAddr: hp[0] + ":" + hp[2],
		})
	}

	data := *dataDir
	if data == "" {
		data = "./data/" + *id
	}

	cfg := raft.Config{
		ID:                *id,
		HTTPAddr:          *httpAddr,
		RaftAddr:          *raftAddr,
		DataDir:           data,
		Peers:             peers,
		ElectionTimeout:   300 * time.Millisecond,
		HeartbeatInterval: 50 * time.Millisecond,
	}

	// 多节点集群必须配置共享密钥，否则节点间 RPC 无法互相认证
	if len(peers) > 0 && *secret == "" {
		log.Fatal("-secret is required when -peers is set")
	}

	// 内部互访用的 scheme：启用 TLS 后 peer 间转发/扇出走 https，
	// 客户端信任 -tls-cert 指定的证书（集群共享自签名证书场景）。
	scheme := "http"
	transport := raft.NewHTTPTransport(*secret)
	if *tlsCert != "" {
		if err := transport.EnableTLS(*tlsCert); err != nil {
			log.Fatalf("enable tls: %v", err)
		}
		scheme = "https"
	}
	peerClient = newPeerHTTPClient(peerUserTimeout, *tlsCert)
	leaderHTTPClient = newPeerHTTPClient(3*time.Second, *tlsCert)

	node, err := raft.NewNode(cfg, transport)
	if err != nil {
		log.Fatal(err)
	}
	node.Start()
	defer node.Stop()

	// 状态机：消费 applyCh（已提交命令），应用后推给本节点的 SSE 订阅者
	state := chat.NewState()
	go func() {
		for cmd := range node.ApplyCh() {
			if err := state.Apply(cmd); err != nil {
				log.Println("apply:", err)
				continue
			}
			hist := state.History()
			if len(hist) > 0 {
				pushLocal(hist[len(hist)-1]) // 推最新这条给相关 SSE 连接
			}
		}
	}()

	// Raft RPC server：节点间通信端口（/raft/vote、/raft/append），带 HMAC 认证
	raftSrv := raft.NewRaftHTTPServer(node, *secret)
	go func() {
		log.Printf("raft rpc listening on %s", *raftAddr)
		var err error
		if *tlsCert != "" {
			err = raftSrv.ListenAndServeTLS(*tlsCert, *tlsKey)
		} else {
			err = raftSrv.ListenAndServe()
		}
		if err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	r := gin.Default()

	// CORS：前端由本服务同源托管，只放行同源跨域请求（不再使用通配符 *）
	r.Use(func(c *gin.Context) {
		if origin := c.GetHeader("Origin"); origin != "" {
			if u, err := url.Parse(origin); err == nil && strings.EqualFold(u.Host, c.Request.Host) {
				c.Header("Access-Control-Allow-Origin", origin)
				c.Header("Vary", "Origin")
				c.Header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
				c.Header("Access-Control-Allow-Headers", "Content-Type, Authorization")
			}
		}

		if c.Request.Method == http.MethodOptions {
			c.Status(http.StatusNoContent)
			return
		}

		c.Next()
	})

	// 请求体大小限制：所有带 body 的请求上限 1 MiB，防止超大 JSON 打爆内存
	r.Use(func(c *gin.Context) {
		c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 1<<20)
		c.Next()
	})

	// 限流器：写接口从严，读接口放宽，SSE 限制并发连接数
	writeLimiter := newIPLimiter(5, 10)
	readLimiter := newIPLimiter(30, 60)
	streamLimiter := newStreamLimiter(10)

	r.StaticFile("/app.js", "./web/app.js")
	r.StaticFile("/style.css", "./web/style.css")

	r.GET("/ping", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"message": "pong"})
	})

	r.GET("/", func(c *gin.Context) {
		c.File("./web/index.html")
	})

	// POST /api/messages 发消息：Leader 直接提交到 Raft 日志，Follower 转发给 Leader。
	// 发送者身份取自会话令牌，客户端提交的 from 字段被忽略（防冒充）。
	r.POST("/api/messages", func(c *gin.Context) {
		if !writeLimiter.allow(c.ClientIP()) {
			c.JSON(http.StatusTooManyRequests, gin.H{"error": "rate limit exceeded"})
			return
		}
		name, ok := requireAuth(c)
		if !ok {
			return
		}

		var req chat.Message
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		if utf8.RuneCountInString(req.Text) > maxTextLen {
			c.JSON(http.StatusBadRequest, gin.H{"error": "text too long"})
			return
		}

		// 服务端补全 ID 和时间戳，并强制使用会话身份作为发送者
		msg := chat.Message{
			ID:   fmt.Sprintf("%d", time.Now().UnixNano()),
			From: name,
			To:   req.To,
			Text: req.Text,
			Ts:   time.Now().Unix(),
		}

		if node.IsLeader() {
			payload, _ := json.Marshal(msg)
			cmd := raft.Command{Type: "message", Payload: payload}
			if err := node.Submit(cmd); err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
				return
			}
			c.JSON(http.StatusOK, gin.H{"ok": true})
			return
		}

		// 不是 Leader → 转发给 Leader 的 HTTP 接口。
		// 已转发过一次的请求不再转发（防多跳/环路）。
		if c.GetHeader("X-Im-Forwarded") != "" {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "leadership changed, retry later"})
			return
		}
		leader := node.LeaderHTTPAddr()
		if leader == "" {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "no leader known yet, retry later"})
			return
		}
		forwardToLeader(c, msg, leader, bearerToken(c), scheme)
	})

	// GET /api/messages/history 返回状态机历史（任何节点都一致）。
	// 只返回请求者可见的消息（群聊 + 自己参与的私聊），?limit 控制条数。
	r.GET("/api/messages/history", func(c *gin.Context) {
		if !readLimiter.allow(c.ClientIP()) {
			c.JSON(http.StatusTooManyRequests, gin.H{"error": "rate limit exceeded"})
			return
		}
		name, ok := requireAuth(c)
		if !ok {
			return
		}

		limit := historyDefaultLimit
		if v, err := strconv.Atoi(c.DefaultQuery("limit", "")); err == nil && v > 0 {
			limit = min(v, historyMaxLimit)
		}

		visible := chat.FilterVisible(state.History(), name)
		if len(visible) > limit {
			visible = visible[len(visible)-limit:]
		}
		c.JSON(http.StatusOK, gin.H{"messages": visible})
	})

	r.POST("/connect", func(c *gin.Context) {
		if !writeLimiter.allow(c.ClientIP()) {
			c.JSON(http.StatusTooManyRequests, gin.H{"error": "rate limit exceeded"})
			return
		}
		var req ConnectRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		// 名字校验：非空、长度限制、禁止控制字符
		name := strings.TrimSpace(req.Name)
		if name == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "name required"})
			return
		}
		if utf8.RuneCountInString(name) > maxNameLen {
			c.JSON(http.StatusBadRequest, gin.H{"error": "name too long"})
			return
		}
		if strings.ContainsAny(name, "\x00\r\n") {
			c.JSON(http.StatusBadRequest, gin.H{"error": "name contains invalid characters"})
			return
		}

		token, err := randomToken()
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to issue token"})
			return
		}

		mu.Lock()
		// 同名重连：旧 token 立即失效（last-connect-wins）
		if old, existed := nameToken[name]; existed {
			delete(sessions, old)
		}
		sessions[token] = name
		nameToken[name] = token
		users[name] = true
		subscribers[name] = make(chan string, 10)
		mu.Unlock()

		c.JSON(http.StatusOK, gin.H{"ok": true, "token": token})
	})

	r.GET("/users", func(c *gin.Context) {
		// 内部扇出专用：只返回本节点在线用户，避免 /users 递归调用自己。
		// 仅集群内节点（持有共享密钥）可调用。
		if c.Query("local") == "1" {
			if *secret == "" || c.GetHeader("X-Node-Secret") != *secret {
				c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
				return
			}
			c.JSON(http.StatusOK, gin.H{"users": localUsers()})
			return
		}
		if !readLimiter.allow(c.ClientIP()) {
			c.JSON(http.StatusTooManyRequests, gin.H{"error": "rate limit exceeded"})
			return
		}
		if _, ok := requireAuth(c); !ok {
			return
		}
		c.JSON(http.StatusOK, gin.H{"users": globalUsers(peers, *secret, scheme)})
	})

	r.GET("/stream/:name", func(c *gin.Context) {
		name := c.Param("name")

		// 订阅者必须持有该名字的会话令牌（EventSource 走 ?token=）
		token := c.Query("token")
		mu.RLock()
		owner, ok := sessions[token]
		mu.RUnlock()
		if token == "" || !ok || owner != name {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
			return
		}

		// 并发连接数与请求频率限制，防止连接耗尽
		ip := c.ClientIP()
		if !readLimiter.allow(ip) || !streamLimiter.acquire(ip) {
			c.JSON(http.StatusTooManyRequests, gin.H{"error": "too many streams"})
			return
		}
		defer streamLimiter.release(ip)

		c.Header("Content-Type", "text/event-stream")
		c.Header("Cache-Control", "no-cache")

		// SSE 是长连接：清除服务器 WriteTimeout 对本连接的写截止时间
		if rc := http.NewResponseController(c.Writer); rc != nil {
			_ = rc.SetWriteDeadline(time.Time{})
		}

		mu.RLock()
		ch, ok := subscribers[name]
		mu.RUnlock()

		if !ok {
			c.JSON(http.StatusNotFound, gin.H{"error": "user not connected"})
			return
		}

		for {
			select {
			case msg := <-ch:
				c.SSEvent("message", msg)
				c.Writer.Flush()
			case <-c.Request.Context().Done():
				// 只有当自己仍是当前 channel 时才清理：页面刷新会触发
				// /connect 用新 channel 覆盖 subscribers[name]，旧连接的清理
				// 若不加判断会把新连接和用户一并删掉（重连竞态）。
				mu.Lock()
				if subscribers[name] == ch {
					delete(subscribers, name)
					delete(users, name)
				}
				mu.Unlock()
				return
			}
		}
	})

	// 显式 http.Server：设置超时防 Slowloris；SSE 在 handler 内清除写截止
	srv := &http.Server{
		Addr:              *httpAddr,
		Handler:           r,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       90 * time.Second,
	}
	log.Printf("http listening on %s", *httpAddr)
	if *tlsCert != "" {
		if err := srv.ListenAndServeTLS(*tlsCert, *tlsKey); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	} else {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}
}

// pushLocal 把一条已提交的消息推给本节点上相关的 SSE 订阅者。
// 数据来源是状态机（不是请求），所以任意节点被浏览器连上都行。
func pushLocal(msg chat.Message) {
	data, _ := json.Marshal(msg)

	mu.RLock()
	defer mu.RUnlock()

	// 群聊：推给所有在线用户
	if msg.To == "" || msg.To == "all" {
		for _, ch := range subscribers {
			select {
			case ch <- string(data):
			default:
			}
		}
		return
	}

	// 私聊：推给发送方和接收方
	for _, name := range []string{msg.From, msg.To} {
		if ch, ok := subscribers[name]; ok {
			select {
			case ch <- string(data):
			default:
			}
		}
	}
}

// forwardToLeader 把消息转发给 Leader 的 HTTP 接口（Follower 上的写请求）。
// 带上发送者的会话令牌，Leader 端重新校验身份；带超时，防止 Leader
// 无响应时请求悬挂耗尽资源；X-Im-Forwarded 标记防多跳转发。
func forwardToLeader(c *gin.Context, msg chat.Message, leader, token, scheme string) {
	data, _ := json.Marshal(msg)
	req, err := http.NewRequest(
		http.MethodPost,
		scheme+"://"+leader+"/api/messages",
		bytes.NewReader(data),
	)
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": "failed to build forward request: " + err.Error()})
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Im-Forwarded", "1")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	resp, err := leaderHTTPClient.Do(req)
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": "failed to reach leader: " + err.Error()})
		return
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	c.Data(resp.StatusCode, "application/json", body)
}
