package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"

	"IMSystem/internal/chat"
	"IMSystem/internal/raft"
)

// ConnectRequest 连接（登录）请求
type ConnectRequest struct {
	Name string `json:"name"`
}

// users 在线用户集合；subscribers 是每个用户的 SSE 推送通道。
// 这两个是"本节点本地"的状态（在线列表不做 Raft 复制，聊天内容才复制）。
var (
	users       = make(map[string]bool)
	subscribers = make(map[string]chan string)
	mu          sync.RWMutex
)

func main() {
	id := flag.String("id", "node-1", "node id")
	httpAddr := flag.String("http", ":8001", "http addr for browser")
	raftAddr := flag.String("raft", ":9001", "raft rpc addr")
	peersStr := flag.String("peers", "", "comma-separated peers: id@host:raftPort:httpPort (e.g. node-2@node-2:9002:8002)")
	dataDir := flag.String("data", "", "data dir (default ./data/<id>)")
	flag.Parse()

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

	node := raft.NewNode(cfg, raft.NewHTTPTransport())
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

	// Raft RPC server：节点间通信端口（/raft/vote、/raft/append）
	raftSrv := raft.NewRaftHTTPServer(node)
	go func() {
		log.Printf("raft rpc listening on %s", *raftAddr)
		log.Fatal(raftSrv.ListenAndServe())
	}()

	r := gin.Default()

	r.Use(func(c *gin.Context) {
		c.Header("Access-Control-Allow-Origin", "*")
		c.Header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		c.Header("Access-Control-Allow-Headers", "Content-Type")

		if c.Request.Method == http.MethodOptions {
			c.Status(http.StatusNoContent)
			return
		}

		c.Next()
	})

	r.StaticFile("/app.js", "./web/app.js")
	r.StaticFile("/style.css", "./web/style.css")

	r.GET("/ping", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"message": "pong"})
	})

	r.GET("/", func(c *gin.Context) {
		c.File("./web/index.html")
	})

	// POST /api/messages 发消息：Leader 直接提交到 Raft 日志，Follower 转发给 Leader
	r.POST("/api/messages", func(c *gin.Context) {
		var req chat.Message
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		// 服务端补全 ID 和时间戳（单点生成，避免客户端伪造重复 ID）
		msg := chat.Message{
			ID:   fmt.Sprintf("%d", time.Now().UnixNano()),
			From: req.From,
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

		// 不是 Leader → 转发给 Leader 的 HTTP 接口
		leader := node.LeaderHTTPAddr()
		if leader == "" {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "no leader known yet, retry later"})
			return
		}
		forwardToLeader(c, req, leader)
	})

	// GET /api/messages/history 返回状态机历史（任何节点都一致）
	r.GET("/api/messages/history", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"messages": state.History()})
	})

	r.POST("/connect", func(c *gin.Context) {
		var req ConnectRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		mu.Lock()
		defer mu.Unlock()
		users[req.Name] = true
		subscribers[req.Name] = make(chan string, 10)

		c.JSON(http.StatusOK, gin.H{"ok": true})
	})

	r.GET("/users", func(c *gin.Context) {
		// 内部扇出专用：只返回本节点在线用户，避免 /users 递归调用自己
		if c.Query("local") == "1" {
			c.JSON(http.StatusOK, gin.H{"users": localUsers()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"users": globalUsers(peers)})
	})

	r.GET("/stream/:name", func(c *gin.Context) {
		name := c.Param("name")

		c.Header("Content-Type", "text/event-stream")
		c.Header("Cache-Control", "no-cache")

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

	log.Printf("http listening on %s", *httpAddr)
	if err := r.Run(*httpAddr); err != nil {
		log.Fatal(err)
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

// forwardToLeader 把消息转发给 Leader 的 HTTP 接口（Follower 上的写请求）
func forwardToLeader(c *gin.Context, msg chat.Message, leader string) {
	data, _ := json.Marshal(msg)
	resp, err := http.Post(
		"http://"+leader+"/api/messages",
		"application/json",
		bytes.NewReader(data),
	)
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": "failed to reach leader: " + err.Error()})
		return
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	c.Data(resp.StatusCode, "application/json", body)
}
