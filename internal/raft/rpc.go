package raft

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

// RequestVoteRequest 用于候选人发起投票请求.
type RequestVoteRequest struct {
	Term uint64 `json:"term"`

	CandidateID string `json:"candidate_id"`

	LastLogIndex uint64 `json:"last_log_index"`

	LastLogTerm uint64 `json:"last_log_term"`
}

// RequestVoteResponse 是投票响应.
type RequestVoteResponse struct {
	Term uint64 `json:"term"`

	VoteGranted bool `json:"vote_granted"`
}

// AppendEntriesRequest 是 Leader 日志复制请求.
type AppendEntriesRequest struct {
	Term uint64 `json:"term"`

	LeaderID string `json:"leader_id"`

	PrevLogIndex uint64 `json:"prev_log_index"`

	PrevLogTerm uint64 `json:"prev_log_term"`

	Entries []LogEntry `json:"entries"`

	LeaderCommit uint64 `json:"leader_commit"`
}

// AppendEntriesResponse 是日志复制响应.
type AppendEntriesResponse struct {
	Term uint64 `json:"term"`

	Success bool `json:"success"`
}

// Transport 抽象 Raft 节点间通信.
// 测试用 memoryTransport（不经过网络），生产用 HTTPTransport（HTTP+JSON）。
type Transport interface {
	RequestVote(
		peer Peer,
		req RequestVoteRequest,
	) RequestVoteResponse

	AppendEntries(
		peer Peer,
		req AppendEntriesRequest,
	) AppendEntriesResponse
}

// RPC 认证：所有节点间请求必须携带共享密钥的 HMAC-SHA256 签名
// 与时间戳，防止外部伪造 RequestVote/AppendEntries 破坏集群
// （任期膨胀 DoS、伪造日志注入、leaderID 投毒等）。
const (
	rpcSigHeader    = "X-Raft-Sig"
	rpcTsHeader     = "X-Raft-Ts"
	rpcMaxBody      = 1 << 20 // 1 MiB，防止超大请求体
	rpcMaxClockSkew = 60 * time.Second
)

// ============================================================
// HTTPTransport — 生产环境的 Transport 实现（HTTP + JSON + HMAC）
// ============================================================

// HTTPTransport 通过 HTTP+JSON 与其他节点通信
type HTTPTransport struct {
	client *http.Client
	secret string
	scheme string
}

// NewHTTPTransport 创建一个带超时与共享密钥的 HTTP transport。
// secret 为空时请求不带签名，只能用于测试。
func NewHTTPTransport(secret string) *HTTPTransport {
	return &HTTPTransport{
		client: &http.Client{Timeout: time.Second},
		secret: secret,
		scheme: "http",
	}
}

// EnableTLS 让 transport 改用 https，并信任 certFile 中的证书作为根证书
// （适用于集群共享自签名证书的场景）。不启用时默认 http。
func (t *HTTPTransport) EnableTLS(certFile string) error {
	pool := x509.NewCertPool()
	pem, err := os.ReadFile(certFile)
	if err != nil {
		return err
	}
	if !pool.AppendCertsFromPEM(pem) {
		return fmt.Errorf("no certificates found in %s", certFile)
	}
	t.scheme = "https"
	t.client.Transport = &http.Transport{
		TLSClientConfig: &tls.Config{
			RootCAs:    pool,
			MinVersion: tls.VersionTLS12,
		},
	}
	return nil
}

// RequestVote 实现 Transport：向 peer 发投票请求
func (t *HTTPTransport) RequestVote(peer Peer, req RequestVoteRequest) RequestVoteResponse {
	var resp RequestVoteResponse
	t.doRPC(peer.RaftAddr, "/raft/vote", req, &resp)
	return resp
}

// AppendEntries 实现 Transport：向 peer 发日志复制请求
func (t *HTTPTransport) AppendEntries(peer Peer, req AppendEntriesRequest) AppendEntriesResponse {
	var resp AppendEntriesResponse
	t.doRPC(peer.RaftAddr, "/raft/append", req, &resp)
	return resp
}

// doRPC 发送一个 RPC 并解析响应；目标不可达时返回零值响应
func (t *HTTPTransport) doRPC(raftAddr, path string, req, resp any) {
	data, err := json.Marshal(req)
	if err != nil {
		return
	}

	httpReq, err := http.NewRequest(
		http.MethodPost,
		t.scheme+"://"+raftAddr+path,
		bytes.NewReader(data),
	)
	if err != nil {
		return
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if t.secret != "" {
		httpReq.Header.Set(rpcTsHeader, strconv.FormatInt(time.Now().Unix(), 10))
		httpReq.Header.Set(rpcSigHeader, signRPC(t.secret, path, data))
	}

	httpResp, err := t.client.Do(httpReq)
	if err != nil {
		return // 目标不可达：零值响应（Term 0, granted false）
	}
	defer httpResp.Body.Close()

	body, _ := io.ReadAll(httpResp.Body)
	_ = json.Unmarshal(body, resp)
}

// signRPC 计算 path+body 的 HMAC-SHA256 签名（hex 编码）。
// 把 path 纳入签名，防止把某一路径的合法请求重放到另一路径。
func signRPC(secret, path string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(path))
	mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}

// ============================================================
// Raft RPC 服务器 — 对外入口，先认证后处理
// ============================================================

// NewRaftHTTPServer 创建 Raft RPC 入口（挂在 RaftAddr 端口）。
// secret 为空时拒绝一切请求（单节点不需要 RPC）。
func NewRaftHTTPServer(n *Node, secret string) *http.Server {
	mux := http.NewServeMux()

	mux.HandleFunc("/raft/vote", func(w http.ResponseWriter, r *http.Request) {
		body, ok := authorizeRPC(w, r, secret)
		if !ok {
			return
		}
		var req RequestVoteRequest
		if err := json.Unmarshal(body, &req); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		// 身份校验：候选人必须是已知集群成员，且不能是本节点 ID。
		// 不满足时直接拒绝，不触碰节点状态（防伪造 ID 污染任期/投票）。
		if req.CandidateID == n.cfg.ID || !n.cfg.HasPeer(req.CandidateID) {
			writeRPCJSON(w, RequestVoteResponse{Term: n.CurrentTerm(), VoteGranted: false})
			return
		}
		writeRPCJSON(w, n.handleRequestVote(req))
	})

	mux.HandleFunc("/raft/append", func(w http.ResponseWriter, r *http.Request) {
		body, ok := authorizeRPC(w, r, secret)
		if !ok {
			return
		}
		var req AppendEntriesRequest
		if err := json.Unmarshal(body, &req); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		// leaderID 必须是已知集群成员且不能是本节点：防止伪造 leaderID
		// 让本节点把写请求转发给任意地址（投毒/自转发死循环）。
		if req.LeaderID == n.cfg.ID || !n.cfg.HasPeer(req.LeaderID) {
			writeRPCJSON(w, AppendEntriesResponse{Term: n.CurrentTerm(), Success: false})
			return
		}
		writeRPCJSON(w, n.handleAppendEntries(req))
	})

	return &http.Server{
		Addr:    n.cfg.RaftAddr,
		Handler: mux,
		// 超时防护：RPC 都是短请求，慢连接（Slowloris）直接断开
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
}

// authorizeRPC 校验请求方法、签名与时间戳，返回原始请求体。
func authorizeRPC(w http.ResponseWriter, r *http.Request, secret string) ([]byte, bool) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return nil, false
	}
	if secret == "" {
		http.Error(w, "raft rpc disabled: no cluster secret", http.StatusServiceUnavailable)
		return nil, false
	}

	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, rpcMaxBody))
	if err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return nil, false
	}

	ts, err := strconv.ParseInt(r.Header.Get(rpcTsHeader), 10, 64)
	if err != nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return nil, false
	}
	skew := time.Since(time.Unix(ts, 0))
	if skew > rpcMaxClockSkew || skew < -rpcMaxClockSkew {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return nil, false
	}

	want := signRPC(secret, r.URL.Path, body)
	if !hmac.Equal([]byte(want), []byte(r.Header.Get(rpcSigHeader))) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return nil, false
	}
	return body, true
}

func writeRPCJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
