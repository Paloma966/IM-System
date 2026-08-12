package raft

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
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

// ============================================================
// HTTPTransport — 生产环境的 Transport 实现（HTTP + JSON）
// ============================================================

// HTTPTransport 通过 HTTP+JSON 与其他节点通信
type HTTPTransport struct {
	client *http.Client
}

// NewHTTPTransport 创建一个带超时的 HTTP transport
func NewHTTPTransport() *HTTPTransport {
	return &HTTPTransport{client: &http.Client{Timeout: time.Second}}
}

func (t *HTTPTransport) RequestVote(peer Peer, req RequestVoteRequest) RequestVoteResponse {
	var resp RequestVoteResponse
	t.doRPC(peer.RaftAddr, "/raft/vote", req, &resp)
	return resp
}

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

	httpResp, err := t.client.Post(
		"http://"+raftAddr+path,
		"application/json",
		bytes.NewReader(data),
	)
	if err != nil {
		return // 目标不可达：零值响应（Term 0, granted false）
	}
	defer httpResp.Body.Close()

	body, _ := io.ReadAll(httpResp.Body)
	_ = json.Unmarshal(body, resp)
}

// NewRaftHTTPServer 创建 Raft RPC 入口（挂在 RaftAddr 端口）
func NewRaftHTTPServer(n *Node) *http.Server {
	mux := http.NewServeMux()

	mux.HandleFunc("/raft/vote", func(w http.ResponseWriter, r *http.Request) {
		var req RequestVoteRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		resp := n.handleRequestVote(req)
		_ = json.NewEncoder(w).Encode(resp)
	})

	mux.HandleFunc("/raft/append", func(w http.ResponseWriter, r *http.Request) {
		var req AppendEntriesRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		resp := n.handleAppendEntries(req)
		_ = json.NewEncoder(w).Encode(resp)
	})

	return &http.Server{Addr: n.cfg.RaftAddr, Handler: mux}
}
