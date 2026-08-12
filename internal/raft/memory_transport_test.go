package raft

import "sync"

// memoryTransport 测试用传输层：不经过网络，直接把 RPC 路由到目标节点的方法。
// 好处：能测完整的 Raft 协议流程，不需要起真实 HTTP server。
type memoryTransport struct {
	mu    sync.Mutex
	nodes map[string]*Node
}

func newMemoryTransport() *memoryTransport {
	return &memoryTransport{nodes: make(map[string]*Node)}
}

// Register 把一个节点注册进路由表
func (t *memoryTransport) Register(n *Node) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.nodes[n.cfg.ID] = n
}

// lookup 找目标节点
func (t *memoryTransport) lookup(id string) *Node {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.nodes[id]
}

func (t *memoryTransport) RequestVote(peer Peer, req RequestVoteRequest) RequestVoteResponse {
	if n := t.lookup(peer.ID); n != nil {
		return n.handleRequestVote(req)
	}
	return RequestVoteResponse{Term: req.Term}
}

func (t *memoryTransport) AppendEntries(peer Peer, req AppendEntriesRequest) AppendEntriesResponse {
	if n := t.lookup(peer.ID); n != nil {
		return n.handleAppendEntries(req)
	}
	return AppendEntriesResponse{Term: req.Term}
}
