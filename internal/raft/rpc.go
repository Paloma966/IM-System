package raft

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
