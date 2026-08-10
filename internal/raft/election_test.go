package raft

import "testing"

// 正常投票：term 更高，日志不落后 → 应该同意
func TestVoteGranted(t *testing.T) {
	n := newTestNode(t, "node-1")
	// 手动设 term（不用 setTerm，直接改字段）
	n.mu.Lock()
	n.currentTerm = 3
	n.mu.Unlock()

	resp := n.handleRequestVote(RequestVoteRequest{
		Term: 4, CandidateID: "node-2",
		LastLogIndex: 0, LastLogTerm: 0,
	})
	if !resp.VoteGranted {
		t.Fatalf("vote should be granted, got %+v", resp)
	}
	if resp.Term != 4 {
		t.Fatalf("resp.Term = %d, want 4", resp.Term)
	}
}

// 拒绝过期 term
func TestVoteDeniedStaleTerm(t *testing.T) {
	n := newTestNode(t, "node-1")
	n.mu.Lock()
	n.currentTerm = 3
	n.mu.Unlock()

	resp := n.handleRequestVote(RequestVoteRequest{
		Term: 2, CandidateID: "node-2", // 比我的 3 小
	})
	if resp.VoteGranted {
		t.Fatal("should deny lower term")
	}
	if resp.Term != 3 {
		t.Fatalf("resp.Term = %d, want 3 (my own term)", resp.Term)
	}
}

// 同一 term 已经投给别人 → 拒绝
func TestVoteDeniedAlreadyVoted(t *testing.T) {
	n := newTestNode(t, "node-1")
	n.mu.Lock()
	n.currentTerm = 4
	n.votedFor = "node-3" // 已经投给 node-3 了
	n.mu.Unlock()

	resp := n.handleRequestVote(RequestVoteRequest{
		Term: 4, CandidateID: "node-2",
	})
	if resp.VoteGranted {
		t.Fatal("should deny, already voted for node-3 this term")
	}
}

// 候选人日志落后 → 拒绝
func TestVoteDeniedStaleLog(t *testing.T) {
	n := newTestNode(t, "node-1")
	n.mu.Lock()
	n.currentTerm = 3
	n.log.AppendRange([]LogEntry{
		{Index: 1, Term: 2},
		{Index: 2, Term: 2},
	})
	n.mu.Unlock()

	// 候选人日志只有 1 条，term 是 1 → 比我旧
	resp := n.handleRequestVote(RequestVoteRequest{
		Term: 4, CandidateID: "node-2",
		LastLogIndex: 1, LastLogTerm: 1,
	})
	if resp.VoteGranted {
		t.Fatal("should deny, candidate log is stale")
	}
}

// 候选人日志更新 → 同意
func TestVoteGrantedLogUpToDate(t *testing.T) {
	n := newTestNode(t, "node-1")
	n.mu.Lock()
	n.currentTerm = 3
	n.log.AppendRange([]LogEntry{
		{Index: 1, Term: 2},
		{Index: 2, Term: 2},
	})
	n.mu.Unlock()

	// 候选人相同 term、更长 → 更新，同意
	resp := n.handleRequestVote(RequestVoteRequest{
		Term: 4, CandidateID: "node-2",
		LastLogIndex: 3, LastLogTerm: 2,
	})
	if !resp.VoteGranted {
		t.Fatal("should grant, candidate log is up to date")
	}
}

// 收到更高 term → 自动降级为 Follower
func TestVoteUpdatesTermAndStepsDown(t *testing.T) {
	n := newTestNode(t, "node-1")
	n.mu.Lock()
	n.currentTerm = 2
	n.role = Candidate // 假设我是 Candidate
	n.mu.Unlock()

	n.handleRequestVote(RequestVoteRequest{
		Term: 5, CandidateID: "node-2",
	})

	if n.CurrentTerm() != 5 {
		t.Fatalf("term = %d, want 5", n.CurrentTerm())
	}
	if n.Role() != Follower {
		t.Fatalf("role = %v, want Follower", n.Role())
	}
}
