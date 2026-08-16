package raft

import (
	"math/rand"
	"time"
)

// ============================================================
// handleRequestVote — 投票规则（Raft 论文 §5.2）
// ============================================================

func (n *Node) handleRequestVote(req RequestVoteRequest) RequestVoteResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	// 规则 1：stale term → 拒绝
	if req.Term < n.currentTerm {
		return RequestVoteResponse{Term: n.currentTerm, VoteGranted: false}
	}

	// 规则 2：发现更高 term → 降级 Follower
	if req.Term > n.currentTerm {
		n.setTerm(req.Term)
	}

	// 规则 3：一任只投一票
	if n.votedFor != "" && n.votedFor != req.CandidateID {
		return RequestVoteResponse{Term: n.currentTerm, VoteGranted: false}
	}

	// 规则 4：日志新旧检查（选举限制）
	myLastTerm := n.log.LastTerm()
	myLastIndex := n.log.LastIndex()
	if req.LastLogTerm < myLastTerm ||
		(req.LastLogTerm == myLastTerm && req.LastLogIndex < myLastIndex) {
		return RequestVoteResponse{Term: n.currentTerm, VoteGranted: false}
	}

	// 规则 5：投票
	n.votedFor = req.CandidateID
	_ = SaveMeta(n.cfg.DataDir, n.currentTerm, n.votedFor)

	// 投了票就重置选举定时器（有 Candidate 在拉票了，我不跟他抢）
	n.resetElectionTimerLocked()
	return RequestVoteResponse{Term: n.currentTerm, VoteGranted: true}
}

// ============================================================
// setTerm — 更新任期（调用者必须持有 n.mu）
// ============================================================

func (n *Node) setTerm(term uint64) {
	n.currentTerm = term
	n.votedFor = ""
	n.role = Follower
	n.leaderID = ""
	_ = SaveMeta(n.cfg.DataDir, n.currentTerm, n.votedFor)
	// 转为 Follower 必须重启选举定时器（Raft §5.2）：否则本节点旧定时器
	// 可能已消耗，此后永远不自发竞选，影响集群可用性。
	n.resetElectionTimerLocked()
}

// ============================================================
// 选举定时器
// ============================================================

// resetElectionTimer 在选举定时器回调或 resetElectionTimerLocked 里调用，
// 启动一个随机时长的定时器，到期后发起选举
func (n *Node) scheduleElectionTimer() {
	// 已停止的节点不再调度新定时器
	select {
	case <-n.stopCh:
		return
	default:
	}

	base := n.cfg.ElectionTimeout
	jitter := time.Duration(rand.Int63n(int64(base)))
	timeout := base + jitter

	n.electionEpoch++
	epoch := n.electionEpoch

	if n.electionTimer != nil {
		n.electionTimer.Stop()
	}

	n.electionTimer = time.AfterFunc(timeout, func() {
		n.mu.Lock()
		select {
		case <-n.stopCh:
			// 节点已停止：不再发起选举
			n.mu.Unlock()
			return
		default:
		}
		if epoch != n.electionEpoch {
			n.mu.Unlock()
			return
		}
		if n.role != Follower && n.role != Candidate {
			n.mu.Unlock()
			return
		}

		// === becomeCandidate ===
		n.currentTerm++
		n.votedFor = n.cfg.ID
		n.role = Candidate
		n.leaderID = ""
		n.votesReceived = map[string]bool{n.cfg.ID: true}
		_ = SaveMeta(n.cfg.DataDir, n.currentTerm, n.votedFor)

		req := RequestVoteRequest{
			Term:         n.currentTerm,
			CandidateID:  n.cfg.ID,
			LastLogIndex: n.log.LastIndex(),
			LastLogTerm:  n.log.LastTerm(),
		}
		peers := n.cfg.Peers
		n.mu.Unlock()

		for _, peer := range peers {
			go n.requestVote(peer, req)
		}

		n.mu.Lock()
		n.checkMajorityLocked()
		if n.role == Follower || n.role == Candidate {
			n.scheduleElectionTimer()
		}
		n.mu.Unlock()
	})
}

// resetElectionTimerLocked 调用者必须持有 n.mu
func (n *Node) resetElectionTimerLocked() {
	n.scheduleElectionTimer()
}

// runElectionTimer 选举定时器循环
func (n *Node) runElectionTimer() {
	n.mu.Lock()
	n.scheduleElectionTimer()
	n.mu.Unlock()
	<-n.stopCh
}

// ============================================================
// 拉票
// ============================================================

// requestVote 向单个 peer 拉票，成功则计入并检查是否过半
func (n *Node) requestVote(peer Peer, req RequestVoteRequest) {
	if n.transport == nil {
		return
	}
	resp := n.transport.RequestVote(peer, req)

	n.mu.Lock()
	defer n.mu.Unlock()

	if resp.Term > n.currentTerm {
		n.setTerm(resp.Term)
		return
	}

	if resp.VoteGranted && n.role == Candidate && n.currentTerm == req.Term {
		n.votesReceived[peer.ID] = true
		n.checkMajorityLocked()
	}
}

// ============================================================
// 过半检查 + 成为 Leader
// ============================================================

// checkMajorityLocked 调用者必须持有 n.mu
func (n *Node) checkMajorityLocked() {
	if n.role != Candidate {
		return
	}
	total := 1 + len(n.cfg.Peers)
	if len(n.votesReceived)*2 > total {
		n.becomeLeaderLocked()
	}
}

// becomeLeaderLocked 调用者必须持有 n.mu
func (n *Node) becomeLeaderLocked() {
	n.role = Leader
	n.leaderID = n.cfg.ID

	lastIdx := n.log.LastIndex()
	for _, peer := range n.cfg.Peers {
		n.nextIndex[peer.ID] = lastIdx + 1
		n.matchIndex[peer.ID] = 0
	}

	go n.runHeartbeatLoop()
}

// ============================================================
// Start / Stop
// ============================================================

// Start 启动节点：进入 Follower，启动选举定时器
func (n *Node) Start() {
	n.mu.Lock()
	n.role = Follower
	n.mu.Unlock()
	go n.runElectionTimer()
}

// Stop 停止节点：关闭定时器并通知各循环退出。可安全地多次调用。
// 先等 apply 循环退出，再关闭 applyCh（避免向已关闭通道发送 panic）。
func (n *Node) Stop() {
	n.stopOnce.Do(func() {
		n.mu.Lock()
		n.role = Follower // 让心跳循环看到 role!=Leader 自己退出
		n.mu.Unlock()
		close(n.stopCh)
		if n.electionTimer != nil {
			n.electionTimer.Stop()
		}
		<-n.applyLoopDone
		close(n.applyCh)
	})
}
