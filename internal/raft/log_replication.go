package raft

import "time"

// handleAppendEntries Follower 端处理日志复制/心跳（Raft 论文 §5.3）
func (n *Node) handleAppendEntries(req AppendEntriesRequest) AppendEntriesResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	// 规则 1：过期 term → 拒绝
	if req.Term < n.currentTerm {
		return AppendEntriesResponse{Term: n.currentTerm, Success: false}
	}

	// 规则 2：更高 term → 更新 term 并降级；认出这个 leader
	if req.Term > n.currentTerm {
		n.setTerm(req.Term)
	}
	n.leaderID = req.LeaderID
	n.role = Follower

	// 规则 3：一致性检查 — PrevLogIndex/PrevLogTerm 必须匹配
	prevTerm, ok := n.log.TermAt(req.PrevLogIndex)
	if !ok || prevTerm != req.PrevLogTerm {
		return AppendEntriesResponse{Term: n.currentTerm, Success: false}
	}

	// 规则 4+5：冲突截断 + 追加新条目
	newEntries := req.Entries
	truncated := false
	i := 0
	for i < len(newEntries) {
		idx := newEntries[i].Index
		if idx > n.log.LastIndex() {
			break // 剩下的全是新条目，直接追加
		}
		if term, ok := n.log.TermAt(idx); ok && term == newEntries[i].Term {
			i++ // 已存在且一致，跳过
			continue
		}
		n.log.TruncateFrom(idx) // 冲突：从这里开始全部删掉
		truncated = true
		break
	}
	newEntries = newEntries[i:]

	// 持久化：follower 也要落盘，否则重启后日志全丢。
	// 追加走 write-ahead（先落盘再改内存）；发生过截断则整写重写。
	// 落盘失败时拒绝本请求（返回失败），Leader 下轮会重试。
	if truncated {
		if err := SaveLog(n.cfg.DataDir, n.log); err != nil {
			return AppendEntriesResponse{Term: n.currentTerm, Success: false}
		}
	} else if err := AppendLog(n.cfg.DataDir, newEntries); err != nil {
		return AppendEntriesResponse{Term: n.currentTerm, Success: false}
	}
	n.log.AppendRange(newEntries)

	// 规则 6：推进 commitIndex（不超过我日志实际有的位置）
	if req.LeaderCommit > n.commitIndex {
		// 不超过自己日志实际有的位置
		last := min(req.LeaderCommit, req.PrevLogIndex+uint64(len(req.Entries)))
		n.commitIndex = last
		n.signalApply() // 有新条目可应用到状态机
	}

	// 规则 7：合法 AppendEntries = leader 还活着 → 重置选举定时器
	n.resetElectionTimerLocked()

	return AppendEntriesResponse{Term: n.currentTerm, Success: true}
}

// broadcastAppendEntries Leader 发一轮心跳/日志复制（给所有 Peer）。
// 每个 Peer 一个 goroutine：一个 peer 挂掉不阻塞其他 peer（HTTP 场景很重要）。
func (n *Node) broadcastAppendEntries() {
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return
	}
	peers := n.cfg.Peers
	n.mu.Unlock()

	for _, peer := range peers {
		go n.sendAppendEntries(peer)
	}
}

// runHeartbeatLoop Leader 周期性广播（心跳 + 日志复制）
func (n *Node) runHeartbeatLoop() {
	for {
		n.mu.Lock()
		if n.role != Leader {
			n.mu.Unlock()
			return // 不再是 leader，退出循环
		}
		n.mu.Unlock()

		n.broadcastAppendEntries()

		time.Sleep(n.cfg.HeartbeatInterval)
	}
}

// sendAppendEntries 给单个 Peer 发送 AppendEntries（心跳或日志复制）
func (n *Node) sendAppendEntries(peer Peer) {
	// —— 加锁准备请求 ——
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return
	}

	prevLogIndex := n.nextIndex[peer.ID] - 1
	prevTerm, _ := n.log.TermAt(prevLogIndex)
	entries := n.log.SliceFrom(n.nextIndex[peer.ID])

	req := AppendEntriesRequest{
		Term:         n.currentTerm,
		LeaderID:     n.cfg.ID,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: n.commitIndex,
	}
	n.mu.Unlock() // ⚠️ 必须在锁外做网络调用

	resp := n.transport.AppendEntries(peer, req)

	// —— 处理响应 ——
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.role != Leader {
		return // 发送期间已经不是 leader 了，忽略
	}

	// 对方 term 更高 → 我过时了，降级
	if resp.Term > n.currentTerm {
		n.setTerm(resp.Term)
		return
	}

	if resp.Success {
		// 游标前进。用 max 防止乱序响应回退进度：
		// 多个并发 sendAppendEntries 打到同一 peer，旧请求的响应可能后到，
		// 无条件覆盖会把已确认的 matchIndex 打回去（重新复制、拖慢提交）。
		// Success 意味着 peer 确实追加到了 matched，所以只前进是安全的。
		matched := prevLogIndex + uint64(len(req.Entries))
		if matched > n.matchIndex[peer.ID] {
			n.matchIndex[peer.ID] = matched
			n.nextIndex[peer.ID] = matched + 1
			n.advanceCommitIndexLocked() // 每确认一个 peer 就检查是否过半可提交
		}
	} else {
		// 一致性检查没过 → 回退一格，下个周期重试
		if n.nextIndex[peer.ID] > 1 {
			n.nextIndex[peer.ID]--
		}
	}
}

// ============================================================
// 提交推进 + 应用到状态机
// ============================================================

// advanceCommitIndexLocked Leader 检查过半 matchIndex，推进 commitIndex。
// 只拿自己 term 的条目当探测点（Raft §5.4.2）。调用者必须持有 n.mu。
func (n *Node) advanceCommitIndexLocked() {
	if n.role != Leader {
		return
	}

	for idx := n.commitIndex + 1; idx <= n.log.LastIndex(); idx++ {
		term, _ := n.log.TermAt(idx)
		if term != n.currentTerm {
			continue // 上一任期的条目不能直接作为提交依据
		}

		count := 1 // 自己算一个
		for _, peer := range n.cfg.Peers {
			if n.matchIndex[peer.ID] >= idx {
				count++
			}
		}
		if count*2 > 1+len(n.cfg.Peers) { // 过半
			n.commitIndex = idx
			n.signalApply()
		}
	}
}

// collectCommitted 锁内收集 lastApplied+1..commitIndex 的命令
func (n *Node) collectCommitted() []Command {
	n.mu.Lock()
	defer n.mu.Unlock()

	var cmds []Command
	for i := n.lastApplied + 1; i <= n.commitIndex; i++ {
		entry, ok := n.log.At(i)
		if !ok {
			break
		}
		cmds = append(cmds, entry.Command)
		n.lastApplied = i
	}
	return cmds
}

// runApplyLoop 等信号，把已提交命令推给状态机（applyCh）
func (n *Node) runApplyLoop() {
	for {
		select {
		case <-n.stopCh:
			return
		case <-n.applySignal:
		}

		// 排干到最新 commitIndex
		for {
			cmds := n.collectCommitted()
			if len(cmds) == 0 {
				break // 没有新的了，回外层等信号
			}
			for _, cmd := range cmds {
				n.applyCh <- cmd // 此时没拿锁，阻塞也没关系
			}
		}
	}
}

// signalApply 非阻塞通知 apply 循环（可安全地在持锁时调用）
func (n *Node) signalApply() {
	select {
	case n.applySignal <- struct{}{}:
	default:
	}
}
