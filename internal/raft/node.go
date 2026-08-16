package raft

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

// ErrNotLeader 非 Leader 提交命令时返回
var ErrNotLeader = errors.New(
	"raft: not leader",
)

// Node 一个 Raft 节点：管理角色、任期、日志，并把已提交命令推给状态机
type Node struct {
	cfg             Config
	transport       Transport
	applyCh         chan Command
	mu              sync.RWMutex
	role            Role
	currentTerm     uint64
	votedFor        string
	leaderID        string
	log             *Log
	commitIndex     uint64
	lastApplied     uint64
	nextIndex       map[string]uint64
	matchIndex      map[string]uint64
	electionTimer   *time.Timer
	heartbeatTimer  *time.Timer
	stopCh          chan struct{}
	replicateSignal chan struct{}
	electionEpoch   uint64
	votesReceived   map[string]bool
	applySignal     chan struct{}
}

// NewNode 创建节点：从磁盘恢复任期与日志。
// 持久化文件损坏时直接返回错误（Raft 要求 fail-fast，
// 静默以空状态重启会破坏任期/日志的安全保证）。
func NewNode(
	cfg Config,
	transport Transport,
) (*Node, error) {

	term, votedFor, err := LoadMeta(
		cfg.DataDir,
	)
	if err != nil {
		return nil, err
	}

	log, err := LoadLog(
		cfg.DataDir,
	)
	if err != nil {
		return nil, err
	}

	n := &Node{

		cfg: cfg,

		transport: transport,

		applyCh: make(
			chan Command,
			100,
		),

		role: Follower,

		currentTerm: term,

		votedFor: votedFor,

		log: log,

		nextIndex: make(
			map[string]uint64,
		),

		matchIndex: make(
			map[string]uint64,
		),

		stopCh: make(
			chan struct{},
		),

		replicateSignal: make(
			chan struct{},
			1,
		),

		applySignal: make(
			chan struct{},
			1,
		),
	}

	return n, nil
}

// signalReplicate 非阻塞唤醒 Leader 的心跳循环（可安全地在持锁时调用）
func (n *Node) signalReplicate() {
	select {
	case n.replicateSignal <- struct{}{}:
	default:
	}
}

// Role 返回当前角色
func (n *Node) Role() Role {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return n.role
}

// CurrentTerm 返回当前任期
func (n *Node) CurrentTerm() uint64 {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return n.currentTerm
}

// IsLeader 当前是否为 Leader
func (n *Node) IsLeader() bool {
	return n.Role() == Leader
}

// LeaderID 返回当前已知的 Leader id（未知时为空）
func (n *Node) LeaderID() string {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return n.leaderID
}

// Log 返回日志（测试用）
func (n *Node) Log() *Log {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return n.log
}

// Submit 客户端入口：Leader 追加命令到日志并立即触发一轮复制。
// 先落盘（write-ahead，只追加新条目 O(1)）再改内存，落盘失败直接报错。
func (n *Node) Submit(command Command) error {
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return ErrNotLeader
	}

	// 追加到自己的日志（带当前 term）
	entry := LogEntry{
		Index:   n.log.LastIndex() + 1,
		Term:    n.currentTerm,
		Command: command,
	}
	if err := AppendLog(n.cfg.DataDir, []LogEntry{entry}); err != nil {
		n.mu.Unlock()
		return fmt.Errorf("persist: %w", err)
	}
	n.log.Append(entry)

	// 立刻尝试推进 commitIndex：单节点自己就过半，直接提交；
	// 多节点 count=1 不过半，等 peer 确认（sendAppendEntries 再推进）。
	n.advanceCommitIndexLocked()
	n.mu.Unlock()

	// 唤醒心跳循环，不用等下一个周期
	n.signalReplicate()

	return nil
}

// ApplyCh 返回已提交命令的通道（状态机消费它）
func (n *Node) ApplyCh() <-chan Command {
	return n.applyCh
}

// LeaderHTTPAddr 当前 leader 的 HTTP 地址（follower 转发写请求用）
func (n *Node) LeaderHTTPAddr() string {
	n.mu.RLock()
	defer n.mu.RUnlock()

	if n.leaderID == "" {
		return ""
	}
	if n.leaderID == n.cfg.ID {
		return n.cfg.HTTPAddr
	}
	p, ok := n.cfg.Peer(n.leaderID)
	if !ok {
		return ""
	}
	return p.HTTPAddr
}
