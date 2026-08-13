package raft

import (
	"errors"
	"sync"
	"time"
)

// ErrNotLeader 非 Leader 提交命令时返回
var ErrNotLeader = errors.New(
	"raft: not leader",
)

// Node 一个 Raft 节点：管理角色、任期、日志，并把已提交命令推给状态机
type Node struct {
	cfg            Config
	transport      Transport
	applyCh        chan Command
	mu             sync.RWMutex
	role           Role
	currentTerm    uint64
	votedFor       string
	leaderID       string
	log            *Log
	commitIndex    uint64
	lastApplied    uint64
	nextIndex      map[string]uint64
	matchIndex     map[string]uint64
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer
	stopCh         chan struct{}
	electionEpoch  uint64
	votesReceived  map[string]bool
	applySignal    chan struct{}
}

// NewNode 创建节点：从磁盘恢复任期与日志（失败则用默认值）
func NewNode(
	cfg Config,
	transport Transport,
) *Node {

	term, votedFor, err := LoadMeta(
		cfg.DataDir,
	)

	if err != nil {

		term = 0
		votedFor = ""
	}

	log, err := LoadLog(
		cfg.DataDir,
	)

	if err != nil {

		log = NewLog()
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

		applySignal: make(
			chan struct{},
			1,
		),
	}

	return n
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

// Submit 客户端入口：Leader 追加命令到日志并立即广播复制
func (n *Node) Submit(command Command) error {
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return ErrNotLeader
	}

	// 追加到自己的日志（带当前 term）
	n.log.Append(LogEntry{
		Index:   n.log.LastIndex() + 1,
		Term:    n.currentTerm,
		Command: command,
	})
	_ = SaveLog(n.cfg.DataDir, n.log)

	// 立刻尝试推进 commitIndex：单节点自己就过半，直接提交；
	// 多节点 count=1 不过半，等 peer 确认（sendAppendEntries 再推进）。
	n.advanceCommitIndexLocked()
	n.mu.Unlock()

	// 立即推一轮，不用等下一个心跳周期
	go n.broadcastAppendEntries()

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
