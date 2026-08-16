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
	stopCh          chan struct{}
	stopOnce        sync.Once
	applyLoopDone   chan struct{}
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

	// 配置兜底：零值超时会 panic 或忙等，钳制到安全区间
	if cfg.ElectionTimeout <= 0 {
		cfg.ElectionTimeout = 300 * time.Millisecond
	}
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = 50 * time.Millisecond
	}
	// 心跳必须显著小于选举超时，否则选不出稳定 Leader
	if maxHB := cfg.ElectionTimeout / 3; cfg.HeartbeatInterval >= maxHB {
		cfg.HeartbeatInterval = maxHB
	}
	if cfg.HeartbeatInterval < time.Millisecond {
		cfg.HeartbeatInterval = time.Millisecond
	}

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

		applyLoopDone: make(
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

	// apply 循环随节点创建即启动：Stop 时先等它退出再关闭 applyCh
	go n.runApplyLoop()

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
// 返回新条目的日志索引，可用于 WaitCommitted 等待提交确认。
func (n *Node) Submit(command Command) (uint64, error) {
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return 0, ErrNotLeader
	}

	// 追加到自己的日志（带当前 term）
	entry := LogEntry{
		Index:   n.log.LastIndex() + 1,
		Term:    n.currentTerm,
		Command: command,
	}
	if err := AppendLog(n.cfg.DataDir, []LogEntry{entry}); err != nil {
		n.mu.Unlock()
		return 0, fmt.Errorf("persist: %w", err)
	}
	n.log.Append(entry)

	// 立刻尝试推进 commitIndex：单节点自己就过半，直接提交；
	// 多节点 count=1 不过半，等 peer 确认（sendAppendEntries 再推进）。
	n.advanceCommitIndexLocked()
	n.mu.Unlock()

	// 唤醒心跳循环，不用等下一个周期
	n.signalReplicate()

	return entry.Index, nil
}

// WaitCommitted 阻塞等待 index 被提交（最多 timeout）。
// 等待期间失去 Leader 身份则立即返回 ErrNotLeader：该条目可能最终仍会
// 被新 Leader 提交，但本节点已无法给出确认。
func (n *Node) WaitCommitted(index uint64, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		n.mu.RLock()
		committed := n.commitIndex >= index
		stillLeader := n.role == Leader
		n.mu.RUnlock()

		if committed {
			return nil
		}
		if !stillLeader {
			return ErrNotLeader
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("raft: commit timeout for index %d", index)
		}
		time.Sleep(10 * time.Millisecond)
	}
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
