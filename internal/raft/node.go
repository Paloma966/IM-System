package raft

import (
	"errors"
	"sync"
	"time"
)

var ErrNotLeader = errors.New(
	"raft: not leader",
)

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
	timerCh        chan struct{}
	stopCh         chan struct{}
}

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

		timerCh: make(
			chan struct{},
		),

		stopCh: make(
			chan struct{},
		),
	}

	return n
}

func (n *Node) Role() Role {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return n.role
}

func (n *Node) CurrentTerm() uint64 {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return n.currentTerm
}

func (n *Node) IsLeader() bool {
	return n.Role() == Leader
}

func (n *Node) LeaderID() string {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return n.leaderID
}

func (n *Node) Log() *Log {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return n.log
}

func (n *Node) Submit(
	command Command,
) error {

	n.mu.Lock()

	defer n.mu.Unlock()

	if n.role != Leader {

		return ErrNotLeader
	}

	return nil
}
