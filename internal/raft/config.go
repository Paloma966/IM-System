package raft

import "time"

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

// 返回对应角色字符串
func (r Role) String() string {
	switch r {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

type Peer struct {
	ID       string
	RaftAddr string
	HTTPAddr string
}

type Config struct {
	ID string

	HTTPAddr string
	RaftAddr string

	DataDir string

	Peers []Peer

	ElectionTimeout  time.Duration
	HearbeatInterval time.Duration
}

// 根据id查信息
func (c Config) Peer(id string) (Peer, bool) {
	for _, peer := range c.Peers {
		if peer.ID == c.ID {
			continue
		}
		if peer.ID == id {
			return peer, true
		}
	}

	return Peer{}, false
}
