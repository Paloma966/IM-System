package raft

import "time"

// Role Raft 节点角色
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

// Peer 集群中的一个对端节点
type Peer struct {
	ID       string
	RaftAddr string
	HTTPAddr string
}

// Config Raft 节点配置
type Config struct {
	ID string

	HTTPAddr string
	RaftAddr string

	DataDir string

	Peers []Peer

	ElectionTimeout   time.Duration
	HeartbeatInterval time.Duration
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

// HasPeer 判断 id 是否为集群成员（不含自己）。
// 用于 RPC 入口校验对端身份，防止伪造的 LeaderID/CandidateID
// 污染节点状态或劫持写转发。
func (c Config) HasPeer(id string) bool {
	for _, peer := range c.Peers {
		if peer.ID == id {
			return true
		}
	}
	return false
}
