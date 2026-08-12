package raft

import (
	"testing"
	"time"
)

func newTestNode(t *testing.T, id string) *Node {

	cfg := Config{
		ID:                id,
		HTTPAddr:          ":8000",
		RaftAddr:          ":9000",
		DataDir:           t.TempDir(),
		ElectionTimeout:   200 * time.Millisecond,
		HeartbeatInterval: 20 * time.Millisecond,
	}

	return NewNode(cfg, nil)
}

func TestNewNodeState(t *testing.T) {

	n := newTestNode(t, "node-1")

	if n.Role() != Follower {
		t.Fatalf(
			"role=%v want Follower",
			n.Role(),
		)
	}

	if n.IsLeader() {
		t.Fatal(
			"new node should not be leader",
		)
	}

	if n.CurrentTerm() != 0 {
		t.Fatalf(
			"term=%d want 0",
			n.CurrentTerm(),
		)
	}

	if n.Log().LastIndex() != 0 {
		t.Fatalf(
			"log index=%d want 0",
			n.Log().LastIndex(),
		)
	}
}

func TestSubmitOnFollower(t *testing.T) {

	n := newTestNode(t, "node-1")

	err := n.Submit(
		Command{
			Type: "message",
		},
	)

	if err != ErrNotLeader {

		t.Fatalf(
			"Submit=%v want ErrNotLeader",
			err,
		)
	}
}
