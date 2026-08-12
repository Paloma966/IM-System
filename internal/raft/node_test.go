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

// TestSingleNodeCommitsAndApplies 单节点是最小集群（无 peers）：
// Submit 追加日志后自己就过半，应立即提交并应用到 applyCh。
// 回归测试：曾因 advanceCommitIndexLocked 只在 peer 确认后调用，
// 导致单节点永远不提交、applyCh 收不到命令。
func TestSingleNodeCommitsAndApplies(t *testing.T) {
	n := newTestNode(t, "node-1")
	n.Start()
	t.Cleanup(n.Stop)

	// 单节点选举后必然成为 leader
	deadline := time.Now().Add(3 * time.Second)
	for !n.IsLeader() {
		if time.Now().After(deadline) {
			t.Fatal("single node did not become leader")
		}
		time.Sleep(10 * time.Millisecond)
	}

	cmd := Command{Type: "message", Payload: []byte(`{"text":"solo"}`)}
	if err := n.Submit(cmd); err != nil {
		t.Fatalf("Submit: %v", err)
	}

	select {
	case got := <-n.applyCh:
		if got.Type != "message" || string(got.Payload) != `{"text":"solo"}` {
			t.Fatalf("applied command = %+v", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("single node command not applied within 3s")
	}

	if n.commitIndex != 1 || n.lastApplied != 1 {
		t.Fatalf("commitIndex=%d lastApplied=%d want 1/1", n.commitIndex, n.lastApplied)
	}
}
