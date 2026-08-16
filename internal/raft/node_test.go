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

	n, err := NewNode(cfg, nil)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	return n
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

	if _, err := n.Submit(
		Command{
			Type: "message",
		},
	); err != ErrNotLeader {

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
	index, err := n.Submit(cmd)
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if index != 1 {
		t.Fatalf("Submit index = %d, want 1", index)
	}
	if err := n.WaitCommitted(index, time.Second); err != nil {
		t.Fatalf("WaitCommitted: %v", err)
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

// TestStopIdempotent 未 Start / Start 后各调两次 Stop 都不应 panic（回归：
// 曾因 stopCh 重复关闭 panic，且 Stop 等 apply 循环退出可能死锁）。
func TestStopIdempotent(t *testing.T) {
	// 未 Start 的节点（runApplyLoop 自 NewNode 起就在跑）
	n := newTestNode(t, "node-1")
	n.Stop()
	n.Stop()

	// Start 过的节点
	n2 := newTestNode(t, "node-1")
	n2.Start()
	n2.Stop()
	n2.Stop()
}

// TestSetTermReschedulesElectionTimer 降级为 Follower 必须重启选举定时器
// （回归：曾因 setTerm 不重设定时器，节点降级后永远不自发竞选）。
func TestSetTermReschedulesElectionTimer(t *testing.T) {
	n := newTestNode(t, "node-1")

	n.mu.Lock()
	n.setTerm(5)
	rescheduled := n.electionTimer != nil
	n.mu.Unlock()

	if !rescheduled {
		t.Fatal("setTerm should reschedule the election timer")
	}
}

// TestConfigClampsZeroTimeouts 零值超时配置被钳制到安全默认值，
// 防止 rand.Int63n(0) panic 与心跳忙等。
func TestConfigClampsZeroTimeouts(t *testing.T) {
	cfg := Config{
		ID:       "node-1",
		HTTPAddr: ":8000",
		RaftAddr: ":9000",
		DataDir:  t.TempDir(),
	}
	n, err := NewNode(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer n.Stop()

	if n.cfg.ElectionTimeout <= 0 {
		t.Fatalf("ElectionTimeout = %v, want > 0", n.cfg.ElectionTimeout)
	}
	if n.cfg.HeartbeatInterval <= 0 {
		t.Fatalf("HeartbeatInterval = %v, want > 0", n.cfg.HeartbeatInterval)
	}
	if n.cfg.HeartbeatInterval*3 >= n.cfg.ElectionTimeout {
		t.Fatalf("heartbeat %v too close to election timeout %v",
			n.cfg.HeartbeatInterval, n.cfg.ElectionTimeout)
	}
}
