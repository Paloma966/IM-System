package raft

import (
	"fmt"
	"testing"
	"time"
)

// newTestCluster 启动一个 3 节点集群（共享一个 memoryTransport）
func newTestCluster(t *testing.T) map[string]*Node {
	t.Helper()

	transport := newMemoryTransport()
	ids := []string{"node-1", "node-2", "node-3"}
	nodes := make(map[string]*Node, len(ids))

	for _, id := range ids {
		cfg := Config{
			ID:                id,
			HTTPAddr:          ":8000",
			RaftAddr:          ":9000",
			DataDir:           t.TempDir(), // 每个节点独立目录
			ElectionTimeout:   200 * time.Millisecond,
			HeartbeatInterval: 20 * time.Millisecond,
		}
		// Peers = 另外两个节点（不含自己）
		for _, other := range ids {
			if other == id {
				continue
			}
			cfg.Peers = append(cfg.Peers, Peer{ID: other})
		}

		n := NewNode(cfg, transport)
		transport.Register(n)
		nodes[id] = n
	}

	// 全部就绪后再一起启动，模拟真实集群同时上线
	for _, n := range nodes {
		n.Start()
		t.Cleanup(n.Stop)
	}

	return nodes
}

// waitForLeader 等待集群收敛出唯一稳定 leader（最多 5 秒）
func waitForLeader(t *testing.T, nodes map[string]*Node) *Node {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for {
		var leaders []string
		for id, n := range nodes {
			if n.IsLeader() {
				leaders = append(leaders, id)
			}
		}
		if len(leaders) > 1 {
			t.Fatalf("multiple leaders: %v", leaders)
		}
		if len(leaders) == 1 && allAgreeOnLeader(nodes, leaders[0]) {
			return nodes[leaders[0]]
		}
		if time.Now().After(deadline) {
			t.Fatalf("no stable leader within 5s: leaders=%v", leaders)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// allAgreeOnLeader 所有节点都把 leaderID 认成 elected
func allAgreeOnLeader(nodes map[string]*Node, leaderID string) bool {
	for _, n := range nodes {
		if n.LeaderID() != leaderID {
			return false
		}
	}
	return true
}

// TestElectionCluster 集群收敛出唯一 leader 且稳定 1 秒（心跳在维持）
func TestElectionCluster(t *testing.T) {
	nodes := newTestCluster(t)
	leader := waitForLeader(t, nodes)
	t.Logf("elected leader: %s", leader.cfg.ID)

	// 再观察 1 秒：leader 不能被推翻
	stable := time.Now().Add(time.Second)
	for time.Now().Before(stable) {
		if !leader.IsLeader() {
			t.Fatalf("leader %s lost leadership during stability window", leader.cfg.ID)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestBurstReplication 连续快速提交 10 条命令：
// 1) 所有节点日志最终都包含全部 10 条（并发 sendAppendEntries 不回退游标）
// 2) 所有节点日志都落盘（回归：follower 端曾不调用 SaveLog）
func TestBurstReplication(t *testing.T) {
	nodes := newTestCluster(t)
	leader := waitForLeader(t, nodes)

	const total = 10
	for i := 1; i <= total; i++ {
		cmd := Command{Type: "message", Payload: fmt.Appendf([]byte(`{"seq":`), "%d}", i)}
		if err := leader.Submit(cmd); err != nil {
			t.Fatalf("Submit #%d: %v", i, err)
		}
	}

	// 所有节点日志都追到 total
	deadline := time.Now().Add(5 * time.Second)
	for {
		allCaught := true
		for _, n := range nodes {
			if n.Log().LastIndex() < total {
				allCaught = false
			}
		}
		if allCaught {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("not all nodes replicated %d entries", total)
		}
		time.Sleep(20 * time.Millisecond)
	}

	// follower 也必须把日志落盘（Fix 1：handleAppendEntries 后 SaveLog）
	for id, n := range nodes {
		loaded, err := LoadLog(n.cfg.DataDir)
		if err != nil {
			t.Fatalf("LoadLog(%s): %v", id, err)
		}
		if loaded.LastIndex() < total {
			t.Fatalf("node %s persisted log has %d entries, want %d", id, loaded.LastIndex(), total)
		}
	}

	// leader 的 applyCh 必须收到全部 total 条
	seen := make(map[string]bool)
	applied := time.Now().Add(5 * time.Second)
	for len(seen) < total {
		select {
		case cmd := <-leader.applyCh:
			seen[string(cmd.Payload)] = true
		case <-time.After(time.Until(applied)):
			t.Fatalf("leader applied %d/%d commands", len(seen), total)
		}
	}
}

// TestReplication 提交命令后：applyCh 收到、所有节点日志都包含
func TestReplication(t *testing.T) {
	nodes := newTestCluster(t)
	leader := waitForLeader(t, nodes)

	cmd := Command{Type: "message", Payload: []byte(`{"text":"hi"}`)}
	if err := leader.Submit(cmd); err != nil {
		t.Fatalf("Submit: %v", err)
	}

	// 1) leader 的 applyCh 最终收到这条命令
	select {
	case got := <-leader.applyCh:
		if got.Type != "message" || string(got.Payload) != `{"text":"hi"}` {
			t.Fatalf("applied command = %+v", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("command not applied within 5s")
	}

	// 2) 所有节点的日志最终都包含这条命令
	deadline := time.Now().Add(5 * time.Second)
	for {
		allHave := true
		for _, n := range nodes {
			entry, ok := n.Log().At(1)
			if !ok || string(entry.Command.Payload) != `{"text":"hi"}` {
				allHave = false
			}
		}
		if allHave {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("not all nodes replicated the command")
		}
		time.Sleep(20 * time.Millisecond)
	}
}
