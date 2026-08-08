package raft

import "testing"

func TestConfigPeerLookup(t *testing.T) {
	cfg := Config{
		ID:       "node-1",
		HTTPAddr: ":8001",
		RaftAddr: ":9001",
		Peers: []Peer{
			{ID: "node-2", RaftAddr: ":9002", HTTPAddr: ":8002"},
			{ID: "node-3", RaftAddr: ":9003", HTTPAddr: ":8003"},
		},
	}
	p, ok := cfg.Peer("node-2")
	if !ok || p.RaftAddr != ":9002" {
		t.Fatalf("Peer(node-2) = %+v, %v; want :9002, true", p, ok)
	}
	if _, ok := cfg.Peer("node-1"); ok {
		t.Fatal("Peer(node-1) should not find self in Peers")
	}
	if _, ok := cfg.Peer("nope"); ok {
		t.Fatal("Peer(nope) should not be found")
	}
}
