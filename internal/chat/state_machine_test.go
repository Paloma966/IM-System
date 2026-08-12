package chat

import (
	"encoding/json"
	"testing"

	"IMSystem/internal/raft"
)

func TestApplyAndHistory(t *testing.T) {
	s := NewState()

	payload, _ := json.Marshal(Message{
		ID: "1", From: "alice", To: "all", Text: "hello", Ts: 100,
	})
	if err := s.Apply(raft.Command{Type: "message", Payload: payload}); err != nil {
		t.Fatal(err)
	}

	payload2, _ := json.Marshal(Message{
		ID: "2", From: "bob", To: "alice", Text: "hi", Ts: 101,
	})
	if err := s.Apply(raft.Command{Type: "message", Payload: payload2}); err != nil {
		t.Fatal(err)
	}

	hist := s.History()
	if len(hist) != 2 {
		t.Fatalf("len(History()) = %d, want 2", len(hist))
	}
	if hist[0].From != "alice" || hist[0].Text != "hello" {
		t.Fatalf("hist[0] = %+v", hist[0])
	}
	if hist[1].To != "alice" || hist[1].Text != "hi" {
		t.Fatalf("hist[1] = %+v", hist[1])
	}
}

func TestApplyBadPayload(t *testing.T) {
	s := NewState()
	if err := s.Apply(raft.Command{Type: "message", Payload: []byte("not json")}); err == nil {
		t.Fatal("expected error for bad payload")
	}
}

func TestApplyUnknownTypeIgnored(t *testing.T) {
	s := NewState()
	if err := s.Apply(raft.Command{Type: "rename"}); err != nil {
		t.Fatal(err)
	}
	if len(s.History()) != 0 {
		t.Fatal("unknown command type should be ignored")
	}
}

func TestHistoryIsCopy(t *testing.T) {
	s := NewState()
	payload, _ := json.Marshal(Message{ID: "1", From: "a", To: "all", Text: "x", Ts: 1})
	_ = s.Apply(raft.Command{Type: "message", Payload: payload})

	hist := s.History()
	hist[0].Text = "MUTATED"
	if s.History()[0].Text == "MUTATED" {
		t.Fatal("History() should return a copy, not the internal slice")
	}
}
