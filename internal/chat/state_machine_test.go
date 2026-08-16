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

func TestFilterVisible(t *testing.T) {
	msgs := []Message{
		{ID: "1", From: "alice", To: "all", Text: "group"},
		{ID: "2", From: "bob", To: "alice", Text: "private to alice"},
		{ID: "3", From: "bob", To: "carol", Text: "private bob-carol"},
		{ID: "4", From: "carol", To: "", Text: "legacy group"},
	}

	// alice 可见：群聊 + 自己参与的私聊
	got := FilterVisible(msgs, "alice")
	if len(got) != 3 || got[0].ID != "1" || got[1].ID != "2" || got[2].ID != "4" {
		t.Fatalf("alice visible = %+v", got)
	}

	// carol 可见：群聊 + 自己参与的私聊
	got = FilterVisible(msgs, "carol")
	if len(got) != 3 || got[0].ID != "1" || got[1].ID != "3" || got[2].ID != "4" {
		t.Fatalf("carol visible = %+v", got)
	}
}

// TestApplyNormalizesEmptyTo 空 to 归一化为 "all"，保证状态机输出编码一致
func TestApplyNormalizesEmptyTo(t *testing.T) {
	s := NewState()
	payload, _ := json.Marshal(Message{ID: "1", From: "a", To: "", Text: "x", Ts: 1})
	if err := s.Apply(raft.Command{Type: "message", Payload: payload}); err != nil {
		t.Fatal(err)
	}

	hist := s.History()
	if hist[0].To != "all" {
		t.Fatalf("To = %q, want %q", hist[0].To, "all")
	}
}

// TestLast 返回最新消息；空状态返回 false
func TestLast(t *testing.T) {
	s := NewState()
	if _, ok := s.Last(); ok {
		t.Fatal("Last() on empty state should return false")
	}

	payload, _ := json.Marshal(Message{ID: "1", From: "a", To: "all", Text: "x", Ts: 1})
	_ = s.Apply(raft.Command{Type: "message", Payload: payload})
	payload2, _ := json.Marshal(Message{ID: "2", From: "b", To: "all", Text: "y", Ts: 2})
	_ = s.Apply(raft.Command{Type: "message", Payload: payload2})

	last, ok := s.Last()
	if !ok || last.ID != "2" {
		t.Fatalf("Last() = %+v, %v; want id 2", last, ok)
	}
}
