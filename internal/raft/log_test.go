package raft

import (
	"encoding/json"
	"testing"
)

func TestLogAppendAndRead(t *testing.T) {
	log := NewLog()

	if got := log.LastIndex(); got != 0 {
		t.Fatalf("LastIndex() = %d, want 0", got)
	}

	log.Append(LogEntry{
		Index: 1,
		Term:  1,
		Command: Command{
			Type:    "message",
			Payload: json.RawMessage(`{"text":"hi"}`),
		},
	})

	log.AppendRange([]LogEntry{
		{
			Index: 2,
			Term:  1,
			Command: Command{
				Type: "message",
			},
		},
		{
			Index: 3,
			Term:  2,
			Command: Command{
				Type: "message",
			},
		},
	})

	if got := log.LastIndex(); got != 3 {
		t.Fatalf("LastIndex() = %d, want 3", got)
	}

	if got := log.LastTerm(); got != 2 {
		t.Fatalf("LastTerm() = %d, want 2", got)
	}

	term, ok := log.TermAt(1)

	if !ok || term != 1 {
		t.Fatalf("TermAt(1) = %d, %v; want 1,true", term, ok)
	}

	if _, ok := log.TermAt(99); ok {
		t.Fatal("TermAt(99) should false")
	}

	entry, ok := log.At(2)

	if !ok || entry.Term != 1 {
		t.Fatalf("At(2) = %+v,%v", entry, ok)
	}
}

func TestLogSliceAndTruncate(t *testing.T) {

	log := NewLog()

	log.AppendRange([]LogEntry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 2},
	})

	got := log.SliceFrom(2)

	if len(got) != 2 ||
		got[0].Index != 2 ||
		got[1].Index != 3 {

		t.Fatalf(
			"SliceFrom(2)=%+v",
			got,
		)
	}

	log.TruncateFrom(2)

	if log.LastIndex() != 1 {
		t.Fatalf(
			"LastIndex after truncate=%d",
			log.LastIndex(),
		)
	}
}
