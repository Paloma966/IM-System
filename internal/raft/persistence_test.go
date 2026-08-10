package raft

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestMetaRoundTrip(t *testing.T) {

	dir := t.TempDir()

	err := SaveMeta(
		dir,
		3,
		"node-2",
	)

	if err != nil {
		t.Fatal(err)
	}

	term, voted, err := LoadMeta(dir)

	if err != nil {
		t.Fatal(err)
	}

	if term != 3 || voted != "node-2" {
		t.Fatalf(
			"loaded=(%d,%q), want (3,node-2)",
			term,
			voted,
		)
	}
}

func TestMetaDefaultsWhenMissing(t *testing.T) {

	dir := t.TempDir()

	term, voted, err := LoadMeta(dir)

	if err != nil {
		t.Fatal(err)
	}

	if term != 0 || voted != "" {
		t.Fatalf(
			"defaults=(%d,%q)",
			term,
			voted,
		)
	}
}

func TestLogRoundTrip(t *testing.T) {

	dir := t.TempDir()

	log := NewLog()

	log.AppendRange([]LogEntry{

		{
			Index: 1,
			Term:  1,

			Command: Command{
				Type: "message",
				Payload: []byte(
					`{"text":"hi"}`,
				),
			},
		},

		{
			Index: 2,
			Term:  1,

			Command: Command{
				Type: "message",
				Payload: []byte(
					`{"text":"yo"}`,
				),
			},
		},
	})

	err := SaveLog(
		dir,
		log,
	)

	if err != nil {
		t.Fatal(err)
	}

	loaded, err := LoadLog(dir)

	if err != nil {
		t.Fatal(err)
	}

	if loaded.LastIndex() != 2 ||
		loaded.LastTerm() != 1 {

		t.Fatalf(
			"last=(%d,%d)",
			loaded.LastIndex(),
			loaded.LastTerm(),
		)
	}

	e, ok := loaded.At(1)
	if !ok {
		t.Fatal("At(1) not found")
	}

	var got, want map[string]string
	if err := json.Unmarshal(e.Command.Payload, &got); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(`{"text":"hi"}`), &want); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("payload=%s", e.Command.Payload)
	}

}

func TestLogDefaultsWhenMissing(t *testing.T) {

	dir := t.TempDir()

	loaded, err := LoadLog(dir)

	if err != nil {
		t.Fatal(err)
	}

	if loaded.LastIndex() != 0 {
		t.Fatalf(
			"LastIndex=%d",
			loaded.LastIndex(),
		)
	}
}
