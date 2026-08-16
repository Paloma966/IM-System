package raft

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
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

func TestAppendLogAppends(t *testing.T) {
	dir := t.TempDir()

	log := NewLog()
	log.AppendRange([]LogEntry{{Index: 1, Term: 1}, {Index: 2, Term: 1}})
	if err := SaveLog(dir, log); err != nil {
		t.Fatal(err)
	}
	if err := AppendLog(dir, []LogEntry{{Index: 3, Term: 2}, {Index: 4, Term: 2}}); err != nil {
		t.Fatal(err)
	}

	loaded, err := LoadLog(dir)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.LastIndex() != 4 {
		t.Fatalf("LastIndex = %d, want 4", loaded.LastIndex())
	}
	term, ok := loaded.TermAt(3)
	if !ok || term != 2 {
		t.Fatalf("TermAt(3) = %d, %v; want 2, true", term, ok)
	}
}

func TestLoadLogCorruptFails(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, logFile)
	if err := os.WriteFile(path, []byte("{not json\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	if _, err := LoadLog(dir); err == nil {
		t.Fatal("LoadLog should fail on corrupt log, not silently reset")
	}
}

func TestLoadMetaCorruptFails(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, metaFile)
	if err := os.WriteFile(path, []byte("garbage"), 0o644); err != nil {
		t.Fatal(err)
	}

	if _, _, err := LoadMeta(dir); err == nil {
		t.Fatal("LoadMeta should fail on corrupt meta, not silently reset")
	}
}

func TestLoadLogLegacyArrayMigration(t *testing.T) {
	dir := t.TempDir()
	// 旧版格式：整个文件是一个 JSON 数组
	legacy := `[{"index":1,"term":1,"command":{"type":"message","payload":{"text":"hi"}}}]`
	if err := os.WriteFile(filepath.Join(dir, logFile), []byte(legacy), 0o644); err != nil {
		t.Fatal(err)
	}

	loaded, err := LoadLog(dir)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.LastIndex() != 1 || loaded.LastTerm() != 1 {
		t.Fatalf("last=(%d,%d), want (1,1)", loaded.LastIndex(), loaded.LastTerm())
	}
}

func TestNewNodeFailsOnCorruptPersistence(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, logFile), []byte("junk"), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg := Config{
		ID:                "node-1",
		HTTPAddr:          ":8000",
		RaftAddr:          ":9000",
		DataDir:           dir,
		ElectionTimeout:   200 * time.Millisecond,
		HeartbeatInterval: 20 * time.Millisecond,
	}
	if _, err := NewNode(cfg, nil); err == nil {
		t.Fatal("NewNode should fail fast on corrupt persistence instead of starting with empty state")
	}
}
