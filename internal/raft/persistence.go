package raft

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
)

const (
	metaFile = "meta.json"
	logFile  = "log.json"
)

// meta 持久化的任期与投票记录
type meta struct {
	CurrentTerm uint64 `json:"current_term"`
	VotedFor    string `json:"voted_for"`
}

// SaveMeta 把 currentTerm 和 votedFor 写到 meta.json
func SaveMeta(
	dataDir string,
	term uint64,
	votedFor string,
) error {

	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return err
	}

	m := meta{
		CurrentTerm: term,
		VotedFor:    votedFor,
	}

	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}

	path := filepath.Join(dataDir, metaFile)

	return os.WriteFile(path, data, 0644)
}

// LoadMeta 从 meta.json 恢复任期与投票；文件不存在时返回 (0,"",nil)
func LoadMeta(dataDir string) (uint64, string, error) {
	path := filepath.Join(dataDir, metaFile)
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, "", nil
	}

	if err != nil {
		return 0, "", err
	}

	var m meta

	if err := json.Unmarshal(data, &m); err != nil {
		return 0, "", err
	}
	return m.CurrentTerm, m.VotedFor, nil
}

// SaveLog 把日志（跳过 index=0 的哨兵）写到 log.json
func SaveLog(dataDir string, log *Log) error {

	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return err
	}

	entries := log.SliceFrom(1)

	data, err := json.MarshalIndent(
		entries,
		"",
		"  ",
	)

	if err != nil {
		return err
	}

	path := filepath.Join(
		dataDir,
		"log.json",
	)

	return os.WriteFile(
		path,
		data,
		0644,
	)
}

// LoadLog 从 log.json 恢复日志；文件不存在时返回带哨兵的空日志
func LoadLog(dataDir string) (*Log, error) {

	path := filepath.Join(
		dataDir,
		"log.json",
	)

	data, err := os.ReadFile(path)

	if errors.Is(err, os.ErrNotExist) {
		return NewLog(), nil
	}

	if err != nil {
		return nil, err
	}

	var entries []LogEntry

	if err := json.Unmarshal(data, &entries); err != nil {
		return nil, err
	}

	log := NewLog()

	log.AppendRange(entries)

	return log, nil
}
