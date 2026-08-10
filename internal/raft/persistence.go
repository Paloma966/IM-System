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

type meta struct {
	CurrentTerm uint64 `json:"current_term"`
	VotedFor    string `json:"voted_for"`
}

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
