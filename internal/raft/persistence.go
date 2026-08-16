package raft

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
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

// writeFileAtomic 原子写文件：先写同目录临时文件并 fsync，
// 再 rename 覆盖目标（同文件系统内 rename 是原子的），最后 fsync 目录。
// 避免进程崩溃/断电留下半截文件破坏持久化状态。
func writeFileAtomic(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}

	tmp, err := os.CreateTemp(filepath.Dir(path), ".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	// rename 成功后此路径已不存在，Remove 是安全的 no-op；失败时清理残留
	defer os.Remove(tmpName)

	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}

	if err := os.Rename(tmpName, path); err != nil {
		return err
	}

	// fsync 目录，保证 rename 本身落盘（某些平台不支持目录 fsync，尽力而为）
	if dir, err := os.Open(filepath.Dir(path)); err == nil {
		_ = dir.Sync()
		_ = dir.Close()
	}
	return nil
}

// SaveMeta 把 currentTerm 和 votedFor 写到 meta.json（原子写 + fsync）
func SaveMeta(
	dataDir string,
	term uint64,
	votedFor string,
) error {

	m := meta{
		CurrentTerm: term,
		VotedFor:    votedFor,
	}

	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}

	return writeFileAtomic(filepath.Join(dataDir, metaFile), data)
}

// LoadMeta 从 meta.json 恢复任期与投票；文件不存在时返回 (0,"",nil)。
// 文件存在但损坏时返回错误（调用方必须快速失败，不能静默清空任期）。
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
		return 0, "", fmt.Errorf("corrupt %s: %w", path, err)
	}
	return m.CurrentTerm, m.VotedFor, nil
}

// encodeLogLine 把一条日志编码为一行 JSON（JSONL 格式）
func encodeLogLine(entry LogEntry) ([]byte, error) {
	data, err := json.Marshal(entry)
	if err != nil {
		return nil, err
	}
	return append(data, '\n'), nil
}

// SaveLog 把日志（跳过 index=0 的哨兵）整写为 JSONL 文件（原子写 + fsync）。
// 用于首次落盘与截断后的重写；常规追加请用 AppendLog（O(1)）。
func SaveLog(dataDir string, log *Log) error {
	entries := log.SliceFrom(1)

	var buf bytes.Buffer
	for _, e := range entries {
		line, err := encodeLogLine(e)
		if err != nil {
			return err
		}
		buf.Write(line)
	}

	return writeFileAtomic(filepath.Join(dataDir, logFile), buf.Bytes())
}

// AppendLog 把新条目追加到日志文件末尾（O_APPEND + fsync）。
// 每条消息只追加几行，不再全量重写整个文件。
func AppendLog(dataDir string, entries []LogEntry) error {
	if len(entries) == 0 {
		return nil
	}
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return err
	}

	f, err := os.OpenFile(
		filepath.Join(dataDir, logFile),
		os.O_APPEND|os.O_CREATE|os.O_WRONLY,
		0o644,
	)
	if err != nil {
		return err
	}

	var buf bytes.Buffer
	for _, e := range entries {
		line, err := encodeLogLine(e)
		if err != nil {
			f.Close()
			return err
		}
		buf.Write(line)
	}

	if _, err := f.Write(buf.Bytes()); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// LoadLog 从 log.json 恢复日志；文件不存在时返回带哨兵的空日志。
// 支持两种格式：JSONL（当前格式）与旧版 JSON 数组（自动迁移）；
// 文件损坏时返回错误（调用方快速失败，不能静默丢日志）。
func LoadLog(dataDir string) (*Log, error) {
	path := filepath.Join(dataDir, logFile)

	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return NewLog(), nil
	}
	if err != nil {
		return nil, err
	}

	entries, err := decodeLogFile(data)
	if err != nil {
		return nil, fmt.Errorf("corrupt %s: %w", path, err)
	}

	log := NewLog()
	log.AppendRange(entries)
	return log, nil
}

// decodeLogFile 解析日志文件内容：JSONL 或旧版 JSON 数组
func decodeLogFile(data []byte) ([]LogEntry, error) {
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 {
		return nil, nil
	}

	// 旧版格式：整个文件是一个 JSON 数组
	if trimmed[0] == '[' {
		var entries []LogEntry
		if err := json.Unmarshal(trimmed, &entries); err != nil {
			return nil, err
		}
		return entries, nil
	}

	// 当前格式：每行一条 JSON LogEntry
	var entries []LogEntry
	scanner := bufio.NewScanner(bytes.NewReader(data))
	scanner.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		var e LogEntry
		if err := json.Unmarshal(line, &e); err != nil {
			return nil, err
		}
		entries = append(entries, e)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return entries, nil
}
