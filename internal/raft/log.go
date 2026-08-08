package raft

import (
	"encoding/json"
	"sync"
)

// raft复制的命令
type Command struct {
	Type    string
	Payload json.RawMessage
}

// 表示一条raft日志
type LogEntry struct {
	Index   uint64
	Term    uint64
	Command Command
}

// log保存raft日志
type Log struct {
	mu      sync.RWMutex
	entries []LogEntry
}

// 创建带哨兵的log
func NewLog() *Log {

	return &Log{
		entries: []LogEntry{
			{
				Index: 0,
				Term:  0,
			},
		},
	}
}

// 返回最后日志索引
func (l *Log) LastIndex() uint64 {

	l.mu.RLock()
	defer l.mu.RUnlock()

	return uint64(len(l.entries) - 1)
}

// 返回最后日志任期
func (l *Log) LastTerm() uint64 {

	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.entries[len(l.entries)-1].Term
}

// 查询指定index的term
func (l *Log) TermAt(index uint64) (uint64, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if index >= uint64(len(l.entries)) {
		return 0, false
	}

	return l.entries[index].Term, true
}

// 查询指定日志
func (l *Log) At(index uint64) (LogEntry, bool) {

	l.mu.RLock()
	defer l.mu.RUnlock()

	if index >= uint64(len(l.entries)) {
		return LogEntry{}, false
	}

	return l.entries[index], true
}

// 添加日志
func (l *Log) Append(entry LogEntry) {

	l.mu.Lock()
	defer l.mu.Unlock()

	l.entries = append(l.entries, entry)
}

// 批量添加日志
func (l *Log) AppendRange(entries []LogEntry) {

	l.mu.Lock()
	defer l.mu.Unlock()

	l.entries = append(l.entries, entries...)
}

// 返回指定位置之后的日志
func (l *Log) SliceFrom(index uint64) []LogEntry {

	l.mu.RLock()
	defer l.mu.RUnlock()

	return append([]LogEntry(nil), l.entries[index:]...)
}

// 删除index之后的日志
func (l *Log) TruncateFrom(index uint64) {

	l.mu.Lock()
	defer l.mu.Unlock()

	l.entries = l.entries[:index]
}
