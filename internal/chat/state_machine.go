package chat

import (
	"encoding/json"
	"sync"

	"IMSystem/internal/raft"
)

// State 聊天状态机：消费已提交的 Raft 命令，维护消息历史。
// 所有节点都跑同一个 State，因此历史全局一致。
type State struct {
	mu       sync.RWMutex
	messages []Message
}

// NewState 创建空状态机
func NewState() *State {
	return &State{}
}

// Apply 应用一条已提交命令（Raft 保证每条日志只 Apply 一次，这里不用去重）
func (s *State) Apply(cmd raft.Command) error {
	if cmd.Type != "message" {
		return nil // 未知命令类型直接忽略
	}

	var msg Message
	if err := json.Unmarshal(cmd.Payload, &msg); err != nil {
		return err
	}

	s.mu.Lock()
	s.messages = append(s.messages, msg)
	s.mu.Unlock()

	return nil
}

// History 返回全部消息的拷贝（Message 无 slice/map 字段，浅拷贝即可）
func (s *State) History() []Message {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return append([]Message(nil), s.messages...)
}
