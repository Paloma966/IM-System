package chat

// Message 一条聊天消息，也是 SSE 推给浏览器的格式。
// ID 和 Ts 由 cmd/main.go 在追加日志时生成，保证 3 个节点 Apply 后状态一致。
type Message struct {
	ID   string `json:"id"`
	From string `json:"from"`
	To   string `json:"to"`
	Text string `json:"text"`
	Ts   int64  `json:"ts"`
}
