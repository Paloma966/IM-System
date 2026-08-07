import { useState, useEffect, useRef, useCallback } from 'react'

const API = '' // Vite proxy handles it in dev; same-origin in production

function App() {
  const [name, setName] = useState('')
  const [connected, setConnected] = useState(false)
  const [users, setUsers] = useState([])
  const [messages, setMessages] = useState([])
  const [input, setInput] = useState('')
  const [to, setTo] = useState('') // empty = broadcast
  const eventSource = useRef(null)
  const messagesEnd = useRef(null)

  // auto-scroll to bottom when new messages arrive
  useEffect(() => {
    messagesEnd.current?.scrollIntoView({ behavior: 'smooth' })
  }, [messages])

  // poll online user list every 3 seconds
  useEffect(() => {
    if (!connected) return
    const fetchUsers = async () => {
      try {
        const res = await fetch(`${API}/users`)
        const data = await res.json()
        setUsers(data.users || [])
      } catch { /* ignore */ }
    }
    fetchUsers()
    const id = setInterval(fetchUsers, 3000)
    return () => clearInterval(id)
  }, [connected])

  const connect = async () => {
    if (!name.trim()) return
    await fetch(`${API}/connect`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name: name.trim() }),
    })

    // open SSE stream
    const es = new EventSource(`${API}/stream/${encodeURIComponent(name.trim())}`)
    es.addEventListener('message', (e) => {
      try {
        const evt = JSON.parse(e.data)
        setMessages((prev) => [...prev, { from: evt.from, to: evt.to, text: evt.text, id: Date.now() + Math.random() }])
      } catch { /* malformed event, skip */ }
    })
    es.onerror = () => {
      // EventSource auto-reconnects; if it fails permanently we stay connected
    }
    eventSource.current = es
    setConnected(true)
  }

  const send = async () => {
    if (!input.trim()) return
    const payload = { from: name.trim(), to: to.trim() || 'all', text: input.trim() }
    setInput('')
    try {
      await fetch(`${API}/api/messages`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      })
    } catch { /* ignore */ }
  }

  const handleKeyDown = useCallback((e) => {
    if (e.key === 'Enter') {
      if (!connected) connect()
      else send()
    }
  }, [connected, name, input, to])

  // filter messages based on current chat mode
  const visibleMessages = messages.filter((m) => {
    if (!to) return m.to === 'all' || m.to === ''  // public: only broadcast
    // private: only messages between me and selected user
    return (m.from === name && m.to === to) || (m.from === to && m.to === name)
  })

  if (!connected) {
    return (
      <div className="login-screen">
        <div className="login-card">
          <h1>💬 IM System</h1>
          <p className="subtitle">Enter your name to join the chat</p>
          <input
            className="name-input"
            value={name}
            onChange={(e) => setName(e.target.value)}
            onKeyDown={(e) => e.key === 'Enter' && connect()}
            placeholder="Your name"
            autoFocus
          />
          <button className="btn-primary" onClick={connect} disabled={!name.trim()}>
            Join
          </button>
        </div>
      </div>
    )
  }

  return (
    <div className="chat-layout">
      {/* sidebar */}
      <aside className="sidebar">
        {/* public chat button at top */}
        <button
          className={`public-chat-btn ${!to ? 'active' : ''}`}
          onClick={() => setTo('')}
        >
          📢 Public Chat
        </button>

        <div className="online-title">Online — {users.length}</div>
        <ul className="user-list">
          {users
            .filter((u) => u !== name)  // don't show yourself in the list
            .map((u) => (
              <li
                key={u}
                className={`user-item ${to === u ? 'active' : ''}`}
                onClick={() => setTo(to === u ? '' : u)}
              >
                <span className="dot" />
                {u}
              </li>
            ))}
        </ul>

        {/* username at bottom */}
        <div className="sidebar-footer">
          👤 {name}
        </div>
      </aside>

      {/* main chat area */}
      <main className="chat-main">
        <div className="messages">
          {visibleMessages.length === 0 && (
            <div className="empty-hint">
              {to ? `No messages with ${to} yet.` : 'No messages yet. Say something!'}
            </div>
          )}
          {visibleMessages.map((m) => (
            <div key={m.id} className={`msg ${m.from === name ? 'msg-own' : ''}`}>
              <div className="msg-sender">{m.from}</div>
              <div className="msg-text">{m.text}</div>
            </div>
          ))}
          <div ref={messagesEnd} />
        </div>

        <div className="input-bar">
          <input
            className="msg-input"
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={handleKeyDown}
            placeholder={to ? `Message ${to}...` : 'Type a message...'}
          />
          <button className="btn-send" onClick={send} disabled={!input.trim()}>
            Send
          </button>
        </div>
      </main>
    </div>
  )
}

export default App
