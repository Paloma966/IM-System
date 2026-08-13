// IM System 前端 —— 原生 JS（无构建步骤）。
// 复刻之前 React 版 App.jsx 的全部行为。

// ---------- 状态 ----------
const state = {
  name: '',
  users: [],     // 在线用户列表
  messages: [],  // 全部消息（群聊 + 私聊）
  to: '',        // 当前聊天对象；'' = 群聊
};

let eventSource = null;
let userPollTimer = null;

// ---------- DOM 引用 ----------
const loginScreen = document.getElementById('login-screen');
const chatLayout = document.getElementById('chat-layout');
const nameInput = document.getElementById('name-input');
const joinBtn = document.getElementById('join-btn');
const publicChatBtn = document.getElementById('public-chat-btn');
const onlineTitle = document.getElementById('online-title');
const userList = document.getElementById('user-list');
const myNameEl = document.getElementById('my-name');
const messagesEl = document.getElementById('messages');
const msgInput = document.getElementById('msg-input');
const sendBtn = document.getElementById('send-btn');

// ---------- 渲染 ----------

// 重绘在线列表：不显示自己；当前私聊对象高亮；同步群聊按钮高亮
function renderUsers() {
  onlineTitle.textContent = `Online — ${state.users.length}`;
  userList.textContent = '';

  state.users
    .filter((u) => u !== state.name)
    .forEach((u) => {
      const li = document.createElement('li');
      li.className = 'user-item' + (state.to === u ? ' active' : '');

      const dot = document.createElement('span');
      dot.className = 'dot';
      li.appendChild(dot);
      li.appendChild(document.createTextNode(u));

      li.addEventListener('click', () => {
        state.to = state.to === u ? '' : u;
        renderUsers();
        renderMessages();
        updatePlaceholder();
      });
      userList.appendChild(li);
    });

  publicChatBtn.classList.toggle('active', state.to === '');
}

// 当前模式下可见的消息
function visibleMessages() {
  if (!state.to) return state.messages.filter((m) => m.to === 'all' || m.to === '');
  return state.messages.filter(
    (m) => (m.from === state.name && m.to === state.to) || (m.from === state.to && m.to === state.name),
  );
}

function renderMessages() {
  const visible = visibleMessages();
  messagesEl.textContent = '';

  if (visible.length === 0) {
    const hint = document.createElement('div');
    hint.className = 'empty-hint';
    hint.textContent = state.to ? `No messages with ${state.to} yet.` : 'No messages yet. Say something!';
    messagesEl.appendChild(hint);
    return;
  }

  visible.forEach((m) => {
    const div = document.createElement('div');
    div.className = 'msg' + (m.from === state.name ? ' msg-own' : '');

    const sender = document.createElement('div');
    sender.className = 'msg-sender';
    sender.textContent = m.from;

    const text = document.createElement('div');
    text.className = 'msg-text';
    text.textContent = m.text;

    div.appendChild(sender);
    div.appendChild(text);
    messagesEl.appendChild(div);
  });

  // 自动滚动到底部
  messagesEl.scrollTop = messagesEl.scrollHeight;
}

function updatePlaceholder() {
  msgInput.placeholder = state.to ? `Message ${state.to}...` : 'Type a message...';
}

// ---------- 数据 ----------

async function connect() {
  const name = nameInput.value.trim();
  if (!name) return;
  state.name = name;

  await fetch('/connect', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ name }),
  });

  // 连接后加载一次历史；后续新消息走 SSE
  try {
    const res = await fetch('/api/messages/history');
    const data = await res.json();
    state.messages = (data.messages || []).map((m) => ({ from: m.from, to: m.to, text: m.text, id: m.id }));
  } catch {
    // 历史不可用，从空开始
  }

  // SSE 实时订阅（EventSource 断线会自动重连）
  eventSource = new EventSource(`/stream/${encodeURIComponent(name)}`);
  eventSource.addEventListener('message', (e) => {
    try {
      const evt = JSON.parse(e.data);
      state.messages = [
        ...state.messages,
        { from: evt.from, to: evt.to, text: evt.text, id: evt.id || Date.now() + Math.random() },
      ];
      renderMessages();
    } catch {
      // 坏事件，忽略
    }
  });

  // 切到聊天界面
  loginScreen.style.display = 'none';
  chatLayout.style.display = '';
  myNameEl.textContent = name;
  renderMessages();
  startUserPolling();
}

async function send() {
  const text = msgInput.value.trim();
  if (!text) return;
  const payload = { from: state.name, to: state.to.trim() || 'all', text };
  msgInput.value = '';
  sendBtn.disabled = true;

  try {
    await fetch('/api/messages', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
  } catch {
    // 忽略发送失败
  }
}

async function fetchUsers() {
  try {
    const res = await fetch('/users');
    const data = await res.json();
    state.users = data.users || [];
    renderUsers();
  } catch {
    // 忽略
  }
}

function startUserPolling() {
  fetchUsers();
  userPollTimer = setInterval(fetchUsers, 3000);
}

// ---------- 事件绑定 ----------

nameInput.addEventListener('input', () => {
  joinBtn.disabled = !nameInput.value.trim();
});
nameInput.addEventListener('keydown', (e) => {
  if (e.key === 'Enter') connect();
});
joinBtn.addEventListener('click', connect);

publicChatBtn.addEventListener('click', () => {
  state.to = '';
  renderUsers();
  renderMessages();
  updatePlaceholder();
});

msgInput.addEventListener('input', () => {
  sendBtn.disabled = !msgInput.value.trim();
});
msgInput.addEventListener('keydown', (e) => {
  if (e.key === 'Enter') send();
});
sendBtn.addEventListener('click', send);
