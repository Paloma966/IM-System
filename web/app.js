// IM System 前端 —— 原生 JS（无构建步骤）。
// 复刻之前 React 版 App.jsx 的全部行为。

// ---------- 状态 ----------
const state = {
  name: '',
  token: '',
  users: [],     // 在线用户列表
  messages: [],  // 全部可见消息（群聊 + 自己参与的私聊）
  to: '',        // 当前聊天对象；'' = 群聊
};

let eventSource = null;
let userPollTimer = null;

// 带会话令牌的请求头（服务端以此校验身份）
function authHeaders() {
  return {
    'Content-Type': 'application/json',
    Authorization: `Bearer ${state.token}`,
  };
}

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

// 按 id 去重合并两组消息，并按时间戳排序。
// 历史与 SSE 会有一段重叠窗口：同一条消息可能两边都收到，
// 用 id 去重即可（服务端生成的 id 全局唯一）。
function mergeMessages(base, incoming) {
  const byId = new Map();
  [...base, ...incoming].forEach((m) => {
    if (m && m.id) byId.set(m.id, m);
  });
  return [...byId.values()].sort((a, b) => (a.ts || 0) - (b.ts || 0));
}

// 拉取历史并合并进本地消息列表（连接/重连后调用，补齐 SSE 断线缺口）
async function loadHistory() {
  try {
    const res = await fetch('/api/messages/history', { headers: authHeaders() });
    if (res.status === 401) {
      alert('登录已失效，请重新加入');
      return;
    }
    const data = await res.json();
    const incoming = (data.messages || []).map((m) => ({ from: m.from, to: m.to, text: m.text, id: m.id, ts: m.ts }));
    state.messages = mergeMessages(state.messages, incoming);
    renderMessages();
  } catch {
    // 历史不可用，保留已有消息
  }
}

async function connect() {
  const name = nameInput.value.trim();
  if (!name) return;
  state.name = name;

  const res = await fetch('/connect', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ name }),
  });
  if (!res.ok) {
    alert('登录失败，请重试');
    return;
  }
  const login = await res.json();
  state.token = login.token;

  // 先开 SSE 再拉历史：两者重叠窗口内的消息靠 id 去重，
  // 保证既不丢也不重复。
  eventSource = new EventSource(`/stream/${encodeURIComponent(name)}?token=${encodeURIComponent(state.token)}`);
  eventSource.addEventListener('message', (e) => {
    try {
      const evt = JSON.parse(e.data);
      state.messages = mergeMessages(state.messages, [
        { from: evt.from, to: evt.to, text: evt.text, id: evt.id, ts: evt.ts },
      ]);
      renderMessages();
    } catch {
      // 坏事件，忽略
    }
  });
  // 首次连接与断线自动重连后都会触发 open：重拉历史补齐断线期间漏掉的消息
  eventSource.addEventListener('open', loadHistory);

  await loadHistory();

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
    const res = await fetch('/api/messages', {
      method: 'POST',
      headers: authHeaders(),
      body: JSON.stringify(payload),
    });
    if (!res.ok) {
      throw new Error(`send failed: ${res.status}`);
    }
  } catch (err) {
    // 发送失败：恢复输入内容并提示，不能让消息静默丢失
    msgInput.value = text;
    alert(`发送失败：${err.message || err}`);
  } finally {
    sendBtn.disabled = false;
    msgInput.focus();
  }
}

async function fetchUsers() {
  try {
    const res = await fetch('/users', { headers: authHeaders() });
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
