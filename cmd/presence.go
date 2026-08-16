package main

import (
	"encoding/json"
	"net/http"
	"sort"
	"time"

	"IMSystem/internal/raft"
)

// peerUserTimeout 扇出拉取单个 peer 在线列表的超时。设短一点，避免某个
// peer 挂掉时把 /users 整个拖慢；前端每 3 秒轮询一次，量很小。
const peerUserTimeout = 500 * time.Millisecond

// peerClient 复用的扇出客户端（不再每次请求新建 http.Client）
var peerClient = &http.Client{Timeout: peerUserTimeout}

// localUsers 返回本节点在线用户（已排序）。数据源是本地 users map。
func localUsers() []string {
	mu.RLock()
	defer mu.RUnlock()

	list := make([]string, 0, len(users))
	for name := range users {
		list = append(list, name)
	}
	sort.Strings(list)
	return list
}

// mergeUsers 合并多组用户，去重并排序（纯函数，便于单测）。
func mergeUsers(sets ...[]string) []string {
	seen := make(map[string]bool)
	for _, set := range sets {
		for _, name := range set {
			seen[name] = true
		}
	}

	list := make([]string, 0, len(seen))
	for name := range seen {
		list = append(list, name)
	}
	sort.Strings(list)
	return list
}

// globalUsers 聚合全局在线用户：本地 + 各 peer 的本地集合。
// 任一 peer 失败/超时就跳过（优雅降级），不会拖垮整个 /users。
// secret 是集群共享密钥，peer 的 ?local=1 接口用它验证来源。
func globalUsers(peers []raft.Peer, secret string) []string {
	sets := [][]string{localUsers()}
	for _, peer := range peers {
		sets = append(sets, fetchPeerUsers(peerClient, peer, secret))
	}
	return mergeUsers(sets...)
}

// fetchPeerUsers 向 peer 拉取其本地在线用户；失败/超时返回 nil。
// 走 ?local=1，只取该节点的本地集合，避免 /users 递归；带共享密钥认证。
func fetchPeerUsers(client *http.Client, peer raft.Peer, secret string) []string {
	req, err := http.NewRequest(http.MethodGet, "http://"+peer.HTTPAddr+"/users?local=1", nil)
	if err != nil {
		return nil
	}
	if secret != "" {
		req.Header.Set("X-Node-Secret", secret)
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()

	var body struct {
		Users []string `json:"users"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return nil
	}
	return body.Users
}
