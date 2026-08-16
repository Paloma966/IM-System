package raft

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"
)

// newRPCTestNode 构造一个带 HTTP RPC 入口的测试节点。
// cfg.ID 固定为 node-1，peers 包含 node-2。
func newRPCTestNode(t *testing.T, secret string) (*Node, *http.Server) {
	t.Helper()
	cfg := Config{
		ID:                "node-1",
		HTTPAddr:          ":8001",
		RaftAddr:          ":9001",
		DataDir:           t.TempDir(),
		Peers:             []Peer{{ID: "node-2", RaftAddr: ":9002", HTTPAddr: ":8002"}},
		ElectionTimeout:   200 * time.Millisecond,
		HeartbeatInterval: 20 * time.Millisecond,
	}
	n, err := NewNode(cfg, nil)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	return n, NewRaftHTTPServer(n, secret)
}

// signedRequest 生成带合法签名与时间戳的 RPC 请求
func signedRequest(t *testing.T, secret, path string, body []byte, ts time.Time) *http.Request {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(rpcTsHeader, strconv.FormatInt(ts.Unix(), 10))
	req.Header.Set(rpcSigHeader, signRPC(secret, path, body))
	return req
}

func TestRPCRejectsMissingSignature(t *testing.T) {
	_, srv := newRPCTestNode(t, "s3cret")
	defer srv.Close()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/raft/append",
		bytes.NewReader([]byte(`{"term":1,"leader_id":"node-2"}`)))
	srv.Handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
}

func TestRPCRejectsWrongSecret(t *testing.T) {
	_, srv := newRPCTestNode(t, "s3cret")
	defer srv.Close()

	rec := httptest.NewRecorder()
	req := signedRequest(t, "wrong-secret", "/raft/vote",
		[]byte(`{"term":1,"candidate_id":"node-2"}`), time.Now())
	srv.Handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
}

func TestRPCRejectsStaleTimestamp(t *testing.T) {
	_, srv := newRPCTestNode(t, "s3cret")
	defer srv.Close()

	rec := httptest.NewRecorder()
	req := signedRequest(t, "s3cret", "/raft/vote",
		[]byte(`{"term":1,"candidate_id":"node-2"}`), time.Now().Add(-10*time.Minute))
	srv.Handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
}

func TestRPCRejectsUnknownCandidate(t *testing.T) {
	n, srv := newRPCTestNode(t, "s3cret")
	defer srv.Close()

	rec := httptest.NewRecorder()
	req := signedRequest(t, "s3cret", "/raft/vote",
		[]byte(`{"term":9,"candidate_id":"mallory"}`), time.Now())
	srv.Handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var resp RequestVoteResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if resp.VoteGranted {
		t.Fatal("unknown candidate should not be granted a vote")
	}
	// 伪造请求不能污染任期
	if n.CurrentTerm() != 0 {
		t.Fatalf("term = %d, want 0 (unaffected)", n.CurrentTerm())
	}
}

func TestRPCRejectsSelfLeaderID(t *testing.T) {
	n, srv := newRPCTestNode(t, "s3cret")
	defer srv.Close()

	// 伪装成 "node-1 自己是 leader" 的 AppendEntries 必须被拒绝，
	// 否则节点会把写请求无限转发给自己。
	rec := httptest.NewRecorder()
	req := signedRequest(t, "s3cret", "/raft/append",
		[]byte(`{"term":9,"leader_id":"node-1"}`), time.Now())
	srv.Handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var resp AppendEntriesResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if resp.Success {
		t.Fatal("self leader_id should be rejected")
	}
	if n.LeaderID() != "" {
		t.Fatalf("leaderID = %q, want empty (unaffected)", n.LeaderID())
	}
	if n.CurrentTerm() != 0 {
		t.Fatalf("term = %d, want 0 (unaffected)", n.CurrentTerm())
	}
}

func TestRPCValidVoteAccepted(t *testing.T) {
	n, srv := newRPCTestNode(t, "s3cret")
	defer srv.Close()

	rec := httptest.NewRecorder()
	req := signedRequest(t, "s3cret", "/raft/vote",
		[]byte(`{"term":2,"candidate_id":"node-2","last_log_index":0,"last_log_term":0}`),
		time.Now())
	srv.Handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var resp RequestVoteResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if !resp.VoteGranted {
		t.Fatalf("valid candidate should be granted, got %+v", resp)
	}
	if n.CurrentTerm() != 2 {
		t.Fatalf("term = %d, want 2", n.CurrentTerm())
	}
}

func TestRPCMethodNotAllowed(t *testing.T) {
	_, srv := newRPCTestNode(t, "s3cret")
	defer srv.Close()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/raft/vote", nil)
	srv.Handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", rec.Code)
	}
}

func TestRPCDisabledWithoutSecret(t *testing.T) {
	_, srv := newRPCTestNode(t, "")
	defer srv.Close()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/raft/vote",
		bytes.NewReader([]byte(`{"term":1,"candidate_id":"node-2"}`)))
	srv.Handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
}
