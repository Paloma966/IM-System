package main

import (
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"IMSystem/internal/raft"
)

func TestMergeUsers(t *testing.T) {
	got := mergeUsers(
		[]string{"bob", "alice"},
		[]string{"alice", "carol"},
		nil,
		[]string{"bob"},
	)
	want := []string{"alice", "bob", "carol"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("mergeUsers = %v, want %v", got, want)
	}
}

func TestFetchPeerUsersOK(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("local") != "1" {
			t.Errorf("want local=1, got %q", r.URL.RawQuery)
		}
		if r.Header.Get("X-Node-Secret") != "s3cret" {
			t.Errorf("want X-Node-Secret=s3cret, got %q", r.Header.Get("X-Node-Secret"))
		}
		_, _ = w.Write([]byte(`{"users":["alice","bob"]}`))
	}))
	defer srv.Close()

	// srv.URL 形如 http://127.0.0.1:port，去掉 scheme 前缀当作 host:port
	addr := srv.URL[len("http://"):]
	got := fetchPeerUsers(&http.Client{Timeout: time.Second}, raft.Peer{HTTPAddr: addr}, "s3cret")
	want := []string{"alice", "bob"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("fetchPeerUsers = %v, want %v", got, want)
	}
}

func TestFetchPeerUsersUnreachable(t *testing.T) {
	// 127.0.0.1:1 基本没有服务监听，连接被拒 → 返回 nil
	got := fetchPeerUsers(&http.Client{Timeout: time.Second}, raft.Peer{HTTPAddr: "127.0.0.1:1"}, "")
	if got != nil {
		t.Fatalf("fetchPeerUsers(unreachable) = %v, want nil", got)
	}
}
