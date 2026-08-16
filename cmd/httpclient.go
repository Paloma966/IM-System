package main

import (
	"crypto/tls"
	"crypto/x509"
	"net/http"
	"os"
	"time"
)

// newPeerHTTPClient 构造带超时的内部互访客户端（peer 扇出 / 转发给 Leader）。
// certFile 非空时启用 TLS 并信任该证书文件（集群共享自签名证书场景），
// 同时强制最低 TLS 1.2。
func newPeerHTTPClient(timeout time.Duration, certFile string) *http.Client {
	client := &http.Client{Timeout: timeout}
	if certFile == "" {
		return client
	}

	pem, err := os.ReadFile(certFile)
	if err != nil {
		return client
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return client
	}

	client.Transport = &http.Transport{
		TLSClientConfig: &tls.Config{
			RootCAs:    pool,
			MinVersion: tls.VersionTLS12,
		},
	}
	return client
}
