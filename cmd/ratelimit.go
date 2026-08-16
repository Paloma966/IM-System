package main

import (
	"sync"
	"time"
)

// tokenBucket 简单令牌桶：以 rate 令牌/秒补充，最多攒 burst 个令牌。
type tokenBucket struct {
	tokens float64
	last   time.Time
}

// ipLimiter 按客户端 IP 限流。桶数量超过阈值时清理闲置条目，防止内存无界增长。
type ipLimiter struct {
	mu      sync.Mutex
	buckets map[string]*tokenBucket
	rate    float64
	burst   int
}

func newIPLimiter(rate float64, burst int) *ipLimiter {
	return &ipLimiter{
		buckets: make(map[string]*tokenBucket),
		rate:    rate,
		burst:   burst,
	}
}

// allow 尝试消耗 1 个令牌；桶不存在时以满桶初始化。
func (l *ipLimiter) allow(key string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := time.Now()
	b, ok := l.buckets[key]
	if !ok {
		if len(l.buckets) > 10000 {
			l.sweep(now)
		}
		b = &tokenBucket{tokens: float64(l.burst), last: now}
		l.buckets[key] = b
	}

	elapsed := now.Sub(b.last).Seconds()
	b.tokens = min(float64(l.burst), b.tokens+elapsed*l.rate)
	b.last = now

	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// sweep 删除超过 1 分钟未使用的桶
func (l *ipLimiter) sweep(now time.Time) {
	for k, b := range l.buckets {
		if now.Sub(b.last) > time.Minute {
			delete(l.buckets, k)
		}
	}
}

// streamLimiter 限制每个 IP 的并发 SSE 连接数，防止连接耗尽
type streamLimiter struct {
	mu     sync.Mutex
	active map[string]int
	max    int
}

func newStreamLimiter(max int) *streamLimiter {
	return &streamLimiter{active: make(map[string]int), max: max}
}

func (l *streamLimiter) acquire(ip string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.active[ip] >= l.max {
		return false
	}
	l.active[ip]++
	return true
}

func (l *streamLimiter) release(ip string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.active[ip] <= 1 {
		delete(l.active, ip)
		return
	}
	l.active[ip]--
}
