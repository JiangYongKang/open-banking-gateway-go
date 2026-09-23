// Package ratelimit 实现按租户与客户端维度的固定窗口限流。
package ratelimit

import (
	"sync"
	"time"
)

// Clock 抽象时间源，便于测试中确定性推进窗口。
type Clock func() time.Time

// Limit 定义单维度限流配额。
type Limit struct {
	Requests int
	Window   time.Duration
}

// Decision 是限流判定结果。
type Decision struct {
	Allowed   bool
	Remaining int
	ResetAt   time.Time
}

type bucket struct {
	windowStart time.Time
	count       int
}

// Limiter 是固定窗口限流器，键为 (tenantID, clientID)，
// 不同租户/客户端之间互不影响。
type Limiter struct {
	limit   Limit
	now     Clock
	mu      sync.Mutex
	buckets map[string]*bucket
}

func NewLimiter(limit Limit, now Clock) *Limiter {
	return &Limiter{limit: limit, now: now, buckets: make(map[string]*bucket)}
}

// Allow 判定 (tenantID, clientID) 维度的一次请求是否放行。
// 窗口按 now() 对齐到固定边界，重置行为确定且可复现。
func (l *Limiter) Allow(tenantID, clientID string) Decision {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	key := tenantID + "/" + clientID
	b, ok := l.buckets[key]
	if !ok || !now.Before(b.windowStart.Add(l.limit.Window)) {
		// 对齐窗口起点，保证重置点确定。
		start := now.Truncate(l.limit.Window)
		b = &bucket{windowStart: start}
		l.buckets[key] = b
	}
	resetAt := b.windowStart.Add(l.limit.Window)
	if b.count >= l.limit.Requests {
		return Decision{Allowed: false, Remaining: 0, ResetAt: resetAt}
	}
	b.count++
	return Decision{Allowed: true, Remaining: l.limit.Requests - b.count, ResetAt: resetAt}
}
