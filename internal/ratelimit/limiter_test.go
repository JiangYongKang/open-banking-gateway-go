package ratelimit

import (
	"testing"
	"time"
)

func TestFixedWindowDeterministic(t *testing.T) {
	start := time.Date(2026, 9, 23, 12, 0, 30, 0, time.UTC)
	now := start
	l := NewLimiter(Limit{Requests: 2, Window: time.Minute}, func() time.Time { return now })

	// 窗口对齐到整分钟，重置点固定。
	d1 := l.Allow("t1", "c1")
	d2 := l.Allow("t1", "c1")
	d3 := l.Allow("t1", "c1")
	t.Logf("decisions: %+v / %+v / %+v", d1, d2, d3)
	if !d1.Allowed || d1.Remaining != 1 {
		t.Fatalf("d1=%+v", d1)
	}
	if !d2.Allowed || d2.Remaining != 0 {
		t.Fatalf("d2=%+v", d2)
	}
	if d3.Allowed {
		t.Fatalf("third request must be limited")
	}
	wantReset := time.Date(2026, 9, 23, 12, 1, 0, 0, time.UTC)
	if !d3.ResetAt.Equal(wantReset) {
		t.Fatalf("reset=%v, want %v", d3.ResetAt, wantReset)
	}

	// 推进到下一窗口后计数重置且仍确定。
	now = start.Add(time.Minute)
	d4 := l.Allow("t1", "c1")
	t.Logf("next window: %+v", d4)
	if !d4.Allowed || d4.Remaining != 1 {
		t.Fatalf("d4=%+v, window must reset", d4)
	}

	// 不同客户端独立计数。
	d5 := l.Allow("t1", "c2")
	t.Logf("other client: %+v", d5)
	if !d5.Allowed || d5.Remaining != 1 {
		t.Fatalf("d5=%+v, clients must be isolated", d5)
	}
}
