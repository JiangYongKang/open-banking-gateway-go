package ratelimit

import (
	"testing"
	"time"

	"github.com/highcumontoa/open-banking-gateway-go/internal/domain"
)

func TestFixedWindowResetsAtDeterministicBoundary(t *testing.T) {
	// 2026-09-23 10:00:30 UTC -> 30s inside the minute window [10:00, 10:01).
	now := time.Date(2026, 9, 23, 10, 0, 30, 0, time.UTC)
	l := NewLimiter(func() time.Time { return now })
	s := Subject{TenantID: "tenant-a", ClientID: "client-1"}
	l.SetRule(s, Rule{RatePerMinute: 2})

	for i := 1; i <= 2; i++ {
		if _, code := l.Allow(s, false); code != "" {
			t.Fatalf("request %d should be allowed", i)
		}
	}
	_, code := l.Allow(s, false)
	if code != domain.ErrRateLimited {
		t.Fatalf("third request must be RATE_LIMITED, got %q", code)
	}
	t.Logf("WINDOW minuteStart=%d used=2/2 -> RATE_LIMITED (third request)", now.Unix()/60)

	// 29s later we are still inside the SAME fixed minute -> still limited.
	now = now.Add(29 * time.Second)
	if _, code := l.Allow(s, false); code != domain.ErrRateLimited {
		t.Fatalf("within same fixed minute, must remain limited, got %q", code)
	}
	t.Logf("WINDOW now=%s still same minuteStart=%d -> RATE_LIMITED (no sliding)", now.Format(time.RFC3339), now.Unix()/60)

	// One more second: new fixed window starts, counter deterministically zeroed.
	now = now.Add(time.Second)
	if _, code := l.Allow(s, false); code != "" {
		t.Fatalf("new window must reset counter, got %q", code)
	}
	snap := l.Snapshot(s)
	t.Logf("RESET now=%s new minuteStart=%d used=%d -> ALLOW (fixed boundary reset)",
		now.Format(time.RFC3339), snap.MinuteStart, snap.MinuteUsed)
	if snap.MinuteUsed != 1 {
		t.Fatalf("expected fresh counter at 1, got %d", snap.MinuteUsed)
	}
}

func TestDailyQuotaOnlyCountsWrites(t *testing.T) {
	now := time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC)
	l := NewLimiter(func() time.Time { return now })
	s := Subject{TenantID: "tenant-a", ClientID: "client-1"}
	l.SetRule(s, Rule{RatePerMinute: 100, QuotaPerDay: 2})

	// Reads never consume the write quota.
	for i := 0; i < 50; i++ {
		if _, code := l.Allow(s, false); code != "" {
			t.Fatalf("read %d unexpectedly limited: %s", i, code)
		}
	}

	if _, code := l.AllowQuota(s); code != "" {
		t.Fatalf("write 1 should be allowed: %s", code)
	}
	if _, code := l.AllowQuota(s); code != "" {
		t.Fatalf("write 2 should be allowed: %s", code)
	}
	if _, code := l.AllowQuota(s); code != domain.ErrQuotaExceeded {
		t.Fatalf("write 3 must be QUOTA_EXCEEDED, got %q", code)
	}
	snap := l.Snapshot(s)
	t.Logf("QUOTA day=%s reads=50 writes=%d/2 -> QUOTA_EXCEEDED (reads do not count)",
		snap.DayKey, snap.DayUsed)
}

func TestQuotaResetsAtUTCDayBoundary(t *testing.T) {
	now := time.Date(2026, 9, 23, 23, 59, 59, 0, time.UTC)
	l := NewLimiter(func() time.Time { return now })
	s := Subject{TenantID: "tenant-a", ClientID: "client-1"}
	l.SetRule(s, Rule{QuotaPerDay: 1})

	if _, code := l.AllowQuota(s); code != "" {
		t.Fatalf("first write: %s", code)
	}
	if _, code := l.AllowQuota(s); code != domain.ErrQuotaExceeded {
		t.Fatalf("second write same day: %q", code)
	}

	now = now.Add(time.Second) // 2026-09-24 00:00:00 UTC
	if _, code := l.AllowQuota(s); code != "" {
		t.Fatalf("quota must reset at UTC midnight, got %q", code)
	}
	t.Logf("DAILY-RESET day rolls %s -> %s: write ALLOW again",
		time.Date(2026, 9, 23, 23, 59, 59, 0, time.UTC).Format("2006-01-02"), now.Format("2006-01-02"))
}

func TestTenantAndClientCountersAreIndependent(t *testing.T) {
	now := time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC)
	l := NewLimiter(func() time.Time { return now })
	a := Subject{TenantID: "tenant-a", ClientID: "client-1"}
	b := Subject{TenantID: "tenant-a", ClientID: "client-2"}
	c := Subject{TenantID: "tenant-b", ClientID: "client-1"}
	l.SetRule(a, Rule{RatePerMinute: 1, QuotaPerDay: 1})
	l.SetRule(b, Rule{RatePerMinute: 1, QuotaPerDay: 1})
	l.SetRule(c, Rule{RatePerMinute: 1, QuotaPerDay: 1})

	// Exhaust tenant-a/client-1 on both windows.
	if _, code := l.Allow(a, true); code != "" {
		t.Fatalf("a: %s", code)
	}
	if _, code := l.Allow(a, true); code != domain.ErrRateLimited {
		t.Fatalf("a second call: %q", code)
	}
	if _, code := l.AllowQuota(a); code != domain.ErrQuotaExceeded {
		t.Fatalf("a quota: %q", code)
	}

	// Other subjects must be completely unaffected.
	for name, other := range map[string]Subject{"a/client-2": b, "tenant-b/client-1": c} {
		if _, code := l.Allow(other, true); code != "" {
			t.Fatalf("exhausting a must not affect %s: %s", name, code)
		}
		snap := l.Snapshot(other)
		t.Logf("ISOLATION after a exhausted: subject=%s minuteUsed=%d dayUsed=%d unaffected",
			name, snap.MinuteUsed, snap.DayUsed)
	}
}

func TestNoRuleMeansOpen(t *testing.T) {
	l := NewLimiter(nil)
	s := Subject{TenantID: "tenant-x", ClientID: "client-x"}
	for i := 0; i < 1000; i++ {
		if _, code := l.Allow(s, true); code != "" {
			t.Fatalf("unconfigured subject must be open: %s", code)
		}
	}
	t.Logf("NO-RULE subject=%s 1000 write requests allowed (open default)", s)
}
