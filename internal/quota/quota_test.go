package quota

import (
	"sync"
	"testing"
	"time"
)

// controllableClock 是测试用可控时钟。
type controllableClock struct{ t time.Time }

func (c *controllableClock) nowFunc() func() time.Time {
	return func() time.Time { return c.t }
}

// 2026-10-05 是周一，方便推算自然周边界。
func baseTime() time.Time { return time.Date(2026, 10, 5, 10, 0, 0, 0, time.UTC) }

func TestPeriodBoundsWeekly(t *testing.T) {
	cases := []struct {
		name string
		now  time.Time
		want time.Time
	}{
		{"monday", time.Date(2026, 10, 5, 10, 0, 0, 0, time.UTC), time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC)},
		{"wednesday", time.Date(2026, 10, 7, 23, 59, 0, 0, time.UTC), time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC)},
		{"sunday", time.Date(2026, 10, 11, 1, 0, 0, 0, time.UTC), time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC)},
		{"next monday resets", time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC), time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			start, reset, ok := periodBounds(tc.now, PeriodWeekly)
			if !ok || !start.Equal(tc.want) {
				t.Fatalf("start=%v ok=%v, want %v", start, ok, tc.want)
			}
			if want := tc.want.AddDate(0, 0, 7); !reset.Equal(want) {
				t.Fatalf("reset=%v, want %v", reset, want)
			}
			t.Logf("now=%s -> window=[%s, %s)", tc.now.Format(time.RFC3339), start, reset)
		})
	}
}

func TestCountLimitExhaustedAndView(t *testing.T) {
	clk := &controllableClock{t: baseTime()}
	m := NewManager(clk.nowFunc())
	if err := m.SetPolicy("c1", Policy{Period: PeriodWeekly, MaxCount: 2}); err != nil {
		t.Fatalf("set policy: %v", err)
	}

	for _, key := range []string{"k1", "k2"} {
		d := m.Hold("c1", key, 1, 1000, "CNY")
		if !d.Allowed {
			t.Fatalf("hold %s should pass: %s", key, d.Reason)
		}
		m.Commit("c1", key)
	}
	u, _ := m.GetUsage("c1")
	t.Logf("after 2 commits: used=%d remaining=%d reset=%s", u.UsedCount, u.RemainingCount(), u.ResetAt)
	if u.UsedCount != 2 || u.RemainingCount() != 0 {
		t.Fatalf("usage=%+v remaining=%d", u, u.RemainingCount())
	}

	d := m.Hold("c1", "k3", 1, 1, "CNY")
	if d.Allowed || d.Reason != ReasonCountExceeded {
		t.Fatalf("third hold allowed=%v reason=%s, want QUOTA_COUNT_EXCEEDED", d.Allowed, d.Reason)
	}
	t.Logf("third hold rejected: reason=%s remaining count=%d resets at %s",
		d.Reason, d.Usage.RemainingCount(), d.Usage.ResetAt.Format(time.RFC3339))
}

func TestAmountLimitExhausted(t *testing.T) {
	clk := &controllableClock{t: baseTime()}
	m := NewManager(clk.nowFunc())
	if err := m.SetPolicy("c1", Policy{Period: PeriodWeekly, MaxAmount: 1500, Currency: "CNY"}); err != nil {
		t.Fatalf("set policy: %v", err)
	}
	if d := m.Hold("c1", "k1", 1, 1000, "CNY"); !d.Allowed {
		t.Fatalf("hold k1: %s", d.Reason)
	}
	m.Commit("c1", "k1")

	d := m.Hold("c1", "k2", 1, 600, "CNY")
	if d.Allowed || d.Reason != ReasonAmountBudget {
		t.Fatalf("hold k2 allowed=%v reason=%s, want amount budget exceeded", d.Allowed, d.Reason)
	}
	t.Logf("1000 committed, hold 600 -> %s, remaining amount=%d/%d resets=%s",
		d.Reason, d.Usage.RemainingAmount(), d.Usage.MaxAmount, d.Usage.ResetAt.Format(time.RFC3339))

	// 币种不匹配单独可区分。
	d2 := m.Hold("c1", "k-x", 1, 1, "USD")
	if d2.Allowed || d2.Reason != ReasonCurrencyMismatch {
		t.Fatalf("USD hold: allowed=%v reason=%s", d2.Allowed, d2.Reason)
	}
}

func TestDuplicateAndConcurrentKeyDeductOnce(t *testing.T) {
	clk := &controllableClock{t: baseTime()}
	m := NewManager(clk.nowFunc())
	if err := m.SetPolicy("c1", Policy{Period: PeriodWeekly, MaxCount: 1}); err != nil {
		t.Fatalf("set policy: %v", err)
	}
	// 首次预占成功。
	if d := m.Hold("c1", "dup", 1, 100, "CNY"); !d.Allowed {
		t.Fatalf("first hold: %s", d.Reason)
	}
	// 同键串行重复 Hold：不再二次扣减，用量不变。
	if d := m.Hold("c1", "dup", 1, 100, "CNY"); !d.Allowed {
		t.Fatalf("duplicate hold should be deduped, got %s", d.Reason)
	}
	m.Commit("c1", "dup")
	m.Commit("c1", "dup") // 重复提交幂等。
	u, _ := m.GetUsage("c1")
	if u.UsedCount != 1 {
		t.Fatalf("used=%d, want 1", u.UsedCount)
	}

	// 新键在额度已满时必须被挡，证明重复提交没有把额度扣穿。
	if d := m.Hold("c1", "other", 1, 1, "CNY"); d.Allowed {
		t.Fatalf("new key must be rejected after budget used")
	}
	t.Logf("same key held/committed repeatedly: usedCount=1, new key correctly rejected")

	// 并发同键：恰好一个真实预占，其余去重，最终用量仍为 1（新策略桶）。
	m2 := NewManager(clk.nowFunc())
	_ = m2.SetPolicy("c2", Policy{Period: PeriodWeekly, MaxCount: 50})
	var wg sync.WaitGroup
	decisions := make([]Decision, 32)
	for i := range decisions {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			decisions[i] = m2.Hold("c2", "storm", 1, 10, "CNY")
		}(i)
	}
	wg.Wait()
	for i, d := range decisions {
		if !d.Allowed {
			t.Fatalf("worker %d denied: %s", i, d.Reason)
		}
	}
	m2.Commit("c2", "storm")
	u2, _ := m2.GetUsage("c2")
	t.Logf("32 concurrent holds same key: allowed all, usedCount=%d (want 1)", u2.UsedCount)
	if u2.UsedCount != 1 {
		t.Fatalf("usedCount=%d, want 1 under concurrent same-key holds", u2.UsedCount)
	}
}

func TestReleaseOnFailureRestoresBudget(t *testing.T) {
	clk := &controllableClock{t: baseTime()}
	m := NewManager(clk.nowFunc())
	_ = m.SetPolicy("c1", Policy{Period: PeriodWeekly, MaxCount: 1, MaxAmount: 500, Currency: "CNY"})

	// 上游失败路径：Hold 后 Release，额度必须原样归还。
	if d := m.Hold("c1", "fail", 1, 500, "CNY"); !d.Allowed {
		t.Fatalf("hold: %s", d.Reason)
	}
	u, _ := m.GetUsage("c1")
	if u.UsedCount != 1 || u.UsedAmount != 500 {
		t.Fatalf("held usage=%+v", u)
	}
	m.Release("c1", "fail")
	u, _ = m.GetUsage("c1")
	if u.UsedCount != 0 || u.UsedAmount != 0 {
		t.Fatalf("after release usage should be zero, got count=%d amount=%d", u.UsedCount, u.UsedAmount)
	}
	// 故障恢复后同键重试可正常预占并提交。
	if d := m.Hold("c1", "fail", 1, 500, "CNY"); !d.Allowed {
		t.Fatalf("retry hold after release: %s", d.Reason)
	}
	m.Commit("c1", "fail")
	u, _ = m.GetUsage("c1")
	t.Logf("released failed hold then retried: usedCount=%d usedAmount=%d", u.UsedCount, u.UsedAmount)
	if u.UsedCount != 1 || u.UsedAmount != 500 {
		t.Fatalf("retry usage: count=%d amount=%d", u.UsedCount, u.UsedAmount)
	}
	// 对已提交的占用再 Release 不应回滚。
	m.Release("c1", "fail")
	u, _ = m.GetUsage("c1")
	if u.UsedCount != 1 {
		t.Fatalf("committed hold must not be released, used=%d", u.UsedCount)
	}
}

func TestWeeklyRolloverResets(t *testing.T) {
	clk := &controllableClock{t: baseTime()} // 周一 10:00
	m := NewManager(clk.nowFunc())
	_ = m.SetPolicy("c1", Policy{Period: PeriodWeekly, MaxCount: 1, MaxAmount: 1000, Currency: "CNY"})
	if d := m.Hold("c1", "k1", 1, 1000, "CNY"); !d.Allowed {
		t.Fatalf("hold k1: %s", d.Reason)
	}
	m.Commit("c1", "k1")
	if d := m.Hold("c1", "k2", 1, 1, "CNY"); d.Allowed {
		t.Fatalf("budget should be exhausted within the week")
	}

	// 推进到下周一 00:00：周期边界固定重置，新键放行。
	clk.t = time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC)
	u, ok := m.GetUsage("c1")
	if !ok || u.UsedCount != 0 || u.UsedAmount != 0 {
		t.Fatalf("after rollover usage=%+v ok=%v", u, ok)
	}
	d := m.Hold("c1", "k2", 1, 1000, "CNY")
	if !d.Allowed {
		t.Fatalf("new week should allow hold, got %s (used=%d/%d amount=%d/%d reset=%s)",
			d.Reason, d.Usage.UsedCount, d.Usage.MaxCount, d.Usage.UsedAmount, d.Usage.MaxAmount, d.Usage.ResetAt)
	}
	t.Logf("rolled over to week of %s: hold k2 allowed, reset=%s", u.PeriodStart, d.Usage.ResetAt)

	// 旧周期已提交的幂等键在新周期重复出现不得再次扣减。
	if d := m.Hold("c1", "k1", 1, 1000, "CNY"); !d.Allowed || d.Usage.UsedCount != 1 {
		t.Fatalf("old committed key must be deduped after rollover: allowed=%v used=%d", d.Allowed, d.Usage.UsedCount)
	}
	t.Logf("old-week committed key k1 in new week: deduped, no double deduction")
}

func TestIsolationBetweenConsents(t *testing.T) {
	clk := &controllableClock{t: baseTime()}
	m := NewManager(clk.nowFunc())
	_ = m.SetPolicy("c-a", Policy{Period: PeriodWeekly, MaxCount: 1})
	_ = m.SetPolicy("c-b", Policy{Period: PeriodWeekly, MaxCount: 1})

	if d := m.Hold("c-a", "a1", 1, 0, ""); !d.Allowed {
		t.Fatalf("hold c-a: %s", d.Reason)
	}
	m.Commit("c-a", "a1")
	// c-a 已用尽。
	if d := m.Hold("c-a", "a2", 1, 0, ""); d.Allowed {
		t.Fatalf("c-a should be exhausted")
	}
	// c-b 完全不受影响，且可独立用尽。
	if d := m.Hold("c-b", "b1", 1, 0, ""); !d.Allowed {
		t.Fatalf("c-b isolated bucket should pass: %s", d.Reason)
	}
	m.Commit("c-b", "b1")
	// 未配置额度的同意一律放行，不被任何其他同意影响。
	if d := m.Hold("c-unconfigured", "u1", 1, 999999999, "ZZZ"); !d.Allowed {
		t.Fatalf("unconfigured consent must always pass: %s", d.Reason)
	}
	if _, ok := m.GetUsage("c-unconfigured"); ok {
		t.Fatalf("unconfigured consent should report no usage view")
	}
	t.Logf("c-a exhausted while c-b served independently; unconfigured consent unaffected")
}

func TestInvalidPolicyRejected(t *testing.T) {
	m := NewManager(baseTime)
	if err := m.SetPolicy("c", Policy{Period: "YEARLY"}); err != ErrInvalidPolicy {
		t.Fatalf("bad period err=%v", err)
	}
	if err := m.SetPolicy("c", Policy{Period: PeriodWeekly, MaxAmount: 10}); err != ErrInvalidPolicy {
		t.Fatalf("amount cap without currency err=%v", err)
	}
	t.Logf("invalid policies rejected with ErrInvalidPolicy")
}
