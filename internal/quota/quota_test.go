package quota

import (
	"errors"
	"sync"
	"testing"
	"time"
)

// testClock 是可控时钟，用于确定性推进周期边界。
type testClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}
func (c *testClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// 2026-10-05 是周一，便于验证自然周边界。
var weekStart = time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC)

func weeklyLimit(count int, amount int64) Limit {
	return Limit{Period: PeriodWeekly, MaxCount: count, MaxAmount: amount}
}

func TestReserveCommitDeductsOncePerKey(t *testing.T) {
	clock := &testClock{now: weekStart.Add(10 * time.Hour)}
	m := NewManager(clock.Now)
	l := weeklyLimit(10, 100000)

	r1, err := m.Reserve("c-1", "k-1", 1000, l)
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	// 同键重复预占（模拟重试）不得重复计数。
	if _, err := m.Reserve("c-1", "k-1", 1000, l); err != nil {
		t.Fatalf("duplicate reserve: %v", err)
	}
	m.Commit(r1)
	st := m.Status("c-1", l)
	t.Logf("after commit: used_count=%d used_amount=%d remaining_count=%d remaining_amount=%d reset_at=%s",
		st.UsedCount, st.UsedAmount, st.RemainingCount, st.RemainingAmount, st.ResetAt.Format(time.RFC3339))
	if st.UsedCount != 1 || st.UsedAmount != 1000 {
		t.Fatalf("used=(%d,%d), want (1,1000): duplicate reserve must count once", st.UsedCount, st.UsedAmount)
	}
}

func TestReleaseRefundsOnFailure(t *testing.T) {
	clock := &testClock{now: weekStart.Add(10 * time.Hour)}
	m := NewManager(clock.Now)
	l := weeklyLimit(1, 1000)

	r, err := m.Reserve("c-1", "k-1", 1000, l)
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	m.Release(r) // 模拟上游失败：全额退回
	st := m.Status("c-1", l)
	t.Logf("after release: used_count=%d used_amount=%d", st.UsedCount, st.UsedAmount)
	if st.UsedCount != 0 || st.UsedAmount != 0 {
		t.Fatalf("used=(%d,%d), want (0,0): failed write must not consume quota", st.UsedCount, st.UsedAmount)
	}
	// 退回后同键可再次预占并成功。
	r2, err := m.Reserve("c-1", "k-1", 1000, l)
	if err != nil {
		t.Fatalf("re-reserve after release: %v", err)
	}
	m.Commit(r2)
	if st := m.Status("c-1", l); st.UsedCount != 1 {
		t.Fatalf("used_count=%d, want 1 after retry+commit", st.UsedCount)
	}
	t.Logf("retry after failure committed, used_count=1")
}

func TestExhaustedIsDistinguishable(t *testing.T) {
	clock := &testClock{now: weekStart.Add(10 * time.Hour)}
	m := NewManager(clock.Now)
	l := weeklyLimit(1, 5000)

	r, _ := m.Reserve("c-1", "k-1", 3000, l)
	m.Commit(r)

	// 笔数已满。
	_, err := m.Reserve("c-1", "k-2", 100, l)
	var ee *ExhaustedError
	if !errors.Is(err, ErrExhausted) || !errors.As(err, &ee) {
		t.Fatalf("err=%v, want ExhaustedError wrapping ErrExhausted", err)
	}
	t.Logf("count exhausted -> err=%v", err)
	if ee.Status.RemainingCount != 0 || ee.Status.RemainingAmount != 2000 {
		t.Fatalf("status=%+v, want remaining_count=0 remaining_amount=2000", ee.Status)
	}
	wantReset := weekStart.AddDate(0, 0, 7)
	if !ee.Status.ResetAt.Equal(wantReset) {
		t.Fatalf("reset_at=%v, want %v (next Monday 00:00 UTC)", ee.Status.ResetAt, wantReset)
	}

	// 金额超限（笔数维度放宽）。
	m2 := NewManager(clock.Now)
	l2 := Limit{Period: PeriodWeekly, MaxAmount: 5000}
	r2, _ := m2.Reserve("c-1", "k-1", 4000, l2)
	m2.Commit(r2)
	_, err = m2.Reserve("c-1", "k-2", 2000, l2)
	if !errors.Is(err, ErrExhausted) {
		t.Fatalf("err=%v, want ErrExhausted for amount overflow", err)
	}
	t.Logf("amount exhausted -> err=%v", err)
}

func TestWeeklyBoundaryReset(t *testing.T) {
	clock := &testClock{now: weekStart.Add(10 * time.Hour)} // 周一 10:00
	m := NewManager(clock.Now)
	l := weeklyLimit(1, 0)

	r, _ := m.Reserve("c-1", "k-1", 100, l)
	m.Commit(r)
	if _, err := m.Reserve("c-1", "k-2", 100, l); !errors.Is(err, ErrExhausted) {
		t.Fatalf("second write in same week should be exhausted, got %v", err)
	}
	t.Logf("same week: second write exhausted as expected")

	// 推进到下周一 00:00 UTC：周期重置。
	clock.Advance(time.Until(weekStart.AddDate(0, 0, 7).Add(time.Hour)))
	r2, err := m.Reserve("c-1", "k-3", 100, l)
	if err != nil {
		t.Fatalf("after weekly reset should pass: %v", err)
	}
	m.Commit(r2)
	st := m.Status("c-1", l)
	t.Logf("next week: used_count=%d reset_at=%s", st.UsedCount, st.ResetAt.Format(time.RFC3339))
	if st.UsedCount != 1 {
		t.Fatalf("used_count=%d, want 1 (previous week must not leak)", st.UsedCount)
	}
}

func TestConsentIsolation(t *testing.T) {
	clock := &testClock{now: weekStart.Add(10 * time.Hour)}
	m := NewManager(clock.Now)
	l := weeklyLimit(1, 1000)

	r, _ := m.Reserve("c-1", "k-1", 1000, l)
	m.Commit(r)
	if _, err := m.Reserve("c-1", "k-2", 1, l); !errors.Is(err, ErrExhausted) {
		t.Fatalf("c-1 should be exhausted, got %v", err)
	}
	// 其他同意的额度完全独立。
	r2, err := m.Reserve("c-2", "k-9", 1000, l)
	if err != nil {
		t.Fatalf("c-2 must not be affected by c-1: %v", err)
	}
	m.Commit(r2)
	t.Logf("c-1 exhausted while c-2 unaffected: isolation OK")
}

func TestConcurrentReserveNeverExceedsLimit(t *testing.T) {
	clock := &testClock{now: weekStart.Add(10 * time.Hour)}
	m := NewManager(clock.Now)
	const max = 20
	l := weeklyLimit(max, 0)

	var wg sync.WaitGroup
	granted := make(chan string, 200)
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := "k-" + string(rune('a'+i%26)) + "-" + string(rune('0'+i/26))
			r, err := m.Reserve("c-1", key, 1, l)
			if err == nil {
				m.Commit(r)
				granted <- key
			}
		}(i)
	}
	wg.Wait()
	close(granted)
	n := len(granted)
	st := m.Status("c-1", l)
	t.Logf("concurrent 200 reserves against limit %d -> granted=%d used_count=%d", max, n, st.UsedCount)
	if n != max || st.UsedCount != max {
		t.Fatalf("granted=%d used=%d, want exactly %d (no overspend under concurrency)", n, st.UsedCount, max)
	}
}

func TestConcurrentSameKeyCountsOnce(t *testing.T) {
	clock := &testClock{now: weekStart.Add(10 * time.Hour)}
	m := NewManager(clock.Now)
	l := weeklyLimit(100, 100000)

	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if r, err := m.Reserve("c-1", "k-same", 500, l); err == nil {
				m.Commit(r)
			}
		}()
	}
	wg.Wait()
	st := m.Status("c-1", l)
	t.Logf("64 concurrent reserves with same key -> used_count=%d used_amount=%d", st.UsedCount, st.UsedAmount)
	if st.UsedCount != 1 || st.UsedAmount != 500 {
		t.Fatalf("used=(%d,%d), want (1,500): same idempotency key must deduct exactly once", st.UsedCount, st.UsedAmount)
	}
}
