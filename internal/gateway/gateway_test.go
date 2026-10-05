package gateway

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/highcumontoa/open-banking-gateway-go/internal/consent"
	"github.com/highcumontoa/open-banking-gateway-go/internal/idempotency"
	"github.com/highcumontoa/open-banking-gateway-go/internal/ledger"
	"github.com/highcumontoa/open-banking-gateway-go/internal/quota"
	"github.com/highcumontoa/open-banking-gateway-go/internal/ratelimit"
)

// testClock 是可控时钟，用于确定性推进限流窗口。
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

type fixture struct {
	gw        *Gateway
	store     *consent.MemoryStore
	lg        *ledger.Mock
	quotas    *quota.Manager
	clock     *testClock
	consentID string
}

func newFixture(t *testing.T, limit ratelimit.Limit) *fixture {
	t.Helper()
	clock := &testClock{now: time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)}
	store := consent.NewMemoryStore()
	if err := store.Create(consent.Consent{
		ID: "c-1", TenantID: "tenant-a", ClientID: "client-1",
		Scopes: []string{"payment:write"}, ExpiresAt: clock.now.Add(time.Hour),
	}); err != nil {
		t.Fatalf("seed consent: %v", err)
	}
	lg := ledger.NewMock()
	qm := quota.NewManager(clock.Now)
	gw := New(
		consent.NewAuthorizer(store, clock.Now),
		idempotency.NewMemoryStore(),
		ratelimit.NewLimiter(limit, clock.Now),
		qm,
		lg,
		clock.Now,
	)
	return &fixture{gw: gw, store: store, lg: lg, quotas: qm, clock: clock, consentID: "c-1"}
}

// addConsentWithQuota 追加一条带周期额度的同意，返回其 ID。
func (f *fixture) addConsentWithQuota(t *testing.T, id string, l quota.Limit) string {
	t.Helper()
	if err := f.store.Create(consent.Consent{
		ID: id, TenantID: "tenant-a", ClientID: "client-1",
		Scopes: []string{"payment:write"}, ExpiresAt: f.clock.Now().Add(time.Hour),
		Quota: &l,
	}); err != nil {
		t.Fatalf("seed consent %s: %v", id, err)
	}
	return id
}

func (f *fixture) req(key string) Request {
	return Request{
		TenantID: "tenant-a", ClientID: "client-1", ConsentID: f.consentID,
		RequiredScopes: []string{"payment:write"}, IdempotencyKey: key,
		AccountID: "acct-1", Amount: 1000, Currency: "CNY", Memo: "test",
	}
}

func TestIdempotentReplay(t *testing.T) {
	f := newFixture(t, ratelimit.Limit{Requests: 100, Window: time.Minute})
	r1, err := f.gw.HandleWrite(f.req("k-1"))
	if err != nil {
		t.Fatalf("first write: %v", err)
	}
	r2, err := f.gw.HandleWrite(f.req("k-1"))
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	t.Logf("first entry=%s replayed=%v; second entry=%s replayed=%v; ledger entries=%d",
		r1.Entry.ID, r1.Replayed, r2.Entry.ID, r2.Replayed, len(f.lg.Entries("tenant-a")))
	if !r2.Replayed || r1.Entry.ID != r2.Entry.ID {
		t.Fatalf("replay mismatch: %+v vs %+v", r1, r2)
	}
	if n := len(f.lg.Entries("tenant-a")); n != 1 {
		t.Fatalf("ledger entries=%d, want 1 (no duplicate posting)", n)
	}
}

func TestIdempotencyKeyConflict(t *testing.T) {
	f := newFixture(t, ratelimit.Limit{Requests: 100, Window: time.Minute})
	if _, err := f.gw.HandleWrite(f.req("k-1")); err != nil {
		t.Fatalf("first write: %v", err)
	}
	mutated := f.req("k-1")
	mutated.Amount = 5000
	_, err := f.gw.HandleWrite(mutated)
	t.Logf("key=k-1 reused with amount 1000->5000 -> err=%v", err)
	ge, ok := err.(*Error)
	if !ok || ge.Code != CodeIdempotencyConflict {
		t.Fatalf("err=%v, want IDEMPOTENCY_CONFLICT", err)
	}
	if n := len(f.lg.Entries("tenant-a")); n != 1 {
		t.Fatalf("ledger entries=%d, want 1", n)
	}
}

func TestConcurrentDuplicateSubmission(t *testing.T) {
	f := newFixture(t, ratelimit.Limit{Requests: 1000, Window: time.Minute})
	const workers = 32
	var wg sync.WaitGroup
	results := make([]error, workers)
	entries := make([]string, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			resp, err := f.gw.HandleWrite(f.req("k-concurrent"))
			if err != nil {
				results[i] = err
				return
			}
			entries[i] = resp.Entry.ID
		}(i)
	}
	wg.Wait()

	seen := map[string]int{}
	for _, id := range entries {
		if id != "" {
			seen[id]++
		}
	}
	n := len(f.lg.Entries("tenant-a"))
	t.Logf("workers=%d ledger entries=%d distinct entry ids=%v", workers, n, seen)
	if n != 1 {
		t.Fatalf("ledger entries=%d, want exactly 1 under concurrent duplicates", n)
	}
	if len(seen) != 1 {
		t.Fatalf("distinct entry ids=%d, want 1 (stable result under retries)", len(seen))
	}
}

func TestRateLimitWindowAndIsolation(t *testing.T) {
	f := newFixture(t, ratelimit.Limit{Requests: 2, Window: time.Minute})
	// 前两个请求放行，第三个被限流。
	for i, key := range []string{"rl-1", "rl-2"} {
		if _, err := f.gw.HandleWrite(f.req(key)); err != nil {
			t.Fatalf("request %d should pass: %v", i+1, err)
		}
	}
	_, err := f.gw.HandleWrite(f.req("rl-3"))
	t.Logf("third request in window -> err=%v", err)
	if ge, ok := err.(*Error); !ok || ge.Code != CodeRateLimited {
		t.Fatalf("err=%v, want RATE_LIMITED", err)
	}

	// 其他租户不受限流影响。
	other := f.req("rl-other")
	other.TenantID = "tenant-b"
	other.ConsentID = "c-b"
	if err := f.store.Create(consent.Consent{
		ID: "c-b", TenantID: "tenant-b", ClientID: "client-1",
		Scopes: []string{"payment:write"}, ExpiresAt: f.clock.Now().Add(time.Hour),
	}); err != nil {
		t.Fatalf("seed consent b: %v", err)
	}
	if _, err := f.gw.HandleWrite(other); err != nil {
		t.Fatalf("other tenant should not be limited: %v", err)
	}
	t.Logf("tenant-b request passed while tenant-a limited")

	// 窗口重置后恢复放行。
	f.clock.Advance(time.Minute)
	if _, err := f.gw.HandleWrite(f.req("rl-4")); err != nil {
		t.Fatalf("after window reset should pass: %v", err)
	}
	t.Logf("window reset at %v, request passed", f.clock.Now())
}

func TestUpstreamErrorNormalization(t *testing.T) {
	cases := []struct {
		name     string
		failWith error
		want     Code
	}{
		{"upstream rejected", ledger.ErrUpstreamRejected, CodeUpstreamRejected},
		{"upstream timeout", ledger.ErrUpstreamTimeout, CodeUpstreamTimeout},
		{"unknown internal", ledger.ErrInternal, CodeInternal},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, ratelimit.Limit{Requests: 100, Window: time.Minute})
			f.lg.FailWith = tc.failWith
			_, err := f.gw.HandleWrite(f.req("k-" + tc.name))
			t.Logf("ledger fails with %v -> gateway err=%v", tc.failWith, err)
			ge, ok := err.(*Error)
			if !ok || ge.Code != tc.want {
				t.Fatalf("err=%v, want code %s", err, tc.want)
			}
			// 失败后幂等占用被释放，恢复故障后同键重试可成功。
			f.lg.FailWith = nil
			if _, err := f.gw.HandleWrite(f.req("k-" + tc.name)); err != nil {
				t.Fatalf("retry after recovery should succeed: %v", err)
			}
			t.Logf("retry with same key after recovery succeeded")
		})
	}
}

func TestConsentDeniedBeforeAnything(t *testing.T) {
	f := newFixture(t, ratelimit.Limit{Requests: 100, Window: time.Minute})
	r := f.req("k-denied")
	r.ConsentID = "ghost"
	_, err := f.gw.HandleWrite(r)
	t.Logf("missing consent -> err=%v", err)
	if ge, ok := err.(*Error); !ok || ge.Code != CodeConsentDenied {
		t.Fatalf("err=%v, want CONSENT_DENIED", err)
	}
	if n := len(f.lg.Entries("tenant-a")); n != 0 {
		t.Fatalf("denied request must not post to ledger, got %d entries", n)
	}
}

// ---------- 同意级周期额度 ----------

// weeklyQuota 是每周（周一 00:00 UTC 对齐）的额度配置。
func weeklyQuota(count int, amount int64) quota.Limit {
	return quota.Limit{Period: quota.PeriodWeekly, MaxCount: count, MaxAmount: amount}
}

func (f *fixture) quotaReq(consentID, key string, amount int64) Request {
	r := f.req(key)
	r.ConsentID = consentID
	r.Amount = amount
	return r
}

func TestQuotaExhaustedDistinguishable(t *testing.T) {
	f := newFixture(t, ratelimit.Limit{Requests: 100, Window: time.Minute})
	cid := f.addConsentWithQuota(t, "c-q", weeklyQuota(2, 0))

	for _, k := range []string{"q-1", "q-2"} {
		if _, err := f.gw.HandleWrite(f.quotaReq(cid, k, 1000)); err != nil {
			t.Fatalf("write %s should pass: %v", k, err)
		}
	}
	_, err := f.gw.HandleWrite(f.quotaReq(cid, "q-3", 1000))
	t.Logf("third write in week -> err=%v", err)
	ge, ok := err.(*Error)
	if !ok || ge.Code != CodeQuotaExceeded {
		t.Fatalf("err=%v, want QUOTA_EXCEEDED", err)
	}
	if ge.Code == CodeConsentDenied || ge.Code == CodeRateLimited {
		t.Fatalf("quota exhaustion must be distinguishable from consent/rate-limit denials, got %s", ge.Code)
	}
	// 拒绝信息需包含本周期剩余与重置时间（下周一 00:00 UTC）。
	if !strings.Contains(ge.Message, "remaining_count=0") ||
		!strings.Contains(ge.Message, "reset_at=2026-09-28T00:00:00Z") {
		t.Fatalf("message=%q, want remaining and reset time", ge.Message)
	}
	if n := len(f.lg.Entries("tenant-a")); n != 2 {
		t.Fatalf("ledger entries=%d, want 2 (rejected write must not post)", n)
	}
}

func TestQuotaConcurrentSameKeyDeductsOnce(t *testing.T) {
	f := newFixture(t, ratelimit.Limit{Requests: 1000, Window: time.Minute})
	cid := f.addConsentWithQuota(t, "c-q", weeklyQuota(5, 0))

	const workers = 32
	var wg sync.WaitGroup
	entries := make([]string, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			// 模拟客户端超时后的重试风暴：同一幂等键并发重发。
			if resp, err := f.gw.HandleWrite(f.quotaReq(cid, "k-storm", 1000)); err == nil {
				entries[i] = resp.Entry.ID
			}
		}(i)
	}
	wg.Wait()

	seen := map[string]bool{}
	for _, id := range entries {
		if id != "" {
			seen[id] = true
		}
	}
	st, err := f.gw.QuotaStatus("tenant-a", cid)
	if err != nil {
		t.Fatalf("quota status: %v", err)
	}
	t.Logf("retry storm: distinct entries=%v ledger=%d used_count=%d remaining_count=%d",
		seen, len(f.lg.Entries("tenant-a")), st.UsedCount, st.RemainingCount)
	if len(seen) != 1 || len(f.lg.Entries("tenant-a")) != 1 {
		t.Fatalf("retry storm must converge to one result, entries=%v", seen)
	}
	if st.UsedCount != 1 {
		t.Fatalf("used_count=%d, want 1: same idempotency key deducts exactly once", st.UsedCount)
	}
}

func TestQuotaWeeklyReset(t *testing.T) {
	f := newFixture(t, ratelimit.Limit{Requests: 100, Window: time.Minute})
	// 同意有效期覆盖跨周推进。
	l := weeklyQuota(1, 0)
	if err := f.store.Create(consent.Consent{
		ID: "c-q", TenantID: "tenant-a", ClientID: "client-1",
		Scopes: []string{"payment:write"}, ExpiresAt: f.clock.Now().Add(10 * 24 * time.Hour),
		Quota: &l,
	}); err != nil {
		t.Fatalf("seed consent: %v", err)
	}

	if _, err := f.gw.HandleWrite(f.quotaReq("c-q", "w-1", 1000)); err != nil {
		t.Fatalf("first write: %v", err)
	}
	if _, err := f.gw.HandleWrite(f.quotaReq("c-q", "w-2", 1000)); err == nil {
		t.Fatal("second write in same week should be quota-exhausted")
	}
	st, _ := f.gw.QuotaStatus("tenant-a", "c-q")
	t.Logf("same week: used=%d remaining=%d reset_at=%s", st.UsedCount, st.RemainingCount, st.ResetAt.Format(time.RFC3339))

	// 推进到下周一 00:00 UTC 之后：周期重置，同一幂等键可重新执行。
	f.clock.Advance(5 * 24 * time.Hour)
	if _, err := f.gw.HandleWrite(f.quotaReq("c-q", "w-2", 1000)); err != nil {
		t.Fatalf("after weekly reset should pass: %v", err)
	}
	st, _ = f.gw.QuotaStatus("tenant-a", "c-q")
	t.Logf("next week: used=%d remaining=%d reset_at=%s", st.UsedCount, st.RemainingCount, st.ResetAt.Format(time.RFC3339))
	if st.UsedCount != 1 || st.RemainingCount != 0 {
		t.Fatalf("used=%d remaining=%d, want 1/0 in new week", st.UsedCount, st.RemainingCount)
	}
}

func TestQuotaNotDeductedOnUpstreamFailure(t *testing.T) {
	cases := []struct {
		name     string
		failWith error
	}{
		{"upstream rejected", ledger.ErrUpstreamRejected},
		{"upstream timeout", ledger.ErrUpstreamTimeout},
		{"internal error", ledger.ErrInternal},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, ratelimit.Limit{Requests: 100, Window: time.Minute})
			cid := f.addConsentWithQuota(t, "c-q", weeklyQuota(1, 0))

			f.lg.FailWith = tc.failWith
			if _, err := f.gw.HandleWrite(f.quotaReq(cid, "k-f", 1000)); err == nil {
				t.Fatal("write should fail")
			}
			st, _ := f.gw.QuotaStatus("tenant-a", cid)
			t.Logf("%s: write failed, used_count=%d (want 0)", tc.name, st.UsedCount)
			if st.UsedCount != 0 {
				t.Fatalf("used_count=%d, want 0: failed write must not consume quota", st.UsedCount)
			}

			// 故障恢复后同键重试正常扣减。
			f.lg.FailWith = nil
			if _, err := f.gw.HandleWrite(f.quotaReq(cid, "k-f", 1000)); err != nil {
				t.Fatalf("retry after recovery: %v", err)
			}
			st, _ = f.gw.QuotaStatus("tenant-a", cid)
			t.Logf("%s: retry succeeded, used_count=%d (want 1)", tc.name, st.UsedCount)
			if st.UsedCount != 1 {
				t.Fatalf("used_count=%d, want 1 after recovery retry", st.UsedCount)
			}
		})
	}
}

func TestQuotaIsolationBetweenConsents(t *testing.T) {
	f := newFixture(t, ratelimit.Limit{Requests: 100, Window: time.Minute})
	c1 := f.addConsentWithQuota(t, "c-q1", weeklyQuota(1, 0))
	c2 := f.addConsentWithQuota(t, "c-q2", weeklyQuota(1, 0))

	if _, err := f.gw.HandleWrite(f.quotaReq(c1, "i-1", 1000)); err != nil {
		t.Fatalf("c-q1 write: %v", err)
	}
	if _, err := f.gw.HandleWrite(f.quotaReq(c1, "i-2", 1000)); err == nil {
		t.Fatal("c-q1 should be exhausted")
	}
	// 另一个带额度的同意不受影响。
	if _, err := f.gw.HandleWrite(f.quotaReq(c2, "i-3", 1000)); err != nil {
		t.Fatalf("c-q2 must not be affected by c-q1: %v", err)
	}
	// 未配置额度的同意完全不受影响。
	if _, err := f.gw.HandleWrite(f.req("i-4")); err != nil {
		t.Fatalf("consent without quota must be unaffected: %v", err)
	}
	t.Logf("c-q1 exhausted; c-q2 and quota-less c-1 unaffected")
}

func TestQuotaAmountLimit(t *testing.T) {
	f := newFixture(t, ratelimit.Limit{Requests: 100, Window: time.Minute})
	cid := f.addConsentWithQuota(t, "c-q", weeklyQuota(0, 5000))

	if _, err := f.gw.HandleWrite(f.quotaReq(cid, "a-1", 3000)); err != nil {
		t.Fatalf("first write: %v", err)
	}
	_, err := f.gw.HandleWrite(f.quotaReq(cid, "a-2", 3000))
	t.Logf("amount overflow -> err=%v", err)
	if ge, ok := err.(*Error); !ok || ge.Code != CodeQuotaExceeded {
		t.Fatalf("err=%v, want QUOTA_EXCEEDED", err)
	} else if !strings.Contains(ge.Message, "remaining_amount=2000") {
		t.Fatalf("message=%q, want remaining_amount=2000", ge.Message)
	}
	// 剩余额度内的写入仍放行。
	if _, err := f.gw.HandleWrite(f.quotaReq(cid, "a-3", 2000)); err != nil {
		t.Fatalf("write within remaining amount should pass: %v", err)
	}
	st, _ := f.gw.QuotaStatus("tenant-a", cid)
	t.Logf("amount used=%d remaining=%d", st.UsedAmount, st.RemainingAmount)
	if st.UsedAmount != 5000 || st.RemainingAmount != 0 {
		t.Fatalf("used=%d remaining=%d, want 5000/0", st.UsedAmount, st.RemainingAmount)
	}
}

func TestQuotaStatusQuery(t *testing.T) {
	f := newFixture(t, ratelimit.Limit{Requests: 100, Window: time.Minute})
	cid := f.addConsentWithQuota(t, "c-q", weeklyQuota(3, 0))

	if _, err := f.gw.HandleWrite(f.quotaReq(cid, "s-1", 1000)); err != nil {
		t.Fatalf("write: %v", err)
	}
	st, err := f.gw.QuotaStatus("tenant-a", cid)
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	t.Logf("status: used=%d remaining=%d reset_at=%s", st.UsedCount, st.RemainingCount, st.ResetAt.Format(time.RFC3339))
	if st.UsedCount != 1 || st.RemainingCount != 2 {
		t.Fatalf("used=%d remaining=%d, want 1/2", st.UsedCount, st.RemainingCount)
	}
	if want := time.Date(2026, 9, 28, 0, 0, 0, 0, time.UTC); !st.ResetAt.Equal(want) {
		t.Fatalf("reset_at=%v, want %v", st.ResetAt, want)
	}

	// 未配置额度的同意：无额度状态可查。
	st, err = f.gw.QuotaStatus("tenant-a", "c-1")
	if err != nil || st != nil {
		t.Fatalf("quota-less consent: status=%v err=%v, want nil/nil", st, err)
	}
	// 跨租户查询被拒绝。
	if _, err = f.gw.QuotaStatus("tenant-b", cid); err == nil {
		t.Fatal("cross-tenant status query must be denied")
	} else {
		t.Logf("cross-tenant status query -> %v", err)
	}
}
