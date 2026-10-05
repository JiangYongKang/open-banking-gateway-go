package gateway

import (
	"fmt"
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
	gw := New(
		consent.NewAuthorizer(store, clock.Now),
		idempotency.NewMemoryStore(),
		ratelimit.NewLimiter(limit, clock.Now),
		quota.NewManager(clock.Now),
		lg,
		clock.Now,
	)
	return &fixture{gw: gw, store: store, lg: lg, clock: clock, consentID: "c-1"}
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

// --- 同意级周期额度的集成测试 ---

// quotaFixture 在 c-1 上配置自然周额度，并额外准备一条无额度同意 c-plain。
type quotaFixture struct {
	*fixture
}

func newQuotaFixture(t *testing.T, p quota.Policy) *quotaFixture {
	t.Helper()
	f := newFixture(t, ratelimit.Limit{Requests: 1000, Window: time.Minute})
	if err := f.store.Create(consent.Consent{
		ID: "c-plain", TenantID: "tenant-a", ClientID: "client-1",
		Scopes: []string{"payment:write"}, ExpiresAt: f.clock.Now().Add(time.Hour),
	}); err != nil {
		t.Fatalf("seed plain consent: %v", err)
	}
	// c-1 已在 newFixture 中建立且不带额度；这里改为带额度的同意 c-q。
	if err := f.store.Create(consent.Consent{
		ID: "c-q", TenantID: "tenant-a", ClientID: "client-1",
		Scopes: []string{"payment:write"}, ExpiresAt: f.clock.Now().Add(time.Hour),
		Quota: p,
	}); err != nil {
		t.Fatalf("seed quota consent: %v", err)
	}
	return &quotaFixture{f}
}

func (f *quotaFixture) qreq(key string, amount int64) Request {
	r := f.req(key)
	r.ConsentID = "c-q"
	r.Amount = amount
	return r
}

func TestQuotaCountExhaustionDistinctCode(t *testing.T) {
	// 基准时间 2026-09-23（周三），自然周重置点应为 2026-09-28 周一 00:00 UTC。
	f := newQuotaFixture(t, quota.Policy{Period: quota.PeriodWeekly, MaxCount: 2})

	if _, err := f.gw.HandleWrite(f.qreq("q-1", 1000)); err != nil {
		t.Fatalf("write #1: %v", err)
	}
	resp, err := f.gw.HandleWrite(f.qreq("q-2", 1000))
	if err != nil {
		t.Fatalf("write #2: %v", err)
	}
	if resp.QuotaUsage == nil || resp.QuotaUsage.RemainingCount() != 0 {
		t.Fatalf("usage after 2 writes: %+v", resp.QuotaUsage)
	}
	t.Logf("2 writes consumed weekly count budget, resets at %s", resp.QuotaUsage.ResetAt.Format(time.RFC3339))

	_, err = f.gw.HandleWrite(f.qreq("q-3", 1))
	ge, ok := err.(*Error)
	if !ok || ge.Code != CodeQuotaExhausted || ge.QuotaReason != quota.ReasonCountExceeded {
		t.Fatalf("third write err=%v, want QUOTA_EXHAUSTED/QUOTA_COUNT_EXCEEDED", err)
	}
	wantReset := time.Date(2026, 9, 28, 0, 0, 0, 0, time.UTC)
	if !ge.Usage.ResetAt.Equal(wantReset) || ge.Usage.RemainingCount() != 0 {
		t.Fatalf("reset=%v remaining=%d, want %s, 0", ge.Usage.ResetAt, ge.Usage.RemainingCount(), wantReset)
	}
	t.Logf("third write rejected: %v", err)

	// 被额度挡下的请求未入账。
	if n := len(f.lg.Entries("tenant-a")); n != 2 {
		t.Fatalf("ledger entries=%d, want 2", n)
	}
	// 与限流错误码必须可区分。
	if ge.Code == CodeRateLimited || ge.Code == CodeConsentDenied {
		t.Fatalf("quota denial must be distinct from rate limit/consent codes")
	}
}

func TestQuotaAmountExhaustion(t *testing.T) {
	f := newQuotaFixture(t, quota.Policy{Period: quota.PeriodWeekly, MaxAmount: 1500, Currency: "CNY"})
	if _, err := f.gw.HandleWrite(f.qreq("a-1", 1000)); err != nil {
		t.Fatalf("write #1: %v", err)
	}
	_, err := f.gw.HandleWrite(f.qreq("a-2", 600))
	ge, ok := err.(*Error)
	if !ok || ge.Code != CodeQuotaExhausted || ge.QuotaReason != quota.ReasonAmountBudget {
		t.Fatalf("over-budget write err=%v, want QUOTA_EXHAUSTED/QUOTA_AMOUNT_BUDGET_EXCEEDED", err)
	}
	t.Logf("1000 committed then 600 rejected: remaining amount=%d resets=%s (%v)",
		ge.Usage.RemainingAmount(), ge.Usage.ResetAt.Format(time.RFC3339), err)
}

func TestQuotaReplayDeductsOnceAndStormConverges(t *testing.T) {
	f := newQuotaFixture(t, quota.Policy{Period: quota.PeriodWeekly, MaxCount: 1})

	// 串行重复提交：回放且不二次扣额度。
	r1, err := f.gw.HandleWrite(f.qreq("storm", 1000))
	if err != nil {
		t.Fatalf("first write: %v", err)
	}
	r2, err := f.gw.HandleWrite(f.qreq("storm", 1000))
	if err != nil || !r2.Replayed || r2.Entry.ID != r1.Entry.ID {
		t.Fatalf("replay: err=%v replayed=%v id=%s vs %s", err, r2.Replayed, r1.Entry.ID, r2.Entry.ID)
	}
	t.Logf("serial replay: entry=%s replayed, no second deduction", r2.Entry.ID)

	// 新额度桶 + 并发重试风暴：32 个同键并发全部得到同一 entry，额度只扣一次。
	if err := f.store.Create(consent.Consent{
		ID: "c-storm", TenantID: "tenant-a", ClientID: "client-1",
		Scopes: []string{"payment:write"}, ExpiresAt: f.clock.Now().Add(time.Hour),
		Quota: quota.Policy{Period: quota.PeriodWeekly, MaxCount: 1},
	}); err != nil {
		t.Fatalf("seed storm consent: %v", err)
	}
	stormReq := f.req("k-storm")
	stormReq.ConsentID = "c-storm"

	const workers = 32
	var wg sync.WaitGroup
	errs := make([]error, workers)
	ids := make([]string, workers)
	replayed := make([]bool, workers)
	start := make(chan struct{})
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			resp, err := f.gw.HandleWrite(stormReq)
			if err != nil {
				errs[i] = err
				return
			}
			ids[i], replayed[i] = resp.Entry.ID, resp.Replayed
		}(i)
	}
	close(start)
	wg.Wait()

	firstID := ""
	okCount := 0
	for i := 0; i < workers; i++ {
		if errs[i] != nil {
			t.Fatalf("worker %d got error in retry storm: %v", i, errs[i])
		}
		if firstID == "" {
			firstID = ids[i]
		}
		if ids[i] != firstID {
			t.Fatalf("worker %d entry=%s, want %s (all must converge)", i, ids[i], firstID)
		}
		if replayed[i] {
			okCount++
		}
	}
	// 恰好一个真实写入，其余 31 个回放；额度仅扣 1，下一个新键必须被额度挡下。
	if replayedCount := okCount; replayedCount != workers-1 {
		t.Fatalf("replayed workers=%d, want %d", replayedCount, workers-1)
	}
	next := f.req("k-after-storm")
	next.ConsentID = "c-storm"
	if _, err := f.gw.HandleWrite(next); err == nil {
		t.Fatalf("new key after storm must be quota-blocked")
	} else if ge, ok := err.(*Error); !ok || ge.Code != CodeQuotaExhausted {
		t.Fatalf("new key err=%v, want QUOTA_EXHAUSTED", err)
	}
	t.Logf("storm of %d converged to entry=%s (1 write + %d replays); next new key blocked by quota, ledger=%d",
		workers, firstID, workers-1, len(f.lg.Entries("tenant-a")))
}

func TestQuotaUpstreamFailureDoesNotConsume(t *testing.T) {
	f := newQuotaFixture(t, quota.Policy{Period: quota.PeriodWeekly, MaxCount: 1, MaxAmount: 500, Currency: "CNY"})
	f.lg.FailWith = ledger.ErrUpstreamTimeout
	_, err := f.gw.HandleWrite(f.qreq("up-1", 500))
	ge, ok := err.(*Error)
	if !ok || ge.Code != CodeUpstreamTimeout {
		t.Fatalf("upstream failure err=%v, want UPSTREAM_TIMEOUT", err)
	}
	t.Logf("upstream timeout normalized: %v", err)

	f.lg.FailWith = nil
	resp, err := f.gw.HandleWrite(f.qreq("up-1", 500))
	if err != nil {
		t.Fatalf("retry after recovery should succeed: %v", err)
	}
	if resp.Replayed || resp.QuotaUsage == nil || resp.QuotaUsage.UsedCount != 1 || resp.QuotaUsage.UsedAmount != 500 {
		t.Fatalf("retry result: replayed=%v usage=%+v", resp.Replayed, resp.QuotaUsage)
	}
	t.Logf("retry after recovery succeeded entry=%s, quota usedCount=1 usedAmount=500", resp.Entry.ID)

	// 额度已被成功重试占用：再发新键应被额度挡下而非放行。
	if _, err := f.gw.HandleWrite(f.qreq("up-2", 1)); err == nil {
		t.Fatalf("budget should be consumed by the successful retry only")
	} else if ge, ok := err.(*Error); !ok || ge.Code != CodeQuotaExhausted {
		t.Fatalf("post-retry err=%v, want QUOTA_EXHAUSTED", err)
	}
	t.Logf("failed attempt did not consume quota; exactly one deduction after recovery")
}

func TestQuotaWeeklyRollover(t *testing.T) {
	f := newQuotaFixture(t, quota.Policy{Period: quota.PeriodWeekly, MaxCount: 1})
	// c-q 默认 1 小时后过期，跨周测试需要更长有效期：重建一条远期待额度同意。
	if err := f.store.Create(consent.Consent{
		ID: "c-qw", TenantID: "tenant-a", ClientID: "client-1",
		Scopes: []string{"payment:write"}, ExpiresAt: f.clock.Now().Add(365 * 24 * time.Hour),
		Quota: quota.Policy{Period: quota.PeriodWeekly, MaxCount: 1},
	}); err != nil {
		t.Fatalf("seed c-qw: %v", err)
	}
	qreq := func(key string) Request {
		r := f.req(key)
		r.ConsentID = "c-qw"
		r.Amount = 100
		return r
	}
	if _, err := f.gw.HandleWrite(qreq("w-1")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := f.gw.HandleWrite(qreq("w-2")); err == nil {
		t.Fatalf("within same week budget must be exhausted")
	}

	// 推进到下周一，自然周重置。
	f.clock.Advance(7 * 24 * time.Hour)
	resp, err := f.gw.HandleWrite(qreq("w-3"))
	if err != nil {
		t.Fatalf("after weekly reset should pass: %v", err)
	}
	if resp.QuotaUsage.UsedCount != 1 {
		t.Fatalf("new week usage=%+v, want usedCount=1", resp.QuotaUsage)
	}
	t.Logf("crossed weekly boundary at %s, write w-3 allowed, next reset=%s",
		f.clock.Now().Format(time.RFC3339), resp.QuotaUsage.ResetAt.Format(time.RFC3339))
}

func TestQuotaIsolationBetweenConsents(t *testing.T) {
	// 两条独立同意各配 1 笔/周额度，另一条无额度。
	f := newQuotaFixture(t, quota.Policy{Period: quota.PeriodWeekly, MaxCount: 1})
	if err := f.store.Create(consent.Consent{
		ID: "c-q2", TenantID: "tenant-a", ClientID: "client-1",
		Scopes: []string{"payment:write"}, ExpiresAt: f.clock.Now().Add(time.Hour),
		Quota: quota.Policy{Period: quota.PeriodWeekly, MaxCount: 1},
	}); err != nil {
		t.Fatalf("seed c-q2: %v", err)
	}

	if _, err := f.gw.HandleWrite(f.qreq("i-1", 100)); err != nil {
		t.Fatalf("c-q write: %v", err)
	}
	if _, err := f.gw.HandleWrite(f.qreq("i-2", 100)); err == nil {
		t.Fatalf("c-q should be exhausted")
	}
	t.Logf("consent c-q exhausted")

	r := f.req("i-3")
	r.ConsentID = "c-q2"
	if _, err := f.gw.HandleWrite(r); err != nil {
		t.Fatalf("independent consent c-q2 must not be affected: %v", err)
	}
	r2 := f.req("i-4")
	r2.ConsentID = "c-plain"
	for i := 0; i < 5; i++ {
		r2.IdempotencyKey = fmt.Sprintf("plain-%d", i)
		if _, err := f.gw.HandleWrite(r2); err != nil {
			t.Fatalf("unconfigured consent write %d must pass: %v", i, err)
		}
	}
	t.Logf("c-q2 served independently; consent c-plain (no quota) served 5 writes unaffected")
}

func TestThreeGatesOrder(t *testing.T) {
	// 额度已用尽但同意同时已撤销：应报 CONSENT_DENIED（同意有效性优先）。
	f := newQuotaFixture(t, quota.Policy{Period: quota.PeriodWeekly, MaxCount: 1})
	if _, err := f.gw.HandleWrite(f.qreq("o-1", 100)); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := f.store.Revoke("c-q"); err != nil {
		t.Fatalf("revoke: %v", err)
	}
	_, err := f.gw.HandleWrite(f.qreq("o-2", 100))
	if ge, ok := err.(*Error); !ok || ge.Code != CodeConsentDenied {
		t.Fatalf("revoked+exhausted err=%v, want CONSENT_DENIED (consent gate first)", err)
	}
	t.Logf("revoked + quota-exhausted -> CONSENT_DENIED (gate order: consent > rate limit > quota)")
}
