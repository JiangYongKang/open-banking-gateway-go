package gateway

import (
	"sync"
	"testing"
	"time"

	"github.com/highcumontoa/open-banking-gateway-go/internal/consent"
	"github.com/highcumontoa/open-banking-gateway-go/internal/idempotency"
	"github.com/highcumontoa/open-banking-gateway-go/internal/ledger"
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
