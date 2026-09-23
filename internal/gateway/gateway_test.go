package gateway

import (
	"bytes"
	"context"
	"errors"
	"log"
	"sync/atomic"
	"testing"
	"time"

	"github.com/highcumontoa/open-banking-gateway-go/internal/consent"
	"github.com/highcumontoa/open-banking-gateway-go/internal/domain"
	"github.com/highcumontoa/open-banking-gateway-go/internal/idempotency"
	"github.com/highcumontoa/open-banking-gateway-go/internal/ledger"
	"github.com/highcumontoa/open-banking-gateway-go/internal/ratelimit"
)

type stack struct {
	gw      *Gateway
	consent *consent.Service
	rates   *ratelimit.Limiter
	core    *ledger.Ledger
	logBuf  *bytes.Buffer
	now     *time.Time
}

func newStack(t *testing.T) *stack {
	t.Helper()
	start := time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC)
	buf := &bytes.Buffer{}
	logger := log.New(buf, "", 0)
	cs := consent.NewService(consent.NewMemoryStore(), func() time.Time { return start })
	rl := ratelimit.NewLimiter(func() time.Time { return start })
	ld := ledger.NewLedger(func() time.Time { return start })
	ld.SetAccount("acct-1", 1_000_000)
	ids := idempotency.NewStore(idempotency.JSONFingerprint{})
	gw := New(rl, cs, ids, ld, logger)
	return &stack{gw: gw, consent: cs, rates: rl, core: ld, logBuf: buf, now: &start}
}

func (s *stack) advance(d time.Duration) { *s.now = s.now.Add(d) }

func (s *stack) mustConsent(t *testing.T, tenant domain.TenantID, client domain.ClientID, account domain.AccountID, scopes ...domain.Scope) domain.Consent {
	t.Helper()
	c, err := s.consent.Create(domain.CreateConsentInput{
		TenantID: tenant, ClientID: client, AccountID: account,
		Scopes: scopes, TTL: time.Hour,
	})
	if err != nil {
		t.Fatalf("create consent: %v", err)
	}
	return c
}

func payment(tenant domain.TenantID, client domain.ClientID, c domain.Consent, key, ref string) domain.PaymentInput {
	return domain.PaymentInput{
		IdempotencyKey: key,
		TenantID:       tenant,
		ClientID:       client,
		ConsentID:      c.ID,
		AccountID:      c.AccountID,
		Amount:         domain.Money{Amount: 5000, Currency: "EUR"},
		Reference:      ref,
	}
}

func wantErrCode(t *testing.T, err error, want domain.ErrorCode) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected error %s, got success", want)
	}
	var ge *domain.GatewayError
	if !errors.As(err, &ge) {
		t.Fatalf("expected *GatewayError, got %T: %v", err, err)
	}
	if ge.Code != want {
		t.Fatalf("code = %s, want %s (%s)", ge.Code, want, ge.Message)
	}
}

func TestSubmitHappyPathAndStableReplay(t *testing.T) {
	s := newStack(t)
	c := s.mustConsent(t, "tenant-a", "client-1", "acct-1", domain.ScopePaymentInit)
	in := payment("tenant-a", "client-1", c, "key-happy", "ref-1")

	res1, err := s.gw.SubmitPayment(context.Background(), in)
	if err != nil {
		t.Fatalf("first submit: %v", err)
	}
	if s.core.CountPayments() != 1 {
		t.Fatalf("core ledger should hold 1 entry, got %d", s.core.CountPayments())
	}

	// Identical retry: same key + same payload -> replay, no second ledger row.
	res2, err := s.gw.SubmitPayment(context.Background(), in)
	if err != nil {
		t.Fatalf("replay submit: %v", err)
	}
	if res2.PaymentID != res1.PaymentID || !res2.Replayed {
		t.Fatalf("replay mismatch: %+v vs %+v", res2, res1)
	}
	if s.core.CountPayments() != 1 {
		t.Fatalf("retry must not double-post: ledger rows=%d", s.core.CountPayments())
	}
	t.Logf("IDEMPOTENT key=%s paymentID=%s rows=%d replayed=%v (duplicate submit did not re-post)",
		in.IdempotencyKey, res2.PaymentID, s.core.CountPayments(), res2.Replayed)
}

func TestIdempotencyKeyReusedWithDifferentPayload(t *testing.T) {
	s := newStack(t)
	c := s.mustConsent(t, "tenant-a", "client-1", "acct-1", domain.ScopePaymentInit)
	in := payment("tenant-a", "client-1", c, "key-reuse", "ref-original")

	if _, err := s.gw.SubmitPayment(context.Background(), in); err != nil {
		t.Fatalf("first submit: %v", err)
	}
	tampered := in
	tampered.Amount = domain.Money{Amount: 999999, Currency: "EUR"}
	t.Logf("CONFLICT-ATTEMPT key=%s originalAmount=5000 tamperedAmount=999999", in.IdempotencyKey)
	_, err := s.gw.SubmitPayment(context.Background(), tampered)
	wantErrCode(t, err, domain.ErrIdempotencyConflict)
	if s.core.CountPayments() != 1 {
		t.Fatalf("conflicting reuse must not post, rows=%d", s.core.CountPayments())
	}

	// Original key still replays the original result.
	res, err := s.gw.SubmitPayment(context.Background(), in)
	if err != nil || res.Replayed != true {
		t.Fatalf("original replay broken: %+v %v", res, err)
	}
	t.Logf("CONFLICT key=%s refused with IDEMPOTENCY_CONFLICT; original replay intact rows=%d",
		in.IdempotencyKey, s.core.CountPayments())
}

func TestConsentRefusalsThroughGateway(t *testing.T) {
	s := newStack(t)
	good := s.mustConsent(t, "tenant-a", "client-1", "acct-1", domain.ScopePaymentInit)
	otherTenant := s.mustConsent(t, "tenant-b", "client-1", "acct-1", domain.ScopePaymentInit)
	otherClient := s.mustConsent(t, "tenant-a", "client-2", "acct-1", domain.ScopePaymentInit)
	readOnly := s.mustConsent(t, "tenant-a", "client-1", "acct-1", domain.ScopeAccountRead)
	shortTTL, err := s.consent.Create(domain.CreateConsentInput{
		TenantID: "tenant-a", ClientID: "client-1", AccountID: "acct-1",
		Scopes: []domain.Scope{domain.ScopePaymentInit}, TTL: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	revoked := s.mustConsent(t, "tenant-a", "client-1", "acct-1", domain.ScopePaymentInit)
	if _, err := s.consent.Revoke(revoked.ID); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	cases := []struct {
		name string
		in   domain.PaymentInput
		want domain.ErrorCode
	}{
		{"cross tenant", payment("tenant-a", "client-1", otherTenant, "k1", "r1"), domain.ErrCrossTenantAccess},
		{"wrong client", payment("tenant-a", "client-1", otherClient, "k2", "r2"), domain.ErrClientMismatch},
		{"missing consent",
			func() domain.PaymentInput {
				p := payment("tenant-a", "client-1", good, "k3", "r3")
				p.ConsentID = "cons-nope"
				return p
			}(),
			domain.ErrConsentMissing},
		{"wrong account",
			func() domain.PaymentInput {
				p := payment("tenant-a", "client-1", good, "k4", "r4")
				p.AccountID = "acct-OTHER"
				return p
			}(),
			domain.ErrAccountMismatch},
		{"scope insufficient", payment("tenant-a", "client-1", readOnly, "k5", "r5"), domain.ErrScopeInsufficient},
		{"revoked", payment("tenant-a", "client-1", revoked, "k6", "r6"), domain.ErrConsentRevoked},
		{"expired", payment("tenant-a", "client-1", shortTTL, "k7", "r7"), domain.ErrConsentExpired},
	}

	s.advance(2 * time.Minute) // drives shortTTL past expiry

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Logf("REQUEST key=%s tenant=%s client=%s consent=%s account=%s",
				tc.in.IdempotencyKey, tc.in.TenantID, tc.in.ClientID, tc.in.ConsentID, tc.in.AccountID)
			_, err := s.gw.SubmitPayment(ctx, tc.in)
			wantErrCode(t, err, tc.want)
			t.Logf("DECISION=DENY code=%s rows=%d (refusal never wrote to ledger)", tc.want, s.core.CountPayments())
		})
	}
}

func TestConcurrentDuplicatesPostExactlyOnceEndToEnd(t *testing.T) {
	s := newStack(t)
	c := s.mustConsent(t, "tenant-a", "client-1", "acct-1", domain.ScopePaymentInit)
	in := payment("tenant-a", "client-1", c, "key-concurrent-e2e", "ref-conc")

	const n = 40
	var (
		ok       atomic.Int32
		denied   atomic.Int32
		start    = make(chan struct{})
		errCodes = make([]domain.ErrorCode, n)
		results  = make([]domain.PaymentResult, n)
	)

	for i := 0; i < n; i++ {
		i := i
		go func() {
			<-start
			r, err := s.gw.SubmitPayment(context.Background(), in)
			if err != nil {
				var ge *domain.GatewayError
				if errors.As(err, &ge) {
					errCodes[i] = ge.Code
				}
				denied.Add(1)
				return
			}
			errCodes[i] = ""
			results[i] = r
			ok.Add(1)
		}()
	}
	close(start)

	// Busy-wait for completion.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if int(ok.Load()+denied.Load()) == n {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if got := ok.Load() + denied.Load(); got != n {
		t.Fatalf("only %d/%d goroutines finished", got, n)
	}
	if denied.Load() != 0 {
		t.Fatalf("no request should be denied: codes=%v", errCodes)
	}
	if s.core.CountPayments() != 1 {
		t.Fatalf("core ledger must contain exactly 1 row, got %d", s.core.CountPayments())
	}
	posted := 0
	for i, r := range results {
		if r.PaymentID == "" || r.PaymentID != results[0].PaymentID {
			t.Fatalf("goroutine %d saw divergent paymentID %q vs %q", i, r.PaymentID, results[0].PaymentID)
		}
		if !r.Replayed {
			posted++
		}
	}
	replayed := int(ok.Load()) - posted
	t.Logf("CONCURRENT-E2E key=%s clients=%d successes=%d (1 posted + %d replays), ledgerRows=1",
		in.IdempotencyKey, n, ok.Load(), replayed)
}

func TestRateLimitedTenantDoesNotAffectOthers(t *testing.T) {
	s := newStack(t)
	ca := s.mustConsent(t, "tenant-a", "client-1", "acct-1", domain.ScopePaymentInit)
	cb := s.mustConsent(t, "tenant-b", "client-1", "acct-1", domain.ScopePaymentInit)
	s.rates.SetRule(ratelimit.Subject{TenantID: "tenant-a", ClientID: "client-1"},
		ratelimit.Rule{RatePerMinute: 1, QuotaPerDay: 100})

	// First call for A consumes its single window token.
	if _, err := s.gw.SubmitPayment(context.Background(), payment("tenant-a", "client-1", ca, "ka-1", "ra1")); err != nil {
		t.Fatal(err)
	}
	// Second call for A is rate limited.
	_, err := s.gw.SubmitPayment(context.Background(), payment("tenant-a", "client-1", ca, "ka-2", "ra2"))
	wantErrCode(t, err, domain.ErrRateLimited)

	// Tenant B has its own counters and stays fully open.
	if _, err := s.gw.SubmitPayment(context.Background(), payment("tenant-b", "client-1", cb, "kb-1", "rb1")); err != nil {
		t.Fatalf("tenant isolation broken: %v", err)
	}
	t.Logf("ISOLATION tenant-a second call=RATE_LIMITED while tenant-b call=ALLOW (independent windows)")
}

func TestUpstreamFailuresNormalizedAndStable(t *testing.T) {
	s := newStack(t)
	c := s.mustConsent(t, "tenant-a", "client-1", "acct-1", domain.ScopePaymentInit)

	// Injected business rejection from the core ledger.
	s.core.InjectFault("ref-rejected", ledger.Fault{Code: domain.ErrUpstreamRejected, Message: "core says no"})
	in := payment("tenant-a", "client-1", c, "key-rej", "ref-rejected")
	_, err := s.gw.SubmitPayment(context.Background(), in)
	wantErrCode(t, err, domain.ErrUpstreamRejected)
	if s.core.CountPayments() != 0 {
		t.Fatalf("rejected request must not post, rows=%d", s.core.CountPayments())
	}
	// Retry of the SAME key replays the stable failure instead of re-calling core.
	_, err2 := s.gw.SubmitPayment(context.Background(), in)
	wantErrCode(t, err2, domain.ErrUpstreamRejected)
	t.Logf("UPSTREAM-REJECTED key=%s code=UPSTREAM_REJECTED on first call and stable on retry, rows=0", in.IdempotencyKey)

	// Injected timeout with a deadline shorter than the delay.
	s.core.InjectFault("ref-slow", ledger.Fault{Delay: 100 * time.Millisecond})
	inSlow := payment("tenant-a", "client-1", c, "key-slow", "ref-slow")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, errSlow := s.gw.SubmitPayment(ctx, inSlow)
	wantErrCode(t, errSlow, domain.ErrUpstreamTimeout)
	t.Logf("UPSTREAM-TIMEOUT key=%s normalized to UPSTREAM_TIMEOUT (deadline 20ms < delay 100ms), raw ctx error hidden",
		inSlow.IdempotencyKey)
}

func TestQuotaExhaustionDoesNotConsumeLedgerOrLockKey(t *testing.T) {
	s := newStack(t)
	// Consent must outlive the 24h day-rollover performed later in this test.
	long, err := s.consent.Create(domain.CreateConsentInput{
		TenantID: "tenant-a", ClientID: "client-1", AccountID: "acct-1",
		Scopes: []domain.Scope{domain.ScopePaymentInit}, TTL: 72 * time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	s.rates.SetRule(ratelimit.Subject{TenantID: "tenant-a", ClientID: "client-1"},
		ratelimit.Rule{RatePerMinute: 100, QuotaPerDay: 1})

	in := payment("tenant-a", "client-1", long, "key-q1", "rq1")
	if _, err := s.gw.SubmitPayment(context.Background(), in); err != nil {
		t.Fatal(err)
	}
	in2 := payment("tenant-a", "client-1", long, "key-q2", "rq2")
	_, err = s.gw.SubmitPayment(context.Background(), in2)
	wantErrCode(t, err, domain.ErrQuotaExceeded)
	if s.core.CountPayments() != 1 {
		t.Fatalf("quota refusal must not post, rows=%d", s.core.CountPayments())
	}

	// After the UTC day rolls over the same request succeeds and the key is
	// not poisoned by the earlier quota refusal (claim was released).
	s.advance(24 * time.Hour)
	if _, err := s.gw.SubmitPayment(context.Background(), in2); err != nil {
		t.Fatalf("retry after quota reset must succeed: %v", err)
	}
	if s.core.CountPayments() != 2 {
		t.Fatalf("expected 2 rows after next-day retry, got %d", s.core.CountPayments())
	}
	t.Logf("QUOTA-LIFECYCLE key=%s rejected on day 1 with QUOTA_EXCEEDED, succeeded after day rollover rows=%d",
		in2.IdempotencyKey, s.core.CountPayments())
}
