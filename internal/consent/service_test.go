package consent

import (
	"errors"
	"testing"
	"time"

	"github.com/highcumontoa/open-banking-gateway-go/internal/domain"
)

type testEnv struct {
	svc *Service
	now *time.Time
}

func newTestEnv(t *testing.T) *testEnv {
	t.Helper()
	start := time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC)
	svc := NewService(NewMemoryStore(), func() time.Time { return start })
	return &testEnv{svc: svc, now: &start}
}

func (e *testEnv) advance(d time.Duration) { *e.now = e.now.Add(d) }

func (e *testEnv) mustConsent(t *testing.T, in domain.CreateConsentInput) domain.Consent {
	t.Helper()
	c, err := e.svc.Create(in)
	if err != nil {
		t.Fatalf("Create consent failed: %v", err)
	}
	return c
}

func baseInput(scopes ...domain.Scope) domain.CreateConsentInput {
	return domain.CreateConsentInput{
		TenantID:  "tenant-a",
		ClientID:  "client-1",
		AccountID: "acct-1",
		Scopes:    scopes,
		TTL:       time.Hour,
	}
}

func TestConsentCheckBoundaries(t *testing.T) {
	tests := []struct {
		name      string
		mutate    func(e *testEnv, c *domain.Consent)
		tenant    domain.TenantID
		client    domain.ClientID
		account   domain.AccountID
		needScope domain.Scope
		wantCode  domain.ErrorCode
	}{
		{
			name:   "happy path: tenant/client/account/scope all match",
			tenant: "tenant-a", client: "client-1", account: "acct-1",
			needScope: domain.ScopePaymentInit, wantCode: "",
		},
		{
			name:   "cross tenant access refused",
			tenant: "tenant-b", client: "client-1", account: "acct-1",
			needScope: domain.ScopePaymentInit, wantCode: domain.ErrCrossTenantAccess,
		},
		{
			name:   "same tenant different client refused",
			tenant: "tenant-a", client: "client-2", account: "acct-1",
			needScope: domain.ScopePaymentInit, wantCode: domain.ErrClientMismatch,
		},
		{
			name:   "different account refused",
			tenant: "tenant-a", client: "client-1", account: "acct-OTHER",
			needScope: domain.ScopePaymentInit, wantCode: domain.ErrAccountMismatch,
		},
		{
			name:   "scope insufficient: read consent used for payment",
			tenant: "tenant-a", client: "client-1", account: "acct-1",
			needScope: domain.ScopePaymentInit, wantCode: domain.ErrScopeInsufficient,
			mutate: func(e *testEnv, c *domain.Consent) {
				c.Scopes = []domain.Scope{domain.ScopeAccountRead}
				e.svc.store.Put(*c)
			},
		},
		{
			name:   "revoked consent refused",
			tenant: "tenant-a", client: "client-1", account: "acct-1",
			needScope: domain.ScopePaymentInit, wantCode: domain.ErrConsentRevoked,
			mutate: func(e *testEnv, c *domain.Consent) { e.svc.Revoke(c.ID) },
		},
		{
			name:   "expired consent refused (clock advanced past TTL)",
			tenant: "tenant-a", client: "client-1", account: "acct-1",
			needScope: domain.ScopePaymentInit, wantCode: domain.ErrConsentExpired,
			mutate: func(e *testEnv, c *domain.Consent) { e.advance(2 * time.Hour) },
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			e := newTestEnv(t)
			c := e.mustConsent(t, baseInput(domain.ScopePaymentInit))
			if tc.mutate != nil {
				tc.mutate(e, &c)
			}

			t.Logf("REQUEST tenant=%q client=%q consent=%s account=%q needScope=%q | BASIS consent.tenant=%q consent.client=%q consent.account=%q status=%s expiresAt=%s scopes=%v now=%s",
				tc.tenant, tc.client, c.ID, tc.account, tc.needScope,
				c.TenantID, c.ClientID, c.AccountID, c.Status, c.ExpiresAt.Format(time.RFC3339), c.Scopes, e.now.Format(time.RFC3339))

			_, err := e.svc.Check(tc.tenant, tc.client, c.ID, tc.account, tc.needScope)
			if tc.wantCode == "" {
				if err != nil {
					t.Fatalf("expected allow, got %v", err)
				}
				t.Logf("DECISION=ALLOW reason=all-boundaries-matched")
				return
			}
			if err == nil {
				t.Fatalf("expected refusal %s, got allow", tc.wantCode)
			}
			var ge *domain.GatewayError
			if !errors.As(err, &ge) {
				t.Fatalf("expected *GatewayError, got %T: %v", err, err)
			}
			t.Logf("DECISION=DENY code=%s message=%q", ge.Code, ge.Message)
			if ge.Code != tc.wantCode {
				t.Fatalf("code mismatch: want %s got %s (%s)", tc.wantCode, ge.Code, ge.Message)
			}
		})
	}
}

func TestConsentMissing(t *testing.T) {
	e := newTestEnv(t)
	t.Logf("REQUEST consent=%s (never created)", "cons-does-not-exist")
	_, err := e.svc.Check("tenant-a", "client-1", "cons-does-not-exist", "acct-1", domain.ScopePaymentInit)
	var ge *domain.GatewayError
	if !errors.As(err, &ge) || ge.Code != domain.ErrConsentMissing {
		t.Fatalf("want CONSENT_MISSING, got %v", err)
	}
	t.Logf("DECISION=DENY code=%s message=%q", ge.Code, ge.Message)
}

func TestExpiryBoundaryIsExact(t *testing.T) {
	e := newTestEnv(t)
	c := e.mustConsent(t, baseInput(domain.ScopePaymentInit))

	// One nanosecond before expiry must still be allowed.
	*e.now = c.ExpiresAt.Add(-time.Nanosecond)
	if _, err := e.svc.Check("tenant-a", "client-1", c.ID, "acct-1", domain.ScopePaymentInit); err != nil {
		t.Fatalf("expected allow just before expiry, got %v", err)
	}
	t.Logf("REQUEST consent=%s now=%s expiresAt=%s DECISION=ALLOW (before boundary)",
		c.ID, e.now.Format(time.RFC3339Nano), c.ExpiresAt.Format(time.RFC3339Nano))

	// Exactly at expiry: refused as expired.
	*e.now = c.ExpiresAt
	_, err := e.svc.Check("tenant-a", "client-1", c.ID, "acct-1", domain.ScopePaymentInit)
	var ge *domain.GatewayError
	if !errors.As(err, &ge) || ge.Code != domain.ErrConsentExpired {
		t.Fatalf("want CONSENT_EXPIRED at boundary, got %v", err)
	}
	t.Logf("REQUEST consent=%s now=%s expiresAt=%s DECISION=DENY code=%s (at boundary)",
		c.ID, e.now.Format(time.RFC3339Nano), c.ExpiresAt.Format(time.RFC3339Nano), ge.Code)
}

func TestRevokeIsIdempotent(t *testing.T) {
	e := newTestEnv(t)
	c := e.mustConsent(t, baseInput(domain.ScopePaymentInit))
	r1, err := e.svc.Revoke(c.ID)
	if err != nil {
		t.Fatal(err)
	}
	r2, err := e.svc.Revoke(c.ID)
	if err != nil {
		t.Fatal(err)
	}
	if r1.RevokedAt == nil || r2.RevokedAt == nil || !r1.RevokedAt.Equal(*r2.RevokedAt) {
		t.Fatalf("revocation timestamp unstable: %v %v", r1.RevokedAt, r2.RevokedAt)
	}
	t.Logf("REVOKE consent=%s twice timestamp=%s stable", c.ID, r1.RevokedAt.Format(time.RFC3339Nano))
}

func TestSweepExpiredTransitionsStatus(t *testing.T) {
	e := newTestEnv(t)
	c := e.mustConsent(t, baseInput(domain.ScopePaymentInit))
	e.advance(2 * time.Hour)
	ids := e.svc.SweepExpired(c.ID)
	if len(ids) != 1 || ids[0] != c.ID {
		t.Fatalf("expected consent %s to be swept, got %v", c.ID, ids)
	}
	got, _ := e.svc.Get(c.ID)
	if got.Status != domain.ConsentExpired {
		t.Fatalf("expected stored status expired, got %s", got.Status)
	}
	t.Logf("SWEEP consent=%s -> status=%s", c.ID, got.Status)
}
