package consent

import (
	"testing"
	"time"
)

func setup(t *testing.T) (*MemoryStore, *Authorizer, time.Time) {
	t.Helper()
	now := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	store := NewMemoryStore()
	seed := []Consent{
		{ID: "ok", TenantID: "t1", ClientID: "c1", Scopes: []string{"payment:write"}, ExpiresAt: now.Add(time.Hour)},
		{ID: "expired", TenantID: "t1", ClientID: "c1", Scopes: []string{"payment:write"}, ExpiresAt: now.Add(-time.Second)},
		{ID: "revoked", TenantID: "t1", ClientID: "c1", Scopes: []string{"payment:write"}, Status: StatusRevoked, ExpiresAt: now.Add(time.Hour)},
		{ID: "narrow", TenantID: "t1", ClientID: "c1", Scopes: []string{"account:read"}, ExpiresAt: now.Add(time.Hour)},
	}
	for _, c := range seed {
		if err := store.Create(c); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	return store, NewAuthorizer(store, func() time.Time { return now }), now
}

func TestAuthorizeBoundaries(t *testing.T) {
	_, auth, _ := setup(t)
	cases := []struct {
		name      string
		tenant    string
		consentID string
		scopes    []string
		allowed   bool
		reason    RejectReason
	}{
		{"valid consent", "t1", "ok", []string{"payment:write"}, true, ""},
		{"missing consent", "t1", "ghost", []string{"payment:write"}, false, ReasonMissing},
		{"cross tenant", "t2", "ok", []string{"payment:write"}, false, ReasonTenantMismatch},
		{"revoked consent", "t1", "revoked", []string{"payment:write"}, false, ReasonRevoked},
		{"expired consent", "t1", "expired", []string{"payment:write"}, false, ReasonExpired},
		{"scope insufficient", "t1", "narrow", []string{"payment:write"}, false, ReasonScopeInsufficient},
		{"extra scope required", "t1", "ok", []string{"payment:write", "account:read"}, false, ReasonScopeInsufficient},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dec := auth.Authorize(tc.tenant, tc.consentID, tc.scopes)
			t.Logf("request tenant=%s consent=%s scopes=%v -> allowed=%v reason=%q",
				tc.tenant, tc.consentID, tc.scopes, dec.Allowed, dec.Reason)
			if dec.Allowed != tc.allowed {
				t.Fatalf("allowed=%v, want %v", dec.Allowed, tc.allowed)
			}
			if !tc.allowed && dec.Reason != tc.reason {
				t.Fatalf("reason=%q, want %q", dec.Reason, tc.reason)
			}
		})
	}
}

func TestRevokeTransitions(t *testing.T) {
	store, auth, _ := setup(t)
	if err := store.Revoke("ok"); err != nil {
		t.Fatalf("revoke: %v", err)
	}
	dec := auth.Authorize("t1", "ok", []string{"payment:write"})
	t.Logf("after revoke: allowed=%v reason=%q", dec.Allowed, dec.Reason)
	if dec.Allowed || dec.Reason != ReasonRevoked {
		t.Fatalf("expected revoked denial, got allowed=%v reason=%q", dec.Allowed, dec.Reason)
	}
	if err := store.Revoke("ghost"); err != ErrNotFound {
		t.Fatalf("revoke missing: err=%v, want ErrNotFound", err)
	}
}
