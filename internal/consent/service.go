package consent

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/highcumontoa/open-banking-gateway-go/internal/domain"
)

// Store persists consents.
type Store interface {
	Put(c domain.Consent) error
	Get(id domain.ConsentID) (domain.Consent, bool)
}

// Clock allows deterministic tests.
type Clock func() time.Time

// Service manages the consent lifecycle.
type Service struct {
	store Store
	now   Clock
	seq   atomic.Uint64
}

func NewService(store Store, now Clock) *Service {
	if now == nil {
		now = time.Now
	}
	return &Service{store: store, now: now}
}

// Create establishes a new consent in the active state.
func (s *Service) Create(in domain.CreateConsentInput) (domain.Consent, error) {
	if in.TenantID == "" || in.ClientID == "" || in.AccountID == "" {
		return domain.Consent{}, domain.NewError(domain.ErrInternal, "tenant, client and account are required")
	}
	if len(in.Scopes) == 0 {
		return domain.Consent{}, domain.NewError(domain.ErrInternal, "at least one scope is required")
	}
	if in.TTL <= 0 {
		return domain.Consent{}, domain.NewError(domain.ErrInternal, "ttl must be positive")
	}
	now := s.now()
	n := s.seq.Add(1)
	c := domain.Consent{
		ID:        domain.ConsentID(fmt.Sprintf("cons-%d-%d", now.UnixNano(), n)),
		TenantID:  in.TenantID,
		ClientID:  in.ClientID,
		AccountID: in.AccountID,
		Scopes:    append([]domain.Scope(nil), in.Scopes...),
		Status:    domain.ConsentActive,
		CreatedAt: now,
		ExpiresAt: now.Add(in.TTL),
	}
	if err := s.store.Put(c); err != nil {
		return domain.Consent{}, domain.NewError(domain.ErrInternal, "failed to persist consent")
	}
	return c, nil
}

// Get returns a consent by id.
func (s *Service) Get(id domain.ConsentID) (domain.Consent, error) {
	c, ok := s.store.Get(id)
	if !ok {
		return domain.Consent{}, domain.NewError(domain.ErrConsentMissing, "consent not found: "+string(id))
	}
	return c, nil
}

// Revoke marks a consent as revoked; revoking again is idempotent.
func (s *Service) Revoke(id domain.ConsentID) (domain.Consent, error) {
	c, ok := s.store.Get(id)
	if !ok {
		return domain.Consent{}, domain.NewError(domain.ErrConsentMissing, "consent not found: "+string(id))
	}
	if c.Status == domain.ConsentRevoked {
		return c, nil
	}
	now := s.now()
	c.Status = domain.ConsentRevoked
	c.RevokedAt = &now
	if err := s.store.Put(c); err != nil {
		return domain.Consent{}, domain.NewError(domain.ErrInternal, "failed to revoke consent")
	}
	return c, nil
}

// SweepExpired transitions active consents past their expiry to expired.
// Check itself evaluates expiry lazily; this makes expiry observable/auditable.
func (s *Service) SweepExpired(ids ...domain.ConsentID) []domain.ConsentID {
	expired := make([]domain.ConsentID, 0, len(ids))
	now := s.now()
	for _, id := range ids {
		c, ok := s.store.Get(id)
		if !ok || c.Status != domain.ConsentActive {
			continue
		}
		if !now.Before(c.ExpiresAt) {
			c.Status = domain.ConsentExpired
			_ = s.store.Put(c)
			expired = append(expired, id)
		}
	}
	return expired
}

// Check authorizes a request against a consent and returns the effective consent.
//
// Refusal reasons are kept distinct and stable:
//
//	CONSENT_MISSING     - no such consent
//	CROSS_TENANT_ACCESS - consent belongs to another tenant
//	CLIENT_MISMATCH     - consent belongs to another client of the same tenant
//	ACCOUNT_MISMATCH    - consent is bound to a different account
//	CONSENT_REVOKED     - status revoked
//	CONSENT_EXPIRED     - active but past ExpiresAt (or status expired)
//	SCOPE_INSUFFICIENT  - consent does not carry the required scope
func (s *Service) Check(tenantID domain.TenantID, clientID domain.ClientID, consentID domain.ConsentID, accountID domain.AccountID, need domain.Scope) (domain.Consent, error) {
	c, ok := s.store.Get(consentID)
	if !ok {
		return domain.Consent{}, domain.NewError(domain.ErrConsentMissing, "consent not found: "+string(consentID))
	}
	if c.TenantID != tenantID {
		return c, domain.NewError(domain.ErrCrossTenantAccess, fmt.Sprintf(
			"consent tenant %q does not match request tenant %q", c.TenantID, tenantID))
	}
	if c.ClientID != clientID {
		return c, domain.NewError(domain.ErrClientMismatch, fmt.Sprintf(
			"consent client %q does not match request client %q", c.ClientID, clientID))
	}
	if c.AccountID != accountID {
		return c, domain.NewError(domain.ErrAccountMismatch, fmt.Sprintf(
			"consent account %q does not match request account %q", c.AccountID, accountID))
	}
	if c.Status == domain.ConsentRevoked || c.RevokedAt != nil {
		return c, domain.NewError(domain.ErrConsentRevoked, "consent was revoked at "+revokedAt(c))
	}
	now := s.now()
	if c.Status == domain.ConsentExpired || !now.Before(c.ExpiresAt) {
		return c, domain.NewError(domain.ErrConsentExpired, fmt.Sprintf(
			"consent expired at %s (now %s)", c.ExpiresAt.Format(time.RFC3339), now.Format(time.RFC3339)))
	}
	for _, sc := range c.Scopes {
		if sc == need {
			return c, nil
		}
	}
	return c, domain.NewError(domain.ErrScopeInsufficient, fmt.Sprintf(
		"required scope %q not granted; consent scopes %v", need, c.Scopes))
}

func revokedAt(c domain.Consent) string {
	if c.RevokedAt == nil {
		return "<unknown>"
	}
	return c.RevokedAt.Format(time.RFC3339)
}
