package domain

import "time"

// ---- Identifiers ----

type TenantID string
type ClientID string
type ConsentID string
type AccountID string

// ---- Scope ----

type Scope string

const (
	ScopeAccountRead Scope = "accounts.read"
	ScopePaymentInit Scope = "payments.initiate"
)

// ---- Consent lifecycle ----

type ConsentStatus string

const (
	ConsentActive  ConsentStatus = "active"
	ConsentRevoked ConsentStatus = "revoked"
	ConsentExpired ConsentStatus = "expired"
)

// Money is the amount carried by a payment in the smallest logical unit.
type Money struct {
	Amount   int64  `json:"amount"`
	Currency string `json:"currency"`
}

// Consent is an authorization granted by a PSU for a client of a tenant.
type Consent struct {
	ID        ConsentID     `json:"id"`
	TenantID  TenantID      `json:"tenant_id"`
	ClientID  ClientID      `json:"client_id"`
	AccountID AccountID     `json:"account_id"`
	Scopes    []Scope       `json:"scopes"`
	Status    ConsentStatus `json:"status"`
	ExpiresAt time.Time     `json:"expires_at"`
	CreatedAt time.Time     `json:"created_at"`
	RevokedAt *time.Time    `json:"revoked_at,omitempty"`
}

// CreateConsentInput is the request body for establishing a consent.
type CreateConsentInput struct {
	TenantID  TenantID      `json:"tenant_id"`
	ClientID  ClientID      `json:"client_id"`
	AccountID AccountID     `json:"account_id"`
	Scopes    []Scope       `json:"scopes"`
	TTL       time.Duration `json:"ttl"`
}

// PaymentInput is a write request that must go through consent + idempotency.
type PaymentInput struct {
	IdempotencyKey string    `json:"idempotency_key"`
	TenantID       TenantID  `json:"tenant_id"`
	ClientID       ClientID  `json:"client_id"`
	ConsentID      ConsentID `json:"consent_id"`
	AccountID      AccountID `json:"account_id"`
	Amount         Money     `json:"amount"`
	Reference      string    `json:"reference"`
}

// PaymentResult is the stable outcome of a payment submission.
type PaymentResult struct {
	PaymentID string    `json:"payment_id"`
	Status    string    `json:"status"`
	Replayed  bool      `json:"replayed"`
	CreatedAt time.Time `json:"created_at"`
}
