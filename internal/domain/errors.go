package domain

// ErrorCode is a stable, machine-readable category for every refusal.
type ErrorCode string

const (
	ErrConsentMissing      ErrorCode = "CONSENT_MISSING"
	ErrConsentRevoked      ErrorCode = "CONSENT_REVOKED"
	ErrConsentExpired      ErrorCode = "CONSENT_EXPIRED"
	ErrScopeInsufficient   ErrorCode = "SCOPE_INSUFFICIENT"
	ErrCrossTenantAccess   ErrorCode = "CROSS_TENANT_ACCESS"
	ErrClientMismatch      ErrorCode = "CLIENT_MISMATCH"
	ErrAccountMismatch     ErrorCode = "ACCOUNT_MISMATCH"
	ErrIdempotencyConflict ErrorCode = "IDEMPOTENCY_CONFLICT"
	ErrRateLimited         ErrorCode = "RATE_LIMITED"
	ErrQuotaExceeded       ErrorCode = "QUOTA_EXCEEDED"
	ErrUpstreamUnavailable ErrorCode = "UPSTREAM_UNAVAILABLE"
	ErrUpstreamTimeout     ErrorCode = "UPSTREAM_TIMEOUT"
	ErrUpstreamRejected    ErrorCode = "UPSTREAM_REJECTED"
	ErrInvalidRequest      ErrorCode = "INVALID_REQUEST"
	ErrInternal            ErrorCode = "INTERNAL"
)

// GatewayError is the only error type crossing the gateway boundary.
type GatewayError struct {
	Code    ErrorCode `json:"code"`
	Message string    `json:"message"`
}

func (e *GatewayError) Error() string { return string(e.Code) + ": " + e.Message }

func NewError(code ErrorCode, msg string) *GatewayError {
	return &GatewayError{Code: code, Message: msg}
}
