package gateway

// Code 是网关归一化后的错误类别。
type Code string

const (
	CodeConsentDenied       Code = "CONSENT_DENIED"
	CodeRateLimited         Code = "RATE_LIMITED"
	CodeIdempotencyConflict Code = "IDEMPOTENCY_CONFLICT"
	CodeUpstreamRejected    Code = "UPSTREAM_REJECTED"
	CodeUpstreamTimeout     Code = "UPSTREAM_TIMEOUT"
	CodeInternal            Code = "INTERNAL"
)

// Error 是网关对外暴露的归一化错误。
type Error struct {
	Code    Code
	Message string
}

func (e *Error) Error() string { return string(e.Code) + ": " + e.Message }
