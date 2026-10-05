package gateway

import "github.com/highcumontoa/open-banking-gateway-go/internal/quota"

// Code 是网关归一化后的错误类别。
type Code string

const (
	CodeConsentDenied       Code = "CONSENT_DENIED"
	CodeRateLimited         Code = "RATE_LIMITED"
	CodeQuotaExhausted      Code = "QUOTA_EXHAUSTED"
	CodeIdempotencyConflict Code = "IDEMPOTENCY_CONFLICT"
	CodeUpstreamRejected    Code = "UPSTREAM_REJECTED"
	CodeUpstreamTimeout     Code = "UPSTREAM_TIMEOUT"
	CodeInternal            Code = "INTERNAL"
)

// Error 是网关对外暴露的归一化错误。
type Error struct {
	Code    Code
	Message string
	// QuotaReason 与 Usage 仅在 Code==QUOTA_EXHAUSTED 时有效，
	// 给出具体的额度拒绝原因、本周期剩余与重置时间。
	QuotaReason quota.DenyReason
	Usage       quota.Usage
}

func (e *Error) Error() string { return string(e.Code) + ": " + e.Message }
