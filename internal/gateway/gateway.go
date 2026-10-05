// Package gateway 编排同意校验、限流、幂等与账务写入。
package gateway

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/highcumontoa/open-banking-gateway-go/internal/consent"
	"github.com/highcumontoa/open-banking-gateway-go/internal/idempotency"
	"github.com/highcumontoa/open-banking-gateway-go/internal/ledger"
	"github.com/highcumontoa/open-banking-gateway-go/internal/quota"
	"github.com/highcumontoa/open-banking-gateway-go/internal/ratelimit"
)

// Request 是一次写入类请求。
type Request struct {
	TenantID       string
	ClientID       string
	ConsentID      string
	RequiredScopes []string
	IdempotencyKey string
	AccountID      string
	Amount         int64
	Currency       string
	Memo           string
}

// Response 是请求处理结果。
type Response struct {
	Entry    *ledger.Entry
	Replayed bool
}

// Gateway 是接口网关。
type Gateway struct {
	consents *consent.Authorizer
	idem     idempotency.Store
	limiter  *ratelimit.Limiter
	quotas   *quota.Manager
	ledger   ledger.Service
	now      func() time.Time
}

func New(consents *consent.Authorizer, idem idempotency.Store, limiter *ratelimit.Limiter, quotas *quota.Manager, lg ledger.Service, now func() time.Time) *Gateway {
	return &Gateway{consents: consents, idem: idem, limiter: limiter, quotas: quotas, ledger: lg, now: now}
}

// fingerprint 对请求内容做稳定哈希，用于检测幂等键复用时的内容不一致。
func fingerprint(req Request) string {
	h := sha256.New()
	fmt.Fprintf(h, "%s|%s|%s|%v|%s|%d|%s|%s",
		req.TenantID, req.ClientID, req.ConsentID, req.RequiredScopes,
		req.AccountID, req.Amount, req.Currency, req.Memo)
	return hex.EncodeToString(h.Sum(nil))
}

// HandleWrite 处理一次写入类请求。
// 处理顺序固定：同意校验 -> 限流 -> 幂等 -> 额度预占 -> 账务写入（成功后额度实扣），
// 保证结论可解释；幂等回放不消耗额度。
func (g *Gateway) HandleWrite(req Request) (*Response, error) {
	// 1. 同意校验：缺省/撤销/过期/范围不足/跨租户均在此拒绝。
	dec := g.consents.Authorize(req.TenantID, req.ConsentID, req.RequiredScopes)
	if !dec.Allowed {
		return nil, &Error{Code: CodeConsentDenied, Message: string(dec.Reason)}
	}

	// 2. 限流：按 (租户, 客户端) 维度计数，互不影响。
	rl := g.limiter.Allow(req.TenantID, req.ClientID)
	if !rl.Allowed {
		return nil, &Error{Code: CodeRateLimited,
			Message: fmt.Sprintf("quota exhausted, retry after %s", rl.ResetAt.UTC().Format(time.RFC3339))}
	}

	// 3. 幂等：同键同内容直接回放；同键不同内容拒绝；同键在处理中拒绝。
	fp := fingerprint(req)
	rec, replay, err := g.idem.Begin(req.IdempotencyKey, fp)
	switch {
	case errors.Is(err, idempotency.ErrConflict):
		return nil, &Error{Code: CodeIdempotencyConflict, Message: "idempotency key reused with different payload"}
	case errors.Is(err, idempotency.ErrInFlight):
		return nil, &Error{Code: CodeIdempotencyConflict, Message: "request with same idempotency key in flight"}
	case err != nil:
		return nil, &Error{Code: CodeInternal, Message: "idempotency store failure"}
	case replay:
		var e ledger.Entry
		if jerr := json.Unmarshal(rec.Body, &e); jerr != nil {
			return nil, &Error{Code: CodeInternal, Message: "corrupt idempotency record"}
		}
		return &Response{Entry: &e, Replayed: true}, nil
	}

	// 4. 同意级额度：仅对配置了额度的同意生效；按幂等键预占，写入成功才实扣。
	//    预占失败时释放幂等占用，下一周期重试可正常进入。
	var res *quota.Reservation
	if dec.Consent.Quota != nil {
		r, qerr := g.quotas.Reserve(req.ConsentID, req.IdempotencyKey, req.Amount, *dec.Consent.Quota)
		if qerr != nil {
			g.idem.Abandon(req.IdempotencyKey)
			var ee *quota.ExhaustedError
			if errors.As(qerr, &ee) {
				return nil, &Error{Code: CodeQuotaExceeded, Message: fmt.Sprintf(
					"consent period allowance exhausted, remaining_count=%d remaining_amount=%d reset_at=%s",
					ee.Status.RemainingCount, ee.Status.RemainingAmount,
					ee.Status.ResetAt.UTC().Format(time.RFC3339))}
			}
			return nil, &Error{Code: CodeInternal, Message: "quota store failure"}
		}
		res = r
	}

	// 5. 账务写入；失败时退回额度预占并释放幂等占用以允许重试，错误归一化为可区分类别。
	entry, lerr := g.ledger.Post(ledger.Entry{
		TenantID:       req.TenantID,
		AccountID:      req.AccountID,
		Amount:         req.Amount,
		Currency:       req.Currency,
		Memo:           req.Memo,
		IdempotencyKey: req.IdempotencyKey,
	})
	if lerr != nil {
		g.quotas.Release(res)
		g.idem.Abandon(req.IdempotencyKey)
		return nil, normalizeLedgerError(lerr)
	}
	// 写入最终成功：额度预占落定为实扣（同一幂等键全程只扣这一次）。
	g.quotas.Commit(res)

	body, jerr := json.Marshal(entry)
	if jerr != nil {
		g.quotas.Release(res)
		g.idem.Abandon(req.IdempotencyKey)
		return nil, &Error{Code: CodeInternal, Message: "failed to persist idempotency record"}
	}
	g.idem.Complete(req.IdempotencyKey, idempotency.Record{
		Key:         req.IdempotencyKey,
		Fingerprint: fp,
		Status:      200,
		Body:        body,
	})
	return &Response{Entry: &entry}, nil
}

// QuotaStatus 查询某同意当前周期的额度用量（已用/剩余/下次重置时间）。
// 同意未配置额度时返回 (nil, nil)；跨租户查询按 CONSENT_DENIED 拒绝，不泄露用量。
func (g *Gateway) QuotaStatus(tenantID, consentID string) (*quota.Status, error) {
	dec := g.consents.Authorize(tenantID, consentID, nil)
	if !dec.Allowed {
		return nil, &Error{Code: CodeConsentDenied, Message: string(dec.Reason)}
	}
	if dec.Consent.Quota == nil {
		return nil, nil
	}
	st := g.quotas.Status(consentID, *dec.Consent.Quota)
	return &st, nil
}

// normalizeLedgerError 将底层账务错误归一化为可区分类别，不透出原始异常。
func normalizeLedgerError(err error) *Error {
	switch {
	case errors.Is(err, ledger.ErrUpstreamTimeout):
		return &Error{Code: CodeUpstreamTimeout, Message: "upstream core banking timed out"}
	case errors.Is(err, ledger.ErrUpstreamRejected):
		return &Error{Code: CodeUpstreamRejected, Message: "upstream core banking rejected the posting"}
	default:
		return &Error{Code: CodeInternal, Message: "internal gateway error"}
	}
}
