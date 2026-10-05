// Package gateway 编排同意校验、限流、幂等与账务写入。
package gateway

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
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
	Entry *ledger.Entry
	// QuotaUsage 是该同意当前周期的用量视图（仅在同意配置了额度时填充）。
	QuotaUsage *quota.Usage
	Replayed   bool
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

// hasQuota 报告策略是否配置了任一维度的额度。
func hasQuota(p quota.Policy) bool { return p.MaxCount > 0 || p.MaxAmount > 0 }

// ensureQuotaPolicy 幂等地把同意上的额度策略登记到额度管理器（首次见到该同意时）。
func (g *Gateway) ensureQuotaPolicy(c *consent.Consent) {
	if g.quotas == nil || !hasQuota(c.Quota) {
		return
	}
	// SetPolicy 对重复登记返回错误，忽略即可：策略在同意建立时即固定。
	_ = g.quotas.SetPolicy(c.ID, c.Quota)
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
// 处理顺序固定：同意校验 -> 限流 -> 幂等 -> 周期额度 -> 账务写入，保证结论可解释。
func (g *Gateway) HandleWrite(req Request) (*Response, error) {
	// 1. 同意校验：缺省/撤销/过期/范围不足/跨租户均在此拒绝。
	dec := g.consents.Authorize(req.TenantID, req.ConsentID, req.RequiredScopes)
	if !dec.Allowed {
		return nil, &Error{Code: CodeConsentDenied, Message: string(dec.Reason)}
	}
	// 同意建立时携带的额度策略在首次使用时幂等登记；未配置额度则行为与以前一致。
	g.ensureQuotaPolicy(dec.Consent)

	// 2. 限流：按 (租户, 客户端) 维度计数，互不影响。
	rl := g.limiter.Allow(req.TenantID, req.ClientID)
	if !rl.Allowed {
		return nil, &Error{Code: CodeRateLimited,
			Message: fmt.Sprintf("quota exhausted, retry after %s", rl.ResetAt.UTC().Format(time.RFC3339))}
	}

	// 3. 幂等：同键同内容直接回放；同键不同内容拒绝；同键在处理中则等待首个请求落定。
	//    BeginOrWait 保证客户端超时重发的重试风暴最终回放同一笔结果，而不是各自冲突。
	fp := fingerprint(req)
	rec, replay, err := g.idem.BeginOrWait(req.IdempotencyKey, fp)
	switch {
	case errors.Is(err, idempotency.ErrConflict):
		return nil, &Error{Code: CodeIdempotencyConflict, Message: "idempotency key reused with different payload"}
	case err != nil:
		return nil, &Error{Code: CodeInternal, Message: "idempotency store failure"}
	case replay:
		var e ledger.Entry
		if jerr := json.Unmarshal(rec.Body, &e); jerr != nil {
			return nil, &Error{Code: CodeInternal, Message: "corrupt idempotency record"}
		}
		// 回放不重复扣额度；附带当前周期用量供调用方观察。
		return &Response{Entry: &e, QuotaUsage: g.usageView(req.ConsentID), Replayed: true}, nil
	}

	// 4. 同意级周期额度：预占。同一幂等键在额度层同样去重，重复/并发提交不二次扣减。
	qd := g.quotas.Hold(req.ConsentID, req.IdempotencyKey, 1, req.Amount, req.Currency)
	if !qd.Allowed {
		// 额度未真实扣减，释放幂等占用允许下一周期（或补额后）重试。
		g.idem.Abandon(req.IdempotencyKey)
		return nil, g.quotaError(qd)
	}

	// 5. 账务写入；失败时同时释放额度预占与幂等占用，故障恢复后重试可正常扣减。
	entry, lerr := g.ledger.Post(ledger.Entry{
		TenantID:       req.TenantID,
		AccountID:      req.AccountID,
		Amount:         req.Amount,
		Currency:       req.Currency,
		Memo:           req.Memo,
		IdempotencyKey: req.IdempotencyKey,
	})
	if lerr != nil {
		g.quotas.Release(req.ConsentID, req.IdempotencyKey)
		g.idem.Abandon(req.IdempotencyKey)
		return nil, normalizeLedgerError(lerr)
	}

	// 入账成功：额度扣减与最终结果在同一成功路径上落定。先提交额度，
	// 再固化幂等结果；此后同键的任何提交/并发都只会回放这一笔。
	g.quotas.Commit(req.ConsentID, req.IdempotencyKey)

	body, jerr := json.Marshal(entry)
	if jerr != nil {
		g.idem.Abandon(req.IdempotencyKey)
		return nil, &Error{Code: CodeInternal, Message: "failed to persist idempotency record"}
	}
	g.idem.Complete(req.IdempotencyKey, idempotency.Record{
		Key:         req.IdempotencyKey,
		Fingerprint: fp,
		Status:      200,
		Body:        body,
	})
	return &Response{Entry: &entry, QuotaUsage: g.usageView(req.ConsentID)}, nil
}

// usageView 返回同意当前周期用量；未配置额度时为 nil。
func (g *Gateway) usageView(consentID string) *quota.Usage {
	if u, ok := g.quotas.GetUsage(consentID); ok {
		return &u
	}
	return nil
}

// quotaError 将额度拒绝归一化为 QUOTA_EXHAUSTED，并携带原因、剩余额度与重置时间。
func (g *Gateway) quotaError(qd quota.Decision) *Error {
	u := qd.Usage
	rc, ra := u.RemainingCount(), u.RemainingAmount()
	return &Error{
		Code:        CodeQuotaExhausted,
		QuotaReason: qd.Reason,
		Usage:       u,
		Message: fmt.Sprintf("consent quota exhausted (%s), remaining count=%s amount=%s%s, resets at %s",
			qd.Reason, unlimited(int64(rc)), unlimited(ra), currencySuffix(u.Currency, ra), u.ResetAt.UTC().Format(time.RFC3339)),
	}
}

// unlimited 把“未配置上限”的 -1 哨兵渲染为可读文本。
func unlimited(v int64) string {
	if v < 0 {
		return "unlimited"
	}
	return strconv.FormatInt(v, 10)
}

// currencySuffix 仅在金额维度实际受限（Currency 非空）时追加币种。
func currencySuffix(currency string, remaining int64) string {
	if currency == "" || remaining < 0 {
		return ""
	}
	return " " + currency
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
