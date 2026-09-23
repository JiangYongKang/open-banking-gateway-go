package gateway

import (
	"context"
	"fmt"
	"log"

	"github.com/highcumontoa/open-banking-gateway-go/internal/domain"
	"github.com/highcumontoa/open-banking-gateway-go/internal/idempotency"
	"github.com/highcumontoa/open-banking-gateway-go/internal/ratelimit"
)

// ConsentChecker authorizes the consent side of a request.
type ConsentChecker interface {
	Check(tenantID domain.TenantID, clientID domain.ClientID, consentID domain.ConsentID, accountID domain.AccountID, need domain.Scope) (domain.Consent, error)
	Get(id domain.ConsentID) (domain.Consent, error)
}

// RateGate enforces per-(tenant,client) limits.
type RateGate interface {
	Allow(s ratelimit.Subject, write bool) (ratelimit.Rule, domain.ErrorCode)
	AllowQuota(s ratelimit.Subject) (ratelimit.Rule, domain.ErrorCode)
}

// IDStore is the idempotency port used by the pipeline.
type IDStore interface {
	Begin(ctx context.Context, tenantID domain.TenantID, key string, in domain.PaymentInput) (idempotency.BeginResult, error)
	Finish(key string, res *domain.PaymentResult, errCode domain.ErrorCode) error
	FailInflight(key string)
}

// Poster is the core ledger port.
type Poster interface {
	Post(ctx context.Context, in domain.PaymentInput) (domain.PaymentResult, error)
}

// Gateway is the request pipeline:
//
//	validate -> rate limit -> consent -> [idempotency claim -> quota -> ledger]
//
// Every refusal short-circuits with a distinct domain.ErrorCode and a log line
// carrying the key request fields plus the decision basis.
type Gateway struct {
	rates    RateGate
	consents ConsentChecker
	idem     IDStore
	poster   Poster
	logger   *log.Logger
}

func New(rates RateGate, consents ConsentChecker, idem IDStore, poster Poster, logger *log.Logger) *Gateway {
	return &Gateway{rates: rates, consents: consents, idem: idem, poster: poster, logger: logger}
}

func (g *Gateway) decide(in domain.PaymentInput, stage string, code domain.ErrorCode, detail string) *domain.GatewayError {
	g.logger.Printf("DECIDE reject key=%s tenant=%s client=%s consent=%s account=%s ref=%s stage=%s code=%s detail=%q",
		in.IdempotencyKey, in.TenantID, in.ClientID, in.ConsentID, in.AccountID, in.Reference, stage, code, detail)
	return domain.NewError(code, detail)
}

// SubmitPayment runs a write request through the full pipeline.
func (g *Gateway) SubmitPayment(ctx context.Context, in domain.PaymentInput) (res domain.PaymentResult, err error) {
	g.logger.Printf("ACCEPT submit key=%s tenant=%s client=%s consent=%s account=%s amount=%d %s ref=%s",
		in.IdempotencyKey, in.TenantID, in.ClientID, in.ConsentID, in.AccountID,
		in.Amount.Amount, in.Amount.Currency, in.Reference)

	if e := validate(in); e != nil {
		return domain.PaymentResult{}, g.decide(in, "validate", e.Code, e.Message)
	}

	subj := ratelimit.Subject{TenantID: in.TenantID, ClientID: in.ClientID}
	// Per-minute rate window counts every attempt; replay of a finished
	// submission still looks like traffic to an external observer.
	if _, code := g.rates.Allow(subj, false); code != "" {
		return domain.PaymentResult{}, g.decide(in, "ratelimit", code, "request rejected by per-minute window")
	}

	// Consent is evaluated BEFORE any idempotency replay: a request made with a
	// revoked/expired/over-scoped consent must be refused even if the key was
	// used successfully earlier.
	if _, cerr := g.consents.Check(in.TenantID, in.ClientID, in.ConsentID, in.AccountID, domain.ScopePaymentInit); cerr != nil {
		ge := asGatewayError(cerr)
		return domain.PaymentResult{}, g.decide(in, "consent", ge.Code, ge.Message)
	}

	begin, berr := g.idem.Begin(ctx, in.TenantID, in.IdempotencyKey, in)
	if berr != nil {
		return domain.PaymentResult{}, g.decide(in, "idempotency", domain.ErrInternal, berr.Error())
	}
	switch begin.Outcome {
	case idempotency.OutcomeConflict:
		return domain.PaymentResult{}, g.decide(in, "idempotency", domain.ErrIdempotencyConflict,
			fmt.Sprintf("idempotency key %q was already used with a different request payload", in.IdempotencyKey))
	case idempotency.OutcomeReplay:
		rec := begin.Record
		if rec.State == idempotency.StateDone {
			if rec.ErrCode != "" {
				return domain.PaymentResult{}, g.decide(in, "replay", rec.ErrCode,
					"replaying stable failure recorded for key")
			}
			out := *rec.Result
			out.Replayed = true
			g.logger.Printf("DECIDE replay key=%s paymentID=%s stable result returned (no new ledger write)",
				in.IdempotencyKey, out.PaymentID)
			return out, nil
		}
		// Unreachable in practice: Replay is only produced after StateDone.
		return domain.PaymentResult{}, g.decide(in, "idempotency", domain.ErrInternal, "unexpected inflight replay")
	}

	// OutcomeNew: we own the claim from here on. The daily write quota is
	// consumed by genuinely new writes only, so replays never burn quota.
	if _, code := g.rates.AllowQuota(subj); code != "" {
		g.idem.FailInflight(in.IdempotencyKey)
		return domain.PaymentResult{}, g.decide(in, "quota", code, "daily write quota exhausted; no ledger write occurred")
	}

	finished := false
	defer func() {
		if !finished {
			// Defensive: a panic/interruption must not strand concurrent waiters.
			if r := recover(); r != nil {
				err = domain.NewError(domain.ErrInternal, fmt.Sprintf("internal failure: %v", r))
			}
			_ = g.idem.Finish(in.IdempotencyKey, nil, domain.ErrInternal)
		}
	}()

	out, perr := g.poster.Post(ctx, in)
	if perr != nil {
		ge := normalizeUpstream(perr)
		if ferr := g.idem.Finish(in.IdempotencyKey, nil, ge.Code); ferr != nil {
			return domain.PaymentResult{}, g.decide(in, "idempotency", domain.ErrInternal, ferr.Error())
		}
		finished = true
		return domain.PaymentResult{}, g.decide(in, "ledger", ge.Code, ge.Message)
	}

	if ferr := g.idem.Finish(in.IdempotencyKey, &out, ""); ferr != nil {
		return domain.PaymentResult{}, g.decide(in, "idempotency", domain.ErrInternal, ferr.Error())
	}
	finished = true
	g.logger.Printf("DECIDE accept key=%s paymentID=%s posted to core ledger", in.IdempotencyKey, out.PaymentID)
	return out, nil
}

// ReadAccount is a read-side example guarded by the same consent/rate rules
// (no idempotency record needed for reads).
func (g *Gateway) ReadAccount(ctx context.Context, tenantID domain.TenantID, clientID domain.ClientID, consentID domain.ConsentID, accountID domain.AccountID) (string, error) {
	if _, code := g.rates.Allow(ratelimit.Subject{TenantID: tenantID, ClientID: clientID}, false); code != "" {
		return "", domain.NewError(code, "request rejected by per-minute window")
	}
	if _, e := g.consents.Check(tenantID, clientID, consentID, accountID, domain.ScopeAccountRead); e != nil {
		return "", asGatewayError(e)
	}
	return fmt.Sprintf("account-summary(%s)", accountID), nil
}

func validate(in domain.PaymentInput) *domain.GatewayError {
	switch {
	case in.IdempotencyKey == "":
		return domain.NewError(domain.ErrInvalidRequest, "idempotency key is required")
	case in.TenantID == "" || in.ClientID == "":
		return domain.NewError(domain.ErrInvalidRequest, "tenant and client are required")
	case in.ConsentID == "" || in.AccountID == "":
		return domain.NewError(domain.ErrInvalidRequest, "consent and account are required")
	case in.Amount.Amount <= 0:
		return domain.NewError(domain.ErrInvalidRequest, "amount must be positive")
	}
	return nil
}

func asGatewayError(e error) *domain.GatewayError {
	if ge, ok := e.(*domain.GatewayError); ok {
		return ge
	}
	return domain.NewError(domain.ErrInternal, e.Error())
}

// normalizeUpstream maps every downstream error to a stable category. Raw
// transport/runtime errors are NEVER passed through verbatim:
//
//   - errors exposing CodeValue() domain.ErrorCode (the ledger's *Problem)
//     keep their normalized UPSTREAM_* category;
//   - context deadline/cancellation reported by the ledger itself arrives as
//     UPSTREAM_TIMEOUT via the same mechanism;
//   - anything else is collapsed into UPSTREAM_UNAVAILABLE.
func normalizeUpstream(e error) *domain.GatewayError {
	if ge, ok := e.(*domain.GatewayError); ok {
		return ge
	}
	if c, ok := e.(interface{ CodeValue() domain.ErrorCode }); ok {
		return domain.NewError(c.CodeValue(), e.Error())
	}
	return domain.NewError(domain.ErrUpstreamUnavailable, "upstream call failed in an unclassified way")
}
