package ledger

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/highcumontoa/open-banking-gateway-go/internal/domain"
)

// Clock allows deterministic tests.
type Clock func() time.Time

// Problem is the normalized failure model of the upstream core service.
// Ledger never leaks raw transport/runtime errors; it maps every failure to a
// stable domain.ErrorCode understood by the gateway.
type Problem struct {
	Code domain.ErrorCode // UPSTREAM_TIMEOUT / UPSTREAM_UNAVAILABLE / UPSTREAM_REJECTED
	Msg  string
}

func (p *Problem) Error() string { return string(p.Code) + ": " + p.Msg }

// CodeValue exposes the normalized error category for the gateway's
// error-normalization layer without the gateway importing this package.
func (p *Problem) CodeValue() domain.ErrorCode { return p.Code }

// Fault describes an injected upstream behavior for a specific reference.
type Fault struct {
	Delay   time.Duration // delay before responding (deadline may expire first)
	Code    domain.ErrorCode
	Message string
}

// paymentRow is one persisted core-ledger entry.
type paymentRow struct {
	ID        string
	AccountID domain.AccountID
	Amount    domain.Money
	Reference string
	PostedAt  time.Time
}

// Ledger is the mocked core banking (account servicing) service.
// It persists writes itself, which lets the end-to-end demo prove that retried
// submissions never post twice.
type Ledger struct {
	mu       sync.Mutex
	seq      atomic.Uint64
	accounts map[domain.AccountID]int64 // balance in account currency (minor units)
	payments map[string]paymentRow      // paymentID -> row
	faults   map[string]Fault           // reference -> injected fault
	now      Clock
}

func NewLedger(now Clock) *Ledger {
	if now == nil {
		now = time.Now
	}
	return &Ledger{
		accounts: make(map[domain.AccountID]int64),
		payments: make(map[string]paymentRow),
		faults:   make(map[string]Fault),
		now:      now,
	}
}

// SetAccount provisions an account with an opening balance.
func (l *Ledger) SetAccount(id domain.AccountID, balance int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.accounts[id] = balance
}

// InjectFault arranges deterministic upstream behavior for a reference.
func (l *Ledger) InjectFault(reference string, f Fault) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.faults[reference] = f
}

// CountPayments returns how many entries the core ledger currently holds.
func (l *Ledger) CountPayments() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.payments)
}

// Post posts a payment to the core ledger.
//
// Order of checks:
//  1. context deadline/cancellation while waiting on injected delay
//     -> UPSTREAM_TIMEOUT (normalized, raw ctx error never leaks)
//  2. injected explicit fault -> its normalized code
//  3. unknown account / insufficient funds -> UPSTREAM_REJECTED
//  4. success: balance debited and one payment row persisted
func (l *Ledger) Post(ctx context.Context, in domain.PaymentInput) (domain.PaymentResult, error) {
	l.mu.Lock()
	fault, hasFault := l.faults[in.Reference]
	l.mu.Unlock()

	if fault.Delay > 0 {
		select {
		case <-time.After(fault.Delay):
		case <-ctx.Done():
			return domain.PaymentResult{}, &Problem{
				Code: domain.ErrUpstreamTimeout,
				Msg:  "core ledger did not respond before deadline",
			}
		}
	}
	if hasFault {
		return domain.PaymentResult{}, &Problem{Code: fault.Code, Msg: fault.Message}
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	balance, ok := l.accounts[in.AccountID]
	if !ok {
		return domain.PaymentResult{}, &Problem{
			Code: domain.ErrUpstreamRejected,
			Msg:  "unknown account " + string(in.AccountID),
		}
	}
	if in.Amount.Amount <= 0 {
		return domain.PaymentResult{}, &Problem{
			Code: domain.ErrUpstreamRejected,
			Msg:  "payment amount must be positive",
		}
	}
	if balance < in.Amount.Amount {
		return domain.PaymentResult{}, &Problem{
			Code: domain.ErrUpstreamRejected,
			Msg:  fmt.Sprintf("insufficient funds: balance %d < requested %d", balance, in.Amount.Amount),
		}
	}
	if currencyInvalid(in.Amount.Currency) {
		return domain.PaymentResult{}, &Problem{
			Code: domain.ErrUpstreamRejected,
			Msg:  "unsupported currency code",
		}
	}

	n := l.seq.Add(1)
	id := fmt.Sprintf("pay-%d-%d", l.now().UnixNano(), n)
	row := paymentRow{
		ID:        id,
		AccountID: in.AccountID,
		Amount:    in.Amount,
		Reference: in.Reference,
		PostedAt:  l.now(),
	}
	l.payments[id] = row
	l.accounts[in.AccountID] = balance - in.Amount.Amount

	return domain.PaymentResult{
		PaymentID: id,
		Status:    "posted",
		Replayed:  false,
		CreatedAt: row.PostedAt,
	}, nil
}

func currencyValid(c string) bool { return !currencyInvalid(c) }

func currencyInvalid(c string) bool {
	if len(c) != 3 {
		return true
	}
	return strings.ToUpper(c) != c
}

var _ = currencyValid
