package idempotency

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/highcumontoa/open-banking-gateway-go/internal/domain"
)

// Fingerprinter produces a deterministic content fingerprint of a request.
// The idempotency key itself MUST be excluded from the fingerprint.
type Fingerprinter interface {
	Fingerprint(in domain.PaymentInput) (string, error)
}

// State of a stored idempotency record.
type State string

const (
	StateInflight State = "inflight"
	StateDone     State = "done"
)

// Record stores the outcome associated with an idempotency key.
type Record struct {
	Key         string
	TenantID    domain.TenantID
	Fingerprint string
	State       State
	Result      *domain.PaymentResult
	ErrCode     domain.ErrorCode
	SavedAt     time.Time
}

// Outcome of a Begin call.
type Outcome int

const (
	OutcomeNew      Outcome = iota // caller must execute and Finish
	OutcomeReplay                  // existing completed record returned
	OutcomeConflict                // same key, different payload
)

// BeginResult describes what the caller should do.
type BeginResult struct {
	Outcome Outcome
	Record  *Record
}

type entry struct {
	rec  Record
	done chan struct{} // closed when the record reaches a terminal state
}

// Store implements replay-safe idempotency with in-flight locking.
type Store struct {
	mu sync.Mutex
	m  map[string]*entry
	fp Fingerprinter
}

func NewStore(fp Fingerprinter) *Store {
	return &Store{m: make(map[string]*entry), fp: fp}
}

// Begin claims the key for tenantID, or returns the existing record.
//
//   - first caller for a key gets OutcomeNew and MUST call Finish exactly once;
//   - concurrent callers with the same payload block until the first call
//     finishes and then get OutcomeReplay with the stable terminal result;
//   - any caller whose payload fingerprint differs gets OutcomeConflict;
//   - if the first claim is released via FailInflight, waiters wake up and
//     compete anew for a fresh claim (so a later retry can legitimately win);
//   - a blocked caller whose context ends gets that context error.
func (s *Store) Begin(ctx context.Context, tenantID domain.TenantID, key string, in domain.PaymentInput) (BeginResult, error) {
	if key == "" {
		return BeginResult{}, domain.NewError(domain.ErrInternal, "idempotency key is required")
	}
	fp, err := s.fp.Fingerprint(in)
	if err != nil {
		return BeginResult{}, err
	}

	for {
		s.mu.Lock()
		e, ok := s.m[key]
		switch {
		case !ok:
			// No claim exists, or a previous one was released with FailInflight.
			e = &entry{
				rec: Record{
					Key:         key,
					TenantID:    tenantID,
					Fingerprint: fp,
					State:       StateInflight,
					SavedAt:     timeNow(),
				},
				done: make(chan struct{}),
			}
			s.m[key] = e
			s.mu.Unlock()
			return BeginResult{Outcome: OutcomeNew, Record: &e.rec}, nil
		case e.rec.Fingerprint != fp || e.rec.TenantID != tenantID:
			s.mu.Unlock()
			return BeginResult{Outcome: OutcomeConflict, Record: &e.rec}, nil
		case e.rec.State == StateDone:
			cp := e.rec
			s.mu.Unlock()
			return BeginResult{Outcome: OutcomeReplay, Record: &cp}, nil
		}
		// Same payload, first caller still in flight: release lock and wait.
		ch := e.done
		s.mu.Unlock()
		select {
		case <-ch:
			continue // re-evaluate under the lock (Finish or released claim)
		case <-ctx.Done():
			return BeginResult{}, ctx.Err()
		}
	}
}

// Finish records the terminal result of a key previously claimed with Begin.
// It must be called exactly once per OutcomeNew claim, even on failure.
func (s *Store) Finish(key string, res *domain.PaymentResult, errCode domain.ErrorCode) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.m[key]
	if !ok {
		return errors.New("idempotency: Finish called for unknown key " + key)
	}
	if e.rec.State == StateDone {
		return errors.New("idempotency: Finish called twice for key " + key)
	}
	e.rec.State = StateDone
	e.rec.Result = res
	e.rec.ErrCode = errCode
	close(e.done)
	return nil
}

// FailInflight releases a claim without a terminal business result, used when
// the caller cannot even start the downstream work (e.g. quota rejection).
// Waiting callers wake up and may claim the key anew; finished records are
// never removed.
func (s *Store) FailInflight(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.m[key]
	if !ok || e.rec.State == StateDone {
		return
	}
	delete(s.m, key)
	close(e.done)
}

// timeNow is indirected solely to keep the file easy to reason about.
func timeNow() time.Time { return time.Now() }
