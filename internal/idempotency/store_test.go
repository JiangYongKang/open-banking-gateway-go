package idempotency

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/highcumontoa/open-banking-gateway-go/internal/domain"
)

func samplePayment(ref string) domain.PaymentInput {
	return domain.PaymentInput{
		IdempotencyKey: "key-1",
		TenantID:       "tenant-a",
		ClientID:       "client-1",
		ConsentID:      "cons-1",
		AccountID:      "acct-1",
		Amount:         domain.Money{Amount: 100, Currency: "EUR"},
		Reference:      ref,
	}
}

func TestFingerprintStableAndSensitive(t *testing.T) {
	fp := JSONFingerprint{}
	in := samplePayment("ref-A")
	h1, err := fp.Fingerprint(in)
	if err != nil {
		t.Fatal(err)
	}
	in2 := samplePayment("ref-A")
	in2.IdempotencyKey = "a-different-key-but-same-business-payload"
	h2, _ := fp.Fingerprint(in2)
	if h1 != h2 {
		t.Fatalf("fingerprint must ignore the idempotency key: %s vs %s", h1, h2)
	}
	t.Logf("FINGERPRINT same-business-payload hash=%s stable across keys", h1)

	diff := samplePayment("ref-B")
	h3, _ := fp.Fingerprint(diff)
	if h1 == h3 {
		t.Fatal("different business payload must produce different fingerprints")
	}
	t.Logf("FINGERPRINT changed-payload hash=%s differs from %s", h3, h1)
}

func TestBeginThenReplayReturnsSameResult(t *testing.T) {
	s := NewStore(JSONFingerprint{})
	in := samplePayment("ref-A")
	ctx := context.Background()

	b1, err := s.Begin(ctx, "tenant-a", in.IdempotencyKey, in)
	if err != nil || b1.Outcome != OutcomeNew {
		t.Fatalf("first begin = %+v, %v", b1, err)
	}
	res := &domain.PaymentResult{PaymentID: "pay-1", Status: "posted"}
	if err := s.Finish(in.IdempotencyKey, res, ""); err != nil {
		t.Fatal(err)
	}

	b2, err := s.Begin(ctx, "tenant-a", in.IdempotencyKey, in)
	if err != nil {
		t.Fatal(err)
	}
	if b2.Outcome != OutcomeReplay || b2.Record == nil || b2.Record.Result.PaymentID != "pay-1" {
		t.Fatalf("expected replay of pay-1, got %+v", b2)
	}
	t.Logf("REPEAT key=%s outcome=REPLAY paymentID=%s (no re-execution)",
		in.IdempotencyKey, b2.Record.Result.PaymentID)
}

func TestSameKeyDifferentPayloadIsConflictNotReplay(t *testing.T) {
	s := NewStore(JSONFingerprint{})
	in := samplePayment("ref-A")
	ctx := context.Background()

	b1, _ := s.Begin(ctx, "tenant-a", in.IdempotencyKey, in)
	if b1.Outcome != OutcomeNew {
		t.Fatalf("want New, got %v", b1.Outcome)
	}
	if err := s.Finish(in.IdempotencyKey, &domain.PaymentResult{PaymentID: "pay-1"}, ""); err != nil {
		t.Fatal(err)
	}

	// Same key, tampered amount: must be refused as conflict, never replayed.
	tampered := in
	tampered.Amount = domain.Money{Amount: 9999, Currency: "EUR"}
	b2, err := s.Begin(ctx, "tenant-a", in.IdempotencyKey, tampered)
	if err != nil {
		t.Fatal(err)
	}
	if b2.Outcome != OutcomeConflict {
		t.Fatalf("expected OutcomeConflict, got %v", b2.Outcome)
	}
	t.Logf("CONFLICT key=%s originalHash=%s tamperedHash=%s outcome=CONFLICT (replay refused)",
		in.IdempotencyKey, b1.Record.Fingerprint, b2.Record.Fingerprint)

	// The original result must remain intact after a conflict attempt.
	b3, _ := s.Begin(ctx, "tenant-a", in.IdempotencyKey, in)
	if b3.Outcome != OutcomeReplay || b3.Record.Result.PaymentID != "pay-1" {
		t.Fatalf("original record must survive conflict, got %+v", b3)
	}
}

func TestConcurrentDuplicatesPostExactlyOnce(t *testing.T) {
	s := NewStore(JSONFingerprint{})
	in := samplePayment("ref-concurrent")
	ctx := context.Background()

	const n = 32
	var (
		wg       sync.WaitGroup
		executed atomic.Int32
		replays  atomic.Int32
		conflict atomic.Int32
		results  = make([]*domain.PaymentResult, n)
		errs     = make([]error, n)
		outcomes = make([]Outcome, n)
		start    = make(chan struct{})
	)

	wg.Add(n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			<-start
			b, err := s.Begin(ctx, "tenant-a", in.IdempotencyKey, in)
			if err != nil {
				errs[i] = err
				return
			}
			outcomes[i] = b.Outcome
			switch b.Outcome {
			case OutcomeNew:
				// Only the single winner reaches the "core ledger".
				executed.Add(1)
				res := &domain.PaymentResult{PaymentID: "pay-unique", Status: "posted"}
				_ = s.Finish(in.IdempotencyKey, res, "")
				results[i] = res
			case OutcomeReplay:
				replays.Add(1)
				results[i] = b.Record.Result
			case OutcomeConflict:
				conflict.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("goroutine %d: %v", i, err)
		}
	}
	if executed.Load() != 1 {
		t.Fatalf("exactly one execution expected, got %d", executed.Load())
	}
	if replays.Load() != int32(n-1) {
		t.Fatalf("expected %d replays, got %d", n-1, replays.Load())
	}
	if conflict.Load() != 0 {
		t.Fatalf("identical concurrent payloads must not conflict, got %d", conflict.Load())
	}
	for i, r := range results {
		if r == nil || r.PaymentID != "pay-unique" {
			t.Fatalf("goroutine %d got unstable result %v", i, r)
		}
	}
	t.Logf("CONCURRENT key=%s goroutines=%d executed=%d replays=%d conflicts=%d paymentID=pay-unique (all results identical)",
		in.IdempotencyKey, n, executed.Load(), replays.Load(), conflict.Load())
}

func TestConcurrentFailureIsReplayedStably(t *testing.T) {
	s := NewStore(JSONFingerprint{})
	in := samplePayment("ref-upstream-fail")
	ctx := context.Background()

	const n = 16
	var (
		wg       sync.WaitGroup
		executed int32
		starts   = make(chan struct{})
		gotCodes = make([]domain.ErrorCode, n)
	)
	wg.Add(n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			<-starts
			b, err := s.Begin(ctx, "tenant-a", in.IdempotencyKey, in)
			if err != nil {
				t.Errorf("begin: %v", err)
				return
			}
			if b.Outcome == OutcomeNew {
				executed++
				_ = s.Finish(in.IdempotencyKey, nil, domain.ErrUpstreamRejected)
				gotCodes[i] = domain.ErrUpstreamRejected
				return
			}
			gotCodes[i] = b.Record.ErrCode
		}()
	}
	close(starts)
	wg.Wait()

	if executed != 1 {
		t.Fatalf("want exactly one execution, got %d", executed)
	}
	for i, c := range gotCodes {
		if c != domain.ErrUpstreamRejected {
			t.Fatalf("goroutine %d code=%s, want stable UPSTREAM_REJECTED", i, c)
		}
	}
	t.Logf("CONCURRENT-FAIL key=%s executed=%d all %d clients received same code=UPSTREAM_REJECTED",
		in.IdempotencyKey, executed, n)
}

func TestWaiterContextCancelDoesNotCorruptRecord(t *testing.T) {
	s := NewStore(JSONFingerprint{})
	in := samplePayment("ref-slow")

	winner, err := s.Begin(context.Background(), "tenant-a", in.IdempotencyKey, in)
	if err != nil || winner.Outcome != OutcomeNew {
		t.Fatalf("winner begin: %+v %v", winner, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, werr := s.Begin(ctx, "tenant-a", in.IdempotencyKey, in)
	if !errors.Is(werr, context.DeadlineExceeded) {
		t.Fatalf("waiter should time out, got %v", werr)
	}
	t.Logf("WAIT-CANCEL after=%s waiter gave up while first execution still in flight", time.Since(start).Round(time.Millisecond))

	// Winner finishes afterwards; a fresh retry must still get the real replay.
	if err := s.Finish(in.IdempotencyKey, &domain.PaymentResult{PaymentID: "pay-after-wait"}, ""); err != nil {
		t.Fatal(err)
	}
	b, err := s.Begin(context.Background(), "tenant-a", in.IdempotencyKey, in)
	if err != nil || b.Outcome != OutcomeReplay || b.Record.Result.PaymentID != "pay-after-wait" {
		t.Fatalf("record corrupted by cancelled waiter: %+v %v", b, err)
	}
	t.Logf("POST-CANCEL key=%s replay paymentID=%s intact", in.IdempotencyKey, b.Record.Result.PaymentID)
}

func TestKeyScopedByTenant(t *testing.T) {
	s := NewStore(JSONFingerprint{})
	in := samplePayment("ref-tenant")
	b1, _ := s.Begin(context.Background(), "tenant-a", in.IdempotencyKey, in)
	if b1.Outcome != OutcomeNew {
		t.Fatalf("want new, got %v", b1.Outcome)
	}
	// Same key string reused by another tenant with an otherwise identical
	// payload: treated as a conflicting reuse, never cross-tenant replay.
	b2, _ := s.Begin(context.Background(), "tenant-b", in.IdempotencyKey, in)
	if b2.Outcome != OutcomeConflict {
		t.Fatalf("cross-tenant key reuse must conflict, got %v", b2.Outcome)
	}
	t.Logf("TENANT-ISOLATION key=%s tenant-a=NEW tenant-b=CONFLICT", in.IdempotencyKey)
}

func TestFailInflightLetsWaitingRetryProceed(t *testing.T) {
	s := NewStore(JSONFingerprint{})
	in := samplePayment("ref-released")

	winner, err := s.Begin(context.Background(), "tenant-a", in.IdempotencyKey, in)
	if err != nil || winner.Outcome != OutcomeNew {
		t.Fatalf("winner begin: %+v %v", winner, err)
	}

	got := make(chan Outcome, 1)
	go func() {
		b, err := s.Begin(context.Background(), "tenant-a", in.IdempotencyKey, in)
		if err != nil {
			got <- -1
			return
		}
		got <- b.Outcome
	}()

	time.Sleep(30 * time.Millisecond)
	// Quota rejection before any downstream work: release the claim.
	s.FailInflight(in.IdempotencyKey)

	select {
	case o := <-got:
		if o != OutcomeNew {
			t.Fatalf("retry after FailInflight should win a fresh claim, got %v", o)
		}
		t.Logf("RETRY key=%s after FailInflight outcome=NEW (claim released cleanly)", in.IdempotencyKey)
	case <-time.After(time.Second):
		t.Fatal("waiting retry did not proceed after FailInflight")
	}
}
