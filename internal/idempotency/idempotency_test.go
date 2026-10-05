package idempotency

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func TestBeginStates(t *testing.T) {
	m := NewMemoryStore()

	// 首次占用。
	if _, replay, err := m.Begin("k1", "fp1"); err != nil || replay {
		t.Fatalf("first begin: replay=%v err=%v", replay, err)
	}
	// 处理中：同指纹返回 ErrInFlight，不同指纹返回 ErrConflict。
	if _, _, err := m.Begin("k1", "fp1"); !errors.Is(err, ErrInFlight) {
		t.Fatalf("in-flight err=%v, want ErrInFlight", err)
	}
	if _, _, err := m.Begin("k1", "fp2"); !errors.Is(err, ErrConflict) {
		t.Fatalf("conflict err=%v, want ErrConflict", err)
	}

	// 完成后同指纹回放，不同指纹冲突。
	m.Complete("k1", Record{Key: "k1", Fingerprint: "fp1", Status: 200, Body: []byte("ok")})
	rec, replay, err := m.Begin("k1", "fp1")
	if err != nil || !replay || string(rec.Body) != "ok" {
		t.Fatalf("replay: rec=%+v replay=%v err=%v", rec, replay, err)
	}
	if _, _, err := m.Begin("k1", "fp2"); !errors.Is(err, ErrConflict) {
		t.Fatalf("post-complete conflict err=%v", err)
	}

	// Abandon 后同键可重新占用（失败重试场景）。
	if _, _, err := m.Begin("k2", "fp1"); err != nil {
		t.Fatalf("begin k2: %v", err)
	}
	m.Abandon("k2")
	if _, replay, err := m.Begin("k2", "fp1"); err != nil || replay {
		t.Fatalf("re-begin after abandon: replay=%v err=%v", replay, err)
	}
}

func TestBeginOrWaitConverges(t *testing.T) {
	m := NewMemoryStore()

	// 首个请求占用，模拟处理中。
	if _, replay, err := m.BeginOrWait("w1", "fp"); err != nil || replay {
		t.Fatalf("first BeginOrWait: replay=%v err=%v", replay, err)
	}

	results := make(chan *Record, 8)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rec, replay, err := m.BeginOrWait("w1", "fp")
			if err != nil || !replay {
				t.Errorf("waiter: replay=%v err=%v", replay, err)
				return
			}
			results <- rec
		}()
	}
	// 给等待者一点时间进入阻塞，再由首个请求落定。
	time.Sleep(20 * time.Millisecond)
	m.Complete("w1", Record{Key: "w1", Fingerprint: "fp", Status: 200, Body: []byte("settled")})
	wg.Wait()
	close(results)
	n := 0
	for rec := range results {
		n++
		if string(rec.Body) != "settled" {
			t.Fatalf("waiter replayed body=%q, want settled", rec.Body)
		}
	}
	if n != 8 {
		t.Fatalf("converged waiters=%d, want 8", n)
	}
	t.Logf("8 waiters blocked in-flight then all replayed the same settled record")

	// 处理中时指纹不同仍是立即冲突。
	m2 := NewMemoryStore()
	_, _, _ = m2.BeginOrWait("k", "fp-a")
	if _, _, err := m2.BeginOrWait("k", "fp-b"); !errors.Is(err, ErrConflict) {
		t.Fatalf("different fingerprint err=%v, want ErrConflict", err)
	}

	// 首个请求失败 Abandon 后，等待者之一应成为新处理者而非回放。
	m3 := NewMemoryStore()
	leader := make(chan struct{})
	go func() {
		_, replay, err := m3.BeginOrWait("r", "fp")
		if err == nil && !replay {
			m3.Abandon("r") // 模拟上游失败后放弃
		}
		close(leader)
	}()
	time.Sleep(20 * time.Millisecond)
	rec, replay, err := m3.BeginOrWait("r", "fp")
	if err != nil || replay || rec != nil {
		t.Fatalf("after abandon waiter should acquire fresh slot: replay=%v rec=%v err=%v", replay, rec, err)
	}
	m3.Complete("r", Record{Key: "r", Fingerprint: "fp", Status: 200, Body: []byte("retry-ok")})
	<-leader
	t.Logf("after leader abandoned, waiter acquired a fresh slot and completed retry")
}
