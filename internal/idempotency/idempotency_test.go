package idempotency

import (
	"errors"
	"testing"
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
