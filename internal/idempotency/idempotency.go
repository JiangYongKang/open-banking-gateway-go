// Package idempotency 提供写入请求的幂等键管理。
package idempotency

import (
	"errors"
	"sync"
)

// ErrInFlight 表示相同幂等键的请求正在处理中。
var ErrInFlight = errors.New("idempotency: request with same key in flight")

// ErrConflict 表示幂等键被复用但请求内容不一致。
var ErrConflict = errors.New("idempotency: key reused with different payload")

// Record 保存一次已完成请求的结果。
type Record struct {
	Key         string
	Fingerprint string
	Status      int
	Body        []byte
}

// Store 是幂等记录的存储接口。
type Store interface {
	// Begin 尝试占用一个幂等键。已存在同指纹记录时返回该记录（replay=true）；
	// 指纹不同返回 ErrConflict；正在处理中返回 ErrInFlight。
	Begin(key, fingerprint string) (rec *Record, replay bool, err error)
	// Complete 写入处理结果并释放占用。
	Complete(key string, rec Record)
	// Abandon 在处理失败时释放占用，允许后续重试。
	Abandon(key string)
}

type slot struct {
	inFlight    bool
	fingerprint string
	rec         Record
}

// MemoryStore 是并发安全的内存实现。
type MemoryStore struct {
	mu    sync.Mutex
	slots map[string]*slot
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{slots: make(map[string]*slot)}
}

func (m *MemoryStore) Begin(key, fingerprint string) (*Record, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.slots[key]
	if !ok {
		m.slots[key] = &slot{inFlight: true, fingerprint: fingerprint}
		return nil, false, nil
	}
	if s.fingerprint != fingerprint {
		return nil, false, ErrConflict
	}
	if s.inFlight {
		return nil, false, ErrInFlight
	}
	rec := s.rec
	return &rec, true, nil
}

func (m *MemoryStore) Complete(key string, rec Record) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.slots[key]
	if !ok {
		return
	}
	s.inFlight = false
	s.rec = rec
}

func (m *MemoryStore) Abandon(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.slots, key)
}
