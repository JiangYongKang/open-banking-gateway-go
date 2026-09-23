// Package ledger 提供本地模拟的核心账务服务。
package ledger

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

// 上游错误的本地模拟类别。
var (
	ErrUpstreamRejected = errors.New("ledger: upstream rejected")
	ErrUpstreamTimeout  = errors.New("ledger: upstream timeout")
	ErrInternal         = errors.New("ledger: internal failure")
)

// Entry 是一条账务流水。
type Entry struct {
	ID             string
	TenantID       string
	AccountID      string
	Amount         int64
	Currency       string
	Memo           string
	IdempotencyKey string
}

// Service 是核心账务服务接口。
type Service interface {
	Post(e Entry) (Entry, error)
	Entries(tenantID string) []Entry
}

// Mock 是本地模拟的核心账务服务，可注入故障行为。
type Mock struct {
	// FailWith 若非 nil，Post 将返回该错误以模拟上游故障。
	FailWith error

	mu      sync.Mutex
	seq     atomic.Int64
	entries []Entry
}

func NewMock() *Mock { return &Mock{} }

func (m *Mock) Post(e Entry) (Entry, error) {
	if m.FailWith != nil {
		return Entry{}, m.FailWith
	}
	if e.Amount <= 0 {
		return Entry{}, fmt.Errorf("%w: amount must be positive", ErrUpstreamRejected)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	e.ID = fmt.Sprintf("txn-%d", m.seq.Add(1))
	m.entries = append(m.entries, e)
	return e, nil
}

func (m *Mock) Entries(tenantID string) []Entry {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []Entry
	for _, e := range m.entries {
		if e.TenantID == tenantID {
			out = append(out, e)
		}
	}
	return out
}
