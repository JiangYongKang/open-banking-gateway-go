package consent

import (
	"sync"

	"github.com/highcumontoa/open-banking-gateway-go/internal/domain"
)

// MemoryStore is a concurrency-safe in-memory consent store.
type MemoryStore struct {
	mu sync.RWMutex
	m  map[domain.ConsentID]domain.Consent
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{m: make(map[domain.ConsentID]domain.Consent)}
}

// Put stores (or overwrites) a consent.
func (s *MemoryStore) Put(c domain.Consent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[c.ID] = c
	return nil
}

// Get returns a consent and whether it existed at read time.
func (s *MemoryStore) Get(id domain.ConsentID) (domain.Consent, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	c, ok := s.m[id]
	return c, ok
}
