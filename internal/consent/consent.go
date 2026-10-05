// Package consent 实现同意（consent）的生命周期管理与授权判定。
package consent

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/highcumontoa/open-banking-gateway-go/internal/quota"
)

// Status 表示同意的生命周期状态。
type Status string

const (
	StatusActive  Status = "ACTIVE"
	StatusRevoked Status = "REVOKED"
)

// Consent 表示一条同意授权记录。
type Consent struct {
	ID        string
	TenantID  string
	ClientID  string
	Scopes    []string
	Status    Status
	ExpiresAt time.Time
	CreatedAt time.Time
	// Quota 是可选的周期用量额度；为 nil 时该同意不受额度约束，行为与之前完全一致。
	Quota *quota.Limit
}

// RejectReason 是可区分的拒绝原因。
type RejectReason string

const (
	ReasonMissing           RejectReason = "CONSENT_MISSING"
	ReasonRevoked           RejectReason = "CONSENT_REVOKED"
	ReasonExpired           RejectReason = "CONSENT_EXPIRED"
	ReasonScopeInsufficient RejectReason = "CONSENT_SCOPE_INSUFFICIENT"
	ReasonTenantMismatch    RejectReason = "CONSENT_TENANT_MISMATCH"
)

// Decision 是授权判定结果。
type Decision struct {
	Allowed bool
	Reason  RejectReason
	Consent *Consent
}

// ErrNotFound 表示同意不存在。
var ErrNotFound = errors.New("consent: not found")

// Store 是同意的存储接口。
type Store interface {
	Create(c Consent) error
	Get(id string) (*Consent, error)
	Revoke(id string) error
}

// MemoryStore 是并发安全的内存同意存储。
type MemoryStore struct {
	mu   sync.RWMutex
	byID map[string]Consent
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{byID: make(map[string]Consent)}
}

func (m *MemoryStore) Create(c Consent) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if c.ID == "" || c.TenantID == "" {
		return errors.New("consent: id and tenant required")
	}
	if _, ok := m.byID[c.ID]; ok {
		return fmt.Errorf("consent: %q already exists", c.ID)
	}
	if c.Status == "" {
		c.Status = StatusActive
	}
	m.byID[c.ID] = c
	return nil
}

func (m *MemoryStore) Get(id string) (*Consent, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	c, ok := m.byID[id]
	if !ok {
		return nil, ErrNotFound
	}
	return &c, nil
}

func (m *MemoryStore) Revoke(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	c, ok := m.byID[id]
	if !ok {
		return ErrNotFound
	}
	c.Status = StatusRevoked
	m.byID[id] = c
	return nil
}

// Authorizer 基于同意做授权判定。
type Authorizer struct {
	store Store
	now   func() time.Time
}

func NewAuthorizer(store Store, now func() time.Time) *Authorizer {
	return &Authorizer{store: store, now: now}
}

// Authorize 判定请求是否被同意放行。判定顺序固定为：
// 存在性 -> 租户归属 -> 状态 -> 有效期 -> 范围，保证拒绝原因可区分且确定。
func (a *Authorizer) Authorize(tenantID, consentID string, requiredScopes []string) Decision {
	c, err := a.store.Get(consentID)
	if err != nil {
		return Decision{Allowed: false, Reason: ReasonMissing}
	}
	if c.TenantID != tenantID {
		return Decision{Allowed: false, Reason: ReasonTenantMismatch, Consent: c}
	}
	if c.Status == StatusRevoked {
		return Decision{Allowed: false, Reason: ReasonRevoked, Consent: c}
	}
	if !a.now().Before(c.ExpiresAt) {
		return Decision{Allowed: false, Reason: ReasonExpired, Consent: c}
	}
	held := make(map[string]bool, len(c.Scopes))
	for _, s := range c.Scopes {
		held[s] = true
	}
	for _, s := range requiredScopes {
		if !held[s] {
			return Decision{Allowed: false, Reason: ReasonScopeInsufficient, Consent: c}
		}
	}
	return Decision{Allowed: true, Consent: c}
}
