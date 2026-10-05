// Package quota 实现同意级的周期性用量额度（笔数与累计金额上限）。
//
// 扣减采用“持有（Hold）-> 入账成功后提交（Commit）/ 失败后释放（Release）”
// 三段式，保证额度扣减与写入最终结果严格一致：同一幂等键只扣一次，
// 上游拒绝、超时或内部错误都不会消耗额度。周期边界按自然时间（UTC）对齐，
// 固定且可复现；各同意以同意 ID 为键独立记账，互不影响。
package quota

import (
	"errors"
	"sync"
	"time"
)

// Period 是额度周期的固定类别，边界按自然时间对齐且可复现。
type Period string

const (
	PeriodDaily  Period = "DAILY"
	PeriodWeekly Period = "WEEKLY"
)

// Policy 是建立同意时携带的周期性额度策略。零值/非正字段表示不设该维度上限。
type Policy struct {
	Period    Period
	MaxCount  int    // 每周期最多成功写入笔数；<=0 表示不限笔数
	MaxAmount int64  // 每周期累计金额上限；<=0 表示不限金额
	Currency  string // 金额上限对应的币种；MaxAmount>0 时必填
}

// DenyReason 是额度拒绝的具体原因。
type DenyReason string

const (
	ReasonCountExceeded    DenyReason = "QUOTA_COUNT_EXCEEDED"
	ReasonAmountBudget     DenyReason = "QUOTA_AMOUNT_BUDGET_EXCEEDED"
	ReasonCurrencyMismatch DenyReason = "QUOTA_CURRENCY_MISMATCH"
)

// ErrInvalidPolicy 表示额度策略非法。
var ErrInvalidPolicy = errors.New("quota: invalid policy")

// Usage 是某条同意当前周期的用量视图。
type Usage struct {
	PeriodStart time.Time
	ResetAt     time.Time
	UsedCount   int
	UsedAmount  int64
	MaxCount    int
	MaxAmount   int64
	Currency    string
}

// RemainingCount 返回本周期剩余笔数；未配置时返回 -1 表示不限。
func (u Usage) RemainingCount() int {
	if u.MaxCount <= 0 {
		return -1
	}
	r := u.MaxCount - u.UsedCount
	if r < 0 {
		return 0
	}
	return r
}

// RemainingAmount 返回本周期剩余金额；未配置时返回 -1 表示不限。
func (u Usage) RemainingAmount() int64 {
	if u.MaxAmount <= 0 {
		return -1
	}
	r := u.MaxAmount - u.UsedAmount
	if r < 0 {
		return 0
	}
	return r
}

// Decision 是一次额度判定的结果。
type Decision struct {
	Allowed bool
	Reason  DenyReason
	Usage   Usage
}

// holdRec 记录一次预占；committed 表示写入已成功、额度已真实扣减。
type holdRec struct {
	count       int
	amount      int64
	currency    string
	windowStart time.Time
	committed   bool
}

// state 是单条同意的额度账本，仅在 Manager.mu 下访问。
type state struct {
	policy     Policy
	start      time.Time
	resetAt    time.Time
	usedCount  int
	usedAmount int64
	holds      map[string]*holdRec
}

// Manager 管理各同意独立的周期额度账本，键为同意 ID，同意之间完全隔离。
type Manager struct {
	now func() time.Time
	mu  sync.Mutex
	st  map[string]*state
}

// NewManager 创建额度管理器，now 为可注入的时间源。
func NewManager(now func() time.Time) *Manager {
	return &Manager{now: now, st: make(map[string]*state)}
}

// periodBounds 返回周期固定起点与下次重置时刻（UTC 自然边界）。
// 周以周一 00:00 UTC 为起点，与请求到达时间无关，因此可复现。
func periodBounds(now time.Time, p Period) (start, reset time.Time, ok bool) {
	u := now.UTC()
	switch p {
	case PeriodDaily:
		start = time.Date(u.Year(), u.Month(), u.Day(), 0, 0, 0, 0, time.UTC)
		reset = start.AddDate(0, 0, 1)
	case PeriodWeekly:
		start = time.Date(u.Year(), u.Month(), u.Day(), 0, 0, 0, 0, time.UTC)
		// Weekday: Sunday=0 ... Saturday=6；偏移到本周一。
		offset := (int(u.Weekday()) + 6) % 7
		start = start.AddDate(0, 0, -offset)
		reset = start.AddDate(0, 0, 7)
	default:
		return time.Time{}, time.Time{}, false
	}
	return start, reset, true
}

// SetPolicy 为同意登记额度策略。未登记的同意不受任何额度限制。
func (m *Manager) SetPolicy(consentID string, p Policy) error {
	if consentID == "" {
		return errors.New("quota: consent id required")
	}
	if _, _, ok := periodBounds(m.now(), p.Period); !ok {
		return ErrInvalidPolicy
	}
	if p.MaxAmount > 0 && p.Currency == "" {
		return ErrInvalidPolicy
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	// 已登记则保持既有账本与累计用量：策略在同意建立时固定，重复登记幂等。
	if _, exists := m.st[consentID]; exists {
		return nil
	}
	m.st[consentID] = &state{policy: p, holds: make(map[string]*holdRec)}
	return nil
}

// rollover 在当前周期外时重置用量；丢弃上一周期未提交的残留预占，
// 已提交的幂等标记保留（同一幂等键永不二次扣减）。
func (s *state) rollover(start, reset time.Time) {
	if !s.start.Equal(start) {
		s.start, s.resetAt = start, reset
		s.usedCount, s.usedAmount = 0, 0
		for k, h := range s.holds {
			if !h.committed {
				delete(s.holds, k)
			}
		}
	}
}

func (s *state) usage() Usage {
	return Usage{
		PeriodStart: s.start, ResetAt: s.resetAt,
		UsedCount: s.usedCount, UsedAmount: s.usedAmount,
		MaxCount: s.policy.MaxCount, MaxAmount: s.policy.MaxAmount,
		Currency: s.policy.Currency,
	}
}

// GetUsage 查询同意当前周期的已用、剩余与下次重置时间；无策略返回 false。
func (m *Manager) GetUsage(consentID string) (Usage, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.st[consentID]
	if !ok {
		return Usage{}, false
	}
	start, reset, _ := periodBounds(m.now(), s.policy.Period)
	s.rollover(start, reset)
	return s.usage(), true
}

// Hold 预占额度。判定顺序固定：幂等去重 -> 币种 -> 笔数 -> 金额。
// 同一幂等键重复 Hold（含并发重试）不重复扣减，直接放行且用量不变。
func (m *Manager) Hold(consentID string, key string, count int, amount int64, currency string) Decision {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.st[consentID]
	if !ok {
		// 未配置额度的同意：完全不受影响。
		return Decision{Allowed: true}
	}
	start, reset, valid := periodBounds(m.now(), s.policy.Period)
	if !valid {
		return Decision{Allowed: false, Reason: ReasonCountExceeded, Usage: s.usage()}
	}
	s.rollover(start, reset)

	// 同一幂等键：已提交（成功入账）或处理中（并发重试）都不再二次扣减。
	if _, dup := s.holds[key]; dup {
		return Decision{Allowed: true, Usage: s.usage()}
	}

	if s.policy.MaxAmount > 0 && currency != s.policy.Currency {
		return Decision{Allowed: false, Reason: ReasonCurrencyMismatch, Usage: s.usage()}
	}
	if s.policy.MaxCount > 0 && s.usedCount+count > s.policy.MaxCount {
		return Decision{Allowed: false, Reason: ReasonCountExceeded, Usage: s.usage()}
	}
	if s.policy.MaxAmount > 0 && s.usedAmount+amount > s.policy.MaxAmount {
		return Decision{Allowed: false, Reason: ReasonAmountBudget, Usage: s.usage()}
	}

	s.usedCount += count
	s.usedAmount += amount
	s.holds[key] = &holdRec{
		count: count, amount: amount, currency: currency,
		windowStart: start, committed: false,
	}
	return Decision{Allowed: true, Usage: s.usage()}
}

// Commit 在写入最终成功后提交预占，额度真正扣减；重复提交幂等。
func (m *Manager) Commit(consentID string, key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.st[consentID]
	if !ok {
		return
	}
	if h, ok := s.holds[key]; ok {
		h.committed = true
	}
}

// Release 在写入失败（上游拒绝/超时/内部错误）时释放预占，额度不扣减；
// 已提交的占用不会被回滚。
func (m *Manager) Release(consentID string, key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.st[consentID]
	if !ok {
		return
	}
	h, ok := s.holds[key]
	if !ok || h.committed {
		return
	}
	s.usedCount -= h.count
	if s.usedCount < 0 {
		s.usedCount = 0
	}
	s.usedAmount -= h.amount
	if s.usedAmount < 0 {
		s.usedAmount = 0
	}
	delete(s.holds, key)
}
