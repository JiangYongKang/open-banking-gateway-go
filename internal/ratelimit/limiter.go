package ratelimit

import (
	"sync"
	"time"

	"github.com/highcumontoa/open-banking-gateway-go/internal/domain"
)

// Subject identifies the (tenant, client) pair a rule applies to.
type Subject struct {
	TenantID domain.TenantID
	ClientID domain.ClientID
}

func (s Subject) String() string { return string(s.TenantID) + "/" + string(s.ClientID) }

// Rule defines a fixed-window rate limit and a periodic quota.
type Rule struct {
	RatePerMinute int   // requests allowed in each fixed minute window; 0 = unlimited
	QuotaPerDay   int64 // write requests allowed per UTC calendar day; 0 = unlimited
}

// Clock allows deterministic tests.
type Clock func() time.Time

type counters struct {
	minuteStart int64 // unix minute index, floor(unix/60)
	minuteUsed  int64
	dayKey      string // UTC YYYY-MM-DD
	dayUsed     int64
}

// Limiter enforces per-(tenant,client) fixed-window limits and daily quotas.
// Every subject has fully independent counters, so a throttled or rejected
// tenant can never affect another tenant's windows.
type Limiter struct {
	mu    sync.Mutex
	rules map[Subject]Rule
	state map[Subject]*counters
	now   Clock
}

func NewLimiter(now Clock) *Limiter {
	if now == nil {
		now = time.Now
	}
	return &Limiter{
		rules: make(map[Subject]Rule),
		state: make(map[Subject]*counters),
		now:   now,
	}
}

// SetRule installs (or replaces) the rule for a subject. Counters already
// accumulated in the current window are kept so rules can be tuned safely.
func (l *Limiter) SetRule(s Subject, r Rule) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.rules[s] = r
}

// Allow consumes one token for the subject. write=true also consumes one slot
// of the daily write quota. The returned ErrorCode is empty when allowed and
// otherwise RATE_LIMITED or QUOTA_EXCEEDED.
func (l *Limiter) Allow(s Subject, write bool) (Rule, domain.ErrorCode) {
	now := l.now()
	minuteStart := now.Unix() / 60
	dayKey := now.UTC().Format("2006-01-02")

	l.mu.Lock()
	defer l.mu.Unlock()

	rule, ok := l.rules[s]
	if !ok {
		// No rule configured: open by default (local demo / bootstrap).
		return Rule{}, ""
	}
	c, ok := l.state[s]
	if !ok {
		c = &counters{minuteStart: minuteStart, dayKey: dayKey}
		l.state[s] = c
	}
	// Fixed-window reset: counters start from zero at a deterministic boundary.
	if c.minuteStart != minuteStart {
		c.minuteStart = minuteStart
		c.minuteUsed = 0
	}
	if c.dayKey != dayKey {
		c.dayKey = dayKey
		c.dayUsed = 0
	}

	// Decide before consuming so a refused request leaves no trace.
	if rule.RatePerMinute > 0 && c.minuteUsed >= int64(rule.RatePerMinute) {
		return rule, domain.ErrRateLimited
	}
	if write && rule.QuotaPerDay > 0 && c.dayUsed >= rule.QuotaPerDay {
		return rule, domain.ErrQuotaExceeded
	}

	c.minuteUsed++
	if write {
		c.dayUsed++
	}
	return rule, ""
}

// Counters is an inspection view used by tests and demo output.
type Counters struct {
	MinuteStart int64
	MinuteUsed  int64
	DayKey      string
	DayUsed     int64
}

// Snapshot returns the current counters for the subject, or zero values.
func (l *Limiter) Snapshot(s Subject) Counters {
	l.mu.Lock()
	defer l.mu.Unlock()
	c, ok := l.state[s]
	if !ok {
		return Counters{}
	}
	return Counters{MinuteStart: c.minuteStart, MinuteUsed: c.minuteUsed, DayKey: c.dayKey, DayUsed: c.dayUsed}
}

// AllowQuota consumes one slot of the daily write quota without touching the
// per-minute window counter. It is used for genuinely new writes only.
func (l *Limiter) AllowQuota(s Subject) (Rule, domain.ErrorCode) {
	now := l.now()
	dayKey := now.UTC().Format("2006-01-02")

	l.mu.Lock()
	defer l.mu.Unlock()

	rule, ok := l.rules[s]
	if !ok {
		return Rule{}, ""
	}
	c, ok := l.state[s]
	if !ok {
		c = &counters{minuteStart: now.Unix() / 60, dayKey: dayKey}
		l.state[s] = c
	}
	if c.dayKey != dayKey {
		c.dayKey = dayKey
		c.dayUsed = 0
	}
	if rule.QuotaPerDay > 0 && c.dayUsed >= rule.QuotaPerDay {
		return rule, domain.ErrQuotaExceeded
	}
	c.dayUsed++
	return rule, ""
}
