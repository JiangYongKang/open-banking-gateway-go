// Package quota 实现同意级的周期性用量额度（笔数/累计金额）。
//
// 扣减模型：Reserve 预占（ tentative 计入已用）-> 写入最终成功后 Commit 落定为实扣，
// 失败则 Release 全额退回。预占按幂等键去重，同一幂等键在同一周期内只会被计一次。
package quota

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

// ErrExhausted 表示本周期额度已用尽。
var ErrExhausted = errors.New("quota: period allowance exhausted")

// ExhaustedError 携带额度用尽时本周期的用量快照，便于上层给出剩余与重置时间。
type ExhaustedError struct {
	ConsentID string
	Status    Status
}

func (e *ExhaustedError) Error() string {
	return fmt.Sprintf("%s (consent=%s remaining_count=%d remaining_amount=%d reset_at=%s)",
		ErrExhausted, e.ConsentID, e.Status.RemainingCount, e.Status.RemainingAmount,
		e.Status.ResetAt.UTC().Format(time.RFC3339))
}

func (e *ExhaustedError) Unwrap() error { return ErrExhausted }

// Period 是额度的周期类型，边界固定、可复现。
type Period string

const (
	PeriodDaily   Period = "DAILY"   // 自然日，00:00 UTC 对齐
	PeriodWeekly  Period = "WEEKLY"  // 自然周，周一 00:00 UTC 对齐（ISO 8601）
	PeriodMonthly Period = "MONTHLY" // 自然月，每月 1 日 00:00 UTC 对齐
)

// Limit 是挂在同意上的周期额度上限；某维度为 0 表示该维度不限。
type Limit struct {
	Period    Period
	MaxCount  int
	MaxAmount int64
}

// Status 是某同意当前周期的用量快照。某维度不限时对应 Remaining 为 -1。
type Status struct {
	UsedCount       int
	UsedAmount      int64
	RemainingCount  int
	RemainingAmount int64
	ResetAt         time.Time
}

// Reservation 是一次待确认的额度占用句柄，由 Manager 的 Commit/Release 结算。
type Reservation struct {
	consentID string
	key       string
	amount    int64
	window    time.Time
}

// windowStart 返回 t 所属周期的固定起点（UTC 对齐，与请求到达时刻无关）。
func windowStart(t time.Time, p Period) time.Time {
	u := t.UTC()
	midnight := time.Date(u.Year(), u.Month(), u.Day(), 0, 0, 0, 0, time.UTC)
	switch p {
	case PeriodWeekly:
		// ISO 8601：周一为一周起点。
		offset := (int(midnight.Weekday()) + 6) % 7
		return midnight.AddDate(0, 0, -offset)
	case PeriodMonthly:
		return time.Date(u.Year(), u.Month(), 1, 0, 0, 0, 0, time.UTC)
	default: // PeriodDaily
		return midnight
	}
}

// windowEnd 返回周期起点对应的重置时刻（即下一周期起点）。
func windowEnd(start time.Time, p Period) time.Time {
	switch p {
	case PeriodWeekly:
		return start.AddDate(0, 0, 7)
	case PeriodMonthly:
		return start.AddDate(0, 1, 0)
	default: // PeriodDaily
		return start.AddDate(0, 0, 1)
	}
}

// window 记录一个同意在一个周期内的用量与未结算预占。
type window struct {
	start        time.Time
	period       Period
	usedCount    int
	usedAmount   int64
	reservations map[string]*reservation // 以幂等键去重
}

type reservation struct {
	amount    int64
	committed bool
}

// Manager 按同意维度跟踪周期用量，并发安全，各同意之间完全隔离。
type Manager struct {
	now     func() time.Time
	mu      sync.Mutex
	windows map[string]*window // key: consentID + 窗口起点
}

func NewManager(now func() time.Time) *Manager {
	return &Manager{now: now, windows: make(map[string]*window)}
}

func mapKey(consentID string, windowStart time.Time) string {
	return consentID + "\x00" + windowStart.Format(time.RFC3339Nano)
}

// purgeLocked 清理已结束的周期窗口，避免状态无限增长。
func (m *Manager) purgeLocked(now time.Time) {
	for k, w := range m.windows {
		if !windowEnd(w.start, w.period).After(now) {
			delete(m.windows, k)
		}
	}
}

// Reserve 为一次写入预占额度。同一幂等键在同一周期内重复预占会去重为同一次，
// 保证重试/并发下只计一次。额度不足时返回 *ExhaustedError（可 errors.Is 到 ErrExhausted）。
func (m *Manager) Reserve(consentID, idemKey string, amount int64, l Limit) (*Reservation, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.now()
	m.purgeLocked(now)
	ws := windowStart(now, l.Period)
	mk := mapKey(consentID, ws)
	w, ok := m.windows[mk]
	if !ok {
		w = &window{start: ws, period: l.Period, reservations: make(map[string]*reservation)}
		m.windows[mk] = w
	}
	// 幂等键去重：同键同周期重复预占直接复用，不重复计数。
	if _, dup := w.reservations[idemKey]; dup {
		return &Reservation{consentID: consentID, key: idemKey, amount: amount, window: ws}, nil
	}
	if (l.MaxCount > 0 && w.usedCount+1 > l.MaxCount) ||
		(l.MaxAmount > 0 && w.usedAmount+amount > l.MaxAmount) {
		return nil, &ExhaustedError{ConsentID: consentID, Status: m.statusLocked(w, ws, l)}
	}
	w.usedCount++
	w.usedAmount += amount
	w.reservations[idemKey] = &reservation{amount: amount}
	return &Reservation{consentID: consentID, key: idemKey, amount: amount, window: ws}, nil
}

// Commit 将预占落定为实扣（写入最终成功后调用）。计数在 Reserve 时已计入，
// 这里只做状态标记，保证一次预占至多一次实扣。
func (m *Manager) Commit(r *Reservation) {
	if r == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if w, ok := m.windows[mapKey(r.consentID, r.window)]; ok {
		if res, ok := w.reservations[r.key]; ok {
			res.committed = true
		}
	}
}

// Release 释放未落定的预占（写入失败时调用），全额退回，不扣额度。
func (m *Manager) Release(r *Reservation) {
	if r == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	mk := mapKey(r.consentID, r.window)
	w, ok := m.windows[mk]
	if !ok {
		return
	}
	res, ok := w.reservations[r.key]
	if !ok || res.committed {
		return
	}
	w.usedCount--
	w.usedAmount -= res.amount
	delete(w.reservations, r.key)
}

// Status 查询某同意当前周期的已用、剩余与下次重置时间。
func (m *Manager) Status(consentID string, l Limit) Status {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.now()
	m.purgeLocked(now)
	ws := windowStart(now, l.Period)
	w := m.windows[mapKey(consentID, ws)]
	if w == nil {
		w = &window{}
	}
	return m.statusLocked(w, ws, l)
}

func (m *Manager) statusLocked(w *window, ws time.Time, l Limit) Status {
	st := Status{
		UsedCount:       w.usedCount,
		UsedAmount:      w.usedAmount,
		RemainingCount:  -1,
		RemainingAmount: -1,
		ResetAt:         windowEnd(ws, l.Period),
	}
	if l.MaxCount > 0 {
		st.RemainingCount = l.MaxCount - w.usedCount
		if st.RemainingCount < 0 {
			st.RemainingCount = 0
		}
	}
	if l.MaxAmount > 0 {
		st.RemainingAmount = l.MaxAmount - w.usedAmount
		if st.RemainingAmount < 0 {
			st.RemainingAmount = 0
		}
	}
	return st
}
