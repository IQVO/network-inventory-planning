package analyticsstore

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/claudioed/network-inventory-planning/internal/analytics/report"
)

// Memory is the in-memory twin of Projection and Reader, for unit tests, the
// HTTP handler tests and the BDD features. It implements the same semantics
// as the SQL (the shared contract test runs against both): idempotent on the
// event id, range [from, to), UTC days, percentile_cont ages.
type Memory struct {
	mu        sync.Mutex
	processed map[string]struct{}
	advances  []report.Event
	stuck     []report.Event
	runs      []report.Event
	lastAt    *time.Time
}

// NewMemory returns an empty Memory store.
func NewMemory() *Memory {
	return &Memory{processed: map[string]struct{}{}}
}

var (
	_ report.Projection = (*Memory)(nil)
	_ report.Reader     = (*Memory)(nil)
)

// Apply implements report.Projection.
func (m *Memory) Apply(_ context.Context, e report.Event) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, dup := m.processed[e.EventID]; dup {
		return false, nil
	}
	m.processed[e.EventID] = struct{}{}
	e.At = e.At.UTC()
	if m.lastAt == nil || e.At.After(*m.lastAt) {
		at := e.At
		m.lastAt = &at
	}
	switch e.Kind {
	case report.KindStateAdvanced:
		m.advances = append(m.advances, e)
	case report.KindStuckDetected:
		m.stuck = append(m.stuck, e)
	case report.KindRebalanceRunCompleted:
		m.runs = append(m.runs, e)
	}
	return true, nil
}

// inRange is the half-open [From, To) test.
func inRange(t time.Time, r report.Range) bool {
	return t.Compare(r.From) >= 0 && t.Compare(r.To) < 0
}

// day is the UTC calendar day of t, at midnight UTC.
func day(t time.Time) time.Time {
	y, mo, d := t.UTC().Date()
	return time.Date(y, mo, d, 0, 0, 0, 0, time.UTC)
}

type dayState struct {
	day   time.Time
	state string
}

func lessDayState(a, b dayState) bool {
	if !a.day.Equal(b.day) {
		return a.day.Before(b.day)
	}
	return a.state < b.state
}

func sortedKeys[V any](m map[dayState]V) []dayState {
	keys := make([]dayState, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return lessDayState(keys[i], keys[j]) })
	return keys
}

// LastEventAt implements report.Reader.
func (m *Memory) LastEventAt(_ context.Context) (*time.Time, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.lastAt == nil {
		return nil, nil
	}
	at := *m.lastAt
	return &at, nil
}

// TransferFunnel implements report.Reader.
func (m *Memory) TransferFunnel(_ context.Context, r report.Range) ([]report.FunnelDay, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	distinct := map[dayState]map[string]struct{}{}
	for _, e := range m.advances {
		if !inRange(e.At, r) {
			continue
		}
		k := dayState{day(e.At), e.To}
		if distinct[k] == nil {
			distinct[k] = map[string]struct{}{}
		}
		distinct[k][e.TransferID] = struct{}{}
	}
	out := make([]report.FunnelDay, 0, len(distinct))
	for _, k := range sortedKeys(distinct) {
		out = append(out, report.FunnelDay{Day: k.day, State: k.state, Transfers: len(distinct[k])})
	}
	return out, nil
}

// StateDwell implements report.Reader.
func (m *Memory) StateDwell(_ context.Context, r report.Range) ([]report.DwellDay, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	type agg struct {
		total   int
		dwells  []float64 // only transitions that carry dwell_seconds
		without int
	}
	days := map[dayState]*agg{}
	for _, e := range m.advances {
		// A creation entry leaves no state: empty `from`, or a state "left"
		// into itself (the aggregate's DRAFT -> DRAFT creation record).
		if e.From == "" || e.From == e.To || !inRange(e.At, r) {
			continue
		}
		k := dayState{day(e.At), e.From}
		if days[k] == nil {
			days[k] = &agg{}
		}
		days[k].total++
		if e.DwellSeconds == nil {
			days[k].without++ // excluded from the percentiles, never zero
			continue
		}
		days[k].dwells = append(days[k].dwells, float64(*e.DwellSeconds))
	}
	out := make([]report.DwellDay, 0, len(days))
	for _, k := range sortedKeys(days) {
		a := days[k]
		d := report.DwellDay{Day: k.day, State: k.state, Transitions: a.total, WithoutDwell: a.without}
		if len(a.dwells) > 0 {
			p50, p95 := report.Percentile(a.dwells, 0.5), report.Percentile(a.dwells, 0.95)
			d.P50Seconds, d.P95Seconds = &p50, &p95
		}
		out = append(out, d)
	}
	return out, nil
}

// StuckDays implements report.Reader.
func (m *Memory) StuckDays(_ context.Context, r report.Range) ([]report.StuckDay, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	type agg struct {
		detections int
		transfers  map[string]struct{}
	}
	days := map[dayState]*agg{}
	for _, e := range m.stuck {
		if !inRange(e.At, r) {
			continue
		}
		k := dayState{day(e.At), e.State}
		if days[k] == nil {
			days[k] = &agg{transfers: map[string]struct{}{}}
		}
		days[k].detections++
		days[k].transfers[e.TransferID] = struct{}{}
	}
	out := make([]report.StuckDay, 0, len(days))
	for _, k := range sortedKeys(days) {
		out = append(out, report.StuckDay{Day: k.day, State: k.state, Detections: days[k].detections, Transfers: len(days[k].transfers)})
	}
	return out, nil
}

// StuckLatest implements report.Reader: newest first, ties by transfer id
// then event id (the SQL's ORDER BY).
func (m *Memory) StuckLatest(_ context.Context, r report.Range, limit int) ([]report.StuckOccurrence, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var in []report.Event
	for _, e := range m.stuck {
		if inRange(e.At, r) {
			in = append(in, e)
		}
	}
	sort.Slice(in, func(i, j int) bool {
		a, b := in[i], in[j]
		switch {
		case !a.At.Equal(b.At):
			return a.At.After(b.At)
		case a.TransferID != b.TransferID:
			return a.TransferID < b.TransferID
		}
		return a.EventID < b.EventID
	})
	if len(in) > limit {
		in = in[:limit]
	}
	out := make([]report.StuckOccurrence, 0, len(in))
	for _, e := range in {
		out = append(out, report.StuckOccurrence{
			TransferID: e.TransferID, State: e.State, AgeSeconds: e.AgeSeconds,
			ThresholdSeconds: e.ThresholdSeconds, DetectedAt: e.At,
		})
	}
	return out, nil
}

// RebalanceDays implements report.Reader.
func (m *Memory) RebalanceDays(_ context.Context, r report.Range) ([]report.RebalanceDay, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	days := map[time.Time]*report.RebalanceDay{}
	for _, e := range m.runs {
		if !inRange(e.At, r) {
			continue
		}
		d := day(e.At)
		if days[d] == nil {
			days[d] = &report.RebalanceDay{Day: d}
		}
		days[d].Runs++
		days[d].Proposals += e.ProposalCount
		days[d].Rejected += e.RejectedCount
		days[d].StaleFacts += e.StaleFacts
	}
	keys := make([]time.Time, 0, len(days))
	for d := range days {
		keys = append(keys, d)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i].Before(keys[j]) })
	out := make([]report.RebalanceDay, 0, len(keys))
	for _, d := range keys {
		out = append(out, *days[d])
	}
	return out, nil
}
