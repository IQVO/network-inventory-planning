// Package memory holds in-memory implementations of this service's outbound
// ports (ADR 0011). They exist so the godog acceptance suite can drive the
// REAL HTTP router and the REAL use cases end to end — the same wiring
// cmd/network-inventory-planning builds — without Postgres or Kafka. They
// implement the same contracts as the Postgres adapters (idempotency key,
// optimistic version, per-transfer audit sequence, newest-first listings),
// but they are test infrastructure: nothing in cmd/ wires them.
package memory

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/claudioed/network-inventory-planning/internal/application/ports"
	"github.com/claudioed/network-inventory-planning/internal/domain/planning"
	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

var (
	_ ports.Clock                      = (*FixedClock)(nil)
	_ ports.UnitOfWork                 = UnitOfWork{}
	_ ports.PlanningSnapshotRepository = (*PlanningFacts)(nil)
	_ ports.RebalanceRunRepository     = (*RebalanceRuns)(nil)
)

// FixedClock is a settable clock: scenarios move it with Advance instead of
// sleeping, so ages, expiry and report days are deterministic.
type FixedClock struct {
	mu  sync.Mutex
	now time.Time
}

// NewFixedClock returns a clock frozen at t.
func NewFixedClock(t time.Time) *FixedClock { return &FixedClock{now: t.UTC()} }

// Now implements ports.Clock.
func (c *FixedClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// Advance moves the clock forward by d.
func (c *FixedClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// UnitOfWork runs fn directly: every in-memory repository is individually
// atomic, which is all the single-goroutine acceptance scenarios need.
type UnitOfWork struct{}

// Do implements ports.UnitOfWork.
func (UnitOfWork) Do(ctx context.Context, fn func(ctx context.Context) error) error {
	return fn(ctx)
}

// PlanningFacts is the declared content of the three local read models
// (site capability, site/SKU demand, published capacity plan). Scenarios
// set it; the fail-closed assembly stays in the domain (BuildSnapshot).
type PlanningFacts struct {
	mu    sync.Mutex
	facts planning.Facts
}

// NewPlanningFacts returns an empty (fail-closed) read model.
func NewPlanningFacts() *PlanningFacts { return &PlanningFacts{} }

// Set replaces the declared facts.
func (p *PlanningFacts) Set(f planning.Facts) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.facts = planning.Facts{
		Capabilities: append([]planning.SiteCapability(nil), f.Capabilities...),
		Demands:      append([]planning.SiteSkuDemand(nil), f.Demands...),
		Plans:        append([]planning.PublishedCapacityPlan(nil), f.Plans...),
	}
}

// Load implements ports.PlanningSnapshotRepository.
func (p *PlanningFacts) Load(context.Context) (planning.Facts, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return planning.Facts{
		Capabilities: append([]planning.SiteCapability(nil), p.facts.Capabilities...),
		Demands:      append([]planning.SiteSkuDemand(nil), p.facts.Demands...),
		Plans:        append([]planning.PublishedCapacityPlan(nil), p.facts.Plans...),
	}, nil
}

// RebalanceRuns is the in-memory scheduled-rebalance run history.
type RebalanceRuns struct {
	mu   sync.Mutex
	next int64
	runs []transfer.RebalanceRun
}

// NewRebalanceRuns returns an empty history.
func NewRebalanceRuns() *RebalanceRuns { return &RebalanceRuns{} }

// Record implements ports.RebalanceRunRepository.
func (r *RebalanceRuns) Record(_ context.Context, run transfer.RebalanceRun) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.next++
	run.ID = r.next
	if run.FailClosedReason != nil {
		reason := *run.FailClosedReason
		run.FailClosedReason = &reason
	}
	r.runs = append(r.runs, run)
	return run.ID, nil
}

// List implements ports.RebalanceRunRepository: newest first (started_at,
// then id, descending), like the Postgres ORDER BY.
func (r *RebalanceRuns) List(_ context.Context, limit int) ([]transfer.RebalanceRun, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if limit <= 0 {
		limit = 50
	}
	out := append([]transfer.RebalanceRun{}, r.runs...)
	sort.SliceStable(out, func(i, j int) bool {
		if !out[i].StartedAt.Equal(out[j].StartedAt) {
			return out[i].StartedAt.After(out[j].StartedAt)
		}
		return out[i].ID > out[j].ID
	})
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}
