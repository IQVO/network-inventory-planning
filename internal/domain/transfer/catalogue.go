package transfer

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// ErrInvalidCatalogueEntry marks a Rebalance Policy or Transfer Lane the
// catalogue refuses on its face (ADR 0015): a missing key, a negative
// quantity or cost, a target below the safety stock, a lane whose origin
// and destination are the same site, or a revision that tries to change
// the entry's key. The HTTP layer answers 422 invalid-catalogue-entry.
var ErrInvalidCatalogueEntry = errors.New("transfer: invalid catalogue entry")

// ErrCatalogueConcurrentModification marks a catalogue write that lost a
// race at the store: another writer created or revised the same entry
// between this writer's read and its guarded write. The HTTP layer answers
// 409 concurrent-modification; the caller re-reads and retries.
var ErrCatalogueConcurrentModification = errors.New("transfer: catalogue entry modified concurrently")

func invalidEntry(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidCatalogueEntry, fmt.Sprintf(format, args...))
}

// Validate checks a policy as an operator-managed catalogue entry. A zero
// safety stock, target stock or unit priority is legal (a target of 0
// means "do not stock this SKU here": the site never receives it).
func (p Policy) Validate() error {
	switch {
	case strings.TrimSpace(p.Version) == "":
		return invalidEntry("policy version is required")
	case strings.TrimSpace(string(p.Site)) == "":
		return invalidEntry("policy site is required")
	case strings.TrimSpace(string(p.SKU)) == "":
		return invalidEntry("policy sku is required")
	case p.SafetyStock < 0:
		return invalidEntry("policy %s/%s: safety stock must not be negative, got %d", p.Site, p.SKU, p.SafetyStock)
	case p.TargetStock < 0:
		return invalidEntry("policy %s/%s: target stock must not be negative, got %d", p.Site, p.SKU, p.TargetStock)
	case p.TargetStock < p.SafetyStock:
		return invalidEntry("policy %s/%s: target stock %d is below the safety stock %d", p.Site, p.SKU, p.TargetStock, p.SafetyStock)
	case p.UnitPriority < 0:
		return invalidEntry("policy %s/%s: unit priority must not be negative, got %d", p.Site, p.SKU, p.UnitPriority)
	}
	return nil
}

// Validate checks a lane as an operator-managed catalogue entry.
func (l Lane) Validate() error {
	switch {
	case strings.TrimSpace(string(l.Origin)) == "":
		return invalidEntry("lane origin is required")
	case strings.TrimSpace(string(l.Destination)) == "":
		return invalidEntry("lane destination is required")
	case l.Origin == l.Destination:
		return invalidEntry("lane %s→%s: origin and destination must differ", l.Origin, l.Destination)
	case l.LeadTime < 0:
		return invalidEntry("lane %s→%s: lead time must not be negative, got %s", l.Origin, l.Destination, l.LeadTime)
	case l.UnitHandlingCost < 0:
		return invalidEntry("lane %s→%s: unit handling cost must not be negative, got %d", l.Origin, l.Destination, l.UnitHandlingCost)
	}
	return nil
}

// RebalancePolicy is one entry of the operator-managed Rebalance Policy
// catalogue (ADR 0015), keyed by site + SKU. Revision is the optimistic
// concurrency token: 1 on creation, +1 per accepted change; an identical
// PUT changes nothing (idempotent).
type RebalancePolicy struct {
	Policy    Policy
	Revision  int64
	UpdatedAt time.Time
}

// NewRebalancePolicy validates p and returns it as a fresh catalogue entry.
func NewRebalancePolicy(p Policy, now time.Time) (RebalancePolicy, error) {
	if err := p.Validate(); err != nil {
		return RebalancePolicy{}, err
	}
	return RebalancePolicy{Policy: p, Revision: 1, UpdatedAt: now.UTC()}, nil
}

// Revise applies next to the entry. changed is false (and the entry is
// returned untouched) when next equals the stored policy. next must keep
// the entry's key (site + SKU).
func (e RebalancePolicy) Revise(next Policy, now time.Time) (RebalancePolicy, bool, error) {
	if err := next.Validate(); err != nil {
		return e, false, err
	}
	if next.Site != e.Policy.Site || next.SKU != e.Policy.SKU {
		return e, false, invalidEntry("policy %s/%s cannot be revised into %s/%s", e.Policy.Site, e.Policy.SKU, next.Site, next.SKU)
	}
	if next == e.Policy {
		return e, false, nil
	}
	return RebalancePolicy{Policy: next, Revision: e.Revision + 1, UpdatedAt: now.UTC()}, true, nil
}

// TransferLane is one entry of the operator-managed Transfer Lane
// catalogue (ADR 0015), keyed by origin + destination (a lane is
// directed). Revision behaves like RebalancePolicy's.
type TransferLane struct {
	Lane      Lane
	Revision  int64
	UpdatedAt time.Time
}

// NewTransferLane validates l and returns it as a fresh catalogue entry.
func NewTransferLane(l Lane, now time.Time) (TransferLane, error) {
	if err := l.Validate(); err != nil {
		return TransferLane{}, err
	}
	return TransferLane{Lane: l, Revision: 1, UpdatedAt: now.UTC()}, nil
}

// Revise applies next to the entry; see RebalancePolicy.Revise.
func (e TransferLane) Revise(next Lane, now time.Time) (TransferLane, bool, error) {
	if err := next.Validate(); err != nil {
		return e, false, err
	}
	if next.Origin != e.Lane.Origin || next.Destination != e.Lane.Destination {
		return e, false, invalidEntry("lane %s→%s cannot be revised into %s→%s", e.Lane.Origin, e.Lane.Destination, next.Origin, next.Destination)
	}
	if next == e.Lane {
		return e, false, nil
	}
	return TransferLane{Lane: next, Revision: e.Revision + 1, UpdatedAt: now.UTC()}, true, nil
}
