package transfer

import (
	"fmt"
	"time"
)

// WorkKind is which leg of the transfer a released work demand executes.
// The enum strings are WES's consumed contract (wes-work-planning ADR-0033)
// — never renamed locally.
type WorkKind string

const (
	WorkKindTransferPick     WorkKind = "TRANSFER_PICK"
	WorkKindTransferDispatch WorkKind = "TRANSFER_DISPATCH"
	WorkKindTransferArrival  WorkKind = "TRANSFER_ARRIVAL"
)

// Valid reports whether k is one of the closed kinds.
func (k WorkKind) Valid() bool {
	switch k {
	case WorkKindTransferPick, WorkKindTransferDispatch, WorkKindTransferArrival:
		return true
	default:
		return false
	}
}

// DemandID is the deterministic identity of one released work demand leg:
// "<transfer_id>:<leg>" where leg is pick | dispatch | arrival. WES mints
// its work unit under exactly this id, so a re-release of the same leg is
// idempotent on the WES side too.
func (id TransferID) DemandID(kind WorkKind) string {
	var leg string
	switch kind {
	case WorkKindTransferPick:
		leg = "pick"
	case WorkKindTransferDispatch:
		leg = "dispatch"
	case WorkKindTransferArrival:
		leg = "arrival"
	}
	return string(id) + ":" + leg
}

// DemandReleased is the WorkDemandReleased COMMAND event: one leg of an
// approved transfer released for warehouse work. It is published through
// the transactional outbox in the SAME transaction as the state change
// that released it (ADR 0005):
//
//   - ALLOCATED (pick leg): the pick demand carries the allocated quantity;
//   - PICKED (dispatch leg): the dispatch demand carries the PICKED
//     quantity, so a short pick dispatches only what was actually picked.
//
// The wire payload is exactly WES's consumed contract: data {demand_id,
// work_kind, transfer_ref, path_id, site_id, cpt, sku, quantity},
// subject/key demand_id.
type DemandReleased struct {
	DemandID    string
	WorkKind    WorkKind
	TransferRef TransferID
	PathID      string
	SiteID      string
	CPT         time.Time
	SKU         string
	Quantity    int
	OccurredAt  time.Time
}

// EventName implements DomainEvent.
func (DemandReleased) EventName() string { return "WorkDemandReleased" }

// Picked is the TransferPicked fact applied to the aggregate: the origin
// pick completed with pickedQuantity units (a short pick — pickedQuantity
// below the allocated quantity — is recorded, not refused; the dispatch
// demand then carries the picked quantity).
type Picked struct {
	PickedQuantity int
}

// MarkPicked applies fulfillment-execution's TransferPicked fact: state
// PICKED with the picked quantity recorded. A short pick (picked <
// allocated) is RECORDED, never refused — the dispatch demand releases
// with the picked quantity so the physical and planned flows stay equal.
func (t *InterWarehouseTransfer) MarkPicked(p Picked, now time.Time) error {
	if t.state != StateAllocated {
		return &IllegalTransitionError{ID: t.id, From: t.state, Action: "mark picked"}
	}
	if p.PickedQuantity <= 0 {
		return fmt.Errorf("%w: transfer %s: picked quantity must be positive, got %d", ErrFactRefused, t.id, p.PickedQuantity)
	}
	if p.PickedQuantity > t.quantity {
		return fmt.Errorf("%w: transfer %s: picked quantity %d exceeds the allocated %d", ErrFactRefused, t.id, p.PickedQuantity, t.quantity)
	}
	t.pickedQuantity = p.PickedQuantity
	t.transition(StatePicked, "TransferPicked", fmt.Sprintf("picked %d of %d", p.PickedQuantity, t.quantity), now.UTC())
	return nil
}

// MarkDispatched applies fulfillment-execution's TransferDispatched fact:
// state IN_TRANSIT.
func (t *InterWarehouseTransfer) MarkDispatched(now time.Time) error {
	if t.state != StatePicked {
		return &IllegalTransitionError{ID: t.id, From: t.state, Action: "mark dispatched"}
	}
	t.transition(StateInTransit, "TransferDispatched", "transfer left the origin site", now.UTC())
	return nil
}

// MarkArrived applies a TransferArrived fact (fulfillment-execution's
// reserved kind) OR inventory-storage's TransferReceiptStaged fact: state
// ARRIVED. Both are accepted as the arrival trigger — scan-driven
// receiving may publish TransferReceiptStaged without any
// TransferArrived ever firing (ADR 0005).
func (t *InterWarehouseTransfer) MarkArrived(event string, now time.Time) error {
	if t.state != StateInTransit {
		return &IllegalTransitionError{ID: t.id, From: t.state, Action: "mark arrived"}
	}
	t.transition(StateArrived, event, "transfer reached the destination site", now.UTC())
	return nil
}

// StowAllocation is one stock unit's stow location, mirrored from
// inventory-storage's TransferStockStowed allocations[].
type StowAllocation struct {
	StockUnitID string
	BinID       string
	Quantity    int
}

// Stowed is the TransferStockStowed fact applied to the aggregate.
type Stowed struct {
	TransferLineID   string
	DestinationSite  string
	SKU              string
	ReceivedQuantity int
	StowedQuantity   int
	Allocations      []StowAllocation
}

// MarkStowed applies inventory-storage's TransferStockStowed fact: state
// RECEIVED (terminal) with the destination stow allocations persisted.
// Like MarkAllocated, every mismatch between the fact and the transfer it
// claims to complete is a deterministic error (the consumer skips the
// message), never a best-effort write.
func (t *InterWarehouseTransfer) MarkStowed(s Stowed, now time.Time) error {
	if t.state != StateArrived {
		return &IllegalTransitionError{ID: t.id, From: t.state, Action: "mark stowed"}
	}
	if err := t.checkStowFact(s); err != nil {
		return err
	}
	t.stowAllocations = append([]StowAllocation(nil), s.Allocations...)
	t.transition(StateReceived, "TransferStockStowed", fmt.Sprintf("stowed %d at %s", s.StowedQuantity, s.DestinationSite), now.UTC())
	return nil
}

// checkStowFact proves the stow fact completes THIS transfer's single line
// at its destination.
func (t *InterWarehouseTransfer) checkStowFact(s Stowed) error {
	if s.TransferLineID != t.id.LineID() {
		return fmt.Errorf("%w: transfer %s: stow fact is for line %q, want %q", ErrFactRefused, t.id, s.TransferLineID, t.id.LineID())
	}
	if s.DestinationSite != t.destinationSiteID {
		return fmt.Errorf("%w: transfer %s: stow fact destination %q, want %q", ErrFactRefused, t.id, s.DestinationSite, t.destinationSiteID)
	}
	if s.SKU != t.sku {
		return fmt.Errorf("%w: transfer %s: stow fact sku %q, want %q", ErrFactRefused, t.id, s.SKU, t.sku)
	}
	if s.StowedQuantity <= 0 {
		return fmt.Errorf("%w: transfer %s: stow fact quantity must be positive, got %d", ErrFactRefused, t.id, s.StowedQuantity)
	}
	if len(s.Allocations) == 0 {
		return fmt.Errorf("%w: transfer %s: stow fact carries no stow allocations", ErrFactRefused, t.id)
	}
	return nil
}
