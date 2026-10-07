package usecases

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/claudioed/network-inventory-planning/internal/application/ports"
	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

// ErrWorkReleaseNotConfigured marks an approval refused because the pick
// leg's release configuration is incomplete (no TRANSFER_PICK_PATH_ID):
// fail-closed at APPROVE time so no transfer can ever be
// allocated-but-unworked (the allocation reply would have nowhere to
// release its pick demand to). The HTTP layer maps it to 503
// config-incomplete.
var ErrWorkReleaseNotConfigured = fmt.Errorf("approve transfer: work release not configured")

// WorkReleaseConfig is the release policy of the two origin work legs
// (ADR 0005): which process path each leg executes on (WES validates it
// against its own PathCatalogue) and how far out the critical pull time
// sits. Both are deployment configuration (TRANSFER_PICK_PATH_ID,
// TRANSFER_PICK_CPT_OFFSET, TRANSFER_DISPATCH_PATH_ID,
// TRANSFER_DISPATCH_CPT_OFFSET), never per-transfer input.
type WorkReleaseConfig struct {
	// PickPathID is TRANSFER_PICK_PATH_ID; required.
	PickPathID string
	// PickCPTOffset is TRANSFER_PICK_CPT_OFFSET; pick CPT = now + offset.
	PickCPTOffset time.Duration
	// DispatchPathID is TRANSFER_DISPATCH_PATH_ID; required for the
	// dispatch leg released on TransferPicked.
	DispatchPathID string
	// DispatchCPTOffset is TRANSFER_DISPATCH_CPT_OFFSET.
	DispatchCPTOffset time.Duration
}

// Validate fails closed: the pick leg's configuration is REQUIRED (an
// approval would otherwise mint a transfer whose allocation can never be
// worked); the dispatch leg's is required only when dispatch demands are
// released from the fact consumer (checked there for a clearer error).
func (c WorkReleaseConfig) Validate() error {
	if c.PickPathID == "" {
		return fmt.Errorf("%w: TRANSFER_PICK_PATH_ID is not set", ErrWorkReleaseNotConfigured)
	}
	if c.PickCPTOffset <= 0 {
		return fmt.Errorf("%w: TRANSFER_PICK_CPT_OFFSET must be positive", ErrWorkReleaseNotConfigured)
	}
	return nil
}

// ValidateDispatch fails closed for the dispatch leg.
func (c WorkReleaseConfig) ValidateDispatch() error {
	if c.DispatchPathID == "" {
		return fmt.Errorf("%w: TRANSFER_DISPATCH_PATH_ID is not set", ErrWorkReleaseNotConfigured)
	}
	if c.DispatchCPTOffset <= 0 {
		return fmt.Errorf("%w: TRANSFER_DISPATCH_CPT_OFFSET must be positive", ErrWorkReleaseNotConfigured)
	}
	return nil
}

// pickDemand builds the deterministic pick-leg WorkDemandReleased event
// for an ALLOCATED transfer: demand_id <transfer_id>:pick … per ADR 0005.
// The path/site/cpt come from deployment configuration, never from the
// event.
func pickDemand(t *transfer.InterWarehouseTransfer, cfg WorkReleaseConfig, now time.Time) transfer.DemandReleased {
	return transfer.DemandReleased{
		DemandID:    string(t.ID()) + ":pick",
		WorkKind:    transfer.WorkKindTransferPick,
		TransferRef: t.ID(),
		PathID:      cfg.PickPathID,
		SiteID:      t.OriginSiteID(),
		CPT:         now.Add(cfg.PickCPTOffset).UTC(),
		SKU:         t.SKU(),
		Quantity:    t.Quantity(),
		OccurredAt:  now.UTC(),
	}
}

// dispatchDemand builds the deterministic dispatch-leg WorkDemandReleased
// event for a PICKED transfer: quantity is the PICKED quantity, so a short
// pick dispatches only what was actually picked.
func dispatchDemand(t *transfer.InterWarehouseTransfer, cfg WorkReleaseConfig, now time.Time) transfer.DemandReleased {
	return transfer.DemandReleased{
		DemandID:    string(t.ID()) + ":dispatch",
		WorkKind:    transfer.WorkKindTransferDispatch,
		TransferRef: t.ID(),
		PathID:      cfg.DispatchPathID,
		SiteID:      t.OriginSiteID(),
		CPT:         now.Add(cfg.DispatchCPTOffset).UTC(),
		SKU:         t.SKU(),
		Quantity:    t.DispatchQuantity(),
		OccurredAt:  now.UTC(),
	}
}

// ApplyTransferAllocation applies a TransferStockAllocated reply: the
// saga moves ALLOCATING → ALLOCATED with reservation id, allocations and
// expiry persisted, AND (ADR 0005) releases the pick work demand — the
// transition, the audit entry and the WorkDemandReleased outbox row commit
// in ONE transaction, so a transfer can never be ALLOCATED with its pick
// work unreleased (or the reverse).
type ApplyTransferAllocation struct {
	Transfers ports.TransferRepository
	Events    ports.TransferEventPublisher
	UoW       ports.UnitOfWork
	// Release is the work-release configuration (ADR 0005). Validate()
	// must pass; the approve-time fail-closed gate guarantees it.
	Release WorkReleaseConfig
	// Now supplies the release clock (the demand's occurred-at when the
	// CE time is unusable; the reply's CE time is preferred).
	Now func() time.Time
}

// Execute applies the reply and releases the pick demand. A reply for an
// unknown transfer or an illegal state is a DETERMINISTIC error (the
// consumer logs and skips); only infrastructure failures are transient.
func (u ApplyTransferAllocation) Execute(ctx context.Context, in ApplyAllocationInput) error {
	ierr := u.UoW.Do(ctx, func(ctx context.Context) error {
		trf, err := u.Transfers.Load(ctx, in.TransferID)
		if err != nil {
			return err
		}
		if err := trf.MarkAllocated(in.Allocation, in.OccurredAt); err != nil {
			return err
		}
		if err := u.Transfers.UpdateState(ctx, trf); err != nil {
			return err
		}
		releaseAt := in.OccurredAt
		if releaseAt.IsZero() {
			releaseAt = u.Now().UTC()
		}
		return u.Events.Publish(ctx, pickDemand(trf, u.Release, releaseAt))
	})
	return ierr
}

// ApplyPickInput is one fulfillment-execution TransferPicked fact.
type ApplyPickInput struct {
	TransferID transfer.TransferID
	Picked     transfer.Picked
	OccurredAt time.Time
}

// ApplyTransferPick applies a TransferPicked fact: ALLOCATED → PICKED with
// the picked quantity recorded (a short pick is recorded, not refused),
// then releases the dispatch work demand carrying the PICKED quantity —
// one transaction (ADR 0005).
type ApplyTransferPick struct {
	Transfers ports.TransferRepository
	Events    ports.TransferEventPublisher
	UoW       ports.UnitOfWork
	Release   WorkReleaseConfig
}

// Execute applies the fact and releases the dispatch demand.
func (u ApplyTransferPick) Execute(ctx context.Context, in ApplyPickInput) error {
	ierr := u.UoW.Do(ctx, func(ctx context.Context) error {
		trf, err := u.Transfers.Load(ctx, in.TransferID)
		if err != nil {
			return err
		}
		if err := trf.MarkPicked(in.Picked, in.OccurredAt); err != nil {
			return err
		}
		if err := u.Transfers.UpdateState(ctx, trf); err != nil {
			return err
		}
		if err := u.Release.ValidateDispatch(); err != nil {
			// A misconfigured dispatch leg must not strand the
			// PICKED state: the fact is still applied (it happened),
			// the transition commits, and the missing demand is an
			// operator-visible failure.
			return err
		}
		return u.Events.Publish(ctx, dispatchDemand(trf, u.Release, in.OccurredAt))
	})
	return ierr
}

// ApplyTransferDispatched applies a TransferDispatched fact: PICKED →
// IN_TRANSIT. No demand is released (the arrival leg is released by
// inventory-storage's destination facts, not by WES work).
type ApplyTransferDispatched struct {
	Transfers ports.TransferRepository
	UoW       ports.UnitOfWork
}

// Execute applies the fact.
func (u ApplyTransferDispatched) Execute(ctx context.Context, in FactInput) error {
	ierr := u.UoW.Do(ctx, func(ctx context.Context) error {
		trf, err := u.Transfers.Load(ctx, in.TransferID)
		if err != nil {
			return err
		}
		if err := trf.MarkDispatched(in.OccurredAt); err != nil {
			return err
		}
		return u.Transfers.UpdateState(ctx, trf)
	})
	return ierr
}

// FactInput is one fulfillment-execution transfer fact keyed by
// transfer_ref (Dispatched / Arrived).
type FactInput struct {
	TransferID transfer.TransferID
	OccurredAt time.Time
}

// ApplyTransferArrival applies a TransferArrived fact (the reserved
// kind): IN_TRANSIT → ARRIVED. Scan-driven receiving may never fire it —
// inventory-storage's TransferReceiptStaged drives the same transition
// (ADR 0005) — so an arrival fact for a transfer already past IN_TRANSIT
// is a deterministic no-op skip, never an error loop.
type ApplyTransferArrival struct {
	Transfers ports.TransferRepository
	UoW       ports.UnitOfWork
}

// Execute applies the fact.
func (u ApplyTransferArrival) Execute(ctx context.Context, in FactInput) error {
	ierr := u.UoW.Do(ctx, func(ctx context.Context) error {
		trf, err := u.Transfers.Load(ctx, in.TransferID)
		if err != nil {
			return err
		}
		if err := trf.MarkArrived("TransferArrived", in.OccurredAt); err != nil {
			return err
		}
		return u.Transfers.UpdateState(ctx, trf)
	})
	return ierr
}

// ApplyReceiptStagedInput is one inventory-storage TransferReceiptStaged
// fact.
type ApplyReceiptStagedInput struct {
	TransferID transfer.TransferID
	LineID     string
	OccurredAt time.Time
}

// ApplyTransferReceiptStaged applies inventory-storage's
// TransferReceiptStaged fact: IN_TRANSIT → ARRIVED (the scan-driven
// receiving path; same transition as the reserved TransferArrived fact).
type ApplyTransferReceiptStaged struct {
	Transfers ports.TransferRepository
	UoW       ports.UnitOfWork
}

// Execute applies the fact.
func (u ApplyTransferReceiptStaged) Execute(ctx context.Context, in ApplyReceiptStagedInput) error {
	ierr := u.UoW.Do(ctx, func(ctx context.Context) error {
		trf, err := u.Transfers.Load(ctx, in.TransferID)
		if err != nil {
			return err
		}
		if err := trf.MarkArrived("TransferReceiptStaged", in.OccurredAt); err != nil {
			return err
		}
		return u.Transfers.UpdateState(ctx, trf)
	})
	return ierr
}

// ApplyStowInput is one inventory-storage TransferStockStowed fact.
type ApplyStowInput struct {
	TransferID transfer.TransferID
	Stowed     transfer.Stowed
	OccurredAt time.Time
}

// ApplyTransferStow applies inventory-storage's TransferStockStowed fact:
// ARRIVED → RECEIVED (terminal), with the destination stow allocations
// persisted on the aggregate.
type ApplyTransferStow struct {
	Transfers ports.TransferRepository
	UoW       ports.UnitOfWork
}

// Execute applies the fact.
func (u ApplyTransferStow) Execute(ctx context.Context, in ApplyStowInput) error {
	ierr := u.UoW.Do(ctx, func(ctx context.Context) error {
		trf, err := u.Transfers.Load(ctx, in.TransferID)
		if err != nil {
			return err
		}
		if err := trf.MarkStowed(in.Stowed, in.OccurredAt); err != nil {
			return err
		}
		return u.Transfers.UpdateState(ctx, trf)
	})
	return ierr
}

// IsDeterministicFact reports whether a fact-apply error must be
// logged-and-skipped by the fact consumers rather than retried: unknown
// transfer (WARN + commit past), illegal transition (a replay or an
// out-of-order fact), or a fact that fails the aggregate's consistency
// checks (transfer.ErrFactRefused). Everything else is transient.
func IsDeterministicFact(err error) bool {
	var illegal *transfer.IllegalTransitionError
	return errors.Is(err, transfer.ErrTransferNotFound) ||
		errors.Is(err, transfer.ErrFactRefused) ||
		errors.As(err, &illegal)
}
