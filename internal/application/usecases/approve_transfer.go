package usecases

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/claudioed/network-inventory-planning/internal/application/ports"
	"github.com/claudioed/network-inventory-planning/internal/domain/planning"
	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

// ErrIdempotencyConflict marks an Idempotency-Key reuse with a DIFFERENT
// payload: the operator must see the mismatch, not a silent replay.
var ErrIdempotencyConflict = fmt.Errorf("approve transfer: idempotency key reuse with a different request")

// ErrInvalidApproval marks a request the domain refuses on its face
// (missing/invalid fields, non-positive staleness): the HTTP layer maps it
// to 422.
var ErrInvalidApproval = fmt.Errorf("approve transfer: invalid request")

// ApproveTransferInput is the approval request's domain-typed boundary: a
// proposal snapshot plus the operator context. IdempotencyKey is required
// (comes from the Idempotency-Key header).
type ApproveTransferInput struct {
	IdempotencyKey    string
	OriginSiteID      string
	DestinationSiteID string
	SKU               string
	Quantity          int
	PolicyVersion     string
	OperatorReason    string
	ProposalAsOf      time.Time
}

// ApproveTransferResult is the persisted outcome.
type ApproveTransferResult struct {
	Transfer          *transfer.InterWarehouseTransfer
	TransferID        transfer.TransferID
	Replayed          bool // true when the idempotency key already existed
	AllocationCommand transfer.AllocationRequested
}

// ApproveTransfer is the Phase-2 saga approval use case: it takes a
// proposal snapshot, validates it against the CURRENT fail-closed read
// models, persists the InterWarehouseTransfer through PROPOSED→APPROVED→
// ALLOCATING and emits TransferPlanApproved + TransferAllocationRequested
// through the transactional outbox — all in ONE UnitOfWork. The relay
// drains the events to Kafka after commit; inventory-storage's reply then
// drives the aggregate to ALLOCATED or UNFULFILLABLE via the reply
// consumer, which also releases the pick work demand (ADR 0005).
type ApproveTransfer struct {
	Transfers    ports.TransferRepository
	Events       ports.TransferEventPublisher
	Snapshot     ports.PlanningSnapshotRepository
	UoW          ports.UnitOfWork
	MaxStaleness time.Duration
	// Release is the work-release configuration (ADR 0005). Validate()
	// MUST pass before any approval is accepted: an approval without a
	// configured pick path would mint a transfer whose allocation reply
	// has no pick demand to release — fail-closed 503 instead.
	Release WorkReleaseConfig
	// Now supplies the approval clock (never time.Now directly).
	Now func() time.Time
}

// defaultApprovalExpiry is how long an approved-but-unallocated transfer
// may age before the saga refuses to keep allocating it (v1: a flat 24h
// horizon; per-lane policy expiry is a later refinement).
const defaultApprovalExpiry = 24 * time.Hour

// Execute runs the approval. Error mapping (the HTTP layer):
//
//   - missing/invalid fields, fail-closed validation refusal → 422
//     (stale/missing facts refuse, never approve);
//   - read models unusable (load error) → 503;
//   - ErrIdempotencyConflict → 409;
//   - replay of the SAME key+payload → 200 with Replayed: true.
func (u ApproveTransfer) Execute(ctx context.Context, in ApproveTransferInput) (ApproveTransferResult, error) {
	if err := u.validate(in); err != nil {
		return ApproveTransferResult{}, err
	}
	now := u.Now().UTC()

	facts, err := u.Snapshot.Load(ctx)
	if err != nil {
		return ApproveTransferResult{}, fmt.Errorf("approve transfer: load read models: %w", err)
	}
	snap, err := planning.BuildSnapshot(planning.SnapshotInput{
		Capabilities: facts.Capabilities,
		Demands:      facts.Demands,
		Plans:        facts.Plans,
		MaxStaleness: u.MaxStaleness,
		Now:          func() time.Time { return now },
	})
	if err != nil {
		return ApproveTransferResult{}, fmt.Errorf("%w: %s", transfer.ErrFactsIncomplete, err)
	}

	proposal := transfer.ProposalInput{
		ID:                transfer.TransferID("trf-" + uuid.NewString()),
		IdempotencyKey:    in.IdempotencyKey,
		OriginSiteID:      in.OriginSiteID,
		DestinationSiteID: in.DestinationSiteID,
		SKU:               in.SKU,
		Quantity:          in.Quantity,
		PolicyVersion:     in.PolicyVersion,
		OperatorReason:    in.OperatorReason,
		ProposalAsOf:      in.ProposalAsOf,
		ExpiresAt:         now.Add(defaultApprovalExpiry),
		Now:               now,
	}
	if err := transfer.ValidateApproval(proposal, transfer.ApprovalFacts{Snapshot: snap, MaxStaleness: u.MaxStaleness}); err != nil {
		return ApproveTransferResult{}, err
	}

	var (
		result ApproveTransferResult
	)
	ierr := u.UoW.Do(ctx, func(ctx context.Context) error {
		trf, err := transfer.ProposeTransfer(proposal)
		if err != nil {
			return err
		}
		approved, err := trf.Approve(now)
		if err != nil {
			return err
		}
		command, err := trf.RequestAllocation(now)
		if err != nil {
			return err
		}
		existing, err := u.Transfers.Create(ctx, trf, in.IdempotencyKey)
		if err != nil {
			return err
		}
		if existing != nil {
			if !sameApproval(existing, in) {
				return ErrIdempotencyConflict
			}
			result = ApproveTransferResult{
				Transfer:   existing,
				TransferID: existing.ID(),
				Replayed:   true,
			}
			return nil
		}
		if err := u.Events.Publish(ctx, approved, command); err != nil {
			return err
		}
		result = ApproveTransferResult{Transfer: trf, TransferID: trf.ID(), AllocationCommand: command}
		return nil
	})
	if ierr != nil {
		return ApproveTransferResult{}, ierr
	}
	return result, nil
}

func (u ApproveTransfer) validate(in ApproveTransferInput) error {
	if u.Now == nil {
		return fmt.Errorf("%w: clock is required", ErrInvalidApproval)
	}
	if u.MaxStaleness <= 0 {
		return fmt.Errorf("%w: max staleness must be positive", ErrInvalidApproval)
	}
	// Fail-closed work-release gate (ADR 0005): without a configured pick
	// path the allocation reply would have no pick demand to release, so
	// no transfer may even be approved. 503, not 422: deployment
	// configuration is missing, the request was fine.
	if err := u.Release.Validate(); err != nil {
		return err
	}
	if in.IdempotencyKey == "" {
		return fmt.Errorf("%w: idempotency key is required", ErrInvalidApproval)
	}
	if in.ProposalAsOf.IsZero() {
		return fmt.Errorf("%w: proposal as-of is required", ErrInvalidApproval)
	}
	return nil
}

// sameApproval reports whether a replayed idempotency key carries the same
// logical request as the persisted transfer.
func sameApproval(t *transfer.InterWarehouseTransfer, in ApproveTransferInput) bool {
	return t.OriginSiteID() == in.OriginSiteID &&
		t.DestinationSiteID() == in.DestinationSiteID &&
		t.SKU() == in.SKU &&
		t.Quantity() == in.Quantity &&
		t.PolicyVersion() == in.PolicyVersion &&
		t.ProposalAsOf().Equal(in.ProposalAsOf)
}

// ApplyAllocationInput is one inventory-storage reply, already decoded by
// the inbound consumer.
type ApplyAllocationInput struct {
	TransferID transfer.TransferID
	Allocation transfer.StockAllocation
	OccurredAt time.Time
}

// ApplyRejectionInput is one TransferStockAllocationRejected reply.
type ApplyRejectionInput struct {
	TransferID transfer.TransferID
	LineID     string
	Reason     transfer.RejectionReason
	OccurredAt time.Time
}

// ApplyTransferRejection applies a TransferStockAllocationRejected reply:
// the saga moves ALLOCATING → UNFULFILLABLE with the closed reason.
type ApplyTransferRejection struct {
	Transfers ports.TransferRepository
	UoW       ports.UnitOfWork
}

// Execute applies the rejection.
func (u ApplyTransferRejection) Execute(ctx context.Context, in ApplyRejectionInput) error {
	ierr := u.UoW.Do(ctx, func(ctx context.Context) error {
		trf, err := u.Transfers.Load(ctx, in.TransferID)
		if err != nil {
			return err
		}
		if err := trf.MarkUnfulfillable(in.LineID, in.Reason, in.OccurredAt); err != nil {
			return err
		}
		return u.Transfers.UpdateState(ctx, trf)
	})
	return ierr
}

// IsDeterministic reports whether err must be logged-and-skipped by the
// reply consumer rather than retried.
func IsDeterministic(err error) bool {
	var illegal *transfer.IllegalTransitionError
	return errors.Is(err, transfer.ErrTransferNotFound) || errors.As(err, &illegal)
}
