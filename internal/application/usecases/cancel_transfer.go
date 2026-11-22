package usecases

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/claudioed/network-inventory-planning/internal/application/ports"
	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

// ErrCancelInvalid marks a cancel request the domain refuses on its face
// (empty reason): the HTTP layer maps it to 422.
var ErrCancelInvalid = fmt.Errorf("cancel transfer: invalid request")

// ErrCancelRefused marks a transfer whose state can never be cancelled:
// the fact-driven tail (PICKED onwards — the stock is physically in
// motion) and the terminal UNFULFILLABLE. The HTTP layer maps it to 409.
var ErrCancelRefused = fmt.Errorf("cancel transfer: state is not cancellable")

// CancelTransferInput is one operator cancel request.
type CancelTransferInput struct {
	TransferID transfer.TransferID
	Reason     string
}

// CancelTransferResult is the persisted outcome. Replayed is true when the
// saga was ALREADY cancelled (or already RELEASING) when the cancel
// arrived — the cancel is idempotent on the aggregate state, so a retried
// request answers the current state instead of a 409.
type CancelTransferResult struct {
	Transfer *transfer.InterWarehouseTransfer
	// Command is the TransferAllocationReleaseRequested command raised by
	// an ALLOCATED cancel (empty for every synchronous cancel).
	Command transfer.ReleaseRequested
	// Async is true when the saga settled into RELEASING: the transfer is
	// NOT cancelled yet — inventory-storage's TransferStockAllocationReleased
	// fact completes it (the HTTP layer answers 202, not 200).
	Async bool
	// Replayed is true when no transition was applied (already CANCELLED
	// or already RELEASING).
	Replayed bool
}

// CancelTransfer is the operator cancel use case (ADR 0022):
//
//   - DRAFT/PROPOSED/APPROVED/ALLOCATING: the pre-release Cancel — the
//     saga moves to CANCELLED synchronously, no hold exists;
//   - ALLOCATED: CancelAfterAllocation — the saga moves to RELEASING and
//     the TransferAllocationReleaseRequested command is outboxed in the
//     SAME transaction as the transition; inventory-storage resolves the
//     origin reservation and the TransferStockAllocationReleased fact
//     settles the saga to CANCELLED;
//   - RELEASING: an in-flight release — answered as the idempotent
//     continuation of the first cancel (Async, Replayed);
//   - CANCELLED: an idempotent replay — answered as the original outcome;
//   - PICKED/IN_TRANSIT/ARRIVED/RECEIVED/UNFULFILLABLE: refused — the
//     stock is physically in motion or the saga already closed
//     (ErrCancelRefused, 409).
type CancelTransfer struct {
	Transfers ports.TransferRepository
	Events    ports.TransferEventPublisher
	UoW       ports.UnitOfWork
	// Now supplies the cancel clock (never time.Now directly).
	Now func() time.Time
}

// Execute runs the cancel. Error mapping (the HTTP layer):
//
//   - empty reason → 422 invalid-cancel;
//   - unknown transfer → 404;
//   - PICKED onwards / UNFULFILLABLE → 409 cancel-refused;
//   - load/persist failure → 503.
func (u CancelTransfer) Execute(ctx context.Context, in CancelTransferInput) (CancelTransferResult, error) {
	if strings.TrimSpace(in.Reason) == "" {
		return CancelTransferResult{}, fmt.Errorf("%w: cancel reason is required", ErrCancelInvalid)
	}
	if u.Now == nil {
		return CancelTransferResult{}, fmt.Errorf("%w: clock is required", ErrCancelInvalid)
	}
	now := u.Now().UTC()

	var result CancelTransferResult
	ierr := u.UoW.Do(ctx, func(ctx context.Context) error {
		trf, err := u.Transfers.Load(ctx, in.TransferID)
		if err != nil {
			return err
		}
		result.Transfer = trf
		return u.applyCancel(ctx, trf, in, now, &result)
	})
	if ierr != nil {
		return CancelTransferResult{}, ierr
	}
	return result, nil
}

// applyCancel drives the loaded aggregate's state-specific cancel path
// inside the caller's UnitOfWork. The replay shortcuts fill result
// without touching the store; every other path persists its transition
// (and its events) before returning.
func (u CancelTransfer) applyCancel(ctx context.Context, trf *transfer.InterWarehouseTransfer, in CancelTransferInput, now time.Time, result *CancelTransferResult) error {
	loadedVersion := trf.Version()
	switch trf.State() {
	case transfer.StateCancelled:
		result.Replayed = true
		return nil
	case transfer.StateReleasing:
		result.Async = true
		result.Replayed = true
		return nil
	case transfer.StateDraft, transfer.StateProposed, transfer.StateApproved, transfer.StateAllocating:
		if err := trf.Cancel(in.Reason, now); err != nil {
			return classifyCancelError(err)
		}
		if err := u.Transfers.UpdateState(ctx, trf); err != nil {
			return err
		}
		return publishStateAdvanced(ctx, u.Events, trf, loadedVersion)
	case transfer.StateAllocated:
		command, err := trf.CancelAfterAllocation(in.Reason, now)
		if err != nil {
			return classifyCancelError(err)
		}
		if err := u.Transfers.UpdateState(ctx, trf); err != nil {
			return err
		}
		// The command and the analytics occurrence for the
		// ALLOCATED→RELEASING transition commit with the state
		// change in ONE transaction (the approval discipline).
		if err := u.Events.Publish(ctx, cancelEvents(trf, command, loadedVersion)...); err != nil {
			return err
		}
		result.Command = command
		result.Async = true
		return nil
	default:
		// PICKED onwards (stock physically in motion) or
		// UNFULFILLABLE (already closed): never cancellable.
		return fmt.Errorf("%w: transfer %s is in state %s", ErrCancelRefused, in.TransferID, trf.State())
	}
}

// classifyCancelError maps the aggregate's own refusals onto the use-case
// error vocabulary (an illegal transition here is a 409 refusal, not a
// skip-and-retry: the operator asked, the answer is no).
func classifyCancelError(err error) error {
	var illegal *transfer.IllegalTransitionError
	if errors.As(err, &illegal) {
		return fmt.Errorf("%w: %w", ErrCancelRefused, err)
	}
	return err
}

// cancelEvents bundles a cancel's integration command with the saga's
// TransferStateAdvanced analytics occurrences (ADR 0007), mirroring
// approvalEvents.
func cancelEvents(t *transfer.InterWarehouseTransfer, command transfer.ReleaseRequested, loadedVersion int64) []transfer.DomainEvent {
	advanced := t.StateAdvancedSince(loadedVersion)
	events := make([]transfer.DomainEvent, 0, 1+len(advanced))
	events = append(events, command)
	for _, a := range advanced {
		events = append(events, a)
	}
	return events
}
