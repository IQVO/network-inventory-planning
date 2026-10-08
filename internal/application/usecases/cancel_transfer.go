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

// ErrInvalidCancellation marks a cancel request the use case refuses on
// its face (blank transfer id or blank reason). The HTTP layer answers 422
// invalid-cancellation.
var ErrInvalidCancellation = errors.New("cancel transfer: invalid request")

// ErrTransferNotCancellable marks a cancel attempted from a state in which
// the aggregate forbids it (ADR 0003: only DRAFT, PROPOSED, APPROVED and
// ALLOCATING are cancellable). The wrapped *transfer.IllegalTransitionError
// carries the offending state. The HTTP layer answers 409
// transfer-not-cancellable.
var ErrTransferNotCancellable = errors.New("cancel transfer: transfer is not cancellable")

// CancelTransferInput is the operator's cancel request.
type CancelTransferInput struct {
	TransferID string
	// Reason is the operator's required, non-blank justification; it
	// becomes the cause of the TransferCancelled audit entry.
	Reason string
}

// CancelTransferResult is the transfer after the cancel.
type CancelTransferResult struct {
	Transfer *transfer.InterWarehouseTransfer
	// AlreadyCancelled is true when the transfer was CANCELLED before this
	// call: the request is an idempotent no-op and nothing was written.
	AlreadyCancelled bool
}

// CancelTransfer is the operator's PRE-RELEASE cancel (ADR 0011): it loads
// the transfer, applies the aggregate's Cancel (the aggregate alone decides
// which states allow it — this use case never widens that), persists the
// state and raises the TransferStateAdvanced analytics occurrence, all in
// ONE UnitOfWork. No integration event is emitted: a cancellable transfer
// has no origin reservation and no released work demand for a downstream
// consumer to undo.
type CancelTransfer struct {
	Transfers ports.TransferRepository
	Events    ports.TransferEventPublisher
	UoW       ports.UnitOfWork
	// Now supplies the cancel clock (never time.Now directly).
	Now func() time.Time
}

// Execute cancels the transfer. Error mapping (the HTTP layer):
//
//   - blank id/reason → ErrInvalidCancellation (422);
//   - unknown transfer → transfer.ErrTransferNotFound (404);
//   - state past release → ErrTransferNotCancellable (409);
//   - an already CANCELLED transfer → success with AlreadyCancelled.
func (u CancelTransfer) Execute(ctx context.Context, in CancelTransferInput) (CancelTransferResult, error) {
	if u.Now == nil {
		return CancelTransferResult{}, fmt.Errorf("cancel transfer: clock is required")
	}
	id := strings.TrimSpace(in.TransferID)
	if id == "" {
		return CancelTransferResult{}, fmt.Errorf("%w: transfer id is required", ErrInvalidCancellation)
	}
	reason := strings.TrimSpace(in.Reason)
	if reason == "" {
		return CancelTransferResult{}, fmt.Errorf("%w: reason is required", ErrInvalidCancellation)
	}
	now := u.Now().UTC()

	var result CancelTransferResult
	err := u.UoW.Do(ctx, func(ctx context.Context) error {
		trf, err := u.Transfers.Load(ctx, transfer.TransferID(id))
		if err != nil {
			return err
		}
		if trf.State() == transfer.StateCancelled {
			result = CancelTransferResult{Transfer: trf, AlreadyCancelled: true}
			return nil
		}
		loadedVersion := trf.Version()
		if err := trf.Cancel(reason, now); err != nil {
			var illegal *transfer.IllegalTransitionError
			if errors.As(err, &illegal) {
				return fmt.Errorf("%w: %w", ErrTransferNotCancellable, err)
			}
			return err
		}
		if err := u.Transfers.UpdateState(ctx, trf); err != nil {
			return err
		}
		// TransferStateAdvanced for the <state>→CANCELLED transition,
		// same transaction (ADR 0007).
		if err := publishStateAdvanced(ctx, u.Events, trf, loadedVersion); err != nil {
			return err
		}
		result = CancelTransferResult{Transfer: trf}
		return nil
	})
	if err != nil {
		return CancelTransferResult{}, err
	}
	return result, nil
}
