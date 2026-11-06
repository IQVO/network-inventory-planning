package usecases

import (
	"context"
	"time"

	"github.com/claudioed/network-inventory-planning/internal/application/ports"
	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

// ApplyReleasedInput is one TransferStockAllocationReleased fact, already
// decoded by the inbound consumer.
type ApplyReleasedInput struct {
	TransferID transfer.TransferID
	Released   transfer.StockAllocationReleased
	OccurredAt time.Time
}

// ApplyTransferAllocationReleased applies inventory-storage's
// TransferStockAllocationReleased fact (ADR 0022): the saga moves
// RELEASING → CANCELLED (terminal). Both outcomes (RELEASED,
// ALREADY_RESOLVED) settle the release — either way the hold is gone.
type ApplyTransferAllocationReleased struct {
	Transfers ports.TransferRepository
	UoW       ports.UnitOfWork
	// Events publishes the TransferStateAdvanced analytics occurrence for
	// the RELEASING→CANCELLED transition (ADR 0007). OPTIONAL: nil skips
	// analytics (never fails a fact).
	Events ports.TransferEventPublisher
}

// Execute applies the released fact. A fact for an unknown transfer or an
// illegal state is a DETERMINISTIC error (the consumer logs and skips); a
// refused fact (wrong line/reservation/site/sku/quantity/outcome) is
// dead-lettered via IsRefusedReply; only infrastructure failures are
// transient.
func (u ApplyTransferAllocationReleased) Execute(ctx context.Context, in ApplyReleasedInput) error {
	ierr := u.UoW.Do(ctx, func(ctx context.Context) error {
		trf, err := u.Transfers.Load(ctx, in.TransferID)
		if err != nil {
			return err
		}
		loadedVersion := trf.Version()
		if err := trf.MarkReleased(in.Released, in.OccurredAt); err != nil {
			return err
		}
		if err := u.Transfers.UpdateState(ctx, trf); err != nil {
			return err
		}
		return publishStateAdvanced(ctx, u.Events, trf, loadedVersion)
	})
	return ierr
}
