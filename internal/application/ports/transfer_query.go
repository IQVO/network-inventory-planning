package ports

import (
	"context"

	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

// TransferQuery is the READ-ONLY query port over the transfer saga store.
// It is deliberately separate from TransferRepository (the aggregate's
// write/rehydrate port): the read side never joins a UnitOfWork, never
// widens the repository, and cannot mutate anything.
type TransferQuery interface {
	// Get returns one transfer with its audit trail, or
	// transfer.ErrTransferNotFound.
	Get(ctx context.Context, id transfer.TransferID) (*transfer.InterWarehouseTransfer, error)
	// List returns one page of transfers (each with its audit trail)
	// matching filter, ordered by filter.Order, plus the total number of
	// matches before paging.
	List(ctx context.Context, filter transfer.ListFilter) (transfer.TransferPage, error)
}
