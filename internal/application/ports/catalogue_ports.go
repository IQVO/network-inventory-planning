package ports

import (
	"context"

	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

// RebalancePolicyRepository persists the operator-managed Rebalance Policy
// catalogue (ADR 0015), one entry per site + SKU.
type RebalancePolicyRepository interface {
	// Get returns the entry for site + sku; found is false when there is
	// none (never an error).
	Get(ctx context.Context, site transfer.SiteID, sku transfer.SKU) (entry transfer.RebalancePolicy, found bool, err error)
	// List returns every entry ordered by site, then SKU.
	List(ctx context.Context) ([]transfer.RebalancePolicy, error)
	// Save writes entry guarded by expectedRevision: 0 creates it (and
	// fails if it already exists), N updates it only while the stored
	// revision is still N. A lost race returns
	// transfer.ErrCatalogueConcurrentModification.
	Save(ctx context.Context, entry transfer.RebalancePolicy, expectedRevision int64) error
}

// TransferLaneRepository persists the operator-managed Transfer Lane
// catalogue (ADR 0015), one entry per directed origin → destination.
type TransferLaneRepository interface {
	// Get returns the entry for origin → destination; found is false when
	// there is none.
	Get(ctx context.Context, origin, destination transfer.SiteID) (entry transfer.TransferLane, found bool, err error)
	// List returns every entry ordered by origin, then destination.
	List(ctx context.Context) ([]transfer.TransferLane, error)
	// Save writes entry guarded by expectedRevision (see
	// RebalancePolicyRepository.Save).
	Save(ctx context.Context, entry transfer.TransferLane, expectedRevision int64) error
}
