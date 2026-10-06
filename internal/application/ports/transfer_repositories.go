package ports

import (
	"context"

	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

// TransferRepository is the persistence port of the InterWarehouseTransfer
// saga aggregate. Both methods participate in the caller's
// ports.UnitOfWork when one is carried in ctx (see pgtx), so the state
// change, the audit append and the outbox inserts commit atomically.
type TransferRepository interface {
	// Create persists a NEW transfer (its full initial audit trail) and
	// enforces the UNIQUE idempotency key: a second Create with the same
	// key returns the FIRST transfer (idempotent approve), never a
	// duplicate.
	Create(ctx context.Context, t *transfer.InterWarehouseTransfer, idempotencyKey string) (existing *transfer.InterWarehouseTransfer, err error)
	// Load rehydrates the aggregate by transfer id.
	Load(ctx context.Context, id transfer.TransferID) (*transfer.InterWarehouseTransfer, error)
	// UpdateState persists the current state, reservation fields and the
	// audit entries appended since version (optimistic concurrency).
	UpdateState(ctx context.Context, t *transfer.InterWarehouseTransfer) error
}

// TransferEventPublisher is the transactional-outbox port: Publish encodes
// each domain event into its Kafka wire form and inserts an outbox_events
// row INSIDE the caller's UnitOfWork (the state change and the events
// commit together or not at all); a background relay drains them to Kafka
// afterwards. A request handler NEVER sends to Kafka directly.
type TransferEventPublisher interface {
	Publish(ctx context.Context, events ...transfer.DomainEvent) error
}

// NewTransferIDs mints fresh transfer ids. Production uses a UUID-based
// implementation; tests inject a deterministic one.
type NewTransferIDs interface {
	NewTransferID() transfer.TransferID
}
