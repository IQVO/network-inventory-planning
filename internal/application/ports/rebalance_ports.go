package ports

import (
	"context"

	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

// StuckTransferReader is the read-only view the saga-health check loop
// depends on (ADR 0007). It returns the narrow StuckView projection —
// never a rehydrated aggregate — because the check loop is observe-only:
// it may not mutate saga state, and a wide projection would couple it to
// aggregate shape.
type StuckTransferReader interface {
	// ListNonTerminal returns every transfer still in a state the saga
	// can leave, oldest-updated first, bounded by limit (<= 0 means the
	// adapter's own default). An error is infrastructure-only; an empty
	// fleet is an empty slice, never an error.
	ListNonTerminal(ctx context.Context, limit int) ([]transfer.StuckView, error)
}

// RebalanceRunRepository persists one row per scheduled rebalance pass
// (ADR 0007). Runs are observe-only: a row records what the planner saw,
// never an approval or an allocation command. The run type and its
// outcome enum live in the domain (internal/domain/transfer): the ports
// package contains interfaces only (architecture rule), and Go's
// structural typing lets the port reference the domain type directly.
type RebalanceRunRepository interface {
	// Record persists run and returns its database id.
	Record(ctx context.Context, run transfer.RebalanceRun) (int64, error)
	// List returns runs newest-first, bounded by limit (<= 0 means the
	// adapter's own default).
	List(ctx context.Context, limit int) ([]transfer.RebalanceRun, error)
}
