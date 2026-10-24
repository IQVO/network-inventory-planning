package ports

import (
	"context"

	"github.com/claudioed/network-inventory-planning/internal/domain/packlevel"
)

// PackLevelView is the local copy of product-master's pack levels (ADR 0017),
// one row per (SKU, level), written only by the pack-level consumer and read
// by the planning use cases. No planning path ever calls product-master: it
// reads this view. Inside a UnitOfWork the writes join the transaction in
// ctx, so the processed-event claim and the level commit together.
type PackLevelView interface {
	// Get returns the stored level, or found=false when none was ever saved.
	Get(ctx context.Context, sku string, level packlevel.Level) (packlevel.PackLevel, bool, error)
	// Save upserts the level keyed (SKU, Level).
	Save(ctx context.Context, level packlevel.PackLevel) error
}
