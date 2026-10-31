package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/network-inventory-planning/internal/domain/packlevel"
)

// PackLevelView is the pgxpool-backed ports.PackLevelView: the local copy of
// product-master's pack levels, one row per (SKU, level). Writes go through
// queryFor, so inside a UnitOfWork they join the caller's transaction.
type PackLevelView struct {
	pool *pgxpool.Pool
}

// NewPackLevelView constructs a PackLevelView over pool.
func NewPackLevelView(pool *pgxpool.Pool) *PackLevelView { return &PackLevelView{pool: pool} }

// Get implements ports.PackLevelView.
func (v *PackLevelView) Get(ctx context.Context, sku string, level packlevel.Level) (packlevel.PackLevel, bool, error) {
	var (
		l             packlevel.PackLevel
		lvl           string
		effectiveFrom time.Time
		effectiveTo   *time.Time
	)
	err := queryFor(ctx, v.pool).QueryRow(ctx, `
		SELECT sku, level, gtin, units_per_pack, effective_from, effective_to, version
		FROM pack_levels WHERE sku = $1 AND level = $2
	`, sku, string(level)).Scan(&l.SKU, &lvl, &l.GTIN, &l.UnitsPerPack, &effectiveFrom, &effectiveTo, &l.Version)
	if errors.Is(err, pgx.ErrNoRows) {
		return packlevel.PackLevel{}, false, nil
	}
	if err != nil {
		return packlevel.PackLevel{}, false, fmt.Errorf("read pack level: %w", err)
	}
	l.Level = packlevel.Level(lvl)
	l.EffectiveFrom = effectiveFrom.UTC()
	if effectiveTo != nil {
		end := effectiveTo.UTC()
		l.EffectiveTo = &end
	}
	return l, true, nil
}

// Save implements ports.PackLevelView: an upsert keyed (sku, level).
func (v *PackLevelView) Save(ctx context.Context, l packlevel.PackLevel) error {
	var effectiveTo *time.Time
	if l.EffectiveTo != nil {
		end := l.EffectiveTo.UTC()
		effectiveTo = &end
	}
	_, err := queryFor(ctx, v.pool).Exec(ctx, `
		INSERT INTO pack_levels (sku, level, gtin, units_per_pack, effective_from, effective_to, version)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (sku, level) DO UPDATE
		SET gtin = EXCLUDED.gtin, units_per_pack = EXCLUDED.units_per_pack,
		    effective_from = EXCLUDED.effective_from, effective_to = EXCLUDED.effective_to,
		    version = EXCLUDED.version, updated_at = now()
	`, l.SKU, string(l.Level), l.GTIN, l.UnitsPerPack, l.EffectiveFrom.UTC(), effectiveTo, l.Version)
	if err != nil {
		return fmt.Errorf("save pack level: %w", err)
	}
	return nil
}
