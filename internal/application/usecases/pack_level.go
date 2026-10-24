package usecases

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/claudioed/network-inventory-planning/internal/application/ports"
	"github.com/claudioed/network-inventory-planning/internal/domain/packlevel"
	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

// ErrInvalidPackLevelEvent marks a ProductPackLevelDeclared / Retired payload
// that can never be applied (blank sku or gtin, an unknown level, units or
// version below 1). It is DETERMINISTIC: the consumer dead-letters it
// (ADR 0013), it never retries it.
var ErrInvalidPackLevelEvent = errors.New("pack level: invalid event")

// PackLevelOutcome is what applying one pack-level event did to the view.
type PackLevelOutcome string

// The outcomes. Applied changed the view; Stale carried a version not
// greater than the stored one (a replay or a late message); Ignored named a
// level or GTIN the view does not hold.
const (
	PackLevelApplied PackLevelOutcome = "APPLIED"
	PackLevelStale   PackLevelOutcome = "STALE"
	PackLevelIgnored PackLevelOutcome = "IGNORED"
)

// PackLevelDeclaredChange is a decoded ProductPackLevelDeclared payload,
// reduced to what this service needs.
type PackLevelDeclaredChange struct {
	SKU           string
	Level         string
	GTIN          string
	UnitsPerPack  int64
	EffectiveFrom time.Time
	Version       int64
}

// PackLevelRetiredChange is a decoded ProductPackLevelRetired payload,
// reduced to what this service needs.
type PackLevelRetiredChange struct {
	SKU         string
	Level       string
	GTIN        string
	EffectiveTo time.Time
	Version     int64
}

// ApplyPackLevel keeps the local pack-level view in step with product-master.
// Run it inside the unit of work that claimed the event id.
type ApplyPackLevel struct {
	PackLevels ports.PackLevelView
}

// Declared applies a ProductPackLevelDeclared. A version not greater than the
// stored one is PackLevelStale; a payload that can never be applied is
// ErrInvalidPackLevelEvent.
func (a ApplyPackLevel) Declared(ctx context.Context, c PackLevelDeclaredChange) (PackLevelOutcome, error) {
	incoming, err := packlevel.New(c.SKU, c.Level, c.GTIN, c.UnitsPerPack, c.EffectiveFrom, c.Version)
	if err != nil {
		return "", fmt.Errorf("%w: %w", ErrInvalidPackLevelEvent, err)
	}
	cur, _, err := a.PackLevels.Get(ctx, incoming.SKU, incoming.Level)
	if err != nil {
		return "", fmt.Errorf("read pack level: %w", err)
	}
	if !incoming.Supersedes(cur) {
		return PackLevelStale, nil
	}
	if err := a.PackLevels.Save(ctx, incoming); err != nil {
		return "", fmt.Errorf("save pack level: %w", err)
	}
	return PackLevelApplied, nil
}

// Retired applies a ProductPackLevelRetired. A retirement of a GTIN that is
// not the stored one (it was superseded already) or of a level never seen is
// PackLevelIgnored; one whose version is not greater than the stored one is
// PackLevelStale. Neither changes the view.
func (a ApplyPackLevel) Retired(ctx context.Context, c PackLevelRetiredChange) (PackLevelOutcome, error) {
	if c.GTIN == "" || c.Version < 1 {
		return "", fmt.Errorf("%w: gtin and a version of at least 1 are required", ErrInvalidPackLevelEvent)
	}
	// The domain constructor validates sku and level, so the same rules
	// apply as to a declaration.
	probe, err := packlevel.New(c.SKU, c.Level, c.GTIN, 1, c.EffectiveTo, c.Version)
	if err != nil {
		return "", fmt.Errorf("%w: %w", ErrInvalidPackLevelEvent, err)
	}
	cur, found, err := a.PackLevels.Get(ctx, probe.SKU, probe.Level)
	if err != nil {
		return "", fmt.Errorf("read pack level: %w", err)
	}
	if !found {
		return PackLevelIgnored, nil
	}
	retired, ok := cur.Retire(c.GTIN, c.EffectiveTo, c.Version)
	if !ok {
		if c.GTIN == cur.GTIN {
			return PackLevelStale, nil
		}
		return PackLevelIgnored, nil
	}
	if err := a.PackLevels.Save(ctx, retired); err != nil {
		return "", fmt.Errorf("save pack level: %w", err)
	}
	return PackLevelApplied, nil
}

// ActiveCaseMultiples reads the CASE level in force at `at` of every SKU from
// the local view and returns the planner's rule table: only SKUs with an
// active CASE level appear (a level never declared, not yet effective or
// end-dated is absent, so that SKU is planned in eaches, unchanged). A nil
// view returns an empty table. Duplicate SKUs are read once.
func ActiveCaseMultiples(ctx context.Context, view ports.PackLevelView, skus []string, at time.Time) (transfer.CaseMultiples, error) {
	multiples := transfer.CaseMultiples{}
	if view == nil {
		return multiples, nil
	}
	for _, sku := range skus {
		if _, seen := multiples[transfer.SKU(sku)]; seen {
			continue
		}
		level, found, err := view.Get(ctx, sku, packlevel.Case)
		if err != nil {
			return nil, fmt.Errorf("read pack level of sku %s: %w", sku, err)
		}
		if found && level.ActiveAt(at) {
			multiples[transfer.SKU(sku)] = int(level.UnitsPerPack)
		}
	}
	return multiples, nil
}
