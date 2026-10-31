//go:build integration

package postgres_integration

import (
	"context"
	"testing"

	"github.com/claudioed/network-inventory-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/network-inventory-planning/internal/application/ports"
	"github.com/claudioed/network-inventory-planning/internal/testing/viewcontract"
)

// TestPackLevelViewContract proves migration 0006 and the Postgres
// PackLevelView against a REAL Postgres, with the SAME contract the
// in-memory double passes.
func TestPackLevelViewContract(t *testing.T) {
	databaseURL := startPostgres(t)
	if err := postgres.RunMigrations(databaseURL, rebalanceMigrationsDir(t)); err != nil {
		t.Fatalf("run migrations (0006 must create pack_levels): %v", err)
	}
	ctx := context.Background()
	pool, err := postgres.NewPool(ctx, databaseURL)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	defer pool.Close()

	viewcontract.PackLevelView(t, func(t *testing.T) ports.PackLevelView {
		if _, err := pool.Exec(ctx, `TRUNCATE pack_levels`); err != nil {
			t.Fatalf("reset pack_levels: %v", err)
		}
		return postgres.NewPackLevelView(pool)
	})
}
