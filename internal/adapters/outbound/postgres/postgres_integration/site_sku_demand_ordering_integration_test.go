//go:build integration

package postgres_integration

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/network-inventory-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/network-inventory-planning/internal/domain/planning"
)

// demandAt builds one ACTIVE demand fact for ord-1 line 1 applied at asOf.
func demandAt(t *testing.T, units int, state planning.DemandState, asOf time.Time) planning.SiteSkuDemand {
	t.Helper()
	d, err := planning.NewSiteSkuDemand("ord-1", 1, "WH1", "SKU-1", units, asOf.Add(4*time.Hour), state, "static-site-v1")
	if err != nil {
		t.Fatalf("demand: %v", err)
	}
	d.AsOf = asOf
	return d
}

// TestSiteSkuDemandUpsertIgnoresOlderEvents is D5: the site_sku_demand
// upsert only applies an event whose occurred time (the CloudEvents time,
// stored in as_of) is not older than the stored row's. A redelivered OLDER
// event — a re-published fact with a fresh CloudEvents id the
// processed_events claim cannot dedupe, or a replay after pruning — is a
// no-op instead of overwriting newer demand.
func TestSiteSkuDemandUpsertIgnoresOlderEvents(t *testing.T) {
	databaseURL := startPostgres(t)
	if err := postgres.RunMigrations(databaseURL, rebalanceMigrationsDir(t)); err != nil {
		t.Fatalf("run migrations: %v", err)
	}
	ctx := context.Background()
	pool, err := postgres.NewPool(ctx, databaseURL)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	defer pool.Close()
	repo := postgres.NewSiteSkuDemandRepo(pool)

	t0 := time.Date(2026, 10, 12, 9, 0, 0, 0, time.UTC)
	newer := demandAt(t, 40, planning.DemandActive, t0.Add(time.Minute))
	older := demandAt(t, 10, planning.DemandActive, t0)

	applied, err := repo.Upsert(ctx, newer)
	if err != nil || !applied {
		t.Fatalf("first upsert: applied=%v err=%v", applied, err)
	}
	applied, err = repo.Upsert(ctx, older)
	if err != nil {
		t.Fatalf("older upsert: %v", err)
	}
	if applied {
		t.Fatal("an OLDER event must not be applied over newer demand")
	}
	assertDemandRow(ctx, t, pool, 40, "ACTIVE", newer.AsOf)

	// A REMOVED tombstone that is older than the stored ACTIVE row must not
	// resurrect or erase it either.
	if applied, err := repo.Upsert(ctx, demandAt(t, 0, planning.DemandRemoved, t0)); err != nil || applied {
		t.Fatalf("older tombstone: applied=%v err=%v", applied, err)
	}
	assertDemandRow(ctx, t, pool, 40, "ACTIVE", newer.AsOf)

	// A NEWER event applies; an event at the SAME instant applies too
	// (partition order is the tie-break: Kafka delivers one key in order).
	newest := demandAt(t, 0, planning.DemandRemoved, t0.Add(2*time.Minute))
	if applied, err := repo.Upsert(ctx, newest); err != nil || !applied {
		t.Fatalf("newer tombstone: applied=%v err=%v", applied, err)
	}
	assertDemandRow(ctx, t, pool, 0, "REMOVED", newest.AsOf)
	sameInstant := demandAt(t, 7, planning.DemandActive, newest.AsOf)
	if applied, err := repo.Upsert(ctx, sameInstant); err != nil || !applied {
		t.Fatalf("same-instant upsert: applied=%v err=%v", applied, err)
	}
	assertDemandRow(ctx, t, pool, 7, "ACTIVE", newest.AsOf)
}

func assertDemandRow(ctx context.Context, t *testing.T, pool *pgxpool.Pool, units int, state string, asOf time.Time) {
	t.Helper()
	var gotUnits int
	var gotState string
	var gotAsOf time.Time
	if err := pool.QueryRow(ctx,
		`SELECT demanded_units, state, as_of FROM site_sku_demand WHERE source_order_id = 'ord-1' AND line_no = 1`,
	).Scan(&gotUnits, &gotState, &gotAsOf); err != nil {
		t.Fatalf("read demand row: %v", err)
	}
	if gotUnits != units || gotState != state || !gotAsOf.Equal(asOf) {
		t.Fatalf("demand row = units %d state %s as_of %s, want %d %s %s", gotUnits, gotState, gotAsOf, units, state, asOf)
	}
}
