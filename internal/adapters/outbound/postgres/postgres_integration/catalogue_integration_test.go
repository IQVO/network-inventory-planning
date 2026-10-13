//go:build integration

package postgres_integration

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/claudioed/network-inventory-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/network-inventory-planning/internal/application/usecases"
	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

// TestRebalanceCatalogueRoundTripAndConcurrency is D6 against a REAL
// Postgres (migration 0005): create, idempotent replay, revise with
// If-Match, stale If-Match, ordered listing, and a real two-goroutine race
// in which exactly one writer wins and the other gets
// ErrCatalogueConcurrentModification.
func TestRebalanceCatalogueRoundTripAndConcurrency(t *testing.T) {
	databaseURL := startPostgres(t)
	if err := postgres.RunMigrations(databaseURL, rebalanceMigrationsDir(t)); err != nil {
		t.Fatalf("run migrations (0005 must create the catalogue tables): %v", err)
	}
	ctx := context.Background()
	pool, err := postgres.NewPool(ctx, databaseURL)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	defer pool.Close()

	now := time.Date(2026, 10, 12, 9, 0, 0, 0, time.UTC)
	policies := postgres.NewRebalancePolicyRepo(pool)
	lanes := postgres.NewTransferLaneRepo(pool)
	putPolicy := usecases.PutRebalancePolicy{Policies: policies, Now: func() time.Time { return now }}
	putLane := usecases.PutTransferLane{Lanes: lanes, Now: func() time.Time { return now }}

	p := transfer.Policy{Version: "pv-1", Site: "WH2", SKU: "SKU-1", SafetyStock: 5, TargetStock: 50, UnitPriority: 10}
	created, err := putPolicy.Execute(ctx, p, nil)
	if err != nil || created.Outcome != usecases.CatalogueCreated {
		t.Fatalf("create policy = %+v err=%v", created, err)
	}
	if replay, err := putPolicy.Execute(ctx, p, nil); err != nil || replay.Outcome != usecases.CatalogueUnchanged {
		t.Fatalf("replay = %+v err=%v", replay, err)
	}
	p.TargetStock = 80
	one := int64(1)
	if updated, err := putPolicy.Execute(ctx, p, &one); err != nil || updated.Entry.Revision != 2 {
		t.Fatalf("update = %+v err=%v", updated, err)
	}
	if _, err := putPolicy.Execute(ctx, p, &one); !errors.Is(err, usecases.ErrCatalogueVersionMismatch) {
		t.Fatalf("stale If-Match: %v", err)
	}
	other := transfer.Policy{Version: "pv-1", Site: "WH1", SKU: "SKU-1", SafetyStock: 0, TargetStock: 10, UnitPriority: 1}
	if _, err := putPolicy.Execute(ctx, other, nil); err != nil {
		t.Fatalf("second policy: %v", err)
	}
	list, err := policies.List(ctx)
	if err != nil || len(list) != 2 || list[0].Policy.Site != "WH1" || list[1].Policy.TargetStock != 80 || list[1].Revision != 2 {
		t.Fatalf("list = %+v err=%v", list, err)
	}
	if !list[1].UpdatedAt.Equal(now) {
		t.Fatalf("updated_at = %s", list[1].UpdatedAt)
	}

	l := transfer.Lane{Origin: "WH1", Destination: "WH2", LeadTime: 36 * time.Hour, UnitHandlingCost: 2, Enabled: true}
	if res, err := putLane.Execute(ctx, l, nil); err != nil || res.Outcome != usecases.CatalogueCreated {
		t.Fatalf("create lane = %+v err=%v", res, err)
	}
	got, found, err := lanes.Get(ctx, "WH1", "WH2")
	if err != nil || !found || got.Lane != l {
		t.Fatalf("lane round trip = %+v found=%v err=%v", got, found, err)
	}
	if _, found, err := lanes.Get(ctx, "WH2", "WH1"); err != nil || found {
		t.Fatalf("lanes are directed: found=%v err=%v", found, err)
	}

	// Two concurrent revisions of the SAME stored revision: exactly one
	// guarded UPDATE matches.
	stored, _, err := policies.Get(ctx, "WH2", "SKU-1")
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			next := stored.Policy
			next.UnitPriority = 20 + i
			revised, _, rerr := stored.Revise(next, now.Add(time.Minute))
			if rerr != nil {
				errs[i] = rerr
				return
			}
			errs[i] = policies.Save(ctx, revised, stored.Revision)
		}(i)
	}
	wg.Wait()
	wins, conflicts := 0, 0
	for _, e := range errs {
		switch {
		case e == nil:
			wins++
		case errors.Is(e, transfer.ErrCatalogueConcurrentModification):
			conflicts++
		default:
			t.Fatalf("unexpected error: %v", e)
		}
	}
	if wins != 1 || conflicts != 1 {
		t.Fatalf("wins=%d conflicts=%d, want exactly one of each", wins, conflicts)
	}
	// A concurrent create of an existing key is a conflict too.
	if err := policies.Save(ctx, stored, 0); !errors.Is(err, transfer.ErrCatalogueConcurrentModification) {
		t.Fatalf("duplicate create: %v", err)
	}
}
