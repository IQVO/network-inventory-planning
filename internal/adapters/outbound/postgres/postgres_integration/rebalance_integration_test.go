//go:build integration

// Package postgres_integration proves the Phase-4 persistence against a
// REAL Postgres (testcontainers): migration 0004 creates rebalance_runs,
// Record persists both outcome shapes, List returns newest-first, and
// TransferRepo.ListNonTerminal returns exactly the non-terminal states
// (ADR 0007).
package postgres_integration

import (
	"context"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/claudioed/network-inventory-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/network-inventory-planning/internal/application/ports"
	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

// startPostgres starts a REAL Postgres 16 container (the fitness tests
// require testcontainers for any infrastructure a test touches).
func startPostgres(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	container, err := tcpostgres.Run(ctx,
		"postgres:16-alpine",
		tcpostgres.WithDatabase("nip_test"),
		tcpostgres.WithUsername("nip_test"),
		tcpostgres.WithPassword("nip_test"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(60*time.Second),
		),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	t.Cleanup(func() {
		if err := container.Terminate(context.Background()); err != nil {
			t.Logf("terminate postgres container: %v", err)
		}
	})
	connStr, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("resolve connection string: %v", err)
	}
	return connStr
}

// rebalanceMigrationsDir resolves the migrations directory relative to
// this file.
func rebalanceMigrationsDir(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate test source file")
	}
	return filepath.Join(filepath.Dir(thisFile), "..", "migrations")
}

func TestRebalanceRunRepoRoundTrip(t *testing.T) {
	databaseURL := startPostgres(t)
	if err := postgres.RunMigrations(databaseURL, rebalanceMigrationsDir(t)); err != nil {
		t.Fatalf("run migrations (0004 must create rebalance_runs): %v", err)
	}
	ctx := context.Background()
	pool, err := postgres.NewPool(ctx, databaseURL)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	defer pool.Close()

	var _ ports.RebalanceRunRepository = postgres.NewRebalanceRunRepo(pool)
	runs := postgres.NewRebalanceRunRepo(pool)

	// A FAILED run first (oldest): no watermark, a fail-closed reason.
	reason := "planning snapshot: no site capability facts; refusing to plan from an empty read model"
	failedAt := time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)
	id1, err := runs.Record(ctx, transfer.RebalanceRun{
		StartedAt:        failedAt,
		Outcome:          transfer.RebalanceFailed,
		FailClosedReason: &reason,
	})
	if err != nil {
		t.Fatalf("record failed run: %v", err)
	}
	// Then a COMPLETED run (newest).
	doneAt := time.Date(2026, 10, 7, 13, 0, 0, 0, time.UTC)
	id2, err := runs.Record(ctx, transfer.RebalanceRun{
		StartedAt:     doneAt,
		SnapshotAsOf:  doneAt.Add(-time.Minute),
		ProposalCount: 3,
		RejectedCount: 1,
		Outcome:       transfer.RebalanceCompleted,
	})
	if err != nil {
		t.Fatalf("record completed run: %v", err)
	}
	if id1 == id2 {
		t.Fatal("the two runs must have distinct ids")
	}

	listed, err := runs.List(ctx, 10)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(listed) != 2 {
		t.Fatalf("listed = %d runs, want 2", len(listed))
	}
	if listed[0].ID != id2 || listed[0].Outcome != transfer.RebalanceCompleted {
		t.Fatalf("newest = %+v, want the COMPLETED run %d first", listed[0], id2)
	}
	if listed[0].ProposalCount != 3 || listed[0].SnapshotAsOf.IsZero() {
		t.Fatalf("newest = %+v, want 3 proposals with a watermark", listed[0])
	}
	if listed[1].ID != id1 || listed[1].Outcome != transfer.RebalanceFailed {
		t.Fatalf("oldest = %+v, want the FAILED run %d", listed[1], id1)
	}
	if listed[1].FailClosedReason == nil || *listed[1].FailClosedReason != reason {
		t.Fatalf("oldest = %+v, want the fail-closed reason round-tripped", listed[1])
	}

	// The limit bounds the page.
	limited, err := runs.List(ctx, 1)
	if err != nil || len(limited) != 1 || limited[0].ID != id2 {
		t.Fatalf("limit 1 = %+v %v, want only the newest", limited, err)
	}
}

func TestListNonTerminalExcludesTerminalStates(t *testing.T) {
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

	seed := func(t *testing.T, id string, state transfer.TransferState, updatedAt time.Time) {
		t.Helper()
		// picked_quantity satisfies migration 0003's CHECK: NULL outside
		// the PICKED..RECEIVED tail, positive within it.
		var pickedQty any
		if state.NonTerminalTail() {
			pickedQty = 5
		}
		_, err := pool.Exec(ctx, `
			INSERT INTO inter_warehouse_transfer
				(transfer_id, idempotency_key, origin_site_id, destination_site_id, sku, quantity,
				 policy_version, operator_reason, proposal_as_of, expires_at, state,
				 picked_quantity, created_at, updated_at, version)
			VALUES ($1, $2, 'WH1', 'WH2', 'SKU-1', 5, 'v1', 'seed', $3, $7, $5, $6, $3, $4, 1)
			ON CONFLICT (idempotency_key) DO NOTHING
		`, id, "idem-"+id, updatedAt.Add(-time.Hour), updatedAt, string(state), pickedQty, updatedAt.Add(23*time.Hour))
		if err != nil {
			t.Fatalf("seed %s: %v", id, err)
		}
	}
	now := time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)
	seed(t, "trf-live", transfer.StateAllocating, now.Add(-2*time.Hour))
	seed(t, "trf-received", transfer.StateReceived, now.Add(-100*time.Hour))
	seed(t, "trf-cancelled", transfer.StateCancelled, now.Add(-100*time.Hour))

	var _ ports.StuckTransferReader = postgres.NewTransferRepo(pool)
	views, err := postgres.NewTransferRepo(pool).ListNonTerminal(ctx, 100)
	if err != nil {
		t.Fatalf("list non-terminal: %v", err)
	}
	if len(views) != 1 {
		t.Fatalf("views = %+v, want exactly the one non-terminal transfer", views)
	}
	if views[0].ID != transfer.TransferID("trf-live") || views[0].State != transfer.StateAllocating {
		t.Fatalf("view = %+v, want trf-live ALLOCATING", views[0])
	}
	// pgx scans timestamptz in the local zone; compare instants.
	if !views[0].UpdatedAt.UTC().Equal(now.Add(-2 * time.Hour).UTC()) {
		t.Fatalf("updated_at = %v, want the seeded value", views[0].UpdatedAt.UTC())
	}
}
