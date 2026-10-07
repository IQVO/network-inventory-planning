//go:build integration

package postgres_test

import (
	"context"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/claudioed/network-inventory-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/network-inventory-planning/internal/application/usecases"
	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

var base = time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)

// startPostgres runs a throwaway Postgres via testcontainers and returns a
// migrated pool (never a skip-gated external database).
func startPostgres(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()
	container, err := tcpostgres.Run(ctx, "postgres:16-alpine",
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
	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("resolve connection string: %v", err)
	}
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate test source file")
	}
	if err := postgres.RunMigrations(dsn, filepath.Join(filepath.Dir(thisFile), "migrations")); err != nil {
		t.Fatalf("run migrations: %v", err)
	}
	pool, err := postgres.NewPool(ctx, dsn)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// seedSpec describes one transfer to persist.
type seedSpec struct {
	id          string
	origin      string
	destination string
	createdAt   time.Time
	movedAt     time.Time
	target      transfer.TransferState
}

// seed drives a transfer through the REAL domain transitions to the target
// state and persists it with the write repository, so the rows (and audit
// trails) are exactly what the saga would have produced. The last
// transition happens at movedAt. ALLOCATED is reached through Create +
// UpdateState (the production reply path); states that carry no reservation
// are persisted by Create alone, which writes the state and the full audit
// trail as it stands.
func seed(t *testing.T, repo *postgres.TransferRepo, s seedSpec) {
	t.Helper()
	ctx := context.Background()
	tr, err := transfer.ProposeTransfer(transfer.ProposalInput{
		ID: transfer.TransferID(s.id), IdempotencyKey: "key-" + s.id, OriginSiteID: s.origin, DestinationSiteID: s.destination,
		SKU: "SKU-1", Quantity: 10, PolicyVersion: "p1", OperatorReason: "rebalance " + s.id,
		ProposalAsOf: s.createdAt, ExpiresAt: s.createdAt.Add(24 * time.Hour), Now: s.createdAt,
	})
	if err != nil {
		t.Fatalf("propose %s: %v", s.id, err)
	}
	if s.target == transfer.StateCancelled {
		if err := tr.Cancel("operator withdrew", s.movedAt); err != nil {
			t.Fatalf("cancel %s: %v", s.id, err)
		}
		create(t, repo, tr, s.id)
		return
	}
	if _, err := tr.Approve(s.createdAt.Add(time.Minute)); err != nil {
		t.Fatalf("approve %s: %v", s.id, err)
	}
	if _, err := tr.RequestAllocation(s.movedAt); err != nil {
		t.Fatalf("request allocation %s: %v", s.id, err)
	}
	switch s.target {
	case transfer.StateAllocating:
		create(t, repo, tr, s.id)
	case transfer.StateUnfulfillable:
		if err := tr.MarkUnfulfillable(tr.ID().LineID(), transfer.RejectionInsufficientUsable, s.movedAt); err != nil {
			t.Fatalf("reject %s: %v", s.id, err)
		}
		create(t, repo, tr, s.id)
	case transfer.StateAllocated:
		create(t, repo, tr, s.id)
		if err := tr.MarkAllocated(transfer.StockAllocation{
			TransferLineID: tr.ID().LineID(), OriginSiteID: s.origin, SKU: "SKU-1", ReservationID: "res-" + s.id, Quantity: 10,
			Allocations: []transfer.Allocation{{StockUnitID: "su-1", BinID: "bin-1", Quantity: 10}},
			ExpiresAt:   s.movedAt.Add(time.Hour),
		}, s.movedAt); err != nil {
			t.Fatalf("allocate %s: %v", s.id, err)
		}
		if err := repo.UpdateState(ctx, tr); err != nil {
			t.Fatalf("update %s: %v", s.id, err)
		}
	default:
		t.Fatalf("seed: unsupported target %s", s.target)
	}
}

func create(t *testing.T, repo *postgres.TransferRepo, tr *transfer.InterWarehouseTransfer, id string) {
	t.Helper()
	if existing, err := repo.Create(context.Background(), tr, "key-"+id); err != nil || existing != nil {
		t.Fatalf("create %s: %v (existing %v)", id, err, existing)
	}
}

func seedAll(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	repo := postgres.NewTransferRepo(pool)
	for _, s := range []seedSpec{
		{"tr-a", "WH1", "WH2", base.Add(-5 * time.Hour), base.Add(-4 * time.Hour), transfer.StateAllocating},
		{"tr-b", "WH1", "WH3", base.Add(-3 * time.Hour), base.Add(-10 * time.Minute), transfer.StateAllocating},
		{"tr-c", "WH2", "WH1", base.Add(-2 * time.Hour), base.Add(-90 * time.Minute), transfer.StateAllocated},
		{"tr-d", "WH1", "WH2", base.Add(-1 * time.Hour), base.Add(-50 * time.Minute), transfer.StateUnfulfillable},
		{"tr-e", "WH3", "WH2", base.Add(-30 * time.Minute), base.Add(-20 * time.Minute), transfer.StateCancelled},
	} {
		seed(t, repo, s)
	}
}

func ids(page transfer.TransferPage) []string {
	out := make([]string, 0, len(page.Items))
	for _, t := range page.Items {
		out = append(out, string(t.ID()))
	}
	return out
}

func assertIDs(t *testing.T, label string, page transfer.TransferPage, want ...string) {
	t.Helper()
	got := ids(page)
	if len(got) != len(want) {
		t.Fatalf("%s: got %v, want %v", label, got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("%s: got %v, want %v", label, got, want)
		}
	}
}

func TestTransferQueryGet(t *testing.T) {
	pool := startPostgres(t)
	seedAll(t, pool)
	query := postgres.NewTransferQueryRepo(pool)
	ctx := context.Background()

	got, err := query.Get(ctx, "tr-c")
	if err != nil {
		t.Fatalf("Get tr-c: %v", err)
	}
	if got.State() != transfer.StateAllocated || got.ReservationID() != "res-tr-c" || got.OriginSiteID() != "WH2" || got.DestinationSiteID() != "WH1" {
		t.Fatalf("tr-c = state %s reservation %q %s->%s", got.State(), got.ReservationID(), got.OriginSiteID(), got.DestinationSiteID())
	}
	if !got.UpdatedAt().Equal(base.Add(-90 * time.Minute)) {
		t.Fatalf("UpdatedAt = %s", got.UpdatedAt())
	}

	// The audit trail comes back complete and in seq order.
	audit := got.Audit()
	wantTo := []transfer.TransferState{transfer.StateDraft, transfer.StateProposed, transfer.StateApproved, transfer.StateAllocating, transfer.StateAllocated}
	if len(audit) != len(wantTo) {
		t.Fatalf("audit = %+v, want %d entries", audit, len(wantTo))
	}
	for i, e := range audit {
		if e.To != wantTo[i] || e.Seq != int64(i+1) {
			t.Fatalf("audit[%d] = %+v, want seq %d to %s", i, e, i+1, wantTo[i])
		}
	}
	if last := audit[len(audit)-1]; last.From != transfer.StateAllocating || last.Reason != "reservation res-tr-c" || !last.OccurredAt.Equal(base.Add(-90*time.Minute)) {
		t.Fatalf("last audit entry = %+v", last)
	}

	if _, err := query.Get(ctx, "no-such-transfer"); err != transfer.ErrTransferNotFound {
		t.Fatalf("Get unknown = %v, want ErrTransferNotFound", err)
	}
}

func TestTransferQueryListOrderingFiltersAndPaging(t *testing.T) {
	pool := startPostgres(t)
	seedAll(t, pool)
	query := postgres.NewTransferQueryRepo(pool)
	ctx := context.Background()

	list := func(label string, f transfer.ListFilter, want ...string) transfer.TransferPage {
		t.Helper()
		page, err := query.List(ctx, f)
		if err != nil {
			t.Fatalf("%s: %v", label, err)
		}
		assertIDs(t, label, page, want...)
		return page
	}

	// Default order: newest created first.
	page := list("unfiltered", transfer.ListFilter{}, "tr-e", "tr-d", "tr-c", "tr-b", "tr-a")
	if page.Total != 5 {
		t.Fatalf("unfiltered total = %d, want 5", page.Total)
	}
	// Every listed transfer carries its audit trail.
	for _, tr := range page.Items {
		if len(tr.Audit()) < 2 {
			t.Fatalf("%s listed without its audit trail: %+v", tr.ID(), tr.Audit())
		}
	}

	list("state", transfer.ListFilter{States: []transfer.TransferState{transfer.StateAllocating}}, "tr-b", "tr-a")
	list("multi-state", transfer.ListFilter{States: []transfer.TransferState{transfer.StateAllocated, transfer.StateCancelled}}, "tr-e", "tr-c")
	list("origin", transfer.ListFilter{OriginSiteID: "WH1"}, "tr-d", "tr-b", "tr-a")
	list("destination", transfer.ListFilter{DestinationSiteID: "WH2"}, "tr-e", "tr-d", "tr-a")
	list("origin+destination", transfer.ListFilter{OriginSiteID: "WH1", DestinationSiteID: "WH2"}, "tr-d", "tr-a")
	list("site is origin OR destination", transfer.ListFilter{SiteID: "WH3"}, "tr-e", "tr-b")
	list("site AND state", transfer.ListFilter{SiteID: "WH3", States: []transfer.TransferState{transfer.StateAllocating}}, "tr-b")
	list("no match", transfer.ListFilter{OriginSiteID: "WH9"})

	// Paging: limit/offset over the stable order, total unaffected by paging.
	page = list("page 1", transfer.ListFilter{Limit: 2}, "tr-e", "tr-d")
	if page.Total != 5 {
		t.Fatalf("page total = %d, want 5", page.Total)
	}
	list("page 2", transfer.ListFilter{Limit: 2, Offset: 2}, "tr-c", "tr-b")
	list("last page", transfer.ListFilter{Limit: 2, Offset: 4}, "tr-a")
	list("past the end", transfer.ListFilter{Limit: 2, Offset: 10})

	// Stalest-first orders by last transition, oldest first.
	list("stalest first", transfer.ListFilter{Order: transfer.OrderStalestFirst}, "tr-a", "tr-c", "tr-d", "tr-e", "tr-b")
}

func TestFindStuckTransfersAgainstPostgres(t *testing.T) {
	pool := startPostgres(t)
	seedAll(t, pool)
	query := postgres.NewTransferQueryRepo(pool)
	ctx := context.Background()

	stuck := usecases.FindStuckTransfers{Query: query, Now: func() time.Time { return base }}

	// Older than 1h: tr-a (4h) and tr-c (90m) qualify, stalest first. tr-b
	// moved 10 minutes ago; tr-d/tr-e are terminal however old.
	page, err := stuck.Execute(ctx, usecases.FindStuckInput{OlderThan: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	assertIDs(t, "stuck 1h", page, "tr-a", "tr-c")
	if page.Total != 2 {
		t.Fatalf("stuck total = %d, want 2", page.Total)
	}

	// The threshold is strict: a transfer last moved EXACTLY threshold ago
	// is not yet stuck.
	page, err = stuck.Execute(ctx, usecases.FindStuckInput{OlderThan: 4 * time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	assertIDs(t, "stuck exactly 4h", page)

	page, err = stuck.Execute(ctx, usecases.FindStuckInput{OlderThan: 4*time.Hour - time.Second})
	if err != nil {
		t.Fatal(err)
	}
	assertIDs(t, "stuck just under 4h", page, "tr-a")

	// Narrowed to one state.
	page, err = stuck.Execute(ctx, usecases.FindStuckInput{OlderThan: time.Minute, State: "ALLOCATED"})
	if err != nil {
		t.Fatal(err)
	}
	assertIDs(t, "stuck ALLOCATED", page, "tr-c")

	// A short threshold pulls in tr-b but still never a terminal transfer.
	page, err = stuck.Execute(ctx, usecases.FindStuckInput{OlderThan: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	assertIDs(t, "stuck 1m", page, "tr-a", "tr-c", "tr-b")
}
