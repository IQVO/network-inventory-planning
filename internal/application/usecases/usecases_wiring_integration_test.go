//go:build integration

// Package usecases_test proves the transfer saga's main WRITE use cases
// against a REAL Postgres (testcontainers): the real TransferRepo, the
// real SnapshotRepo, the real UnitOfWork, and a buffering event publisher
// that records every published domain event, wired exactly like the
// composition root in cmd/network-inventory-planning. These are
// integration tests in the fleet's sense: they execute the real
// cross-component contracts (approve idempotency, the atomic
// Publish-inside-UoW bracket, the ALLOCATING→ALLOCATED transition with
// its pick-demand release) against real infrastructure, with no
// in-memory repo fakes anywhere in the path.
//
// The package boots its own throwaway Postgres via testcontainers in
// TestMain: one container for the whole package, migrated once, one
// private database per test. Never an external DATABASE_URL, never
// t.Skip.
package usecases_test

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/claudioed/network-inventory-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/network-inventory-planning/internal/application/usecases"
	"github.com/claudioed/network-inventory-planning/internal/domain/planning"
	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

// One Postgres container serves the whole package (containers are slow to
// boot). TestMain starts it, applies every migration ONCE into a template
// database, and each test then gets its own database cloned from that
// template (CREATE DATABASE ... TEMPLATE, a file-level copy: milliseconds).
// Isolation is therefore total — no TRUNCATE bookkeeping, no dependence on
// test order, and tests that assert on global state still start pristine.
//
// Never an external DATABASE_URL, never t.Skip.
const usecasesTemplateDB = "usecases_migrated_template"

var (
	usecasesBaseURL string // connection URL of the container's default database
	usecasesDBSeq   atomic.Uint64
)

func TestMain(m *testing.M) {
	os.Exit(usecasesRunTests(m))
}

func usecasesRunTests(m *testing.M) int {
	ctx := context.Background()
	container, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("nip_usecases"),
		tcpostgres.WithUsername("nip_test"),
		tcpostgres.WithPassword("nip_test"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(90*time.Second),
		),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "start postgres container: %v\n", err)
		return 1
	}
	defer func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			fmt.Fprintf(os.Stderr, "terminate postgres container: %v\n", err)
		}
	}()

	usecasesBaseURL, err = container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		fmt.Fprintf(os.Stderr, "postgres connection string: %v\n", err)
		return 1
	}

	// Migrate a template database once; every test clones it.
	if err := usecasesCreateDatabase(ctx, usecasesTemplateDB); err != nil {
		fmt.Fprintf(os.Stderr, "create template database: %v\n", err)
		return 1
	}
	if err := postgres.RunMigrations(usecasesWithDB(usecasesBaseURL, usecasesTemplateDB), usecasesMigrationsDir()); err != nil {
		fmt.Fprintf(os.Stderr, "migrate template: %v\n", err)
		return 1
	}

	return m.Run()
}

// usecasesMigrationsDir resolves the repo's migrations directory relative
// to THIS FILE (go test's working directory varies by runner).
func usecasesMigrationsDir() string {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		panic("cannot locate test source file")
	}
	return filepath.Join(filepath.Dir(thisFile), "..", "..", "adapters", "outbound", "postgres", "migrations")
}

// usecasesWithDB rewrites the path of a connection URL to the named database.
func usecasesWithDB(baseURL, name string) string {
	u, err := url.Parse(baseURL)
	if err != nil {
		panic(err)
	}
	u.Path = "/" + name
	return u.String()
}

// usecasesCreateDatabase creates an empty database inside the shared container.
func usecasesCreateDatabase(ctx context.Context, name string) error {
	conn, err := pgx.Connect(ctx, usecasesBaseURL)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, fmt.Sprintf("CREATE DATABASE %q", name)); err != nil {
		return fmt.Errorf("create database %s: %w", name, err)
	}
	return nil
}

// usecasesMigratedDB hands the test a connection URL to its own private
// database, cloned from the migrated template. Cloning is a file-level
// copy, so it costs milliseconds and the test's writes never leak into
// another test.
func usecasesMigratedDB(t *testing.T) string {
	t.Helper()
	name := fmt.Sprintf("usecases_%d", usecasesDBSeq.Add(1))
	conn, err := pgx.Connect(context.Background(), usecasesBaseURL)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(context.Background())
	if _, err := conn.Exec(context.Background(), fmt.Sprintf(
		"CREATE DATABASE %q WITH TEMPLATE %q", name, usecasesTemplateDB)); err != nil {
		t.Fatalf("clone database: %v", err)
	}
	return usecasesWithDB(usecasesBaseURL, name)
}

// usecasesNow is the deterministic approval clock: fixed timestamps keep
// the published events and the persisted rows comparable.
var usecasesNow = time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)

// usecasesReleaseConfig is a fully-configured WorkReleaseConfig (production
// reads the TRANSFER_* env vars at the composition root).
func usecasesReleaseConfig() usecases.WorkReleaseConfig {
	return usecases.WorkReleaseConfig{
		PickPathID:        "transfer-pick-path",
		PickCPTOffset:     2 * time.Hour,
		DispatchPathID:    "transfer-dispatch-path",
		DispatchCPTOffset: 3 * time.Hour,
	}
}

// usecasesFacts seeds the three read models so the approval's fail-closed
// validation passes: WH1 (origin, capacity 500, demand 40) → WH2
// (destination, in-window SKU-1 demand 10), all fresh as of the fixed now.
func usecasesFacts() planning.Facts {
	due := usecasesNow.Add(2 * time.Hour)
	cap1, _ := planning.NewSiteCapability("WH1", true, true, 3, usecasesNow.Add(-time.Minute))
	cap2, _ := planning.NewSiteCapability("WH2", true, true, 3, usecasesNow.Add(-time.Minute))
	d1, _ := planning.NewSiteSkuDemand("ord-1", 1, "WH1", "SKU-1", 40, due, planning.DemandActive, "static-site-v1")
	d1.AsOf = usecasesNow.Add(-time.Minute)
	d2, _ := planning.NewSiteSkuDemand("ord-2", 1, "WH2", "SKU-1", 10, due, planning.DemandActive, "static-site-v1")
	d2.AsOf = usecasesNow.Add(-time.Minute)
	plan1, _ := planning.NewPublishedCapacityPlan(planning.PublishedCapacityPlan{
		PlanID: "plan-1", SiteID: "WH1", Location: "PATH-ZONE-A", PathID: "pick-rebin-pack",
		WindowStart: usecasesNow.Add(time.Hour), WindowEnd: usecasesNow.Add(8 * time.Hour),
		AssignedDemand: 100, CapacityOverWindow: 500, PublishedAt: usecasesNow.Add(-time.Minute),
	})
	plan2, _ := planning.NewPublishedCapacityPlan(planning.PublishedCapacityPlan{
		PlanID: "plan-2", SiteID: "WH2", Location: "PATH-ZONE-B", PathID: "pick-rebin-pack",
		WindowStart: usecasesNow.Add(time.Hour), WindowEnd: usecasesNow.Add(8 * time.Hour),
		AssignedDemand: 10, CapacityOverWindow: 300, PublishedAt: usecasesNow.Add(-time.Minute),
	})
	return planning.Facts{
		Capabilities: []planning.SiteCapability{cap1, cap2},
		Demands:      []planning.SiteSkuDemand{d1, d2},
		Plans:        []planning.PublishedCapacityPlan{plan1, plan2},
	}
}

// recordingPublisher buffers every domain event the use cases publish so
// the tests can assert the event stream (the same role wes-work-planning's
// buffering LogPublisher plays in its wiring suite).
type recordingPublisher struct {
	mu     sync.Mutex
	events []transfer.DomainEvent
}

func (p *recordingPublisher) Publish(_ context.Context, events ...transfer.DomainEvent) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = append(p.events, events...)
	return nil
}

func (p *recordingPublisher) names() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]string, 0, len(p.events))
	for _, e := range p.events {
		out = append(out, e.EventName())
	}
	return out
}

func (p *recordingPublisher) has(name string) bool {
	for _, got := range p.names() {
		if got == name {
			return true
		}
	}
	return false
}

// usecasesFixture is the real adapter stack over a private migrated
// database, wired exactly like cmd/network-inventory-planning's
// composition root for the approval flow.
type usecasesFixture struct {
	pool       *pgxpool.Pool
	approve    *usecases.ApproveTransfer
	allocate   *usecases.ApplyTransferAllocation
	publisher  *recordingPublisher
	transfers  *postgres.TransferRepo
	readTransf *usecases.GetTransfer
}

// newUsecasesFixture seeds the read models on a fresh private database and
// wires the real write use cases over it.
func newUsecasesFixture(t *testing.T) *usecasesFixture {
	t.Helper()
	ctx := context.Background()
	pool, err := postgres.NewPool(ctx, usecasesMigratedDB(t))
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(pool.Close)

	uow := postgres.NewUnitOfWork(pool)
	publisher := &recordingPublisher{}
	transfers := postgres.NewTransferRepo(pool)
	approve := &usecases.ApproveTransfer{
		Transfers:    transfers,
		Events:       publisher,
		Snapshot:     postgres.NewSnapshotRepo(pool),
		UoW:          uow,
		MaxStaleness: 10 * time.Minute,
		Release:      usecasesReleaseConfig(),
		Now:          func() time.Time { return usecasesNow },
	}
	allocate := &usecases.ApplyTransferAllocation{
		Transfers: transfers,
		Events:    publisher,
		UoW:       uow,
		Release:   usecasesReleaseConfig(),
		Now:       func() time.Time { return usecasesNow },
	}

	facts := usecasesFacts()
	if err := uow.Do(ctx, func(ctx context.Context) error {
		for _, c := range facts.Capabilities {
			if _, err := postgres.NewSiteCapabilityRepo(pool).Upsert(ctx, c); err != nil {
				return err
			}
		}
		for _, d := range facts.Demands {
			if _, err := postgres.NewSiteSkuDemandRepo(pool).Upsert(ctx, d); err != nil {
				return err
			}
		}
		for _, p := range facts.Plans {
			if _, err := postgres.NewPublishedCapacityPlanRepo(pool).Upsert(ctx, p); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("seed read models: %v", err)
	}

	return &usecasesFixture{
		pool:       pool,
		approve:    approve,
		allocate:   allocate,
		publisher:  publisher,
		transfers:  transfers,
		readTransf: &usecases.GetTransfer{Query: postgres.NewTransferQueryRepo(pool)},
	}
}

// usecasesApprovalInput is one valid WH1→WH2 approval request.
func usecasesApprovalInput(key string) usecases.ApproveTransferInput {
	return usecases.ApproveTransferInput{
		IdempotencyKey:    key,
		OriginSiteID:      "WH1",
		DestinationSiteID: "WH2",
		SKU:               "SKU-1",
		Quantity:          5,
		PolicyVersion:     "policy-v3",
		OperatorReason:    "operator approved rebalance",
		ProposalAsOf:      usecasesNow.Add(-5 * time.Minute),
	}
}

// TestUsecases_ApprovePersistsSagaAndPublishesEvents drives the MAIN write
// use case end-to-end: the approval persists the aggregate through
// ALLOCATING (state + full audit trail) and publishes the two integration
// events plus the four TransferStateAdvanced analytics occurrences —
// through the real UnitOfWork, against the real Postgres.
func TestUsecases_ApprovePersistsSagaAndPublishesEvents(t *testing.T) {
	f := newUsecasesFixture(t)
	ctx := context.Background()

	result, err := f.approve.Execute(ctx, usecasesApprovalInput("usecases-idem-1"))
	if err != nil {
		t.Fatalf("approve: %v", err)
	}
	if result.Replayed {
		t.Fatal("first approval must not be a replay")
	}
	if result.Transfer.State() != transfer.StateAllocating {
		t.Fatalf("state = %s, want ALLOCATING (awaiting inventory reply)", result.Transfer.State())
	}

	// Persisted state, rehydrated through the real repo.
	loaded, err := f.transfers.Load(ctx, result.TransferID)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if loaded.State() != transfer.StateAllocating || loaded.PolicyVersion() != "policy-v3" {
		t.Fatalf("loaded = state %s policy %s", loaded.State(), loaded.PolicyVersion())
	}
	if audit := loaded.Audit(); len(audit) != 4 {
		t.Fatalf("audit = %d entries, want 4 (drafted/proposed/approved/allocation-requested): %+v", len(audit), audit)
	}

	// Published events: TransferPlanApproved + TransferAllocationRequested
	// (the integration contract) and 4 TransferStateAdvanced analytics
	// occurrences of the creation transitions (ADR 0007).
	names := f.publisher.names()
	if len(names) != 6 {
		t.Fatalf("published events = %v, want 6 (2 integration + 4 analytics)", names)
	}
	if names[0] != "TransferPlanApproved" || names[1] != "TransferAllocationRequested" {
		t.Fatalf("integration events = %v, want TransferPlanApproved then TransferAllocationRequested first", names[:2])
	}
	approved, ok := f.publisher.events[0].(transfer.PlanApproved)
	if !ok || approved.TransferID != result.TransferID || approved.OperatorReason != "operator approved rebalance" {
		t.Fatalf("first event = %+v", f.publisher.events[0])
	}
	command, ok := f.publisher.events[1].(transfer.AllocationRequested)
	if !ok || command.TransferLineID != result.TransferID.LineID() || command.Quantity != 5 || command.OriginSiteID != "WH1" {
		t.Fatalf("second event = %+v", f.publisher.events[1])
	}
}

// TestUsecases_ApproveIsIdempotentAndConflictsOnKeyReuse proves the
// idempotency contract over the real UNIQUE idempotency_key constraint: a
// replay of the same key+payload returns the FIRST transfer with no second
// fan-out, and a DIFFERENT payload under the reused key is a conflict.
func TestUsecases_ApproveIsIdempotentAndConflictsOnKeyReuse(t *testing.T) {
	f := newUsecasesFixture(t)
	ctx := context.Background()

	first, err := f.approve.Execute(ctx, usecasesApprovalInput("usecases-idem-2"))
	if err != nil {
		t.Fatalf("first approve: %v", err)
	}

	replay, err := f.approve.Execute(ctx, usecasesApprovalInput("usecases-idem-2"))
	if err != nil {
		t.Fatalf("replayed approve: %v", err)
	}
	if !replay.Replayed || replay.TransferID != first.TransferID {
		t.Fatalf("replay = replayed %v id %s (first %s)", replay.Replayed, replay.TransferID, first.TransferID)
	}
	if got := len(f.publisher.names()); got != 6 {
		t.Fatalf("published events after replay = %d, want 6 (no re-fan-out)", got)
	}

	conflicting := usecasesApprovalInput("usecases-idem-2")
	conflicting.PolicyVersion = "policy-v9" // same key, DIFFERENT payload
	if _, err := f.approve.Execute(ctx, conflicting); err != usecases.ErrIdempotencyConflict {
		t.Fatalf("err = %v, want ErrIdempotencyConflict", err)
	}
}

// TestUsecases_AllocationReplyAdvancesSagaAndReleasesPickDemand drives the
// second main write use case: an inventory allocation reply moves the
// aggregate ALLOCATING → ALLOCATED with the reservation persisted, and the
// pick WorkDemandReleased is published in the SAME unit of work. The read
// model (GetTransfer over the real query repo) agrees.
func TestUsecases_AllocationReplyAdvancesSagaAndReleasesPickDemand(t *testing.T) {
	f := newUsecasesFixture(t)
	ctx := context.Background()

	result, err := f.approve.Execute(ctx, usecasesApprovalInput("usecases-idem-3"))
	if err != nil {
		t.Fatalf("approve: %v", err)
	}

	replyAt := usecasesNow.Add(time.Minute)
	err = f.allocate.Execute(ctx, usecases.ApplyAllocationInput{
		TransferID: result.TransferID,
		Allocation: transfer.StockAllocation{
			TransferLineID: result.TransferID.LineID(),
			OriginSiteID:   "WH1",
			SKU:            "SKU-1",
			ReservationID:  "res-usecases-1",
			Quantity:       5,
			Allocations:    []transfer.Allocation{{StockUnitID: "su-1", BinID: "bin-1", Quantity: 5}},
			ExpiresAt:      replyAt.Add(24 * time.Hour),
		},
		OccurredAt: replyAt,
	})
	if err != nil {
		t.Fatalf("apply allocation: %v", err)
	}

	// The read model agrees: ALLOCATED, reservation and one allocation row.
	read, err := f.readTransf.Execute(ctx, string(result.TransferID))
	if err != nil {
		t.Fatalf("read transfer: %v", err)
	}
	if read.State() != transfer.StateAllocated {
		t.Fatalf("state = %s, want ALLOCATED", read.State())
	}
	if read.ReservationID() != "res-usecases-1" || len(read.Allocations()) != 1 {
		t.Fatalf("reservation = %q allocations = %+v", read.ReservationID(), read.Allocations())
	}
	if audit := read.Audit(); len(audit) != 5 {
		t.Fatalf("audit = %d entries, want 5: %+v", len(audit), audit)
	}

	// The pick work demand was published with the allocation, same UoW.
	demand, ok := func() (transfer.DemandReleased, bool) {
		f.publisher.mu.Lock()
		defer f.publisher.mu.Unlock()
		for _, e := range f.publisher.events {
			if d, is := e.(transfer.DemandReleased); is {
				return d, true
			}
		}
		return transfer.DemandReleased{}, false
	}()
	if !ok {
		t.Fatal("allocation reply must publish the pick WorkDemandReleased")
	}
	wantDemandID := string(result.TransferID) + ":pick"
	if demand.DemandID != wantDemandID || demand.WorkKind != transfer.WorkKindTransferPick ||
		demand.PathID != "transfer-pick-path" || demand.SiteID != "WH1" ||
		demand.SKU != "SKU-1" || demand.Quantity != 5 {
		t.Fatalf("pick demand = %+v", demand)
	}
}
