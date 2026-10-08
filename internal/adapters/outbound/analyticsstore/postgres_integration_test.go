//go:build integration

package analyticsstore_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/claudioed/network-inventory-planning/internal/adapters/outbound/analyticsstore"
	"github.com/claudioed/network-inventory-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/network-inventory-planning/internal/analytics/report"
)

// One real Postgres (testcontainers, never skip-gated) serves the package;
// every test starts from truncated tables.
var (
	pgOnce      sync.Once
	pgURL       string
	pgContainer testcontainers.Container
	pgErr       error
)

func TestMain(m *testing.M) {
	code := m.Run()
	if pgContainer != nil {
		if err := testcontainers.TerminateContainer(pgContainer); err != nil {
			fmt.Fprintf(os.Stderr, "terminate postgres container: %v\n", err)
		}
	}
	os.Exit(code)
}

func analyticsMigrationsDir(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("unable to resolve test file path")
	}
	return filepath.Join(filepath.Dir(thisFile), "..", "..", "..", "..", "analytics", "migrations")
}

// analyticalDB boots the analytical database once, applies the analytics
// migrations over the direct DSN, and returns its DSN.
func analyticalDB(t *testing.T) string {
	t.Helper()
	pgOnce.Do(func() {
		ctx := context.Background()
		c, err := tcpostgres.Run(ctx, "postgres:16-alpine",
			tcpostgres.WithDatabase("nip_analytics"),
			tcpostgres.WithUsername("projector"),
			tcpostgres.WithPassword("projector"),
			testcontainers.WithWaitStrategy(
				wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(60*time.Second)),
		)
		if err != nil {
			pgErr = err
			return
		}
		pgContainer = c
		if pgURL, pgErr = c.ConnectionString(ctx, "sslmode=disable"); pgErr != nil {
			return
		}
		pgErr = postgres.RunMigrations(pgURL, analyticsMigrationsDir(t))
	})
	if pgErr != nil {
		t.Fatalf("analytical database: %v", pgErr)
	}
	return pgURL
}

const allFactTables = `TRUNCATE transfer_state_advances, transfer_stuck_detections, rebalance_run_facts, analytics_processed_events`

// freshStores returns the writer and the (read-only) reader over truncated
// tables.
func freshStores(t *testing.T) (*analyticsstore.Projection, *analyticsstore.Reader, *pgxpool.Pool) {
	t.Helper()
	url := analyticalDB(t)
	ctx := context.Background()
	w, err := analyticsstore.NewPool(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(w.Close)
	if _, err := w.Exec(ctx, allFactTables); err != nil {
		t.Fatal(err)
	}
	ro, err := analyticsstore.NewReadOnlyPool(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(ro.Close)
	return analyticsstore.NewProjection(w), analyticsstore.NewReader(ro), w
}

func TestPostgresStore_SatisfiesTheStoreContract(t *testing.T) {
	runStoreContract(t, func(t *testing.T) (report.Projection, report.Reader) {
		p, r, _ := freshStores(t)
		return p, r
	})
}

func count(t *testing.T, pool *pgxpool.Pool, table string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM `+table).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func advancedEvent(id string) report.Event {
	return report.Event{Kind: report.KindStateAdvanced, EventID: id, At: at(5, 8, 0, 0), TransferID: "tr-" + id, From: "PROPOSED", To: "APPROVED", AgeSeconds: 12}
}

// The fact write and the processed-id mark are ONE transaction: when the fact
// insert fails the mark is rolled back with it, and the redelivered event
// applies cleanly once the failure is gone.
func TestPostgresStore_ATransientFailureLeavesNothingWrittenAndTheMarkUnrecorded(t *testing.T) {
	p, _, pool := freshStores(t)
	ctx := context.Background()
	for _, stmt := range []string{
		`CREATE OR REPLACE FUNCTION fail_advances() RETURNS trigger AS $$ BEGIN RAISE EXCEPTION 'simulated outage'; END $$ LANGUAGE plpgsql`,
		`CREATE TRIGGER fail_advances BEFORE INSERT ON transfer_state_advances FOR EACH ROW EXECUTE FUNCTION fail_advances()`,
	} {
		if _, err := pool.Exec(ctx, stmt); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DROP TRIGGER IF EXISTS fail_advances ON transfer_state_advances`)
	})

	applied, err := p.Apply(ctx, advancedEvent("evt-1"))
	if err == nil || applied {
		t.Fatalf("Apply = %v, %v; want an error", applied, err)
	}
	if errors.Is(err, report.ErrRejected) {
		t.Fatalf("a raised exception is transient infrastructure, not poison: %v", err)
	}
	if n := count(t, pool, "transfer_state_advances"); n != 0 {
		t.Errorf("transfer_state_advances rows = %d, want 0", n)
	}
	if n := count(t, pool, "analytics_processed_events"); n != 0 {
		t.Errorf("processed marks = %d, want 0 (the mark must roll back with the effect)", n)
	}

	if _, err := pool.Exec(ctx, `DROP TRIGGER fail_advances ON transfer_state_advances`); err != nil {
		t.Fatal(err)
	}
	if applied, err := p.Apply(ctx, advancedEvent("evt-1")); err != nil || !applied {
		t.Fatalf("redelivery = %v, %v; want applied", applied, err)
	}
	if count(t, pool, "transfer_state_advances") != 1 || count(t, pool, "analytics_processed_events") != 1 {
		t.Fatal("the redelivered event must write the row and the mark")
	}
}

func TestPostgresStore_ADeterministicRejectionIsClassifiedAndWritesNothing(t *testing.T) {
	p, _, pool := freshStores(t)
	e := advancedEvent("evt-bad")
	e.AgeSeconds = -1 // violates CHECK (age_seconds >= 0): integrity class 23
	applied, err := p.Apply(context.Background(), e)
	if applied || !errors.Is(err, report.ErrRejected) {
		t.Fatalf("Apply = %v, %v; want report.ErrRejected", applied, err)
	}
	if count(t, pool, "transfer_state_advances") != 0 || count(t, pool, "analytics_processed_events") != 0 {
		t.Fatal("a rejected event must leave nothing behind, not even its mark")
	}
}

func TestPostgresStore_AnUnknownKindIsRejectedAndLeavesNoMark(t *testing.T) {
	p, _, pool := freshStores(t)
	applied, err := p.Apply(context.Background(), report.Event{Kind: "Other", EventID: "x", At: at(5, 1, 0, 0)})
	if applied || !errors.Is(err, report.ErrRejected) {
		t.Fatalf("Apply = %v, %v; want report.ErrRejected", applied, err)
	}
	if count(t, pool, "analytics_processed_events") != 0 {
		t.Fatal("an unknown kind must not leave a mark")
	}
}

func TestPostgresStore_ConcurrentDeliveriesOfOneIdApplyExactlyOnce(t *testing.T) {
	p, _, pool := freshStores(t)
	var wg sync.WaitGroup
	results := make(chan bool, 12)
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			applied, err := p.Apply(context.Background(), advancedEvent("evt-race"))
			if err != nil {
				t.Errorf("Apply: %v", err)
			}
			results <- applied
		}()
	}
	wg.Wait()
	close(results)
	won := 0
	for ok := range results {
		if ok {
			won++
		}
	}
	if won != 1 || count(t, pool, "transfer_state_advances") != 1 {
		t.Fatalf("applied %d times, rows %d; want exactly 1 and 1", won, count(t, pool, "transfer_state_advances"))
	}
}

func TestPostgresStore_TheReportsPoolIsReadOnly(t *testing.T) {
	_, _, _ = freshStores(t)
	ro, err := analyticsstore.NewReadOnlyPool(context.Background(), analyticalDB(t))
	if err != nil {
		t.Fatal(err)
	}
	defer ro.Close()
	if _, err := ro.Exec(context.Background(), `INSERT INTO analytics_processed_events (event_id, event_type, occurred_at) VALUES ('x', 'y', now())`); err == nil {
		t.Fatal("the reports pool wrote to the analytical database")
	}
}

// Statement timeouts are applied to every connection of both pools.
func TestPostgresStore_PoolsApplyTheirStatementTimeouts(t *testing.T) {
	url := analyticalDB(t)
	ctx := context.Background()
	w, _ := analyticsstore.NewPool(ctx, url)
	ro, _ := analyticsstore.NewReadOnlyPool(ctx, url)
	defer w.Close()
	defer ro.Close()
	for pool, want := range map[*pgxpool.Pool]string{w: "10s", ro: "15s"} {
		var got string
		if err := pool.QueryRow(ctx, `SHOW statement_timeout`).Scan(&got); err != nil || got != want {
			t.Errorf("statement_timeout = %q, %v; want %q", got, err, want)
		}
	}
}

func TestAnalyticsMigration_DownThenUpIsClean(t *testing.T) {
	_, _, pool := freshStores(t)
	ctx := context.Background()
	dir := analyticsMigrationsDir(t)
	read := func(name string) string {
		b, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}
	run := func(name string) {
		t.Helper()
		if _, err := pool.Exec(ctx, read(name)); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
	run("0002_state_advance_dwell.down.sql")
	var hasCol bool
	if err := pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'transfer_state_advances' AND column_name = 'dwell_seconds')`).Scan(&hasCol); err != nil || hasCol {
		t.Fatalf("dwell_seconds still exists after 0002 down (%v, %v)", hasCol, err)
	}
	run("0001_saga_facts.down.sql")
	for _, table := range []string{"transfer_state_advances", "transfer_stuck_detections", "rebalance_run_facts", "analytics_processed_events"} {
		var exists bool
		if err := pool.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, table).Scan(&exists); err != nil || exists {
			t.Fatalf("%s still exists after down (%v, %v)", table, exists, err)
		}
	}
	run("0001_saga_facts.up.sql")

	// A row projected BEFORE 0002 (no dwell column yet) must survive the
	// additive migration as NULL, not 0.
	if _, err := pool.Exec(ctx, `INSERT INTO transfer_state_advances (event_id, transfer_id, from_state, to_state, age_seconds, occurred_at)
		VALUES ('old-1', 't-old', 'PROPOSED', 'APPROVED', 77, now())`); err != nil {
		t.Fatal(err)
	}
	run("0002_state_advance_dwell.up.sql")
	var dwell *int64
	if err := pool.QueryRow(ctx, `SELECT dwell_seconds FROM transfer_state_advances WHERE event_id = 'old-1'`).Scan(&dwell); err != nil || dwell != nil {
		t.Fatalf("pre-migration row dwell_seconds = %v, %v; want NULL", dwell, err)
	}
}

// The dwell round-trips as stored, an event without it is NULL (never 0), and
// a negative dwell is a deterministic rejection.
func TestPostgresStore_StoresDwellWhenPresentAndNullWhenAbsent(t *testing.T) {
	p, _, pool := freshStores(t)
	ctx := context.Background()
	with := advancedEvent("evt-with")
	d := int64(0)
	with.DwellSeconds = &d // a genuine zero is stored as 0
	without := advancedEvent("evt-without")
	for _, e := range []report.Event{with, without} {
		if applied, err := p.Apply(ctx, e); err != nil || !applied {
			t.Fatalf("Apply(%s) = %v, %v", e.EventID, applied, err)
		}
	}
	read := func(id string) *int64 {
		var v *int64
		if err := pool.QueryRow(ctx, `SELECT dwell_seconds FROM transfer_state_advances WHERE event_id = $1`, id).Scan(&v); err != nil {
			t.Fatal(err)
		}
		return v
	}
	if got := read("evt-with"); got == nil || *got != 0 {
		t.Errorf("stored dwell for evt-with = %v, want 0", got)
	}
	if got := read("evt-without"); got != nil {
		t.Errorf("stored dwell for evt-without = %d, want NULL", *got)
	}

	neg := advancedEvent("evt-neg")
	n := int64(-5)
	neg.DwellSeconds = &n
	if applied, err := p.Apply(ctx, neg); applied || !errors.Is(err, report.ErrRejected) {
		t.Fatalf("negative dwell Apply = %v, %v; want report.ErrRejected", applied, err)
	}
}
