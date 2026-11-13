//go:build integration

// End-to-end proof of the REAL entry point of cmd/nip-reports on
// testcontainers: the analytical database is migrated with the repo's own
// runner and seeded with the repo's own analytics writer (the projection
// the projector owns), the binary boots with nothing but its documented
// environment, the documented probe answers, every documented report (the
// four + freshness of ADR 0009) answers with the seeded facts, and SIGTERM
// drains cleanly.
//
// Doc sources: docs/docs/adr/0009-analytics-read-side.md (cmd/nip-reports is
// the read-only reader of the analytical database;
// ANALYTICS_READER_DATABASE_URL falling back to ANALYTICS_DATABASE_URL,
// HTTP_ADDR :8092; GET /reports/{transfer-funnel,state-dwell,
// stuck-transfers,rebalance-runs,freshness} and /healthz),
// docs/docs/api-reference/rest (the reports surface).
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/claudioed/network-inventory-planning/internal/adapters/outbound/analyticsstore"
	"github.com/claudioed/network-inventory-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/network-inventory-planning/internal/analytics/report"
)

// itMigrationsPathValue resolves the repo's ANALYTICAL migrations directory
// (analytics/migrations) relative to this file: the migrations runner takes
// a path a go test run cannot rely on being CWD-relative for.
var itMigrationsPathValue = func() string {
	_, thisFile, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(thisFile), "..", "..", "analytics", "migrations")
}()

// TestMain holds the package-level SIGTERM registration for the whole test
// run: the real run() installs (and on return RESTORES) its own signal
// handler, so a SIGTERM arriving outside a live run() would otherwise kill
// the test binary itself.
func TestMain(m *testing.M) {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	os.Exit(m.Run())
}

// itPostgres starts a throwaway analytical Postgres and returns its DSN.
func itPostgres(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	container, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("nip_reports_itest"),
		tcpostgres.WithUsername("nip"),
		tcpostgres.WithPassword("nip"),
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
	return dsn
}

// service is one booted instance of the real run().
type service struct {
	base     string
	done     <-chan error
	stopOnce sync.Once
	stopErr  error
}

// itBootReports sets the environment a deployment sets, starts the REAL
// run() in-process and waits for the documented probe /healthz.
func itBootReports(t *testing.T, env map[string]string) *service {
	t.Helper()
	for k, v := range env {
		t.Setenv(k, v)
	}
	addr := freeAddr(t)
	t.Setenv("HTTP_ADDR", addr)
	done := make(chan error, 1)
	go func() { done <- run() }()
	s := &service{base: "http://" + addr, done: done}
	t.Cleanup(func() { itAssertCleanDrain(t, s.stop()) })
	waitUntil(t, "the documented probe /healthz to answer 200", func() bool {
		code, _ := getJSON(t, s.base+"/healthz")
		return code == http.StatusOK
	})
	return s
}

// stop drives the binary's real shutdown path (SIGTERM — run() installs its
// own signal.NotifyContext); safe because TestMain keeps a signal sink.
func (s *service) stop() error {
	s.stopOnce.Do(func() {
		select {
		case s.stopErr = <-s.done: // already stopped on its own
		default:
		}
		if s.stopErr == nil {
			if killErr := syscall.Kill(syscall.Getpid(), syscall.SIGTERM); killErr != nil {
				s.stopErr = killErr
				return
			}
			select {
			case s.stopErr = <-s.done:
			case <-time.After(60 * time.Second):
				s.stopErr = fmt.Errorf("the reports server did not stop")
			}
		}
	})
	return s.stopErr
}

// itAssertCleanDrain checks run()'s shutdown outcome: a SIGTERM-driven stop
// is a clean shutdown and run() returns nil.
func itAssertCleanDrain(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("the reports server must drain cleanly on SIGTERM, got %v", err)
	}
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	return l.Addr().String()
}

func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(90 * time.Second); time.Now().Before(deadline); time.Sleep(200 * time.Millisecond) {
		if cond() {
			return
		}
	}
	t.Fatalf("timed out waiting for %s", what)
}

func getJSON(t *testing.T, url string) (int, map[string]any) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		return 0, nil
	}
	defer func() { _ = resp.Body.Close() }()
	var body map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&body)
	return resp.StatusCode, body
}

// findDay answers the day entry of state, or nil when the report has none.
func findDay(body map[string]any, state string) map[string]any {
	days, _ := body["days"].([]any)
	for _, d := range days {
		day, _ := d.(map[string]any)
		if day["state"] == state {
			return day
		}
	}
	return nil
}

// dayOf answers the funnel/dwell/stuck day entry of state, scanning the
// response instead of assuming the day/state order.
func dayOf(t *testing.T, body map[string]any, state string) map[string]any {
	t.Helper()
	if day := findDay(body, state); day != nil {
		return day
	}
	t.Fatalf("no day entry for state %q in %v", state, body)
	return nil
}

// TestReportsServeTheProjectionAndStopCleanly proves the shipped reader's
// contract: the analytical migrations + the repo's own writer seed the
// projection, then env only -> /healthz -> every documented report answers
// the seeded facts (distinct, non-zero operands) -> SIGTERM drains cleanly.
func TestReportsServeTheProjectionAndStopCleanly(t *testing.T) {
	dsn := itPostgres(t)
	if err := postgres.RunMigrations(dsn, itMigrationsPathValue); err != nil {
		t.Fatalf("run analytics migrations: %v", err)
	}
	ctx := context.Background()
	writer, err := analyticsstore.NewPool(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	projection := analyticsstore.NewProjection(writer)

	day := time.Now().UTC().Truncate(24 * time.Hour).Add(-24 * time.Hour)
	events := []report.Event{
		{Kind: report.KindStateAdvanced, EventID: "rep-e1", At: day.Add(8 * time.Hour),
			TransferID: "TRF-REPORTS-1", From: "DRAFT", To: "PROPOSED", AgeSeconds: 120},
		{Kind: report.KindStateAdvanced, EventID: "rep-e2", At: day.Add(9 * time.Hour),
			TransferID: "TRF-REPORTS-1", From: "PROPOSED", To: "ALLOCATED", AgeSeconds: 240},
		{Kind: report.KindStuckDetected, EventID: "rep-e3", At: day.Add(10 * time.Hour),
			TransferID: "TRF-REPORTS-2", State: "ALLOCATING", AgeSeconds: 5400, ThresholdSeconds: 3600},
		{Kind: report.KindRebalanceRunCompleted, EventID: "rep-e4", At: day.Add(11 * time.Hour),
			RunID: "RUN-REPORTS-1", ProposalCount: 7, RejectedCount: 3, StaleFacts: 2},
	}
	for _, e := range events {
		if _, err := projection.Apply(ctx, e); err != nil {
			t.Fatalf("seed %s: %v", e.EventID, err)
		}
	}

	s := itBootReports(t, map[string]string{
		"ANALYTICS_READER_DATABASE_URL": dsn,
		"LOG_LEVEL":                     "error",
	})

	from, to := day.Format(time.RFC3339), day.Add(24*time.Hour).Format(time.RFC3339)
	window := "?from=" + from + "&to=" + to

	code, body := getJSON(t, s.base+"/reports/transfer-funnel"+window)
	if code != http.StatusOK {
		t.Fatalf("transfer-funnel = %d %v", code, body)
	}
	if d := dayOf(t, body, "PROPOSED"); d["transfers"] != float64(1) {
		t.Fatalf("funnel PROPOSED = %v (want the one distinct transfer)", d)
	}
	if d := dayOf(t, body, "ALLOCATED"); d["transfers"] != float64(1) {
		t.Fatalf("funnel ALLOCATED = %v", d)
	}

	code, body = getJSON(t, s.base+"/reports/state-dwell"+window)
	if code != http.StatusOK {
		t.Fatalf("state-dwell = %d %v", code, body)
	}
	// DwellDay is the age distribution of the transitions that LEFT the
	// state (TransferStateAdvanced `from`): the creation entry (from="")
	// leaves nothing, and a state never left has no row.
	if d := dayOf(t, body, "DRAFT"); d["transitions"] != float64(1) || d["p50_age_seconds"] != float64(120) || d["p95_age_seconds"] != float64(120) {
		t.Fatalf("dwell DRAFT = %v", d)
	}
	if d := dayOf(t, body, "PROPOSED"); d["transitions"] != float64(1) || d["p50_age_seconds"] != float64(240) || d["p95_age_seconds"] != float64(240) {
		t.Fatalf("dwell PROPOSED = %v", d)
	}
	if d := findDay(body, "ALLOCATED"); d != nil {
		t.Fatalf("dwell ALLOCATED = %v (a state never left has no row)", d)
	}

	code, body = getJSON(t, s.base+"/reports/stuck-transfers"+window)
	if code != http.StatusOK {
		t.Fatalf("stuck-transfers = %d %v", code, body)
	}
	if d := dayOf(t, body, "ALLOCATING"); d["detections"] != float64(1) || d["transfers"] != float64(1) {
		t.Fatalf("stuck day = %v", d)
	}
	latest, _ := body["latest"].([]any)
	if len(latest) != 1 {
		t.Fatalf("stuck latest = %v", body)
	}
	occ, _ := latest[0].(map[string]any)
	if occ["transfer_id"] != "TRF-REPORTS-2" || occ["age_seconds"] != float64(5400) || occ["threshold_seconds"] != float64(3600) {
		t.Fatalf("stuck occurrence = %v", occ)
	}

	code, body = getJSON(t, s.base+"/reports/rebalance-runs"+window)
	if code != http.StatusOK {
		t.Fatalf("rebalance-runs = %d %v", code, body)
	}
	days, _ := body["days"].([]any)
	if len(days) != 1 {
		t.Fatalf("rebalance days = %v", body)
	}
	rd, _ := days[0].(map[string]any)
	if rd["runs"] != float64(1) || rd["proposals"] != float64(7) || rd["rejected"] != float64(3) ||
		rd["rejection_rate"] == nil || rd["rejection_rate"].(float64) <= 0.42 || rd["rejection_rate"].(float64) >= 0.43 ||
		rd["stale_facts"] != float64(2) {
		t.Fatalf("rebalance day = %v (want 3/7 rejection)", rd)
	}

	code, body = getJSON(t, s.base+"/reports/freshness")
	if code != http.StatusOK || body["as_of"] == nil || body["lag_seconds"] == nil {
		t.Fatalf("freshness = %d %v", code, body)
	}
	if body["as_of"] != day.Add(11*time.Hour).Format("2006-01-02T15:04:05Z07:00") {
		t.Fatalf("as_of = %v (want the newest seeded event %s)", body["as_of"], day.Add(11*time.Hour))
	}

	itAssertCleanDrain(t, s.stop())
}
