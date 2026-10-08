package http

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/claudioed/network-inventory-planning/internal/analytics/report"
)

var reportsNow = time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)

// stubReader returns canned rows and records the range/limit it was asked
// for; err makes every method fail.
type stubReader struct {
	err        error
	gotRange   report.Range
	gotLimit   int
	asOf       *time.Time
	funnel     []report.FunnelDay
	dwell      []report.DwellDay
	stuckDays  []report.StuckDay
	stuckLast  []report.StuckOccurrence
	rebalances []report.RebalanceDay
}

func (s *stubReader) TransferFunnel(_ context.Context, r report.Range) ([]report.FunnelDay, error) {
	s.gotRange = r
	return s.funnel, s.err
}
func (s *stubReader) StateDwell(_ context.Context, r report.Range) ([]report.DwellDay, error) {
	s.gotRange = r
	return s.dwell, s.err
}
func (s *stubReader) StuckDays(_ context.Context, r report.Range) ([]report.StuckDay, error) {
	s.gotRange = r
	return s.stuckDays, s.err
}
func (s *stubReader) StuckLatest(_ context.Context, r report.Range, limit int) ([]report.StuckOccurrence, error) {
	s.gotRange, s.gotLimit = r, limit
	return s.stuckLast, s.err
}
func (s *stubReader) RebalanceDays(_ context.Context, r report.Range) ([]report.RebalanceDay, error) {
	s.gotRange = r
	return s.rebalances, s.err
}
func (s *stubReader) LastEventAt(context.Context) (*time.Time, error) { return s.asOf, s.err }

func reportsGet(t *testing.T, rd report.Reader, target string) *httptest.ResponseRecorder {
	t.Helper()
	srv := &ReportsServer{Reader: rd, Now: func() time.Time { return reportsNow }}
	rec := httptest.NewRecorder()
	srv.Routes().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, target, nil))
	return rec
}

func decodeInto(t *testing.T, rec *httptest.ResponseRecorder, v any) {
	t.Helper()
	if err := json.Unmarshal(rec.Body.Bytes(), v); err != nil {
		t.Fatalf("body %q is not JSON: %v", rec.Body.String(), err)
	}
}

var reportPaths = []string{
	"/reports/transfer-funnel", "/reports/state-dwell", "/reports/stuck-transfers", "/reports/rebalance-runs",
}

func TestReports_BadRangeIs400ProblemJSONOnEveryRangedReport(t *testing.T) {
	bad := map[string]string{
		"from not RFC 3339":  "?from=yesterday",
		"to not RFC 3339":    "?to=2026-10-07",
		"inverted":           "?from=2026-10-07T00:00:00Z&to=2026-10-05T00:00:00Z",
		"empty":              "?from=2026-10-05T00:00:00Z&to=2026-10-05T00:00:00Z",
		"more than 366 days": "?from=2025-01-01T00:00:00Z&to=2026-10-07T00:00:00Z",
	}
	for _, path := range reportPaths {
		for name, q := range bad {
			t.Run(path+" "+name, func(t *testing.T) {
				rec := reportsGet(t, &stubReader{}, path+q)
				if rec.Code != http.StatusBadRequest {
					t.Fatalf("status = %d, want 400; body %s", rec.Code, rec.Body)
				}
				if ct := rec.Header().Get("Content-Type"); ct != "application/problem+json" {
					t.Fatalf("content-type = %q", ct)
				}
				var p problem
				decodeInto(t, rec, &p)
				if p.Status != 400 || !strings.HasSuffix(p.Type, "/invalid-report-range") || p.Detail == "" {
					t.Fatalf("problem = %+v", p)
				}
			})
		}
	}
}

func TestReports_DefaultAndExplicitRangesReachTheReader(t *testing.T) {
	rd := &stubReader{}
	rec := reportsGet(t, rd, "/reports/transfer-funnel")
	if rec.Code != 200 {
		t.Fatalf("status = %d", rec.Code)
	}
	want := report.Range{From: reportsNow.Add(-30 * 24 * time.Hour), To: reportsNow}
	if rd.gotRange != want {
		t.Fatalf("default range = %+v, want %+v", rd.gotRange, want)
	}
	var body struct{ From, To time.Time }
	decodeInto(t, rec, &body)
	if !body.From.Equal(want.From) || !body.To.Equal(want.To) {
		t.Fatalf("echoed range = %+v", body)
	}
	reportsGet(t, rd, "/reports/state-dwell?from=2026-10-05T00:00:00Z&to=2026-10-06T00:00:00Z")
	if rd.gotRange.From != time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC) || rd.gotRange.To != time.Date(2026, 10, 6, 0, 0, 0, 0, time.UTC) {
		t.Fatalf("explicit range = %+v", rd.gotRange)
	}
}

func TestReports_EmptyResultsAre200WithEmptyArraysNotNull(t *testing.T) {
	for _, path := range reportPaths {
		rec := reportsGet(t, &stubReader{}, path)
		if rec.Code != 200 {
			t.Fatalf("%s status = %d", path, rec.Code)
		}
		if strings.Contains(rec.Body.String(), "null") {
			t.Errorf("%s body has null: %s", path, rec.Body)
		}
		if !strings.Contains(rec.Body.String(), `"days":[]`) {
			t.Errorf("%s body lacks empty days array: %s", path, rec.Body)
		}
	}
	if rec := reportsGet(t, &stubReader{}, "/reports/stuck-transfers"); !strings.Contains(rec.Body.String(), `"latest":[]`) {
		t.Errorf("stuck-transfers body lacks empty latest array: %s", rec.Body)
	}
}

func TestReports_ShapesAreSnakeCaseWithUTCDates(t *testing.T) {
	day := time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC)
	rd := &stubReader{
		funnel:     []report.FunnelDay{{Day: day, State: "APPROVED", Transfers: 5}},
		dwell:      []report.DwellDay{{Day: day, State: "PROPOSED", Transitions: 6, P50Seconds: 200, P95Seconds: 601.25}},
		stuckDays:  []report.StuckDay{{Day: day, State: "ALLOCATING", Detections: 2, Transfers: 1}},
		stuckLast:  []report.StuckOccurrence{{TransferID: "t1", State: "ALLOCATING", AgeSeconds: 700, ThresholdSeconds: 600, DetectedAt: day.Add(12 * time.Hour)}},
		rebalances: []report.RebalanceDay{{Day: day, Runs: 2, Proposals: 14, Rejected: 2, StaleFacts: 1}},
	}
	want := map[string]string{
		"/reports/transfer-funnel": `{"day":"2026-10-05","state":"APPROVED","transfers":5}`,
		"/reports/state-dwell":     `{"day":"2026-10-05","state":"PROPOSED","transitions":6,"p50_age_seconds":200,"p95_age_seconds":601.25}`,
		"/reports/stuck-transfers": `{"day":"2026-10-05","state":"ALLOCATING","detections":2,"transfers":1}`,
		"/reports/rebalance-runs":  `{"day":"2026-10-05","runs":2,"proposals":14,"rejected":2,"rejection_rate":0.14285714285714285,"stale_facts":1}`,
	}
	for path, row := range want {
		rec := reportsGet(t, rd, path)
		if rec.Code != 200 || !strings.Contains(rec.Body.String(), row) {
			t.Errorf("%s = %d %s; want it to contain %s", path, rec.Code, rec.Body, row)
		}
	}
	rec := reportsGet(t, rd, "/reports/stuck-transfers")
	latest := `{"transfer_id":"t1","state":"ALLOCATING","age_seconds":700,"threshold_seconds":600,"detected_at":"2026-10-05T12:00:00Z"}`
	if !strings.Contains(rec.Body.String(), latest) {
		t.Errorf("latest occurrence missing: %s", rec.Body)
	}
}

func TestReports_StuckLimitIsValidatedAndDefaulted(t *testing.T) {
	rd := &stubReader{}
	if rec := reportsGet(t, rd, "/reports/stuck-transfers"); rec.Code != 200 || rd.gotLimit != 20 {
		t.Fatalf("default: status %d limit %d, want 200 and 20", rec.Code, rd.gotLimit)
	}
	if rec := reportsGet(t, rd, "/reports/stuck-transfers?limit=100"); rec.Code != 200 || rd.gotLimit != 100 {
		t.Fatalf("limit=100: status %d limit %d", rec.Code, rd.gotLimit)
	}
	for _, bad := range []string{"0", "101", "-1", "abc", "1.5"} {
		rec := reportsGet(t, &stubReader{}, "/reports/stuck-transfers?limit="+bad)
		var p problem
		decodeInto(t, rec, &p)
		if rec.Code != 400 || !strings.HasSuffix(p.Type, "/invalid-report-limit") {
			t.Errorf("limit=%s: status %d problem %+v, want 400 invalid-report-limit", bad, rec.Code, p)
		}
	}
}

func TestReports_AStoreFailureIs500WithoutLeakingTheCause(t *testing.T) {
	paths := append(append([]string{}, reportPaths...), "/reports/freshness")
	for _, path := range paths {
		rec := reportsGet(t, &stubReader{err: errors.New("dial tcp 10.0.0.1:5432: password=hunter2")}, path)
		var p problem
		decodeInto(t, rec, &p)
		if rec.Code != 500 || p.Status != 500 || !strings.HasSuffix(p.Type, "/report-store-error") {
			t.Errorf("%s = %d %+v; want 500 report-store-error", path, rec.Code, p)
		}
		if strings.Contains(rec.Body.String(), "hunter2") || strings.Contains(rec.Body.String(), "5432") {
			t.Errorf("%s leaked the cause: %s", path, rec.Body)
		}
	}
}

func TestReports_FreshnessReportsLagAndNullsWhenEmpty(t *testing.T) {
	rec := reportsGet(t, &stubReader{}, "/reports/freshness")
	if rec.Code != 200 || strings.TrimSpace(rec.Body.String()) != `{"as_of":null,"lag_seconds":null}` {
		t.Fatalf("empty freshness = %d %s", rec.Code, rec.Body)
	}
	asOf := reportsNow.Add(-90 * time.Second)
	rec = reportsGet(t, &stubReader{asOf: &asOf}, "/reports/freshness")
	var f report.Freshness
	decodeInto(t, rec, &f)
	if rec.Code != 200 || f.LagSeconds == nil || *f.LagSeconds != 90 || f.AsOf == nil || !f.AsOf.Equal(asOf) {
		t.Fatalf("freshness = %d %+v", rec.Code, f)
	}
}

func TestReports_HealthzAndWrongMethodAndUnknownPath(t *testing.T) {
	if rec := reportsGet(t, &stubReader{}, "/healthz"); rec.Code != 200 || strings.TrimSpace(rec.Body.String()) != `{"status":"ok"}` {
		t.Fatalf("healthz = %d %s", rec.Code, rec.Body)
	}
	srv := &ReportsServer{Reader: &stubReader{}}
	rec := httptest.NewRecorder()
	srv.Routes().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/reports/transfer-funnel", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST status = %d, want 405 (read-only)", rec.Code)
	}
	if rec := reportsGet(t, &stubReader{}, "/reports/nope"); rec.Code != http.StatusNotFound {
		t.Fatalf("unknown report status = %d", rec.Code)
	}
}

func TestReports_ServerWithoutAClockUsesTheWallClock(t *testing.T) {
	srv := &ReportsServer{Reader: &stubReader{}}
	if got := srv.now(); time.Since(got) > time.Minute || time.Since(got) < -time.Minute {
		t.Fatalf("now() = %v, want ~time.Now()", got)
	}
}
