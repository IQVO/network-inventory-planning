package http

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/claudioed/network-inventory-planning/internal/application/usecases"
	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

// stubRebalanceRuns implements ports.RebalanceRunRepository in memory.
type stubRebalanceRuns struct {
	runs []transfer.RebalanceRun
}

func (s *stubRebalanceRuns) Record(_ context.Context, run transfer.RebalanceRun) (int64, error) {
	run.ID = int64(len(s.runs) + 1)
	s.runs = append(s.runs, run)
	return run.ID, nil
}

func (s *stubRebalanceRuns) List(_ context.Context, limit int) ([]transfer.RebalanceRun, error) {
	// Newest first, like the Postgres adapter.
	out := make([]transfer.RebalanceRun, 0, len(s.runs))
	for i := len(s.runs) - 1; i >= 0 && len(out) < limit; i-- {
		out = append(out, s.runs[i])
	}
	return out, nil
}

func runRows() []transfer.RebalanceRun {
	reason := "planning snapshot: no site capability facts; refusing to plan from an empty read model"
	// Insertion order is OLDEST first; the stub's List returns newest first.
	return []transfer.RebalanceRun{
		{ID: 1, StartedAt: time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC),
			Outcome: transfer.RebalanceFailed, FailClosedReason: &reason},
		{ID: 2, StartedAt: time.Date(2026, 10, 7, 13, 0, 0, 0, time.UTC),
			SnapshotAsOf:  time.Date(2026, 10, 7, 12, 59, 0, 0, time.UTC),
			ProposalCount: 3, RejectedCount: 1, Outcome: transfer.RebalanceCompleted},
	}
}

func TestRebalanceRunsEndpointListsNewestFirst(t *testing.T) {
	h := Handler{ListRebalanceRuns: &usecases.ListRebalanceRuns{Runs: &stubRebalanceRuns{runs: runRows()}}}
	request := httptest.NewRequest(http.MethodGet, "/v1/rebalance-runs", nil)
	response := httptest.NewRecorder()
	h.Routes().ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", response.Code, response.Body.String())
	}
	var body struct {
		Runs []rebalanceRunDTO `json:"runs"`
	}
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(body.Runs) != 2 {
		t.Fatalf("runs = %d, want 2", len(body.Runs))
	}
	if body.Runs[0].ID != 2 || body.Runs[0].Outcome != "COMPLETED" || body.Runs[0].Proposals != 3 {
		t.Fatalf("newest = %+v, want run 2 COMPLETED with 3 proposals", body.Runs[0])
	}
	if body.Runs[0].SnapshotAsOf == nil {
		t.Fatal("snapshotAsOf missing on a COMPLETED run")
	}
	if body.Runs[1].ID != 1 || body.Runs[1].Outcome != "FAILED" {
		t.Fatalf("oldest = %+v, want run 1 FAILED", body.Runs[1])
	}
	if body.Runs[1].FailClosedReason == nil || *body.Runs[1].FailClosedReason == "" {
		t.Fatal("failClosedReason missing on a FAILED run")
	}
	if body.Runs[1].SnapshotAsOf != nil {
		t.Fatal("snapshotAsOf must be absent on a run that never reached a watermark")
	}
}

func TestRebalanceRunsEndpointLimit(t *testing.T) {
	h := Handler{ListRebalanceRuns: &usecases.ListRebalanceRuns{Runs: &stubRebalanceRuns{runs: runRows()}}}
	for _, tc := range []struct {
		query string
		want  int
		code  int
	}{
		{"", 2, http.StatusOK},
		{"?limit=1", 1, http.StatusOK},
		{"?limit=0", 0, http.StatusBadRequest},
		{"?limit=201", 0, http.StatusBadRequest},
		{"?limit=abc", 0, http.StatusBadRequest},
	} {
		request := httptest.NewRequest(http.MethodGet, "/v1/rebalance-runs"+tc.query, nil)
		response := httptest.NewRecorder()
		h.Routes().ServeHTTP(response, request)
		if response.Code != tc.code {
			t.Fatalf("query %q status = %d, want %d (%s)", tc.query, response.Code, tc.code, response.Body.String())
		}
		if tc.code == http.StatusOK {
			var body struct {
				Runs []rebalanceRunDTO `json:"runs"`
			}
			_ = json.NewDecoder(response.Body).Decode(&body)
			if len(body.Runs) != tc.want {
				t.Fatalf("query %q runs = %d, want %d", tc.query, len(body.Runs), tc.want)
			}
		}
	}
}

func TestRebalanceRunsEndpointUnconfiguredIs503(t *testing.T) {
	h := Handler{}
	request := httptest.NewRequest(http.MethodGet, "/v1/rebalance-runs", nil)
	response := httptest.NewRecorder()
	h.Routes().ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 (never an empty 200 that fakes a history)", response.Code)
	}
}
