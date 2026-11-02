package http

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/claudioed/network-inventory-planning/internal/adapters/inbound/http/page"
)

// rebalanceRuns serves GET /v1/rebalance-runs: the scheduled-rebalance
// run history, newest first (ADR 0007). ?limit=N bounds the page
// (default 100, max 500) and cursor continue the keyset.
func (h Handler) rebalanceRuns(w http.ResponseWriter, r *http.Request) {
	if h.ListRebalanceRuns == nil {
		writeProblem(w, http.StatusServiceUnavailable, "rebalance-runs-unavailable",
			"Rebalance run history is not configured", "this instance runs without the persistence the run history requires")
		return
	}
	limit, after, ok := pageParams(w, r)
	if !ok {
		return
	}
	runs, err := h.ListRebalanceRuns.Execute(r.Context(), after, page.FetchSize(limit))
	if isInvalidCursor(err) {
		writeInvalidCursor(w, err)
		return
	}
	if err != nil {
		writeProblem(w, http.StatusServiceUnavailable, "rebalance-runs-unavailable",
			"Rebalance run history could not be read", err.Error())
		return
	}
	dtos := make([]rebalanceRunDTO, 0, len(runs))
	for _, run := range runs {
		dto := rebalanceRunDTO{
			ID:        run.ID,
			StartedAt: run.StartedAt,
			Proposals: run.ProposalCount,
			Rejected:  run.RejectedCount,
			Outcome:   string(run.Outcome),
		}
		if !run.SnapshotAsOf.IsZero() {
			dto.SnapshotAsOf = &run.SnapshotAsOf
		}
		if run.FailClosedReason != nil {
			dto.FailClosedReason = run.FailClosedReason
		}
		dtos = append(dtos, dto)
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(page.Of(dtos, limit, func(d rebalanceRunDTO) page.Position {
		return page.Position{Sort: d.StartedAt.UTC().Format(time.RFC3339Nano), ID: strconv.FormatInt(d.ID, 10)}
	}))
}

// rebalanceRunDTO is the wire shape of one run row.
type rebalanceRunDTO struct {
	ID               int64      `json:"id"`
	StartedAt        time.Time  `json:"startedAt"`
	SnapshotAsOf     *time.Time `json:"snapshotAsOf,omitempty"`
	Proposals        int        `json:"proposalCount"`
	Rejected         int        `json:"rejectedCount"`
	Outcome          string     `json:"outcome"`
	FailClosedReason *string    `json:"failClosedReason,omitempty"`
}
