package http

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/claudioed/network-inventory-planning/internal/analytics/report"
)

// ReportsServer is the inbound HTTP adapter of cmd/nip-reports: the read-only
// saga-health analytics reports of ADR 0009. It depends only on the
// report.Reader port over the analytical database; it never touches the OLTP
// use cases, and it writes nothing.
type ReportsServer struct {
	Reader report.Reader
	// Now is the clock the default range ends at (time.Now when nil).
	Now func() time.Time
}

const reportDayLayout = "2006-01-02"

// The wire shapes (snake_case, like the analytics event payloads and the
// fleet's other reports); days are calendar dates (UTC) and the report
// structs never leak onto the wire.
type funnelDayDTO struct {
	Day       string `json:"day"`
	State     string `json:"state"`
	Transfers int    `json:"transfers"`
}

type dwellDayDTO struct {
	Day           string  `json:"day"`
	State         string  `json:"state"`
	Transitions   int     `json:"transitions"`
	P50AgeSeconds float64 `json:"p50_age_seconds"`
	P95AgeSeconds float64 `json:"p95_age_seconds"`
}

type stuckDayDTO struct {
	Day        string `json:"day"`
	State      string `json:"state"`
	Detections int    `json:"detections"`
	Transfers  int    `json:"transfers"`
}

type stuckOccurrenceDTO struct {
	TransferID       string    `json:"transfer_id"`
	State            string    `json:"state"`
	AgeSeconds       int64     `json:"age_seconds"`
	ThresholdSeconds int64     `json:"threshold_seconds"`
	DetectedAt       time.Time `json:"detected_at"`
}

type rebalanceDayDTO struct {
	Day           string  `json:"day"`
	Runs          int     `json:"runs"`
	Proposals     int     `json:"proposals"`
	Rejected      int     `json:"rejected"`
	RejectionRate float64 `json:"rejection_rate"`
	StaleFacts    int     `json:"stale_facts"`
}

// reportRangeDTO echoes the effective half-open range [from, to) of a
// response.
type reportRangeDTO struct {
	From time.Time `json:"from"`
	To   time.Time `json:"to"`
}

type funnelReportDTO struct {
	reportRangeDTO
	Days []funnelDayDTO `json:"days"`
}

type dwellReportDTO struct {
	reportRangeDTO
	Days []dwellDayDTO `json:"days"`
}

type stuckReportDTO struct {
	reportRangeDTO
	Days   []stuckDayDTO        `json:"days"`
	Latest []stuckOccurrenceDTO `json:"latest"`
}

type rebalanceReportDTO struct {
	reportRangeDTO
	Days []rebalanceDayDTO `json:"days"`
}

// Routes builds the router of cmd/nip-reports: /healthz and GET
// /reports/{transfer-funnel,state-dwell,stuck-transfers,rebalance-runs,freshness}.
// No auth middleware (fleet rule), no CORS: the service is cluster-internal.
func (s *ReportsServer) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeReportJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /reports/transfer-funnel", s.handleTransferFunnel)
	mux.HandleFunc("GET /reports/state-dwell", s.handleStateDwell)
	mux.HandleFunc("GET /reports/stuck-transfers", s.handleStuckTransfers)
	mux.HandleFunc("GET /reports/rebalance-runs", s.handleRebalanceRuns)
	mux.HandleFunc("GET /reports/freshness", s.handleFreshness)
	return mux
}

func writeReportJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func (s *ReportsServer) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

// handleFreshness serves GET /reports/freshness: how far the projection is
// behind (now - the newest applied event time), for all the reports, which
// read one projection. Both fields are null until the first event is applied.
func (s *ReportsServer) handleFreshness(w http.ResponseWriter, r *http.Request) {
	asOf, err := s.Reader.LastEventAt(r.Context())
	if err != nil {
		writeReportError(w)
		return
	}
	writeReportJSON(w, http.StatusOK, report.ComputeFreshness(asOf, s.now()))
}

// parseRange validates ?from=&to= (RFC 3339, both optional) and writes the
// RFC 7807 400 itself when they are unusable.
func (s *ReportsServer) parseRange(w http.ResponseWriter, r *http.Request) (report.Range, bool) {
	q := r.URL.Query()
	rg, err := report.ParseRange(q.Get("from"), q.Get("to"), s.now())
	if err != nil {
		writeProblem(w, http.StatusBadRequest, "invalid-report-range",
			"from and to must form a valid RFC 3339 range of at most 366 days", err.Error())
		return report.Range{}, false
	}
	return rg, true
}

// writeReportError is the 500 for a failed analytical query. The cause is not
// echoed: it can carry SQL and connection details.
func writeReportError(w http.ResponseWriter) {
	writeProblem(w, http.StatusInternalServerError, "report-store-error",
		"The report could not be served", "the analytical database query failed")
}

func (s *ReportsServer) handleTransferFunnel(w http.ResponseWriter, r *http.Request) {
	rg, ok := s.parseRange(w, r)
	if !ok {
		return
	}
	days, err := s.Reader.TransferFunnel(r.Context(), rg)
	if err != nil {
		writeReportError(w)
		return
	}
	out := funnelReportDTO{reportRangeDTO: reportRangeDTO{rg.From, rg.To}, Days: make([]funnelDayDTO, 0, len(days))}
	for _, d := range days {
		out.Days = append(out.Days, funnelDayDTO{Day: d.Day.UTC().Format(reportDayLayout), State: d.State, Transfers: d.Transfers})
	}
	writeReportJSON(w, http.StatusOK, out)
}

func (s *ReportsServer) handleStateDwell(w http.ResponseWriter, r *http.Request) {
	rg, ok := s.parseRange(w, r)
	if !ok {
		return
	}
	days, err := s.Reader.StateDwell(r.Context(), rg)
	if err != nil {
		writeReportError(w)
		return
	}
	out := dwellReportDTO{reportRangeDTO: reportRangeDTO{rg.From, rg.To}, Days: make([]dwellDayDTO, 0, len(days))}
	for _, d := range days {
		out.Days = append(out.Days, dwellDayDTO{
			Day: d.Day.UTC().Format(reportDayLayout), State: d.State, Transitions: d.Transitions,
			P50AgeSeconds: d.P50Seconds, P95AgeSeconds: d.P95Seconds,
		})
	}
	writeReportJSON(w, http.StatusOK, out)
}

func (s *ReportsServer) handleStuckTransfers(w http.ResponseWriter, r *http.Request) {
	rg, ok := s.parseRange(w, r)
	if !ok {
		return
	}
	limit, err := report.ParseLimit(r.URL.Query().Get("limit"))
	if err != nil {
		writeProblem(w, http.StatusBadRequest, "invalid-report-limit", "limit must be an integer from 1 to 100", err.Error())
		return
	}
	days, err := s.Reader.StuckDays(r.Context(), rg)
	if err != nil {
		writeReportError(w)
		return
	}
	latest, err := s.Reader.StuckLatest(r.Context(), rg, limit)
	if err != nil {
		writeReportError(w)
		return
	}
	out := stuckReportDTO{
		reportRangeDTO: reportRangeDTO{rg.From, rg.To},
		Days:           make([]stuckDayDTO, 0, len(days)),
		Latest:         make([]stuckOccurrenceDTO, 0, len(latest)),
	}
	for _, d := range days {
		out.Days = append(out.Days, stuckDayDTO{Day: d.Day.UTC().Format(reportDayLayout), State: d.State, Detections: d.Detections, Transfers: d.Transfers})
	}
	for _, o := range latest {
		out.Latest = append(out.Latest, stuckOccurrenceDTO{
			TransferID: o.TransferID, State: o.State, AgeSeconds: o.AgeSeconds,
			ThresholdSeconds: o.ThresholdSeconds, DetectedAt: o.DetectedAt.UTC(),
		})
	}
	writeReportJSON(w, http.StatusOK, out)
}

func (s *ReportsServer) handleRebalanceRuns(w http.ResponseWriter, r *http.Request) {
	rg, ok := s.parseRange(w, r)
	if !ok {
		return
	}
	days, err := s.Reader.RebalanceDays(r.Context(), rg)
	if err != nil {
		writeReportError(w)
		return
	}
	trend := report.RebalanceTrend(days)
	out := rebalanceReportDTO{reportRangeDTO: reportRangeDTO{rg.From, rg.To}, Days: make([]rebalanceDayDTO, 0, len(trend))}
	for _, d := range trend {
		out.Days = append(out.Days, rebalanceDayDTO{
			Day: d.Day.UTC().Format(reportDayLayout), Runs: d.Runs, Proposals: d.Proposals,
			Rejected: d.Rejected, RejectionRate: d.RejectionRate, StaleFacts: d.StaleFacts,
		})
	}
	writeReportJSON(w, http.StatusOK, out)
}
