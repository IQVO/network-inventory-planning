package http

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/claudioed/network-inventory-planning/internal/application/usecases"
	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

// Handler exposes the planning API. It accepts an explicit snapshot rather
// than reaching synchronously into sibling bounded contexts, and serves the
// advisory-only simulation from the LOCAL read models.
type Handler struct {
	Generate usecases.GenerateTransferProposals
	// Simulate is the read-model path (Phase 1). It is nil-able: a
	// service run without DATABASE_URL/consumers has no read models, and
	// the endpoint then answers 503 rather than fabricating a snapshot.
	Simulate *usecases.SimulateTransferOptions
	// Approve is the Phase-2 saga approval path (nil-able with the same
	// meaning as Simulate: unconfigured means 503, never a fabricated
	// approval).
	Approve *usecases.ApproveTransfer
	// ListRebalanceRuns serves GET /v1/rebalance-runs (nil-able: an
	// instance without persistence answers 503, never an empty 200 that
	// would look like a real run history).
	ListRebalanceRuns *usecases.ListRebalanceRuns
}

// Routes creates the HTTP surface for this service.
func (h Handler) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", h.health)
	mux.HandleFunc("POST /v1/transfer-proposals:generate", h.generate)
	mux.HandleFunc("GET /v1/transfer-simulations", h.simulate)
	mux.HandleFunc("POST /v1/transfers:approve", h.approve)
	mux.HandleFunc("GET /v1/rebalance-runs", h.rebalanceRuns)
	return mux
}

type generateRequest struct {
	AsOf      time.Time           `json:"asOf"`
	Positions []transfer.Position `json:"positions"`
	Policies  []transfer.Policy   `json:"policies"`
	Lanes     []transfer.Lane     `json:"lanes"`
}

type problem struct {
	Type   string `json:"type"`
	Title  string `json:"title"`
	Status int    `json:"status"`
	Detail string `json:"detail"`
}

func (h Handler) health(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusNoContent)
}

// simulateSiteDTO is the wire shape of one site's advisory view.
type simulateSiteDTO struct {
	Site               string    `json:"site"`
	OriginEnabled      bool      `json:"originEnabled"`
	DestinationEnabled bool      `json:"destinationEnabled"`
	TotalDemand        int       `json:"totalDemand"`
	CapacityOverWindow float64   `json:"capacityOverWindow"`
	CapacityHeadroom   int       `json:"capacityHeadroom"`
	WindowStart        time.Time `json:"windowStart"`
	WindowEnd          time.Time `json:"windowEnd"`
}

// simulate serves the advisory-only simulation from the local read models.
// Fail-closed read models yield 503 (Service Unavailable): the endpoint
// never degrades into an empty-but-200 answer that would look like a valid
// simulation of nothing.
func (h Handler) simulate(w http.ResponseWriter, r *http.Request) {
	if h.Simulate == nil {
		writeProblem(w, http.StatusServiceUnavailable, "read-models-unavailable",
			"Planning read models are not configured", "this instance runs without the Phase-1 read models; the simulation requires them")
		return
	}
	result, err := h.Simulate.Execute(r.Context())
	if err != nil {
		writeProblem(w, http.StatusServiceUnavailable, "read-models-incomplete",
			"Planning read models are incomplete or stale", err.Error())
		return
	}
	sites := make([]simulateSiteDTO, 0, len(result.Sites))
	for _, s := range result.Sites {
		sites = append(sites, simulateSiteDTO{
			Site: s.Site, OriginEnabled: s.OriginEnabled, DestinationEnabled: s.DestinationEnabled,
			TotalDemand: s.TotalDemand, CapacityOverWindow: s.CapacityOverWindow,
			CapacityHeadroom: s.CapacityHeadroom, WindowStart: s.WindowStart, WindowEnd: s.WindowEnd,
		})
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(struct {
		Advisory bool              `json:"advisory"`
		AsOf     time.Time         `json:"asOf"`
		Sites    []simulateSiteDTO `json:"sites"`
	}{Advisory: result.Advisory, AsOf: result.AsOf, Sites: sites})
}

func (h Handler) generate(w http.ResponseWriter, r *http.Request) {
	defer func() { _ = r.Body.Close() }()
	var request generateRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		writeProblem(w, http.StatusBadRequest, "invalid-request", "Invalid request", err.Error())
		return
	}
	proposals, err := h.Generate.Execute(usecases.GenerateInput{
		AsOf: request.AsOf, Positions: request.Positions, Policies: request.Policies, Lanes: request.Lanes,
	})
	if err != nil {
		writeProblem(w, http.StatusUnprocessableEntity, "invalid-planning-snapshot", "Invalid planning snapshot", err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(struct {
		Proposals []transfer.Proposal `json:"proposals"`
	}{Proposals: proposals})
}

func writeProblem(w http.ResponseWriter, status int, kind, title, detail string) {
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(problem{Type: "https://warehouse.example/problems/" + kind, Title: title, Status: status, Detail: detail})
}

// approveRequest is the wire shape of POST /v1/transfers:approve: a
// proposal snapshot plus operator context. The Idempotency-Key HEADER is
// mandatory (fleet idempotency rule) and is the transfer-level idempotency
// key.
type approveRequest struct {
	OriginSiteID      string    `json:"originSiteId"`
	DestinationSiteID string    `json:"destinationSiteId"`
	SKU               string    `json:"sku"`
	Quantity          int       `json:"quantity"`
	PolicyVersion     string    `json:"policyVersion"`
	OperatorReason    string    `json:"operatorReason"`
	ProposalAsOf      time.Time `json:"proposalAsOf"`
}

// approveTransferDTO is the response view of the persisted saga state.
type approveTransferDTO struct {
	TransferID        string    `json:"transferId"`
	State             string    `json:"state"`
	OriginSiteID      string    `json:"originSiteId"`
	DestinationSiteID string    `json:"destinationSiteId"`
	SKU               string    `json:"sku"`
	Quantity          int       `json:"quantity"`
	PolicyVersion     string    `json:"policyVersion"`
	ReservationID     string    `json:"reservationId,omitempty"`
	RejectionReason   string    `json:"rejectionReason,omitempty"`
	Replayed          bool      `json:"replayed"`
	ExpiresAt         time.Time `json:"expiresAt"`
	TransferLineID    string    `json:"transferLineId"`
}

// approve serves POST /v1/transfers:approve: the Phase-2 saga approval.
// Fail-closed read models refuse with 422/503; a replayed Idempotency-Key
// with the same payload answers 200 with replayed: true; a DIFFERENT
// payload under a reused key is a 409 problem.
func (h Handler) approve(w http.ResponseWriter, r *http.Request) {
	if h.Approve == nil {
		writeProblem(w, http.StatusServiceUnavailable, "saga-unavailable",
			"Transfer saga is not configured", "this instance runs without the read models and persistence the approval requires")
		return
	}
	idempotencyKey := r.Header.Get("Idempotency-Key")
	if idempotencyKey == "" {
		writeProblem(w, http.StatusBadRequest, "idempotency-key-required",
			"Idempotency-Key header is required", "the approval endpoint is idempotency-keyed; send a fresh key per logical approval")
		return
	}
	defer func() { _ = r.Body.Close() }()
	var request approveRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		writeProblem(w, http.StatusBadRequest, "invalid-request", "Invalid request", err.Error())
		return
	}

	result, err := h.Approve.Execute(r.Context(), usecases.ApproveTransferInput{
		IdempotencyKey:    idempotencyKey,
		OriginSiteID:      request.OriginSiteID,
		DestinationSiteID: request.DestinationSiteID,
		SKU:               request.SKU,
		Quantity:          request.Quantity,
		PolicyVersion:     request.PolicyVersion,
		OperatorReason:    request.OperatorReason,
		ProposalAsOf:      request.ProposalAsOf,
	})
	if err != nil {
		writeApproveProblem(w, err)
		return
	}
	dto := approveTransferDTO{
		TransferID:        string(result.TransferID),
		State:             string(result.Transfer.State()),
		OriginSiteID:      result.Transfer.OriginSiteID(),
		DestinationSiteID: result.Transfer.DestinationSiteID(),
		SKU:               result.Transfer.SKU(),
		Quantity:          result.Transfer.Quantity(),
		PolicyVersion:     result.Transfer.PolicyVersion(),
		ReservationID:     result.Transfer.ReservationID(),
		RejectionReason:   string(result.Transfer.RejectionReason()),
		Replayed:          result.Replayed,
		ExpiresAt:         result.Transfer.ExpiresAt(),
		TransferLineID:    result.TransferID.LineID(),
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(dto)
}

// writeApproveProblem maps use-case errors onto RFC 7807 statuses.
func writeApproveProblem(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, usecases.ErrIdempotencyConflict):
		writeProblem(w, http.StatusConflict, "idempotency-conflict",
			"Idempotency key reused with a different request", err.Error())
	case errors.Is(err, usecases.ErrWorkReleaseNotConfigured):
		writeProblem(w, http.StatusServiceUnavailable, "config-incomplete",
			"Work release is not configured", "set TRANSFER_PICK_PATH_ID and TRANSFER_PICK_CPT_OFFSET so an approved transfer's pick demand can be released (ADR 0005); approval is refused while unset")
	case errors.Is(err, usecases.ErrInvalidApproval):
		writeProblem(w, http.StatusUnprocessableEntity, "invalid-approval",
			"Invalid approval request", err.Error())
	case errors.Is(err, transfer.ErrFactsIncomplete):
		writeProblem(w, http.StatusUnprocessableEntity, "facts-incomplete",
			"Planning facts are incomplete or stale for approval", err.Error())
	case errors.Is(err, transfer.ErrProposalExpired):
		writeProblem(w, http.StatusUnprocessableEntity, "proposal-expired",
			"The proposal snapshot has expired", err.Error())
	default:
		writeProblem(w, http.StatusServiceUnavailable, "approval-unavailable",
			"Approval could not be completed", err.Error())
	}
}

// rebalanceRuns serves GET /v1/rebalance-runs: the scheduled-rebalance
// run history, newest first (ADR 0007). ?limit=N bounds the page
// (default 50, max 200).
func (h Handler) rebalanceRuns(w http.ResponseWriter, r *http.Request) {
	if h.ListRebalanceRuns == nil {
		writeProblem(w, http.StatusServiceUnavailable, "rebalance-runs-unavailable",
			"Rebalance run history is not configured", "this instance runs without the persistence the run history requires")
		return
	}
	limit := 50
	if raw := r.URL.Query().Get("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 || parsed > 200 {
			writeProblem(w, http.StatusBadRequest, "invalid-limit",
				"limit must be an integer between 1 and 200", fmt.Sprintf("got %q", raw))
			return
		}
		limit = parsed
	}
	runs, err := h.ListRebalanceRuns.Execute(r.Context(), limit)
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
	_ = json.NewEncoder(w).Encode(struct {
		Runs []rebalanceRunDTO `json:"runs"`
	}{Runs: dtos})
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
