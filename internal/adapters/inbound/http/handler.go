package http

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/riandyrn/otelchi"
	otelchimetric "github.com/riandyrn/otelchi/metric"

	"github.com/claudioed/network-inventory-planning/internal/application/ports"
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
	// Cancel is the operator cancel path of the saga (ADR 0022): the
	// pre-release synchronous cancel and the ALLOCATED transfer's
	// RELEASING compensation. Nil-able with the same meaning.
	Cancel *usecases.CancelTransfer
	// ListRebalanceRuns serves GET /v1/rebalance-runs (nil-able: an
	// instance without persistence answers 503, never an empty 200 that
	// would look like a real run history).
	ListRebalanceRuns *usecases.ListRebalanceRuns
	// GetTransfer and ListTransfers are the read side of the saga store
	// (GET /v1/transfers/{id}, GET /v1/transfers). Nil-able like the
	// others: unconfigured means 503, never a fabricated empty answer.
	GetTransfer   *usecases.GetTransfer
	ListTransfers *usecases.ListTransfers
	// PutPolicy/ListPolicies and PutLane/ListLanes serve the
	// operator-managed Rebalance Policy and Transfer Lane catalogues
	// (ADR 0015). Nil-able: unconfigured means 503.
	PutPolicy    *usecases.PutRebalancePolicy
	ListPolicies *usecases.ListRebalancePolicies
	PutLane      *usecases.PutTransferLane
	ListLanes    *usecases.ListTransferLanes
	// Idempotency backs the Idempotency-Key middleware of every POST route
	// (fleet REST guideline section 2). Nil-able like the others:
	// unconfigured means the keyed routes answer 503, never an unkeyed
	// execution.
	Idempotency ports.IdempotencyStore
}

// DefaultServiceName is the OTel instrumentation scope for the HTTP surface.
// The OTel *service.name* resource attribute (what Prometheus/Grafana filter
// on) is set by telemetry.Setup from the same string at the composition root.
const DefaultServiceName = "network-inventory-planning"

// Routes creates the HTTP surface for this service: a chi router carrying
// the fleet's Tier-1 HTTP RED middleware stack (ADR-0013), in the mandated
// order -- request ID, otelchi tracing, otelchi's RED duration metric, the
// request logger, then panic recovery. otelchi.Middleware and
// otelchimetric.NewServerRequestDuration resolve the global MeterProvider
// at construction, so callers must install it (telemetry.Setup) before
// calling Routes.
func (h Handler) Routes() http.Handler {
	r := chi.NewRouter()
	metricCfg := otelchimetric.NewBaseConfig(DefaultServiceName)

	r.Use(middleware.RequestID)
	r.Use(otelchi.Middleware(DefaultServiceName, otelchi.WithChiRoutes(r)))
	// Emits http.server.request.duration (seconds) per OTel HTTP semantic
	// conventions, labelled by http.route (the chi pattern, not the raw
	// path) -- the fleet's Grafana per-context dashboards read this by
	// route.
	r.Use(otelchimetric.NewServerRequestDuration(metricCfg))
	r.Use(RequestLogger(slog.Default()))
	r.Use(middleware.Recoverer)

	r.Get("/healthz", h.health)
	keyed := h.idempotent()
	r.With(keyed).Post("/v1/transfer-proposal-runs", h.generate)
	r.Get("/v1/transfer-simulations", h.simulate)
	r.With(keyed).Post("/v1/transfer-approvals", h.approve)
	r.With(keyed).Post("/v1/transfers/{transferId}/cancellation", h.cancelTransfer)
	r.Get("/v1/rebalance-runs", h.rebalanceRuns)
	r.Get("/v1/transfers", h.listTransfers)
	r.Get("/v1/transfers/{id}", h.getTransfer)
	r.Get("/v1/rebalance-policies", h.listPolicies)
	r.Put("/v1/rebalance-policies", h.putPolicy)
	r.Get("/v1/transfer-lanes", h.listLanes)
	r.Put("/v1/transfer-lanes", h.putLane)
	return r
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
	proposals, err := h.Generate.Execute(r.Context(), usecases.GenerateInput{
		AsOf: request.AsOf, Positions: request.Positions, Policies: request.Policies, Lanes: request.Lanes,
	})
	if errors.Is(err, usecases.ErrPackLevelsUnavailable) {
		writeProblem(w, http.StatusServiceUnavailable, "pack-levels-unavailable",
			"Pack levels are unavailable", err.Error())
		return
	}
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
