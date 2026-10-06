package http

import (
	"encoding/json"
	"net/http"
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
}

// Routes creates the HTTP surface for this service.
func (h Handler) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", h.health)
	mux.HandleFunc("POST /v1/transfer-proposals:generate", h.generate)
	mux.HandleFunc("GET /v1/transfer-simulations", h.simulate)
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
