package http

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/claudioed/network-inventory-planning/internal/application/usecases"
	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

// Handler exposes the planning API. It accepts an explicit snapshot rather
// than reaching synchronously into sibling bounded contexts.
type Handler struct {
	Generate usecases.GenerateTransferProposals
}

// Routes creates the HTTP surface for this service.
func (h Handler) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", h.health)
	mux.HandleFunc("POST /v1/transfer-proposals:generate", h.generate)
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
