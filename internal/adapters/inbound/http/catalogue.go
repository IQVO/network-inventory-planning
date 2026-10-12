package http

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/claudioed/network-inventory-planning/internal/application/usecases"
	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

// rebalancePolicyEntryDTO is the wire shape of one Rebalance Policy
// catalogue entry (ADR 0015): the EXISTING Policy schema (the same shape
// POST /v1/transfer-proposals:generate accepts) plus the catalogue's
// revision. Outcome is set only on a PUT response.
type rebalancePolicyEntryDTO struct {
	Policy    transfer.Policy `json:"policy"`
	Revision  int64           `json:"revision"`
	UpdatedAt time.Time       `json:"updatedAt"`
	Outcome   string          `json:"outcome,omitempty"`
}

// transferLaneEntryDTO is the wire shape of one Transfer Lane catalogue
// entry: the EXISTING Lane schema plus the revision.
type transferLaneEntryDTO struct {
	Lane      transfer.Lane `json:"lane"`
	Revision  int64         `json:"revision"`
	UpdatedAt time.Time     `json:"updatedAt"`
	Outcome   string        `json:"outcome,omitempty"`
}

func policyDTO(e transfer.RebalancePolicy, outcome usecases.CatalogueOutcome) rebalancePolicyEntryDTO {
	return rebalancePolicyEntryDTO{Policy: e.Policy, Revision: e.Revision, UpdatedAt: e.UpdatedAt, Outcome: string(outcome)}
}

func laneDTO(e transfer.TransferLane, outcome usecases.CatalogueOutcome) transferLaneEntryDTO {
	return transferLaneEntryDTO{Lane: e.Lane, Revision: e.Revision, UpdatedAt: e.UpdatedAt, Outcome: string(outcome)}
}

func catalogueUnavailable(w http.ResponseWriter) {
	writeProblem(w, http.StatusServiceUnavailable, "catalogue-unavailable",
		"Rebalance catalogue is not configured", "this instance runs without the persistence the policy and lane catalogues require")
}

// listPolicies serves GET /v1/rebalance-policies.
func (h Handler) listPolicies(w http.ResponseWriter, r *http.Request) {
	if h.ListPolicies == nil {
		catalogueUnavailable(w)
		return
	}
	entries, err := h.ListPolicies.Execute(r.Context())
	if err != nil {
		writeProblem(w, http.StatusServiceUnavailable, "catalogue-unavailable", "Rebalance policies could not be listed", err.Error())
		return
	}
	out := make([]rebalancePolicyEntryDTO, 0, len(entries))
	for _, e := range entries {
		out = append(out, policyDTO(e, ""))
	}
	writeJSON(w, http.StatusOK, "", struct {
		Policies []rebalancePolicyEntryDTO `json:"policies"`
	}{Policies: out})
}

// putPolicy serves PUT /v1/rebalance-policies: an idempotent upsert of
// the Policy in the body (keyed by its Site + SKU) with optional If-Match
// optimistic concurrency.
func (h Handler) putPolicy(w http.ResponseWriter, r *http.Request) {
	if h.PutPolicy == nil {
		catalogueUnavailable(w)
		return
	}
	ifMatch, ok := parseIfMatch(w, r)
	if !ok {
		return
	}
	var policy transfer.Policy
	if !decodeStrict(w, r, &policy) {
		return
	}
	result, err := h.PutPolicy.Execute(r.Context(), policy, ifMatch)
	if err != nil {
		writeCatalogueProblem(w, err)
		return
	}
	writeJSON(w, putStatus(result.Outcome), etag(result.Entry.Revision), policyDTO(result.Entry, result.Outcome))
}

// listLanes serves GET /v1/transfer-lanes.
func (h Handler) listLanes(w http.ResponseWriter, r *http.Request) {
	if h.ListLanes == nil {
		catalogueUnavailable(w)
		return
	}
	entries, err := h.ListLanes.Execute(r.Context())
	if err != nil {
		writeProblem(w, http.StatusServiceUnavailable, "catalogue-unavailable", "Transfer lanes could not be listed", err.Error())
		return
	}
	out := make([]transferLaneEntryDTO, 0, len(entries))
	for _, e := range entries {
		out = append(out, laneDTO(e, ""))
	}
	writeJSON(w, http.StatusOK, "", struct {
		Lanes []transferLaneEntryDTO `json:"lanes"`
	}{Lanes: out})
}

// putLane serves PUT /v1/transfer-lanes: an idempotent upsert of the Lane
// in the body (keyed by its directed Origin → Destination).
func (h Handler) putLane(w http.ResponseWriter, r *http.Request) {
	if h.PutLane == nil {
		catalogueUnavailable(w)
		return
	}
	ifMatch, ok := parseIfMatch(w, r)
	if !ok {
		return
	}
	var lane transfer.Lane
	if !decodeStrict(w, r, &lane) {
		return
	}
	result, err := h.PutLane.Execute(r.Context(), lane, ifMatch)
	if err != nil {
		writeCatalogueProblem(w, err)
		return
	}
	writeJSON(w, putStatus(result.Outcome), etag(result.Entry.Revision), laneDTO(result.Entry, result.Outcome))
}

// parseIfMatch reads the optional If-Match header: a revision as a
// strong ETag ("3") or bare (3). Anything else is a 400.
func parseIfMatch(w http.ResponseWriter, r *http.Request) (*int64, bool) {
	raw := strings.TrimSpace(r.Header.Get("If-Match"))
	if raw == "" {
		return nil, true
	}
	revision, err := strconv.ParseInt(strings.Trim(raw, `"`), 10, 64)
	if err != nil || revision < 1 {
		writeProblem(w, http.StatusBadRequest, "invalid-if-match",
			"If-Match must carry a catalogue revision", fmt.Sprintf(`got %q; send the ETag of the entry (for example "3")`, raw))
		return nil, false
	}
	return &revision, true
}

func decodeStrict(w http.ResponseWriter, r *http.Request, into any) bool {
	defer func() { _ = r.Body.Close() }()
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(into); err != nil {
		writeProblem(w, http.StatusBadRequest, "invalid-request", "Invalid request", err.Error())
		return false
	}
	return true
}

func putStatus(outcome usecases.CatalogueOutcome) int {
	if outcome == usecases.CatalogueCreated {
		return http.StatusCreated
	}
	return http.StatusOK
}

func etag(revision int64) string { return strconv.Quote(strconv.FormatInt(revision, 10)) }

func writeJSON(w http.ResponseWriter, status int, entityTag string, body any) {
	w.Header().Set("Content-Type", "application/json")
	if entityTag != "" {
		w.Header().Set("ETag", entityTag)
	}
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

// writeCatalogueProblem maps catalogue use-case errors onto RFC 7807.
func writeCatalogueProblem(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, transfer.ErrInvalidCatalogueEntry):
		writeProblem(w, http.StatusUnprocessableEntity, "invalid-catalogue-entry", "Invalid catalogue entry", err.Error())
	case errors.Is(err, usecases.ErrCatalogueVersionMismatch):
		writeProblem(w, http.StatusPreconditionFailed, "version-mismatch",
			"If-Match does not match the current revision", "re-fetch the entry and retry with its current ETag")
	case errors.Is(err, transfer.ErrCatalogueConcurrentModification):
		writeProblem(w, http.StatusConflict, "concurrent-modification",
			"The entry was modified concurrently", "another writer changed this entry; re-fetch and retry")
	default:
		writeProblem(w, http.StatusServiceUnavailable, "catalogue-unavailable", "Catalogue write could not be completed", err.Error())
	}
}
