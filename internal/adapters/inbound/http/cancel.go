package http

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/claudioed/network-inventory-planning/internal/application/usecases"
	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

// cancelSuffix is the custom-method suffix of POST /v1/transfers/{id}:cancel.
// net/http's mux cannot express a literal after a wildcard inside one path
// segment, so the route captures the whole segment and the handler splits it.
const cancelSuffix = ":cancel"

// cancelRequest is the wire shape of POST /v1/transfers/{id}:cancel.
type cancelRequest struct {
	Reason string `json:"reason"`
}

// cancelTransfer serves POST /v1/transfers/{id}:cancel: the operator's
// PRE-RELEASE cancel (ADR 0011). REST only — the MCP surface stays
// read-only (ADR 0008). 200 with the transfer view (an already CANCELLED
// transfer is an idempotent 200), 404 unknown transfer, 409
// transfer-not-cancellable past release, 422 invalid-cancellation for a
// blank reason, 503 when unconfigured.
func (h Handler) cancelTransfer(w http.ResponseWriter, r *http.Request) {
	id, ok := strings.CutSuffix(r.PathValue("idAction"), cancelSuffix)
	if !ok || id == "" {
		writeProblem(w, http.StatusNotFound, "not-found", "Not found", "the only POST action on a transfer is {id}:cancel")
		return
	}
	if h.Cancel == nil {
		writeProblem(w, http.StatusServiceUnavailable, "cancel-unavailable",
			"Transfer cancellation is not configured", "this instance runs without the persistence and outbox the cancel requires")
		return
	}
	defer func() { _ = r.Body.Close() }()
	var request cancelRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		writeProblem(w, http.StatusBadRequest, "invalid-request", "Invalid request", err.Error())
		return
	}
	result, err := h.Cancel.Execute(r.Context(), usecases.CancelTransferInput{TransferID: id, Reason: request.Reason})
	if err != nil {
		writeCancelProblem(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(toTransferDTO(result.Transfer))
}

// writeCancelProblem maps cancel use-case errors onto RFC 7807 statuses.
func writeCancelProblem(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, usecases.ErrInvalidCancellation):
		writeProblem(w, http.StatusUnprocessableEntity, "invalid-cancellation", "Invalid cancellation request", err.Error())
	case errors.Is(err, transfer.ErrTransferNotFound):
		writeProblem(w, http.StatusNotFound, "transfer-not-found", "Transfer not found", err.Error())
	case errors.Is(err, usecases.ErrTransferNotCancellable):
		writeProblem(w, http.StatusConflict, "transfer-not-cancellable",
			"Transfer can no longer be cancelled", err.Error())
	default:
		writeProblem(w, http.StatusServiceUnavailable, "cancel-unavailable",
			"Cancellation could not be completed", err.Error())
	}
}
