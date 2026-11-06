package http

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/claudioed/network-inventory-planning/internal/application/usecases"
	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

// cancelRequest is the wire shape of POST
// /v1/transfers/{transferId}/cancellation: just the operator's reason.
// The Idempotency-Key HEADER is mandatory (enforced by the route
// middleware); the cancel itself is idempotent on the saga state.
type cancelRequest struct {
	Reason string `json:"reason"`
}

// cancelTransferResponse is the response view: the saga's CURRENT state
// after the cancel.
type cancelTransferResponse struct {
	TransferID      string `json:"transferId"`
	State           string `json:"state"`
	TransferLineID  string `json:"transferLineId"`
	ReservationID   string `json:"reservationId,omitempty"`
	RejectionReason string `json:"rejectionReason,omitempty"`
}

// cancelTransfer serves POST /v1/transfers/{transferId}/cancellation
// (ADR 0022): the operator cancel of the transfer saga.
//
//   - pre-release states (DRAFT/PROPOSED/APPROVED/ALLOCATING): cancelled
//     synchronously, 200 with state CANCELLED;
//   - ALLOCATED: 202 with state RELEASING — the origin hold is being
//     released; inventory-storage's TransferStockAllocationReleased fact
//     settles the saga to CANCELLED;
//   - RELEASING (a retried cancel): 202 again, same state;
//   - CANCELLED (a retried pre-release cancel): 200 again, same state;
//   - PICKED onwards or UNFULFILLABLE: 409 cancel-refused — the stock is
//     physically in motion or the saga already closed;
//   - unknown transfer: 404.
func (h Handler) cancelTransfer(w http.ResponseWriter, r *http.Request) {
	if h.Cancel == nil {
		writeProblem(w, http.StatusServiceUnavailable, "saga-unavailable",
			"Transfer saga is not configured", "this instance runs without the persistence the cancellation requires")
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

	result, err := h.Cancel.Execute(r.Context(), usecases.CancelTransferInput{
		TransferID: transfer.TransferID(chi.URLParam(r, "transferId")),
		Reason:     request.Reason,
	})
	if err != nil {
		writeCancelProblem(w, err)
		return
	}
	status := http.StatusOK
	if result.Async {
		status = http.StatusAccepted
	}
	writeCancelResponse(w, status, result.Transfer)
}

func writeCancelResponse(w http.ResponseWriter, status int, t *transfer.InterWarehouseTransfer) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(cancelTransferResponse{
		TransferID:      string(t.ID()),
		State:           string(t.State()),
		TransferLineID:  t.ID().LineID(),
		ReservationID:   t.ReservationID(),
		RejectionReason: string(t.RejectionReason()),
	})
}

// writeCancelProblem maps use-case errors onto RFC 9457 statuses.
func writeCancelProblem(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, usecases.ErrCancelInvalid):
		writeProblem(w, http.StatusUnprocessableEntity, "invalid-cancel",
			"Invalid cancellation request", err.Error())
	case errors.Is(err, transfer.ErrTransferNotFound):
		writeProblem(w, http.StatusNotFound, "transfer-not-found",
			"Transfer not found", err.Error())
	case errors.Is(err, usecases.ErrCancelRefused):
		writeProblem(w, http.StatusConflict, "cancel-refused",
			"Transfer cannot be cancelled in its current state",
			"the fact-driven tail (PICKED onwards) is never cancellable — the stock is physically in motion — and a terminal transfer stays closed; cancel an ALLOCATED transfer goes through RELEASING (ADR 0022)")
	default:
		writeProblem(w, http.StatusServiceUnavailable, "cancel-unavailable",
			"Cancellation failed", err.Error())
	}
}
