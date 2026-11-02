package http

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/claudioed/network-inventory-planning/internal/application/usecases"
	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

// approveRequest is the wire shape of POST /v1/transfer-approvals: a
// proposal snapshot plus operator context. The Idempotency-Key HEADER is
// mandatory (enforced by the route middleware) and is the transfer-level
// idempotency key.
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
	TransferID        string `json:"transferId"`
	State             string `json:"state"`
	OriginSiteID      string `json:"originSiteId"`
	DestinationSiteID string `json:"destinationSiteId"`
	SKU               string `json:"sku"`
	Quantity          int    `json:"quantity"`
	// CaseRounding: see transferDTO (ADR 0017).
	*transfer.CaseRounding
	PolicyVersion   string    `json:"policyVersion"`
	ReservationID   string    `json:"reservationId,omitempty"`
	RejectionReason string    `json:"rejectionReason,omitempty"`
	Replayed        bool      `json:"replayed"`
	ExpiresAt       time.Time `json:"expiresAt"`
	TransferLineID  string    `json:"transferLineId"`
}

// approve serves POST /v1/transfer-approvals: the Phase-2 saga approval.
// Fail-closed read models refuse with 422/503; a replayed Idempotency-Key
// with the same payload answers 200 with replayed: true; a DIFFERENT
// payload under a reused key is a 409 problem.
func (h Handler) approve(w http.ResponseWriter, r *http.Request) {
	if h.Approve == nil {
		writeProblem(w, http.StatusServiceUnavailable, "saga-unavailable",
			"Transfer saga is not configured", "this instance runs without the read models and persistence the approval requires")
		return
	}
	// The Idempotency-Key middleware has already validated the header; the
	// key is also the transfer-level idempotency key of the saga.
	idempotencyKey := r.Header.Get(idempotencyKeyHeader)
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
		CaseRounding:      result.Transfer.CaseRounding(),
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
		writeProblem(w, http.StatusConflict, "idempotency-key-reused",
			"Idempotency-Key was already used with a different request", err.Error())
	case errors.Is(err, usecases.ErrWorkReleaseNotConfigured):
		writeProblem(w, http.StatusServiceUnavailable, "config-incomplete",
			"Work release is not configured", "set TRANSFER_PICK_PATH_ID and TRANSFER_PICK_CPT_OFFSET so an approved transfer's pick demand can be released (ADR 0005); approval is refused while unset")
	case errors.Is(err, transfer.ErrBelowOneCase):
		writeProblem(w, http.StatusUnprocessableEntity, "below-one-case",
			"The origin cannot send one whole case", err.Error())
	case errors.Is(err, usecases.ErrPackLevelsUnavailable):
		writeProblem(w, http.StatusServiceUnavailable, "pack-levels-unavailable",
			"Pack levels are unavailable", err.Error())
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
