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

// transferDTO is the wire shape of one transfer in GET /v1/transfers and
// GET /v1/transfers/{id}. Optional facts (reservation, picked quantity,
// rejection) are omitted until the saga has produced them.
type transferDTO struct {
	ID                string    `json:"id"`
	State             string    `json:"state"`
	SKU               string    `json:"sku"`
	Quantity          int       `json:"quantity"`
	PickedQuantity    int       `json:"pickedQuantity,omitempty"`
	OriginSiteID      string    `json:"originSiteId"`
	DestinationSiteID string    `json:"destinationSiteId"`
	PolicyVersion     string    `json:"policyVersion"`
	ReservationID     string    `json:"reservationId,omitempty"`
	RejectionReason   string    `json:"rejectionReason,omitempty"`
	ExpiresAt         time.Time `json:"expiresAt"`
	CreatedAt         time.Time `json:"createdAt"`
	UpdatedAt         time.Time `json:"updatedAt"`
	Version           int64     `json:"version"`
}

// auditEntryDTO is one state transition of the audit trail: when it
// happened, what moved and the cause that moved it.
type auditEntryDTO struct {
	Seq        int64     `json:"seq"`
	From       string    `json:"from,omitempty"`
	To         string    `json:"to"`
	Event      string    `json:"event"`
	Cause      string    `json:"cause"`
	OccurredAt time.Time `json:"occurredAt"`
}

// transferDetailDTO is the single-get view: the transfer plus its audit trail.
type transferDetailDTO struct {
	transferDTO
	Audit []auditEntryDTO `json:"audit"`
}

// transferListDTO is one page of GET /v1/transfers.
type transferListDTO struct {
	Items  []transferDTO `json:"items"`
	Total  int           `json:"total"`
	Limit  int           `json:"limit"`
	Offset int           `json:"offset"`
}

func toTransferDTO(t *transfer.InterWarehouseTransfer) transferDTO {
	return transferDTO{
		ID:                string(t.ID()),
		State:             string(t.State()),
		SKU:               t.SKU(),
		Quantity:          t.Quantity(),
		PickedQuantity:    t.PickedQuantity(),
		OriginSiteID:      t.OriginSiteID(),
		DestinationSiteID: t.DestinationSiteID(),
		PolicyVersion:     t.PolicyVersion(),
		ReservationID:     t.ReservationID(),
		RejectionReason:   string(t.RejectionReason()),
		ExpiresAt:         t.ExpiresAt(),
		CreatedAt:         t.CreatedAt(),
		UpdatedAt:         t.UpdatedAt(),
		Version:           t.Version(),
	}
}

func toTransferDetailDTO(t *transfer.InterWarehouseTransfer) transferDetailDTO {
	audit := t.Audit()
	entries := make([]auditEntryDTO, 0, len(audit))
	for _, e := range audit {
		entries = append(entries, auditEntryDTO{
			Seq: e.Seq, From: string(e.From), To: string(e.To), Event: e.Event, Cause: e.Reason, OccurredAt: e.OccurredAt,
		})
	}
	return transferDetailDTO{transferDTO: toTransferDTO(t), Audit: entries}
}

// getTransfer serves GET /v1/transfers/{id}: the persisted transfer with
// its audit trail, 404 problem+json for an unknown id.
func (h Handler) getTransfer(w http.ResponseWriter, r *http.Request) {
	if h.GetTransfer == nil {
		writeReadSideUnavailable(w)
		return
	}
	t, err := h.GetTransfer.Execute(r.Context(), r.PathValue("id"))
	if err != nil {
		writeReadProblem(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(toTransferDetailDTO(t))
}

// listTransfers serves GET /v1/transfers (newest first), filtered by state,
// originSiteId and destinationSiteId and paged by limit (<=200) / offset.
func (h Handler) listTransfers(w http.ResponseWriter, r *http.Request) {
	if h.ListTransfers == nil {
		writeReadSideUnavailable(w)
		return
	}
	query := r.URL.Query()
	limit, err := intParam(query.Get("limit"), "limit")
	if err != nil {
		writeReadProblem(w, err)
		return
	}
	offset, err := intParam(query.Get("offset"), "offset")
	if err != nil {
		writeReadProblem(w, err)
		return
	}
	page, err := h.ListTransfers.Execute(r.Context(), usecases.ListTransfersInput{
		State:             query.Get("state"),
		OriginSiteID:      query.Get("originSiteId"),
		DestinationSiteID: query.Get("destinationSiteId"),
		Limit:             limit,
		Offset:            offset,
	})
	if err != nil {
		writeReadProblem(w, err)
		return
	}
	items := make([]transferDTO, 0, len(page.Items))
	for _, t := range page.Items {
		items = append(items, toTransferDTO(t))
	}
	if limit == 0 {
		limit = transfer.DefaultListLimit
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(transferListDTO{Items: items, Total: page.Total, Limit: limit, Offset: offset})
}

// intParam parses an optional non-negative integer query parameter; absent
// means 0 (the use case applies its default). A malformed value is a
// client error, reported through the same invalid-query path.
func intParam(raw, name string) (int, error) {
	if raw == "" {
		return 0, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("%w: %s must be an integer, got %q", usecases.ErrInvalidTransferQuery, name, raw)
	}
	return n, nil
}

func writeReadSideUnavailable(w http.ResponseWriter) {
	writeProblem(w, http.StatusServiceUnavailable, "read-side-unavailable",
		"Transfer read side is not configured", "this instance runs without DATABASE_URL, so there is no transfer store to read")
}

// writeReadProblem maps read-side use-case errors onto RFC 7807 statuses.
func writeReadProblem(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, usecases.ErrInvalidTransferQuery):
		writeProblem(w, http.StatusBadRequest, "invalid-query", "Invalid query", err.Error())
	case errors.Is(err, transfer.ErrTransferNotFound):
		writeProblem(w, http.StatusNotFound, "transfer-not-found", "Transfer not found", err.Error())
	default:
		writeProblem(w, http.StatusServiceUnavailable, "read-side-unavailable", "Transfer read side is unavailable", "the transfer store could not be read")
	}
}
