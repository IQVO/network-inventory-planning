package http

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/claudioed/network-inventory-planning/internal/application/usecases"
	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

var httpCancelNow = time.Date(2026, 11, 5, 11, 0, 0, 0, time.UTC)

// seedHttpTransfer stores a rehydrated transfer in the fake store.
func seedHttpTransfer(f *httpFakeTransfers, id string, state transfer.TransferState, reservation bool) {
	snap := transfer.Snapshot{
		ID: transfer.TransferID(id), IdempotencyKey: "key-" + id,
		OriginSiteID: "WH1", DestinationSiteID: "WH2", SKU: "SKU-1",
		Quantity: 5, PolicyVersion: "pv-1",
		ProposalAsOf: httpCancelNow.Add(-time.Hour), ExpiresAt: httpCancelNow.Add(24 * time.Hour),
		State:     state,
		Audit:     []transfer.AuditEntry{{To: state, Event: "seeded", Reason: "seed", OccurredAt: httpCancelNow}},
		CreatedAt: httpCancelNow, UpdatedAt: httpCancelNow,
	}
	if reservation {
		snap.ReservationID = "res-1"
		snap.Allocations = []transfer.Allocation{{StockUnitID: "su-1", BinID: "BIN-A", Quantity: 5}}
		snap.AllocationExpiresAt = httpCancelNow.Add(12 * time.Hour)
	}
	t := transfer.Rehydrate(snap)
	f.rows[string(t.ID())] = t
	f.byKey["key-"+id] = string(t.ID())
}

func httpCancelHandler(f *httpFakeTransfers, events *httpFakeEvents) Handler {
	return Handler{
		Idempotency: newFakeIdempotency(),
		Cancel: &usecases.CancelTransfer{
			Transfers: f,
			Events:    events,
			UoW:       httpFakeUoW{},
			Now:       func() time.Time { return httpCancelNow },
		},
	}
}

func httpPostCancel(h Handler, key, transferID string, body []byte) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/v1/transfers/"+transferID+"/cancellation", bytes.NewReader(body))
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	rec := httptest.NewRecorder()
	h.Routes().ServeHTTP(rec, req)
	return rec
}

func cancelBody(reason string) []byte {
	body, _ := json.Marshal(map[string]any{"reason": reason})
	return body
}

func TestCancelEndpointPreReleaseAnswers200Cancelled(t *testing.T) {
	f := newHttpFakeTransfers()
	events := &httpFakeEvents{}
	seedHttpTransfer(f, "trf-pre", transfer.StateAllocating, false)
	h := httpCancelHandler(f, events)

	rec := httpPostCancel(h, "idem-cancel-1", "trf-pre", cancelBody("operator changed mind"))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body.String())
	}
	var dto cancelTransferResponse
	if err := json.NewDecoder(rec.Body).Decode(&dto); err != nil {
		t.Fatal(err)
	}
	if dto.State != string(transfer.StateCancelled) {
		t.Fatalf("state = %s, want CANCELLED", dto.State)
	}
}

func TestCancelEndpointAllocatedAnswers202Releasing(t *testing.T) {
	f := newHttpFakeTransfers()
	events := &httpFakeEvents{}
	seedHttpTransfer(f, "trf-alloc", transfer.StateAllocated, true)
	h := httpCancelHandler(f, events)

	rec := httpPostCancel(h, "idem-cancel-2", "trf-alloc", cancelBody("wrong lane"))
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body.String())
	}
	var dto cancelTransferResponse
	if err := json.NewDecoder(rec.Body).Decode(&dto); err != nil {
		t.Fatal(err)
	}
	if dto.State != string(transfer.StateReleasing) {
		t.Fatalf("state = %s, want RELEASING", dto.State)
	}
	if dto.ReservationID != "res-1" {
		t.Fatalf("reservationId = %q, want res-1 (the hold being released)", dto.ReservationID)
	}
	sawCommand := false
	for _, e := range events.events {
		if e.EventName() == "TransferAllocationReleaseRequested" {
			sawCommand = true
		}
	}
	if !sawCommand {
		t.Fatal("the ALLOCATED cancel must outbox the release command")
	}
}

func TestCancelEndpointRefusesTheFactDrivenTailWith409(t *testing.T) {
	f := newHttpFakeTransfers()
	seedHttpTransfer(f, "trf-picked", transfer.StatePicked, true)
	h := httpCancelHandler(f, &httpFakeEvents{})

	rec := httpPostCancel(h, "idem-cancel-3", "trf-picked", cancelBody("too late"))
	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body.String())
	}
	if !bytes.Contains(rec.Body.Bytes(), []byte("cancel-refused")) {
		t.Fatalf("body = %s, want the cancel-refused problem type", rec.Body.String())
	}
}

func TestCancelEndpointUnknown404AndEmptyReason422(t *testing.T) {
	h := httpCancelHandler(newHttpFakeTransfers(), &httpFakeEvents{})

	rec := httpPostCancel(h, "idem-cancel-4", "trf-ghost", cancelBody("reason"))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body.String())
	}

	f := newHttpFakeTransfers()
	seedHttpTransfer(f, "trf-alloc2", transfer.StateAllocated, true)
	h = httpCancelHandler(f, &httpFakeEvents{})
	rec = httpPostCancel(h, "idem-cancel-5", "trf-alloc2", cancelBody("  "))
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body.String())
	}
	if !bytes.Contains(rec.Body.Bytes(), []byte("invalid-cancel")) {
		t.Fatalf("body = %s, want the invalid-cancel problem type", rec.Body.String())
	}
}

func TestCancelEndpointUnconfiguredYields503(t *testing.T) {
	h := Handler{Idempotency: newFakeIdempotency()}
	rec := httpPostCancel(h, "idem-cancel-6", "trf-any", cancelBody("reason"))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body.String())
	}
}

func TestCancelEndpointRequiresIdempotencyKey(t *testing.T) {
	f := newHttpFakeTransfers()
	seedHttpTransfer(f, "trf-pre2", transfer.StateApproved, false)
	h := httpCancelHandler(f, &httpFakeEvents{})

	rec := httpPostCancel(h, "", "trf-pre2", cancelBody("reason"))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body.String())
	}
}
