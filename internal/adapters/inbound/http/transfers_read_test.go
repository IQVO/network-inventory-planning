package http

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/claudioed/network-inventory-planning/internal/adapters/inbound/http/page"
	"github.com/claudioed/network-inventory-planning/internal/application/usecases"
	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

var readAt = time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)

// stubTransferQuery implements ports.TransferQuery.
type stubTransferQuery struct {
	byID      map[string]*transfer.InterWarehouseTransfer
	page      transfer.TransferPage
	err       error
	gotFilter transfer.ListFilter
}

func (s *stubTransferQuery) Get(_ context.Context, id transfer.TransferID) (*transfer.InterWarehouseTransfer, error) {
	if s.err != nil {
		return nil, s.err
	}
	t, ok := s.byID[string(id)]
	if !ok {
		return nil, transfer.ErrTransferNotFound
	}
	return t, nil
}

func (s *stubTransferQuery) List(_ context.Context, f transfer.ListFilter) (transfer.TransferPage, error) {
	s.gotFilter = f
	return s.page, s.err
}

// allocatedTransfer rehydrates a transfer that has progressed to ALLOCATED
// with a reservation, so the DTO's optional fields are populated.
func allocatedTransfer() *transfer.InterWarehouseTransfer {
	return transfer.Rehydrate(transfer.Snapshot{
		ID: "tr-1", IdempotencyKey: "k", OriginSiteID: "WH1", DestinationSiteID: "WH2", SKU: "SKU-1", Quantity: 10,
		PolicyVersion: "p1", OperatorReason: "rebalance", ProposalAsOf: readAt, ExpiresAt: readAt.Add(24 * time.Hour),
		State: transfer.StatePicked, ReservationID: "res-9", PickedQuantity: 8,
		Audit: []transfer.AuditEntry{
			{Seq: 1, From: "", To: transfer.StateDraft, Event: "TransferDrafted", Reason: "drafted", OccurredAt: readAt},
			{Seq: 2, From: transfer.StateDraft, To: transfer.StateProposed, Event: "TransferProposed", Reason: "rebalance", OccurredAt: readAt.Add(time.Minute)},
		},
		CreatedAt: readAt, UpdatedAt: readAt.Add(time.Hour), Version: 2,
	})
}

func readHandler(q *stubTransferQuery) Handler {
	return Handler{
		GetTransfer:   &usecases.GetTransfer{Query: q},
		ListTransfers: &usecases.ListTransfers{Query: q},
	}
}

func serve(h Handler, target string) *httptest.ResponseRecorder {
	response := httptest.NewRecorder()
	h.Routes().ServeHTTP(response, httptest.NewRequest(http.MethodGet, target, nil))
	return response
}

func TestGetTransferReturnsTransferWithAuditTrail(t *testing.T) {
	q := &stubTransferQuery{byID: map[string]*transfer.InterWarehouseTransfer{"tr-1": allocatedTransfer()}}
	response := serve(readHandler(q), "/v1/transfers/tr-1")

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", response.Code, response.Body.String())
	}
	if ct := response.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("Content-Type = %q", ct)
	}
	type auditBody struct {
		Seq        int64  `json:"seq"`
		From       string `json:"from"`
		To         string `json:"to"`
		Event      string `json:"event"`
		Cause      string `json:"cause"`
		OccurredAt string `json:"occurredAt"`
	}
	type transferBody struct {
		ID                string `json:"id"`
		State             string `json:"state"`
		SKU               string `json:"sku"`
		Quantity          int    `json:"quantity"`
		PickedQuantity    int    `json:"pickedQuantity"`
		OriginSiteID      string `json:"originSiteId"`
		DestinationSiteID string `json:"destinationSiteId"`
		ReservationID     string `json:"reservationId"`
		CreatedAt         string `json:"createdAt"`
		UpdatedAt         string `json:"updatedAt"`
		Version           int64  `json:"version"`
	}
	var got struct {
		transferBody
		Audit []auditBody `json:"audit"`
	}
	if err := json.NewDecoder(response.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	wantTransfer := transferBody{
		ID: "tr-1", State: "PICKED", SKU: "SKU-1", Quantity: 10, PickedQuantity: 8, OriginSiteID: "WH1", DestinationSiteID: "WH2",
		ReservationID: "res-9", CreatedAt: "2026-10-06T12:00:00Z", UpdatedAt: "2026-10-06T13:00:00Z", Version: 2,
	}
	if got.transferBody != wantTransfer {
		t.Fatalf("transfer = %+v, want %+v", got.transferBody, wantTransfer)
	}
	wantAudit := []auditBody{
		{Seq: 1, To: "DRAFT", Event: "TransferDrafted", Cause: "drafted", OccurredAt: "2026-10-06T12:00:00Z"},
		{Seq: 2, From: "DRAFT", To: "PROPOSED", Event: "TransferProposed", Cause: "rebalance", OccurredAt: "2026-10-06T12:01:00Z"},
	}
	if len(got.Audit) != len(wantAudit) {
		t.Fatalf("audit = %+v, want %+v", got.Audit, wantAudit)
	}
	for i := range wantAudit {
		if got.Audit[i] != wantAudit[i] {
			t.Fatalf("audit[%d] = %+v, want %+v", i, got.Audit[i], wantAudit[i])
		}
	}
}

func TestGetTransferOmitsFactsTheSagaHasNotProduced(t *testing.T) {
	proposed, err := transfer.ProposeTransfer(transfer.ProposalInput{
		ID: "tr-2", IdempotencyKey: "k2", OriginSiteID: "WH1", DestinationSiteID: "WH2", SKU: "SKU-1", Quantity: 3,
		PolicyVersion: "p1", ProposalAsOf: readAt, ExpiresAt: readAt.Add(time.Hour), Now: readAt,
	})
	if err != nil {
		t.Fatal(err)
	}
	q := &stubTransferQuery{byID: map[string]*transfer.InterWarehouseTransfer{"tr-2": proposed}}
	var raw map[string]any
	if err := json.NewDecoder(serve(readHandler(q), "/v1/transfers/tr-2").Body).Decode(&raw); err != nil {
		t.Fatal(err)
	}
	for _, absent := range []string{"reservationId", "pickedQuantity", "rejectionReason"} {
		if _, present := raw[absent]; present {
			t.Errorf("%s must be omitted before the saga produces it", absent)
		}
	}
}

func TestGetTransferUnknownIDIsA404Problem(t *testing.T) {
	response := serve(readHandler(&stubTransferQuery{}), "/v1/transfers/nope")
	if response.Code != http.StatusNotFound {
		t.Fatalf("status = %d: %s", response.Code, response.Body.String())
	}
	if ct := response.Header().Get("Content-Type"); ct != "application/problem+json" {
		t.Fatalf("Content-Type = %q", ct)
	}
	var p problem
	if err := json.NewDecoder(response.Body).Decode(&p); err != nil {
		t.Fatal(err)
	}
	if p.Status != http.StatusNotFound || p.Type != "https://warehouse.example/problems/transfer-not-found" {
		t.Fatalf("problem = %+v", p)
	}
}

func TestGetTransferStoreFailureIs503WithoutLeakingDetail(t *testing.T) {
	q := &stubTransferQuery{err: errors.New("pq: password authentication failed for user secret")}
	response := serve(readHandler(q), "/v1/transfers/tr-1")
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d: %s", response.Code, response.Body.String())
	}
	if body := response.Body.String(); strings.Contains(body, "secret") {
		t.Fatalf("infrastructure detail leaked: %s", body)
	}
}

func TestReadSideUnconfiguredIs503(t *testing.T) {
	for _, target := range []string{"/v1/transfers", "/v1/transfers/tr-1"} {
		response := serve(Handler{}, target)
		if response.Code != http.StatusServiceUnavailable {
			t.Fatalf("%s status = %d: %s", target, response.Code, response.Body.String())
		}
		if ct := response.Header().Get("Content-Type"); ct != "application/problem+json" {
			t.Fatalf("%s Content-Type = %q", target, ct)
		}
	}
}

func TestListTransfersMapsQueryParametersAndPage(t *testing.T) {
	q := &stubTransferQuery{page: transfer.TransferPage{Items: []*transfer.InterWarehouseTransfer{allocatedTransfer(), allocatedTransfer()}}}
	response := serve(readHandler(q), "/v1/transfers?state=picked&originSiteId=WH1&destinationSiteId=WH2&limit=1")

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", response.Code, response.Body.String())
	}
	f := q.gotFilter
	// The store is asked for limit+1 rows: the extra one proves a next page.
	if len(f.States) != 1 || f.States[0] != transfer.StatePicked || f.OriginSiteID != "WH1" || f.DestinationSiteID != "WH2" || f.Limit != 2 || f.After != nil || f.Offset != 0 {
		t.Fatalf("filter = %+v", f)
	}
	var got struct {
		Items      []map[string]any `json:"items"`
		NextCursor string           `json:"nextCursor"`
	}
	if err := json.NewDecoder(response.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if len(got.Items) != 1 || got.Items[0]["id"] != "tr-1" || got.NextCursor == "" {
		t.Fatalf("page = %+v", got)
	}
	if _, hasAudit := got.Items[0]["audit"]; hasAudit {
		t.Fatal("the audit trail belongs to the single-get view only")
	}

	// The cursor continues exactly after the last returned row.
	serve(readHandler(q), "/v1/transfers?limit=1&cursor="+got.NextCursor)
	if q.gotFilter.After == nil || q.gotFilter.After.ID != "tr-1" {
		t.Fatalf("keyset after = %+v, want id tr-1", q.gotFilter.After)
	}
}

func TestListTransfersLastPageHasNoNextCursor(t *testing.T) {
	response := serve(readHandler(&stubTransferQuery{}), "/v1/transfers")
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", response.Code, response.Body.String())
	}
	var raw map[string]json.RawMessage
	if err := json.NewDecoder(response.Body).Decode(&raw); err != nil {
		t.Fatal(err)
	}
	if string(raw["items"]) != "[]" {
		t.Fatalf("items = %s, want []", raw["items"])
	}
	if _, has := raw["nextCursor"]; has {
		t.Fatalf("nextCursor must be absent on the last page: %s", raw["nextCursor"])
	}
}

func TestListTransfersBadQueryIs400Problem(t *testing.T) {
	for target, want := range map[string]string{
		"/v1/transfers?state=FLYING":      "invalid-query",
		"/v1/transfers?limit=0":           "invalid-limit",
		"/v1/transfers?limit=501":         "invalid-limit",
		"/v1/transfers?limit=abc":         "invalid-limit",
		"/v1/transfers?limit=":            "invalid-limit",
		"/v1/transfers?limit=1&limit=2":   "invalid-limit",
		"/v1/transfers?cursor=%21%21":     "invalid-cursor",
		"/v1/transfers?cursor=a&cursor=b": "invalid-cursor",
		"/v1/transfers?offset=1":          "unknown-query-parameter",
		"/v1/transfers?after=x":           "unknown-query-parameter",
		"/v1/transfers?nextToken=x":       "unknown-query-parameter",
		"/v1/transfers?site=WH1":          "unknown-query-parameter",
	} {
		response := serve(readHandler(&stubTransferQuery{}), target)
		if response.Code != http.StatusBadRequest {
			t.Fatalf("%s status = %d: %s", target, response.Code, response.Body.String())
		}
		if ct := response.Header().Get("Content-Type"); ct != "application/problem+json" {
			t.Fatalf("%s Content-Type = %q", target, ct)
		}
		if got := problemType(t, response); got != want {
			t.Fatalf("%s problem = %s, want %s", target, got, want)
		}
	}
}

func TestListTransfersCursorOfAnotherCollectionIsInvalidCursor(t *testing.T) {
	q := &stubTransferQuery{err: transfer.ErrInvalidPageAfter}
	response := serve(readHandler(q), "/v1/transfers?cursor="+page.Encode(page.Position{Sort: "not-a-time", ID: "x"}))
	if response.Code != http.StatusBadRequest || problemType(t, response) != "invalid-cursor" {
		t.Fatalf("status = %d: %s", response.Code, response.Body.String())
	}
}

func TestListTransfersStoreFailureIs503(t *testing.T) {
	response := serve(readHandler(&stubTransferQuery{err: errors.New("boom")}), "/v1/transfers")
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d: %s", response.Code, response.Body.String())
	}
}

func TestReadRoutesDoNotShadowTheApprovalRoute(t *testing.T) {
	// POST /v1/transfer-approvals must still reach the approval handler (503
	// unconfigured), not the GET /v1/transfers/{id} read route.
	response := httptest.NewRecorder()
	h := readHandler(&stubTransferQuery{})
	h.Idempotency = newFakeIdempotency()
	request := httptest.NewRequest(http.MethodPost, "/v1/transfer-approvals", nil)
	request.Header.Set("Idempotency-Key", "shadow-1")
	h.Routes().ServeHTTP(response, request)
	var p problem
	if err := json.NewDecoder(response.Body).Decode(&p); err != nil {
		t.Fatal(err)
	}
	if p.Type != "https://warehouse.example/problems/saga-unavailable" {
		t.Fatalf("problem = %+v", p)
	}
}
