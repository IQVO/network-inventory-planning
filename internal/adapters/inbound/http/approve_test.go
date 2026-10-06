package http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/claudioed/network-inventory-planning/internal/application/usecases"
	"github.com/claudioed/network-inventory-planning/internal/domain/planning"
	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

// Local in-memory fakes driving the REAL ApproveTransfer use case through
// the real HTTP mux — the full inbound path except Postgres/Kafka.

type httpFakeTransfers struct {
	mu    sync.Mutex
	rows  map[string]*transfer.InterWarehouseTransfer
	byKey map[string]string
}

func newHttpFakeTransfers() *httpFakeTransfers {
	return &httpFakeTransfers{rows: map[string]*transfer.InterWarehouseTransfer{}, byKey: map[string]string{}}
}

func (f *httpFakeTransfers) Create(_ context.Context, t *transfer.InterWarehouseTransfer, key string) (*transfer.InterWarehouseTransfer, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if id, ok := f.byKey[key]; ok {
		return f.rows[id], nil
	}
	f.rows[string(t.ID())] = t
	f.byKey[key] = string(t.ID())
	t.SetVersion(1)
	return nil, nil
}

func (f *httpFakeTransfers) Load(_ context.Context, id transfer.TransferID) (*transfer.InterWarehouseTransfer, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	t, ok := f.rows[string(id)]
	if !ok {
		return nil, transfer.ErrTransferNotFound
	}
	return t, nil
}

func (f *httpFakeTransfers) UpdateState(_ context.Context, t *transfer.InterWarehouseTransfer) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rows[string(t.ID())] = t
	return nil
}

type httpFakeEvents struct {
	mu     sync.Mutex
	events []transfer.DomainEvent
}

func (f *httpFakeEvents) Publish(_ context.Context, events ...transfer.DomainEvent) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, events...)
	return nil
}

type httpFakeSnapshot struct {
	facts planning.Facts
}

func (f httpFakeSnapshot) Load(ctx context.Context) (planning.Facts, error) { return f.facts, nil }

type httpFakeUoW struct{}

func (httpFakeUoW) Do(ctx context.Context, fn func(ctx context.Context) error) error { return fn(ctx) }

var httpApproveNow = time.Date(2026, 10, 6, 15, 0, 0, 0, time.UTC)

func httpApproveFacts() planning.Facts {
	due := httpApproveNow.Add(2 * time.Hour)
	cap1, _ := planning.NewSiteCapability("WH1", true, true, 3, httpApproveNow.Add(-time.Minute))
	cap2, _ := planning.NewSiteCapability("WH2", true, true, 3, httpApproveNow.Add(-time.Minute))
	d1, _ := planning.NewSiteSkuDemand("ord-1", 1, "WH1", "SKU-1", 40, due, planning.DemandActive, "static-site-v1")
	d1.AsOf = httpApproveNow.Add(-time.Minute)
	d2, _ := planning.NewSiteSkuDemand("ord-2", 1, "WH2", "SKU-1", 10, due, planning.DemandActive, "static-site-v1")
	d2.AsOf = httpApproveNow.Add(-time.Minute)
	plan1, _ := planning.NewPublishedCapacityPlan(planning.PublishedCapacityPlan{
		PlanID: "plan-1", SiteID: "WH1", Location: "PATH-ZONE-A", PathID: "pick-rebin-pack",
		WindowStart: httpApproveNow.Add(time.Hour), WindowEnd: httpApproveNow.Add(8 * time.Hour),
		AssignedDemand: 100, CapacityOverWindow: 500, PublishedAt: httpApproveNow.Add(-time.Minute),
	})
	plan2, _ := planning.NewPublishedCapacityPlan(planning.PublishedCapacityPlan{
		PlanID: "plan-2", SiteID: "WH2", Location: "PATH-ZONE-B", PathID: "pick-rebin-pack",
		WindowStart: httpApproveNow.Add(time.Hour), WindowEnd: httpApproveNow.Add(8 * time.Hour),
		AssignedDemand: 10, CapacityOverWindow: 300, PublishedAt: httpApproveNow.Add(-time.Minute),
	})
	return planning.Facts{
		Capabilities: []planning.SiteCapability{cap1, cap2},
		Demands:      []planning.SiteSkuDemand{d1, d2},
		Plans:        []planning.PublishedCapacityPlan{plan1, plan2},
	}
}

func httpApproveHandler(transfers *httpFakeTransfers, events *httpFakeEvents, facts planning.Facts) Handler {
	return Handler{
		Approve: &usecases.ApproveTransfer{
			Transfers:    transfers,
			Events:       events,
			Snapshot:     httpFakeSnapshot{facts: facts},
			UoW:          httpFakeUoW{},
			MaxStaleness: 10 * time.Minute,
			Now:          func() time.Time { return httpApproveNow },
		},
	}
}

func httpApproveBody(policyVersion string) []byte {
	body, _ := json.Marshal(map[string]any{
		"originSiteId":      "WH1",
		"destinationSiteId": "WH2",
		"sku":               "SKU-1",
		"quantity":          5,
		"policyVersion":     policyVersion,
		"operatorReason":    "operator approved rebalance",
		"proposalAsOf":      httpApproveNow.Add(-5 * time.Minute).Format(time.RFC3339),
	})
	return body
}

func httpPostApprove(h Handler, key string, body []byte) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/v1/transfers:approve", bytes.NewReader(body))
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	rec := httptest.NewRecorder()
	h.Routes().ServeHTTP(rec, req)
	return rec
}

func TestApproveEndpointPersistsSaga(t *testing.T) {
	transfers := newHttpFakeTransfers()
	events := &httpFakeEvents{}
	h := httpApproveHandler(transfers, events, httpApproveFacts())

	rec := httpPostApprove(h, "idem-http-1", httpApproveBody("policy-v3"))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body.String())
	}
	var dto approveTransferDTO
	if err := json.NewDecoder(rec.Body).Decode(&dto); err != nil {
		t.Fatal(err)
	}
	if dto.State != string(transfer.StateAllocating) {
		t.Fatalf("state = %s, want ALLOCATING", dto.State)
	}
	if dto.TransferLineID != dto.TransferID+":1" {
		t.Fatalf("transferLineId = %q (transfer %s)", dto.TransferLineID, dto.TransferID)
	}
	if dto.Replayed {
		t.Fatal("first approval must not report replayed")
	}
	if len(events.events) != 2 {
		t.Fatalf("events = %d, want 2", len(events.events))
	}

	// Same key + payload again: 200 replayed, no second fan-out.
	rec2 := httpPostApprove(h, "idem-http-1", httpApproveBody("policy-v3"))
	if rec2.Code != http.StatusOK {
		t.Fatalf("replay status = %d: %s", rec2.Code, rec2.Body.String())
	}
	var dto2 approveTransferDTO
	_ = json.NewDecoder(rec2.Body).Decode(&dto2)
	if !dto2.Replayed || dto2.TransferID != dto.TransferID {
		t.Fatalf("replay = %+v", dto2)
	}
	if len(events.events) != 2 {
		t.Fatalf("events after replay = %d, want 2", len(events.events))
	}
}

func TestApproveEndpointRequiresIdempotencyKey(t *testing.T) {
	h := httpApproveHandler(newHttpFakeTransfers(), &httpFakeEvents{}, httpApproveFacts())
	rec := httpPostApprove(h, "", httpApproveBody("policy-v3"))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/problem+json" {
		t.Fatalf("Content-Type = %q", ct)
	}
	var p problem
	if err := json.NewDecoder(rec.Body).Decode(&p); err != nil {
		t.Fatal(err)
	}
	if p.Type == "" || p.Title == "" || p.Status != http.StatusBadRequest {
		t.Fatalf("problem = %+v", p)
	}
}

func TestApproveEndpointFailClosedFactsYield422(t *testing.T) {
	h := httpApproveHandler(newHttpFakeTransfers(), &httpFakeEvents{}, planning.Facts{})
	rec := httpPostApprove(h, "idem-http-2", httpApproveBody("policy-v3"))
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body.String())
	}
	var p problem
	_ = json.NewDecoder(rec.Body).Decode(&p)
	if p.Type != "https://warehouse.example/problems/facts-incomplete" {
		t.Fatalf("problem type = %q", p.Type)
	}
}

func TestApproveEndpointStaleFactsYield422(t *testing.T) {
	facts := httpApproveFacts()
	for i := range facts.Capabilities {
		facts.Capabilities[i].AsOf = facts.Capabilities[i].AsOf.Add(-2 * time.Hour)
	}
	h := httpApproveHandler(newHttpFakeTransfers(), &httpFakeEvents{}, facts)
	rec := httpPostApprove(h, "idem-http-3", httpApproveBody("policy-v3"))
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body.String())
	}
}

func TestApproveEndpointUnconfiguredYields503(t *testing.T) {
	h := Handler{}
	rec := httpPostApprove(h, "idem-http-4", httpApproveBody("policy-v3"))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body.String())
	}
}

func TestApproveEndpointInvalidBodyYields400(t *testing.T) {
	h := httpApproveHandler(newHttpFakeTransfers(), &httpFakeEvents{}, httpApproveFacts())
	rec := httpPostApprove(h, "idem-http-5", []byte(`{"unknown": true}`))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body.String())
	}
}

func TestApproveEndpointConflictYields409(t *testing.T) {
	transfers := newHttpFakeTransfers()
	h := httpApproveHandler(transfers, &httpFakeEvents{}, httpApproveFacts())
	rec := httpPostApprove(h, "idem-http-6", httpApproveBody("policy-v3"))
	if rec.Code != http.StatusOK {
		t.Fatalf("first status = %d: %s", rec.Code, rec.Body.String())
	}
	rec2 := httpPostApprove(h, "idem-http-6", httpApproveBody("policy-v9"))
	if rec2.Code != http.StatusConflict {
		t.Fatalf("status = %d: %s", rec2.Code, rec2.Body.String())
	}
	if fmt.Sprint(rec2.Code) != "409" {
		t.Fatal("unreachable")
	}
}
