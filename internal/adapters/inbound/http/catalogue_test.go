package http

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/claudioed/network-inventory-planning/internal/application/usecases"
	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

// stubPolicies / stubLanes are minimal in-memory catalogues (an inbound
// adapter test must not import the outbound memory adapter).
type stubPolicies struct{ entries []transfer.RebalancePolicy }

func (s *stubPolicies) Get(_ context.Context, site transfer.SiteID, sku transfer.SKU) (transfer.RebalancePolicy, bool, error) {
	for _, e := range s.entries {
		if e.Policy.Site == site && e.Policy.SKU == sku {
			return e, true, nil
		}
	}
	return transfer.RebalancePolicy{}, false, nil
}

func (s *stubPolicies) List(context.Context) ([]transfer.RebalancePolicy, error) {
	return s.entries, nil
}

func (s *stubPolicies) Save(_ context.Context, e transfer.RebalancePolicy, _ int64) error {
	for i := range s.entries {
		if s.entries[i].Policy.Site == e.Policy.Site && s.entries[i].Policy.SKU == e.Policy.SKU {
			s.entries[i] = e
			return nil
		}
	}
	s.entries = append(s.entries, e)
	return nil
}

type stubLanes struct{ entries []transfer.TransferLane }

func (s *stubLanes) Get(_ context.Context, origin, destination transfer.SiteID) (transfer.TransferLane, bool, error) {
	for _, e := range s.entries {
		if e.Lane.Origin == origin && e.Lane.Destination == destination {
			return e, true, nil
		}
	}
	return transfer.TransferLane{}, false, nil
}

func (s *stubLanes) List(context.Context) ([]transfer.TransferLane, error) { return s.entries, nil }

func (s *stubLanes) Save(_ context.Context, e transfer.TransferLane, _ int64) error {
	for i := range s.entries {
		if s.entries[i].Lane.Origin == e.Lane.Origin && s.entries[i].Lane.Destination == e.Lane.Destination {
			s.entries[i] = e
			return nil
		}
	}
	s.entries = append(s.entries, e)
	return nil
}

var catalogueClock = func() time.Time { return time.Date(2026, 10, 12, 9, 0, 0, 0, time.UTC) }

func catalogueHandler() Handler {
	policies, lanes := &stubPolicies{}, &stubLanes{}
	return Handler{
		PutPolicy:    &usecases.PutRebalancePolicy{Policies: policies, Now: catalogueClock},
		ListPolicies: &usecases.ListRebalancePolicies{Policies: policies},
		PutLane:      &usecases.PutTransferLane{Lanes: lanes, Now: catalogueClock},
		ListLanes:    &usecases.ListTransferLanes{Lanes: lanes},
	}
}

func do(t *testing.T, h http.Handler, method, path, ifMatch, body string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(method, path, strings.NewReader(body))
	if ifMatch != "" {
		request.Header.Set("If-Match", ifMatch)
	}
	response := httptest.NewRecorder()
	h.ServeHTTP(response, request)
	return response
}

func problemType(t *testing.T, r *httptest.ResponseRecorder) string {
	t.Helper()
	var p problem
	if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
		t.Fatalf("decode problem: %v", err)
	}
	return strings.TrimPrefix(p.Type, "https://warehouse.example/problems/")
}

// The bodies are the EXISTING Policy / Lane schemas (the generate
// endpoint's shapes), keyed by their own Site+SKU / Origin+Destination.
const policyBody = `{"Version":"pv-1","Site":"WH2","SKU":"SKU-1","SafetyStock":5,"TargetStock":50,"UnitPriority":10}`

func TestPolicyCatalogueEndpoints(t *testing.T) {
	h := catalogueHandler().Routes()

	created := do(t, h, http.MethodPut, "/v1/rebalance-policies", "", policyBody)
	if created.Code != http.StatusCreated || created.Header().Get("ETag") != `"1"` {
		t.Fatalf("create: %d etag=%q %s", created.Code, created.Header().Get("ETag"), created.Body.String())
	}
	var dto rebalancePolicyEntryDTO
	if err := json.NewDecoder(created.Body).Decode(&dto); err != nil {
		t.Fatal(err)
	}
	want := transfer.Policy{Version: "pv-1", Site: "WH2", SKU: "SKU-1", SafetyStock: 5, TargetStock: 50, UnitPriority: 10}
	if dto.Policy != want || dto.Revision != 1 || dto.Outcome != "CREATED" {
		t.Fatalf("dto = %+v", dto)
	}

	replay := do(t, h, http.MethodPut, "/v1/rebalance-policies", `"1"`, policyBody)
	if replay.Code != http.StatusOK || !strings.Contains(replay.Body.String(), `"outcome":"UNCHANGED"`) {
		t.Fatalf("idempotent replay: %d %s", replay.Code, replay.Body.String())
	}
	updated := do(t, h, http.MethodPut, "/v1/rebalance-policies", `"1"`,
		`{"Version":"pv-2","Site":"WH2","SKU":"SKU-1","SafetyStock":5,"TargetStock":80,"UnitPriority":10}`)
	if updated.Code != http.StatusOK || updated.Header().Get("ETag") != `"2"` {
		t.Fatalf("update: %d etag=%q %s", updated.Code, updated.Header().Get("ETag"), updated.Body.String())
	}
}

func TestPolicyCatalogueRefusals(t *testing.T) {
	h := catalogueHandler().Routes()
	if r := do(t, h, http.MethodPut, "/v1/rebalance-policies", "", policyBody); r.Code != http.StatusCreated {
		t.Fatalf("seed: %d", r.Code)
	}
	stale := do(t, h, http.MethodPut, "/v1/rebalance-policies", `"7"`, policyBody)
	if stale.Code != http.StatusPreconditionFailed || problemType(t, stale) != "version-mismatch" {
		t.Fatalf("stale If-Match: %d %s", stale.Code, stale.Body.String())
	}
	invalid := do(t, h, http.MethodPut, "/v1/rebalance-policies", "",
		`{"Version":"pv-3","Site":"WH2","SKU":"SKU-1","SafetyStock":9,"TargetStock":1,"UnitPriority":10}`)
	if invalid.Code != http.StatusUnprocessableEntity || problemType(t, invalid) != "invalid-catalogue-entry" {
		t.Fatalf("invalid: %d %s", invalid.Code, invalid.Body.String())
	}
	badIfMatch := do(t, h, http.MethodPut, "/v1/rebalance-policies", `"abc"`, policyBody)
	if badIfMatch.Code != http.StatusBadRequest || problemType(t, badIfMatch) != "invalid-if-match" {
		t.Fatalf("bad If-Match: %d %s", badIfMatch.Code, badIfMatch.Body.String())
	}
	unknownField := do(t, h, http.MethodPut, "/v1/rebalance-policies", "", `{"Warehouse":"WH9"}`)
	if unknownField.Code != http.StatusBadRequest {
		t.Fatalf("unknown field: %d", unknownField.Code)
	}
}

func TestPolicyCatalogueListsTheCurrentRevision(t *testing.T) {
	h := catalogueHandler().Routes()
	do(t, h, http.MethodPut, "/v1/rebalance-policies", "", policyBody)
	do(t, h, http.MethodPut, "/v1/rebalance-policies", `"1"`,
		`{"Version":"pv-2","Site":"WH2","SKU":"SKU-1","SafetyStock":5,"TargetStock":80,"UnitPriority":10}`)
	list := do(t, h, http.MethodGet, "/v1/rebalance-policies", "", "")
	var body struct {
		Policies []rebalancePolicyEntryDTO `json:"policies"`
	}
	if err := json.NewDecoder(list.Body).Decode(&body); err != nil || list.Code != http.StatusOK {
		t.Fatalf("list: %d %v", list.Code, err)
	}
	if len(body.Policies) != 1 || body.Policies[0].Policy.TargetStock != 80 || body.Policies[0].Revision != 2 || body.Policies[0].Outcome != "" {
		t.Fatalf("list = %+v", body.Policies)
	}
}

func TestLaneCatalogueEndpoints(t *testing.T) {
	h := catalogueHandler().Routes()
	// LeadTime is nanoseconds (the Lane schema's Go time.Duration shape).
	lane := `{"Origin":"WH1","Destination":"WH2","LeadTime":172800000000000,"UnitHandlingCost":2,"Enabled":true}`

	created := do(t, h, http.MethodPut, "/v1/transfer-lanes", "", lane)
	if created.Code != http.StatusCreated || created.Header().Get("ETag") != `"1"` {
		t.Fatalf("create: %d %s", created.Code, created.Body.String())
	}
	var dto transferLaneEntryDTO
	if err := json.NewDecoder(created.Body).Decode(&dto); err != nil {
		t.Fatal(err)
	}
	if dto.Lane != (transfer.Lane{Origin: "WH1", Destination: "WH2", LeadTime: 48 * time.Hour, UnitHandlingCost: 2, Enabled: true}) {
		t.Fatalf("dto = %+v", dto)
	}
	disabled := do(t, h, http.MethodPut, "/v1/transfer-lanes", "1",
		`{"Origin":"WH1","Destination":"WH2","LeadTime":172800000000000,"UnitHandlingCost":2,"Enabled":false}`)
	if disabled.Code != http.StatusOK || disabled.Header().Get("ETag") != `"2"` {
		t.Fatalf("disable (bare If-Match revision accepted): %d %s", disabled.Code, disabled.Body.String())
	}
	self := do(t, h, http.MethodPut, "/v1/transfer-lanes", "", `{"Origin":"WH1","Destination":"WH1","Enabled":true}`)
	if self.Code != http.StatusUnprocessableEntity {
		t.Fatalf("self lane: %d %s", self.Code, self.Body.String())
	}
	list := do(t, h, http.MethodGet, "/v1/transfer-lanes", "", "")
	if list.Code != http.StatusOK || !strings.Contains(list.Body.String(), `"Enabled":false`) {
		t.Fatalf("list: %d %s", list.Code, list.Body.String())
	}
}

func TestCatalogueEndpointsUnconfiguredAre503(t *testing.T) {
	h := Handler{}.Routes()
	for _, tc := range []struct{ method, path string }{
		{http.MethodGet, "/v1/rebalance-policies"},
		{http.MethodPut, "/v1/rebalance-policies"},
		{http.MethodGet, "/v1/transfer-lanes"},
		{http.MethodPut, "/v1/transfer-lanes"},
	} {
		r := do(t, h, tc.method, tc.path, "", "{}")
		if r.Code != http.StatusServiceUnavailable || problemType(t, r) != "catalogue-unavailable" {
			t.Fatalf("%s %s: %d", tc.method, tc.path, r.Code)
		}
	}
}

// racingLanes loses every write race (another writer always got there first).
type racingLanes struct{ stubLanes }

func (*racingLanes) Save(context.Context, transfer.TransferLane, int64) error {
	return transfer.ErrCatalogueConcurrentModification
}

func TestCatalogueLostRaceIs409(t *testing.T) {
	h := Handler{PutLane: &usecases.PutTransferLane{Lanes: &racingLanes{}, Now: catalogueClock}}.Routes()
	r := do(t, h, http.MethodPut, "/v1/transfer-lanes", "",
		`{"Origin":"WH1","Destination":"WH2","LeadTime":0,"UnitHandlingCost":1,"Enabled":true}`)
	if r.Code != http.StatusConflict || problemType(t, r) != "concurrent-modification" {
		t.Fatalf("lost race: %d %s", r.Code, r.Body.String())
	}
}
