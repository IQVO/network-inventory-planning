package http

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/claudioed/network-inventory-planning/internal/application/usecases"
	"github.com/claudioed/network-inventory-planning/internal/domain/planning"
)

type stubSnapshotRepo struct {
	facts planning.Facts
	err   error
}

func (s stubSnapshotRepo) Load(ctx context.Context) (planning.Facts, error) {
	return s.facts, s.err
}

func simNow() time.Time { return time.Date(2026, 10, 6, 15, 0, 0, 0, time.UTC) }

func stubFacts() planning.Facts {
	due := simNow().Add(2 * time.Hour)
	cap1, _ := planning.NewSiteCapability("WH1", true, true, 3, simNow().Add(-time.Minute))
	cap2, _ := planning.NewSiteCapability("WH2", true, true, 3, simNow().Add(-time.Minute))
	d1, _ := planning.NewSiteSkuDemand("ord-1", 1, "WH1", "SKU-1", 40, due, planning.DemandActive, "static-site-v1")
	d1.AsOf = simNow().Add(-time.Minute)
	d2, _ := planning.NewSiteSkuDemand("ord-2", 1, "WH2", "SKU-1", 10, due, planning.DemandActive, "static-site-v1")
	d2.AsOf = simNow().Add(-time.Minute)
	plan1, _ := planning.NewPublishedCapacityPlan(planning.PublishedCapacityPlan{
		PlanID: "plan-1", SiteID: "WH1", Location: "PATH-ZONE-A", PathID: "pick-rebin-pack",
		WindowStart: simNow().Add(time.Hour), WindowEnd: simNow().Add(8 * time.Hour),
		AssignedDemand: 100, CapacityOverWindow: 500, PublishedAt: simNow().Add(-time.Minute),
	})
	plan2 := plan1
	plan2.PlanID = "plan-2"
	plan2.SiteID = "WH2"
	return planning.Facts{
		Capabilities: []planning.SiteCapability{cap1, cap2},
		Demands:      []planning.SiteSkuDemand{d1, d2},
		Plans:        []planning.PublishedCapacityPlan{plan1, plan2},
	}
}

func TestSimulateReturnsAdvisorySimulation(t *testing.T) {
	h := Handler{
		Simulate: &usecases.SimulateTransferOptions{
			Snapshot:     stubSnapshotRepo{facts: stubFacts()},
			MaxStaleness: time.Hour,
			Now:          simNow,
		},
	}
	request := httptest.NewRequest(http.MethodGet, "/v1/transfer-simulations", nil)
	response := httptest.NewRecorder()
	h.Routes().ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", response.Code, response.Body.String())
	}
	var result struct {
		Advisory bool `json:"advisory"`
		AsOf     time.Time
		Sites    []struct {
			Site               string `json:"site"`
			OriginEnabled      bool   `json:"originEnabled"`
			DestinationEnabled bool   `json:"destinationEnabled"`
			TotalDemand        int    `json:"totalDemand"`
			CapacityHeadroom   int    `json:"capacityHeadroom"`
		} `json:"sites"`
	}
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	if !result.Advisory {
		t.Fatal("simulation must carry advisory: true")
	}
	if len(result.Sites) != 2 || result.Sites[0].Site != "WH1" {
		t.Fatalf("sites = %+v", result.Sites)
	}
	if result.Sites[0].TotalDemand != 40 || result.Sites[0].CapacityHeadroom != 460 {
		t.Fatalf("WH1 = %+v", result.Sites[0])
	}
}

func TestSimulateReturnsProblemWhenReadModelsIncomplete(t *testing.T) {
	h := Handler{
		Simulate: &usecases.SimulateTransferOptions{
			Snapshot:     stubSnapshotRepo{facts: planning.Facts{}},
			MaxStaleness: time.Hour,
			Now:          simNow,
		},
	}
	request := httptest.NewRequest(http.MethodGet, "/v1/transfer-simulations", nil)
	response := httptest.NewRecorder()
	h.Routes().ServeHTTP(response, request)

	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d: %s", response.Code, response.Body.String())
	}
	if ct := response.Header().Get("Content-Type"); ct != "application/problem+json" {
		t.Fatalf("Content-Type = %q", ct)
	}
}

func TestSimulateReturnsServiceUnavailableWhenUnconfigured(t *testing.T) {
	// No Simulate use case wired (the service runs without consumers):
	// the endpoint answers 503, never a panic and never an empty 200 that
	// would look like a valid simulation.
	h := Handler{}
	request := httptest.NewRequest(http.MethodGet, "/v1/transfer-simulations", nil)
	response := httptest.NewRecorder()
	h.Routes().ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d: %s", response.Code, response.Body.String())
	}
}
