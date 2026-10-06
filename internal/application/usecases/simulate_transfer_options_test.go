package usecases

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/claudioed/network-inventory-planning/internal/domain/planning"
)

type fakeSnapshotRepo struct {
	facts planning.Facts
	err   error
}

func (f fakeSnapshotRepo) Load(ctx context.Context) (planning.Facts, error) {
	return f.facts, f.err
}

func simTime() time.Time { return snapshotTimeUseCase() }

func snapshotTimeUseCase() time.Time {
	return time.Date(2026, 10, 6, 15, 0, 0, 0, time.UTC)
}

func simCapability(site string, origin, dest bool) planning.SiteCapability {
	c, err := planning.NewSiteCapability(site, origin, dest, 3, simTime().Add(-time.Minute))
	if err != nil {
		panic(err)
	}
	return c
}

func simDemand(order string, line int, site, sku string, units int, due time.Time) planning.SiteSkuDemand {
	d, err := planning.NewSiteSkuDemand(order, line, site, sku, units, due, planning.DemandActive, "static-site-v1")
	if err != nil {
		panic(err)
	}
	d.AsOf = simTime().Add(-time.Minute)
	return d
}

func simPlan(site string) planning.PublishedCapacityPlan {
	p, err := planning.NewPublishedCapacityPlan(planning.PublishedCapacityPlan{
		PlanID:             "plan-" + site,
		SiteID:             site,
		Location:           "PATH-ZONE-A",
		PathID:             "pick-rebin-pack",
		WindowStart:        simTime().Add(time.Hour),
		WindowEnd:          simTime().Add(8 * time.Hour),
		AssignedDemand:     100,
		CapacityOverWindow: 500,
		PublishedAt:        simTime().Add(-time.Minute),
	})
	if err != nil {
		panic(err)
	}
	return p
}

func completeFacts() planning.Facts {
	due := simTime().Add(2 * time.Hour)
	return planning.Facts{
		Capabilities: []planning.SiteCapability{simCapability("WH1", true, true), simCapability("WH2", true, true)},
		Demands: []planning.SiteSkuDemand{
			simDemand("ord-1", 1, "WH1", "SKU-1", 40, due),
			simDemand("ord-2", 1, "WH2", "SKU-1", 10, due),
		},
		Plans: []planning.PublishedCapacityPlan{simPlan("WH1"), simPlan("WH2")},
	}
}

func TestSimulateTransferOptionsServesAdvisoryResult(t *testing.T) {
	uc := SimulateTransferOptions{
		Snapshot:     fakeSnapshotRepo{facts: completeFacts()},
		MaxStaleness: time.Hour,
		Now:          simTime,
	}
	result, err := uc.Execute(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Advisory != true {
		t.Fatal("simulation must be flagged advisory-only")
	}
	if len(result.Sites) != 2 {
		t.Fatalf("Sites = %+v", result.Sites)
	}
	if result.AsOf.IsZero() {
		t.Fatal("result must carry the snapshot AsOf watermark")
	}
}

func TestSimulateTransferOptionsPropagatesRepositoryError(t *testing.T) {
	uc := SimulateTransferOptions{
		Snapshot:     fakeSnapshotRepo{err: errBoom},
		MaxStaleness: time.Hour,
		Now:          simTime,
	}
	if _, err := uc.Execute(context.Background()); err == nil {
		t.Fatal("repository error must propagate, not be swallowed into an empty simulation")
	}
}

var errBoom = &boomError{}

type boomError struct{}

func (*boomError) Error() string { return "boom" }

func TestSimulateTransferOptionsFailsClosedOnEmptyReadModel(t *testing.T) {
	uc := SimulateTransferOptions{
		Snapshot:     fakeSnapshotRepo{facts: planning.Facts{}},
		MaxStaleness: time.Hour,
		Now:          simTime,
	}
	_, err := uc.Execute(context.Background())
	if err == nil || !strings.Contains(err.Error(), "no site capability") {
		t.Fatalf("error = %v, want fail-closed no-capability error", err)
	}
}

func TestSimulateTransferOptionsReportsCapacityHeadroom(t *testing.T) {
	facts := completeFacts()
	facts.Demands = append(facts.Demands, simDemand("ord-3", 1, "WH1", "SKU-2", 30, simTime().Add(3*time.Hour)))
	uc := SimulateTransferOptions{
		Snapshot:     fakeSnapshotRepo{facts: facts},
		MaxStaleness: time.Hour,
		Now:          simTime,
	}
	result, err := uc.Execute(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var wh1 SiteSimulation
	for _, s := range result.Sites {
		if s.Site == "WH1" {
			wh1 = s
		}
	}
	if wh1.TotalDemand != 70 {
		t.Fatalf("WH1 total demand = %d, want 70 (40+30)", wh1.TotalDemand)
	}
	if wh1.CapacityOverWindow != 500 {
		t.Fatalf("WH1 capacity = %f, want 500", wh1.CapacityOverWindow)
	}
	if wh1.CapacityHeadroom != 430 {
		t.Fatalf("WH1 headroom = %d, want 430 (500-70)", wh1.CapacityHeadroom)
	}
}
