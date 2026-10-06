package planning

import (
	"testing"
	"time"
)

func TestFactsWatermarkIsNewestFactTime(t *testing.T) {
	base := time.Date(2026, 10, 6, 15, 0, 0, 0, time.UTC)
	cap1, _ := NewSiteCapability("WH1", true, true, 1, base.Add(-3*time.Hour))
	cap2, _ := NewSiteCapability("WH2", true, true, 2, base.Add(-1*time.Hour))
	d1, _ := NewSiteSkuDemand("o", 1, "WH1", "S", 1, base, DemandActive, "v")
	d1.AsOf = base.Add(-2 * time.Hour)
	plan, _ := NewPublishedCapacityPlan(PublishedCapacityPlan{
		PlanID: "p", SiteID: "WH1", Location: "L", PathID: "P",
		WindowStart: base, WindowEnd: base.Add(time.Hour), PublishedAt: base.Add(-30 * time.Minute),
	})
	f := Facts{Capabilities: []SiteCapability{cap1, cap2}, Demands: []SiteSkuDemand{d1}, Plans: []PublishedCapacityPlan{plan}}
	if !f.Watermark().Equal(base.Add(-30 * time.Minute)) {
		t.Fatalf("Watermark = %v, want the newest fact time %v", f.Watermark(), base.Add(-30*time.Minute))
	}
	empty := Facts{}
	if !empty.Watermark().IsZero() {
		t.Fatalf("empty Facts watermark = %v, want zero", empty.Watermark())
	}
}

func TestBuildSnapshotValidateEdgeCases(t *testing.T) {
	// MaxStaleness must be positive.
	in := snapshotInputForEdges()
	in.MaxStaleness = 0
	if _, err := BuildSnapshot(in); err == nil {
		t.Fatal("zero MaxStaleness must fail")
	}
	// A nil Now clock panics-proof: BuildSnapshot calls in.Now(); the
	// validate step catches staleness first, so nil Now with valid input
	// is a programming error surfaced as a panic is NOT acceptable —
	// guard by validate order only when MaxStaleness <= 0. Here we just
	// assert the happy path builds.
	in = snapshotInputForEdges()
	if _, err := BuildSnapshot(in); err != nil {
		t.Fatalf("happy path: %v", err)
	}
}

func TestBuildSnapshotStaleDemandFailsClosed(t *testing.T) {
	in := snapshotInputForEdges()
	in.Demands[0].AsOf = in.Demands[0].AsOf.Add(-2 * time.Hour)
	if _, err := BuildSnapshot(in); err == nil {
		t.Fatal("stale demand must fail closed")
	}
}

func TestBuildSnapshotOlderCapabilityIsNoop(t *testing.T) {
	in := snapshotInputForEdges()
	older := in.Capabilities[0]
	older.Revision = 1
	older.TransferOriginEnabled = false
	in.Capabilities = append(in.Capabilities, older) // same site, lower revision
	snap, err := BuildSnapshot(in)
	if err != nil {
		t.Fatal(err)
	}
	if !snap.Capabilities["WH1"].TransferOriginEnabled {
		t.Fatal("an older capability revision must not overwrite the newer snapshot")
	}
}

func TestBuildSnapshotOlderPlanIsNoop(t *testing.T) {
	in := snapshotInputForEdges()
	older := in.Plans[0]
	older.PlanID = "plan-old"
	older.PublishedAt = older.PublishedAt.Add(-30 * time.Second) // newer than staleness budget, older than p1
	older.CapacityOverWindow = 1
	in.Plans = append(in.Plans, older)
	snap, err := BuildSnapshot(in)
	if err != nil {
		t.Fatal(err)
	}
	if snap.CapacityBySite["WH1"].CapacityOverWindow == 1 {
		t.Fatal("an older plan must not overwrite the newer snapshot")
	}
}

func snapshotInputForEdges() SnapshotInput {
	base := time.Date(2026, 10, 6, 15, 0, 0, 0, time.UTC)
	cap1, _ := NewSiteCapability("WH1", true, true, 5, base.Add(-time.Minute))
	cap2, _ := NewSiteCapability("WH2", true, true, 5, base.Add(-time.Minute))
	d1, _ := NewSiteSkuDemand("o1", 1, "WH1", "S", 5, base.Add(2*time.Hour), DemandActive, "v")
	d1.AsOf = base.Add(-time.Minute)
	d2, _ := NewSiteSkuDemand("o2", 1, "WH2", "S", 5, base.Add(2*time.Hour), DemandActive, "v")
	d2.AsOf = base.Add(-time.Minute)
	plan := func(site, id string) PublishedCapacityPlan {
		p, _ := NewPublishedCapacityPlan(PublishedCapacityPlan{
			PlanID: id, SiteID: site, Location: "L", PathID: "P",
			WindowStart: base.Add(time.Hour), WindowEnd: base.Add(8 * time.Hour),
			AssignedDemand: 10, CapacityOverWindow: 100, PublishedAt: base.Add(-time.Minute),
		})
		return p
	}
	return SnapshotInput{
		Capabilities: []SiteCapability{cap1, cap2},
		Demands:      []SiteSkuDemand{d1, d2},
		Plans:        []PublishedCapacityPlan{plan("WH1", "p1"), plan("WH2", "p2")},
		MaxStaleness: time.Hour,
		Now:          func() time.Time { return base },
	}
}
