package planning

import (
	"strings"
	"testing"
	"time"
)

func snapshotTime() time.Time {
	return time.Date(2026, 10, 6, 15, 0, 0, 0, time.UTC)
}

func capableSite(site string, origin, destination bool) SiteCapability {
	c, err := NewSiteCapability(site, origin, destination, 3, snapshotTime().Add(-time.Minute))
	if err != nil {
		panic(err)
	}
	return c
}

func activeDemand(order string, line int, site, sku string, units int, due time.Time) SiteSkuDemand {
	d, err := NewSiteSkuDemand(order, line, site, sku, units, due, DemandActive, "static-site-v1")
	if err != nil {
		panic(err)
	}
	d.AsOf = snapshotTime().Add(-time.Minute)
	return d
}

func sitePlan(site string, start, end time.Time) PublishedCapacityPlan {
	p, err := NewPublishedCapacityPlan(PublishedCapacityPlan{
		PlanID:             "plan-" + site,
		SiteID:             site,
		Location:           "PATH-ZONE-A",
		PathID:             "pick-rebin-pack",
		WindowStart:        start,
		WindowEnd:          end,
		AssignedDemand:     100,
		CapacityOverWindow: 500,
		PublishedAt:        snapshotTime().Add(-time.Minute),
	})
	if err != nil {
		panic(err)
	}
	return p
}

func completeSnapshotInput() SnapshotInput {
	due := snapshotTime().Add(2 * time.Hour)
	windowStart := snapshotTime().Add(time.Hour)
	windowEnd := snapshotTime().Add(8 * time.Hour)
	return SnapshotInput{
		Capabilities: []SiteCapability{
			capableSite("WH1", true, true),
			capableSite("WH2", true, true),
		},
		Demands: []SiteSkuDemand{
			activeDemand("ord-1", 1, "WH1", "SKU-1", 40, due),
			activeDemand("ord-2", 1, "WH2", "SKU-1", 10, due),
		},
		Plans:        []PublishedCapacityPlan{sitePlan("WH1", windowStart, windowEnd), sitePlan("WH2", windowStart, windowEnd)},
		MaxStaleness: time.Hour,
		Now:          snapshotTime,
	}
}

func TestBuildSnapshotSucceedsOnCompleteFreshInput(t *testing.T) {
	snap, err := BuildSnapshot(completeSnapshotInput())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(snap.DemandBySiteSKU) != 2 {
		t.Fatalf("DemandBySiteSKU = %+v", snap.DemandBySiteSKU)
	}
	if !snap.AsOf.Equal(snapshotTime().Add(-time.Minute)) {
		t.Fatalf("AsOf = %v, want oldest watermark %v", snap.AsOf, snapshotTime().Add(-time.Minute))
	}
}

// Fail-closed: every missing or stale input must refuse, never degrade.
func TestBuildSnapshotFailsClosed(t *testing.T) {
	cases := []struct {
		name string
		mut  func(*SnapshotInput)
		want string
	}{
		{"no capabilities", func(in *SnapshotInput) { in.Capabilities = nil }, "no site capability"},
		{"no demands", func(in *SnapshotInput) { in.Demands = nil }, "no site sku demand"},
		{"no plans", func(in *SnapshotInput) { in.Plans = nil }, "no published capacity plan"},
		{"stale capability", func(in *SnapshotInput) {
			in.Capabilities[0].AsOf = snapshotTime().Add(-2 * time.Hour)
		}, "stale site capability"},
		{"stale plan", func(in *SnapshotInput) {
			in.Plans[0].PublishedAt = snapshotTime().Add(-2 * time.Hour)
		}, "stale published capacity plan"},
		{"origin disabled", func(in *SnapshotInput) {
			in.Capabilities[0].TransferOriginEnabled = false
		}, "origin"},
		{"destination disabled", func(in *SnapshotInput) {
			in.Capabilities[1].TransferDestinationEnabled = false
		}, "destination"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := completeSnapshotInput()
			tc.mut(&in)
			_, err := BuildSnapshot(in)
			if err == nil {
				t.Fatalf("%s: BuildSnapshot succeeded, want fail-closed error", tc.name)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("%s: error = %v, want it to mention %q", tc.name, err, tc.want)
			}
		})
	}
}

func TestBuildSnapshotExcludesSitesWithoutAllThreeFacts(t *testing.T) {
	in := completeSnapshotInput()
	// WH3 has capability but no demand and no plan: excluded, not zero-filled.
	in.Capabilities = append(in.Capabilities, capableSite("WH3", true, true))
	snap, err := BuildSnapshot(in)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := snap.Capabilities["WH3"]; ok {
		t.Fatal("a site without demand and plan must be excluded from the snapshot")
	}
}

func TestBuildSnapshotFiltersDemandByDueAtWindow(t *testing.T) {
	in := completeSnapshotInput()
	// A demand due BEFORE the capacity window: excluded by due_at filtering.
	in.Demands = append(in.Demands, activeDemand("ord-3", 1, "WH1", "SKU-2", 5, snapshotTime().Add(-time.Hour)))
	snap, err := BuildSnapshot(in)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := snap.DemandBySiteSKU["WH1\x00SKU-2"]; ok {
		t.Fatal("demand due outside the site's capacity window must be excluded")
	}
}

func TestSnapshotDemandAggregation(t *testing.T) {
	in := completeSnapshotInput()
	due := snapshotTime().Add(2 * time.Hour)
	in.Demands = append(in.Demands, activeDemand("ord-4", 2, "WH1", "SKU-1", 15, due))
	snap, err := BuildSnapshot(in)
	if err != nil {
		t.Fatal(err)
	}
	if got := snap.DemandBySiteSKU["WH1\x00SKU-1"]; got != 55 {
		t.Fatalf("aggregated WH1/SKU-1 demand = %d, want 55 (40+15)", got)
	}
}
