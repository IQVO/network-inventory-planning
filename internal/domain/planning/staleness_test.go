package planning

import (
	"strings"
	"testing"
	"time"
)

// Per-site staleness (ADR 0018): a stale fact excludes ITS site, the
// snapshot is refused only when no site survives.

// ageFact moves one fact of one site (kind: capability, demand, plan) so
// that it is exactly `age` older than the snapshot clock.
func ageFact(in *SnapshotInput, site, kind string, age time.Duration) {
	asOf := snapshotTime().Add(-age)
	switch kind {
	case kindCapability:
		for i := range in.Capabilities {
			if in.Capabilities[i].Site == site {
				in.Capabilities[i].AsOf = asOf
			}
		}
	case kindDemand:
		for i := range in.Demands {
			if in.Demands[i].Site == site {
				in.Demands[i].AsOf = asOf
			}
		}
	case kindPlan:
		for i := range in.Plans {
			if in.Plans[i].SiteID == site {
				in.Plans[i].PublishedAt = asOf
			}
		}
	}
}

var factKinds = []string{kindCapability, kindDemand, kindPlan}

func TestBuildSnapshotFreshnessBoundaryIsInclusive(t *testing.T) {
	for _, kind := range factKinds {
		t.Run(kind, func(t *testing.T) {
			in := completeSnapshotInput()
			ageFact(&in, "WH1", kind, in.MaxStaleness) // exactly MaxStaleness old
			snap, err := BuildSnapshot(in)
			if err != nil {
				t.Fatalf("exactly MaxStaleness old must be fresh: %v", err)
			}
			if _, ok := snap.Capabilities["WH1"]; !ok || len(snap.Capabilities) != 2 {
				t.Fatalf("both sites must participate, got %v", keysOf(snap))
			}
			if len(snap.StaleSites) != 0 {
				t.Fatalf("nothing is stale at the boundary, got %v", snap.StaleSites)
			}

			ageFact(&in, "WH1", kind, in.MaxStaleness+time.Nanosecond)
			snap, err = BuildSnapshot(in)
			if err != nil {
				t.Fatalf("one stale site must not refuse the snapshot: %v", err)
			}
			if _, ok := snap.Capabilities["WH1"]; ok {
				t.Fatal("MaxStaleness+1ns old must exclude the site")
			}
			if _, ok := snap.Capabilities["WH2"]; !ok {
				t.Fatal("the fresh site must stay")
			}
		})
	}
}

func keysOf(snap PlanningSnapshot) []string {
	sites := make([]string, 0, len(snap.Capabilities))
	for site := range snap.Capabilities {
		sites = append(sites, site)
	}
	return sites
}

func TestBuildSnapshotStaleFactExcludesOnlyItsSite(t *testing.T) {
	wants := map[string]string{
		kindCapability: "stale site capability",
		kindDemand:     "stale site sku demand ord-1#1",
		kindPlan:       "stale published capacity plan plan-WH1",
	}
	for _, kind := range factKinds {
		t.Run(kind, func(t *testing.T) {
			in := completeSnapshotInput()
			ageFact(&in, "WH1", kind, 2*time.Hour)
			snap, err := BuildSnapshot(in)
			if err != nil {
				t.Fatal(err)
			}
			assertOnlyWH2Remains(t, snap)
			reason := snap.StaleSites["WH1"]
			if !strings.Contains(reason, wants[kind]) || !strings.Contains(reason, "older than 1h0m0s") {
				t.Fatalf("StaleSites[WH1] = %q, want it to name %q and the budget", reason, wants[kind])
			}
			if _, ok := snap.StaleSites["WH2"]; ok {
				t.Fatal("a fresh site must not be listed as stale")
			}
		})
	}
}

func assertOnlyWH2Remains(t *testing.T, snap PlanningSnapshot) {
	t.Helper()
	if len(snap.Capabilities) != 1 || len(snap.CapacityBySite) != 1 {
		t.Fatalf("only WH2 may remain, got caps=%v plans=%v", keysOf(snap), snap.CapacityBySite)
	}
	if got := snap.DemandBySiteSKU["WH2\x00SKU-1"]; got != 10 {
		t.Fatalf("WH2 demand = %d, want 10", got)
	}
	if _, leaked := snap.DemandBySiteSKU["WH1\x00SKU-1"]; leaked {
		t.Fatal("the excluded site must contribute no demand")
	}
}

func TestBuildSnapshotRefusesOnlyWhenNoSiteSurvives(t *testing.T) {
	in := completeSnapshotInput()
	ageFact(&in, "WH1", kindCapability, 2*time.Hour)
	ageFact(&in, "WH2", kindPlan, 3*time.Hour)
	_, err := BuildSnapshot(in)
	if err == nil {
		t.Fatal("every site stale must refuse the snapshot")
	}
	want := "no site has capability, demand and capacity facts together; excluded: " +
		"WH1: stale site capability (as of 2026-10-06T13:00:00Z, older than 1h0m0s); " +
		"WH2: stale published capacity plan plan-WH2 (as of 2026-10-06T12:00:00Z, older than 1h0m0s)"
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("error = %q, want it to contain %q", err, want)
	}
}

func TestBuildSnapshotAllStaleReasonsListEveryStaleKindInOrder(t *testing.T) {
	in := completeSnapshotInput()
	ageFact(&in, "WH1", kindCapability, 2*time.Hour)
	ageFact(&in, "WH1", kindDemand, 3*time.Hour)
	ageFact(&in, "WH1", kindPlan, 4*time.Hour)
	ageFact(&in, "WH2", kindCapability, 2*time.Hour)
	_, err := BuildSnapshot(in)
	if err == nil {
		t.Fatal("must refuse")
	}
	want := "excluded: WH1: stale site capability (as of 2026-10-06T13:00:00Z, older than 1h0m0s); " +
		"stale site sku demand ord-1#1 (as of 2026-10-06T12:00:00Z, older than 1h0m0s); " +
		"stale published capacity plan plan-WH1 (as of 2026-10-06T11:00:00Z, older than 1h0m0s); " +
		"WH2: stale site capability"
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("error = %q, want it to contain %q", err, want)
	}
}

func TestBuildSnapshotNoSurvivorWithoutStaleFactsKeepsPlainMessage(t *testing.T) {
	in := completeSnapshotInput()
	in.Plans = []PublishedCapacityPlan{sitePlan("WH9", snapshotTime(), snapshotTime().Add(time.Hour))}
	_, err := BuildSnapshot(in)
	if err == nil {
		t.Fatal("no site with all three facts must refuse")
	}
	if strings.Contains(err.Error(), "excluded") {
		t.Fatalf("nothing is stale, error must not list exclusions: %v", err)
	}
}

func TestBuildSnapshotAsOfIsOldestOfIncludedFacts(t *testing.T) {
	in := completeSnapshotInput()
	// The excluded WH1 holds the oldest facts overall; they must not drag AsOf.
	ageFact(&in, "WH1", kindCapability, 5*time.Hour)
	ageFact(&in, "WH2", kindCapability, 30*time.Minute)
	ageFact(&in, "WH2", kindPlan, 40*time.Minute)
	ageFact(&in, "WH2", kindDemand, 20*time.Minute)
	snap, err := BuildSnapshot(in)
	if err != nil {
		t.Fatal(err)
	}
	if want := snapshotTime().Add(-40 * time.Minute); !snap.AsOf.Equal(want) {
		t.Fatalf("AsOf = %v, want the oldest INCLUDED watermark %v", snap.AsOf, want)
	}
}

func TestBuildSnapshotAsOfCountsIncludedDemand(t *testing.T) {
	in := completeSnapshotInput()
	ageFact(&in, "WH2", kindDemand, 50*time.Minute)
	snap, err := BuildSnapshot(in)
	if err != nil {
		t.Fatal(err)
	}
	if want := snapshotTime().Add(-50 * time.Minute); !snap.AsOf.Equal(want) {
		t.Fatalf("AsOf = %v, want %v", snap.AsOf, want)
	}
}

func TestBuildSnapshotKeepsTheFirstStaleDemandReason(t *testing.T) {
	in := completeSnapshotInput()
	due := snapshotTime().Add(2 * time.Hour)
	first := activeDemand("ord-7", 1, "WH1", "SKU-2", 3, due)
	second := activeDemand("ord-8", 1, "WH1", "SKU-3", 3, due)
	first.AsOf = snapshotTime().Add(-2 * time.Hour)
	second.AsOf = snapshotTime().Add(-3 * time.Hour)
	in.Demands = append(in.Demands, first, second)
	snap, err := BuildSnapshot(in)
	if err != nil {
		t.Fatal(err)
	}
	reason := snap.StaleSites["WH1"]
	if !strings.Contains(reason, "ord-7#1") || strings.Contains(reason, "ord-8#1") {
		t.Fatalf("reason = %q, want only the first stale demand", reason)
	}
}

func TestBuildSnapshotStaleRemovedDemandStillExcludesItsSite(t *testing.T) {
	in := completeSnapshotInput()
	gone, err := NewSiteSkuDemand("ord-9", 1, "WH1", "SKU-1", 0, snapshotTime(), DemandRemoved, "static-site-v1")
	if err != nil {
		t.Fatal(err)
	}
	gone.AsOf = snapshotTime().Add(-2 * time.Hour)
	in.Demands = append(in.Demands, gone)
	snap, err := BuildSnapshot(in)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := snap.Capabilities["WH1"]; ok {
		t.Fatal("a stale demand row of any state excludes its site")
	}
}

func TestBuildSnapshotNeverAppliedDemandWatermarkIsNotStale(t *testing.T) {
	in := completeSnapshotInput()
	in.Demands[0].AsOf = time.Time{}
	snap, err := BuildSnapshot(in)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := snap.Capabilities["WH1"]; !ok || len(snap.StaleSites) != 0 {
		t.Fatalf("a zero watermark is not stale: %v %v", keysOf(snap), snap.StaleSites)
	}
}

func TestBuildSnapshotJudgesTheNewestCapabilityAndPlanOnly(t *testing.T) {
	in := completeSnapshotInput()
	oldCap := in.Capabilities[0]
	oldCap.Revision = 1
	oldCap.AsOf = snapshotTime().Add(-5 * time.Hour)
	oldPlan := in.Plans[0]
	oldPlan.PlanID = "plan-old"
	oldPlan.PublishedAt = snapshotTime().Add(-5 * time.Hour)
	in.Capabilities = append(in.Capabilities, oldCap)
	in.Plans = append(in.Plans, oldPlan)
	snap, err := BuildSnapshot(in)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := snap.Capabilities["WH1"]; !ok || len(snap.StaleSites) != 0 {
		t.Fatalf("a superseded stale row must not exclude the site: %v", snap.StaleSites)
	}
}

func TestBuildSnapshotStaleNewestCapabilityExcludesEvenWithFreshOlderRevision(t *testing.T) {
	in := completeSnapshotInput()
	newer := in.Capabilities[0]
	newer.Revision = 9
	newer.AsOf = snapshotTime().Add(-5 * time.Hour)
	in.Capabilities = append(in.Capabilities, newer)
	snap, err := BuildSnapshot(in)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := snap.Capabilities["WH1"]; ok {
		t.Fatal("the NEWEST capability revision is what is judged; it is stale")
	}
}

func TestBuildSnapshotSiteDisabledForOneDirectionStillParticipates(t *testing.T) {
	cases := map[string]func(*SiteCapability){
		"origin disabled":      func(c *SiteCapability) { c.TransferOriginEnabled = false },
		"destination disabled": func(c *SiteCapability) { c.TransferDestinationEnabled = false },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			in := completeSnapshotInput()
			mutate(&in.Capabilities[0])
			snap, err := BuildSnapshot(in)
			if err != nil {
				t.Fatalf("a direction-disabled site must not refuse the snapshot: %v", err)
			}
			got, ok := snap.Capabilities["WH1"]
			if !ok || len(snap.Capabilities) != 2 {
				t.Fatalf("both sites participate, got %v", keysOf(snap))
			}
			if got.OriginAllowed() == got.DestinationAllowed() {
				t.Fatalf("the disabled direction must be preserved, got origin=%v destination=%v", got.OriginAllowed(), got.DestinationAllowed())
			}
		})
	}
}

func TestBuildSnapshotSiteDisabledInBothDirectionsStillParticipates(t *testing.T) {
	in := completeSnapshotInput()
	in.Capabilities[0].TransferOriginEnabled = false
	in.Capabilities[0].TransferDestinationEnabled = false
	snap, err := BuildSnapshot(in)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := snap.Capabilities["WH1"]; !ok {
		t.Fatal("direction gates live where directions are used, not in the snapshot")
	}
}
