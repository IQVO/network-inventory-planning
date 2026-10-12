package transfer

import (
	"testing"
	"time"

	"github.com/claudioed/network-inventory-planning/internal/domain/planning"
)

var scheduledAsOf = time.Date(2026, 10, 12, 8, 59, 0, 0, time.UTC)

// scheduledSnapshot builds a fail-closed snapshot by hand: every site in
// capacity participates with both directions allowed unless overridden.
func scheduledSnapshot(capacity map[string]float64, demand map[string]int) planning.PlanningSnapshot {
	snap := planning.PlanningSnapshot{
		AsOf:            scheduledAsOf,
		Capabilities:    map[string]planning.SiteCapability{},
		DemandBySiteSKU: map[string]int{},
		CapacityBySite:  map[string]planning.PublishedCapacityPlan{},
	}
	for site, c := range capacity {
		snap.Capabilities[site] = planning.SiteCapability{Site: site, TransferOriginEnabled: true, TransferDestinationEnabled: true, Revision: 1, AsOf: scheduledAsOf}
		snap.CapacityBySite[site] = planning.PublishedCapacityPlan{SiteID: site, CapacityOverWindow: c}
	}
	for key, units := range demand {
		snap.DemandBySiteSKU[key] = units
	}
	return snap
}

func key(site, sku string) string { return site + "\x00" + sku }

func policy(site, sku string, safety, target, priority int) Policy {
	return Policy{Version: "pv-" + site, Site: SiteID(site), SKU: SKU(sku), SafetyStock: safety, TargetStock: target, UnitPriority: priority}
}

func lane(origin, destination string, cost int) Lane {
	return Lane{Origin: SiteID(origin), Destination: SiteID(destination), LeadTime: 24 * time.Hour, UnitHandlingCost: cost, Enabled: true}
}

// TestPlanFromSnapshot_ProposesWithinNeedTargetAndHeadroom: the destination
// need is its in-window demand capped by the policy's target stock; the
// origin donates from its capacity headroom minus its safety stock — the
// same site-level rules ValidateApproval enforces, so the proposal is
// approvable as generated.
func TestPlanFromSnapshot_ProposesWithinNeedTargetAndHeadroom(t *testing.T) {
	snap := scheduledSnapshot(
		map[string]float64{"WH1": 500, "WH2": 300},
		map[string]int{key("WH1", "SKU-1"): 40, key("WH2", "SKU-1"): 30},
	)
	proposals, rejected := Planner{}.PlanFromSnapshot(snap,
		[]Policy{policy("WH1", "SKU-1", 10, 100, 1), policy("WH2", "SKU-1", 0, 25, 10)},
		[]Lane{lane("WH1", "WH2", 2)})
	if rejected != 0 || len(proposals) != 1 {
		t.Fatalf("proposals = %+v rejected = %d, want 1 and 0", proposals, rejected)
	}
	p := proposals[0]
	// need = min(demand 30, target 25) = 25; donor = 500-40-10 = 450.
	if p.Origin != "WH1" || p.Destination != "WH2" || p.SKU != "SKU-1" || p.Quantity != 25 {
		t.Fatalf("proposal = %+v", p)
	}
	if p.PolicyVersion != "pv-WH2" || !p.PositionAsOf.Equal(scheduledAsOf) {
		t.Fatalf("proposal provenance = %q %s", p.PolicyVersion, p.PositionAsOf)
	}
	if p.ScoreBreakdown != (ScoreBreakdown{PriorityBenefit: 250, HandlingPenalty: 50, LeadTimePenalty: 1}) {
		t.Fatalf("score = %+v", p.ScoreBreakdown)
	}
	approval := ProposalInput{OriginSiteID: "WH1", DestinationSiteID: "WH2", SKU: "SKU-1", Quantity: p.Quantity}
	if err := ValidateApproval(approval, ApprovalFacts{Snapshot: snap}); err != nil {
		t.Fatalf("a scheduled proposal must be approvable against the same snapshot: %v", err)
	}
}

// TestPlanFromSnapshot_LaneNeedsCapabilityOnBothEnds is D6's capability
// gate: a lane is used only when site_capability allows its origin to send
// and its destination to receive (a site without complete facts has no
// capability in the snapshot at all).
func TestPlanFromSnapshot_LaneNeedsCapabilityOnBothEnds(t *testing.T) {
	base := func() planning.PlanningSnapshot {
		return scheduledSnapshot(map[string]float64{"WH1": 500, "WH2": 300},
			map[string]int{key("WH1", "SKU-1"): 40, key("WH2", "SKU-1"): 30})
	}
	policies := []Policy{policy("WH1", "SKU-1", 0, 100, 1), policy("WH2", "SKU-1", 0, 100, 10), policy("WH3", "SKU-1", 0, 100, 10)}
	cases := map[string]struct {
		snap  func() planning.PlanningSnapshot
		lanes []Lane
	}{
		"origin cannot send": {func() planning.PlanningSnapshot {
			s := base()
			c := s.Capabilities["WH1"]
			c.TransferOriginEnabled = false
			s.Capabilities["WH1"] = c
			return s
		}, []Lane{lane("WH1", "WH2", 1)}},
		"destination cannot receive": {func() planning.PlanningSnapshot {
			s := base()
			c := s.Capabilities["WH2"]
			c.TransferDestinationEnabled = false
			s.Capabilities["WH2"] = c
			return s
		}, []Lane{lane("WH1", "WH2", 1)}},
		"destination has no capability fact": {base, []Lane{lane("WH1", "WH3", 1)}},
		"origin has no capability fact":      {base, []Lane{lane("WH3", "WH2", 1)}},
		"lane disabled": {base, []Lane{func() Lane {
			l := lane("WH1", "WH2", 1)
			l.Enabled = false
			return l
		}()}},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			proposals, rejected := Planner{}.PlanFromSnapshot(tc.snap(), policies, tc.lanes)
			if len(proposals) != 0 || rejected != 0 {
				t.Fatalf("proposals = %+v rejected = %d, want none", proposals, rejected)
			}
		})
	}
}

// TestPlanFromSnapshot_NeedsPoliciesAndPositiveBudgets: no destination
// demand, no origin policy, a zero target or a headroom eaten by safety
// stock all yield nothing.
func TestPlanFromSnapshot_NeedsPoliciesAndPositiveBudgets(t *testing.T) {
	snap := scheduledSnapshot(map[string]float64{"WH1": 100, "WH2": 300},
		map[string]int{key("WH1", "SKU-1"): 40, key("WH2", "SKU-1"): 30, key("WH2", "SKU-2"): 0})
	lanes := []Lane{lane("WH1", "WH2", 1)}
	cases := map[string][]Policy{
		"no destination policy":    {policy("WH1", "SKU-1", 0, 100, 1)},
		"no origin policy":         {policy("WH2", "SKU-1", 0, 100, 10)},
		"zero target":              {policy("WH1", "SKU-1", 0, 100, 1), policy("WH2", "SKU-1", 0, 0, 10)},
		"safety eats the headroom": {policy("WH1", "SKU-1", 60, 100, 1), policy("WH2", "SKU-1", 0, 100, 10)},
		"no destination demand":    {policy("WH1", "SKU-2", 0, 100, 1), policy("WH2", "SKU-2", 0, 100, 10)},
	}
	for name, policies := range cases {
		t.Run(name, func(t *testing.T) {
			proposals, rejected := Planner{}.PlanFromSnapshot(snap, policies, lanes)
			if len(proposals) != 0 || rejected != 0 {
				t.Fatalf("proposals = %+v rejected = %d, want none", proposals, rejected)
			}
		})
	}
}

// TestPlanFromSnapshot_UnprofitableCandidateIsRejected: a candidate whose
// handling and lead-time cost outweigh its priority benefit fails the
// proposal's own validity check and is counted as rejected.
func TestPlanFromSnapshot_UnprofitableCandidateIsRejected(t *testing.T) {
	snap := scheduledSnapshot(map[string]float64{"WH1": 500, "WH2": 300},
		map[string]int{key("WH1", "SKU-1"): 40, key("WH2", "SKU-1"): 30})
	proposals, rejected := Planner{}.PlanFromSnapshot(snap,
		[]Policy{policy("WH1", "SKU-1", 0, 100, 1), policy("WH2", "SKU-1", 0, 100, 1)},
		[]Lane{lane("WH1", "WH2", 5)})
	if len(proposals) != 0 || rejected != 1 {
		t.Fatalf("proposals = %+v rejected = %d, want 0 and 1", proposals, rejected)
	}
}

// TestPlanFromSnapshot_GreedyAllocationSharesBudgets: two lanes into the
// same destination share its need, and two lanes out of the same origin
// share its headroom — the higher-scored candidate is served first and no
// budget is ever exceeded.
func TestPlanFromSnapshot_GreedyAllocationSharesBudgets(t *testing.T) {
	snap := scheduledSnapshot(
		map[string]float64{"WH1": 100, "WH2": 300, "WH3": 400, "WH4": 300},
		map[string]int{key("WH1", "SKU-1"): 40, key("WH3", "SKU-1"): 10, key("WH2", "SKU-1"): 50, key("WH4", "SKU-1"): 50},
	)
	policies := []Policy{
		policy("WH1", "SKU-1", 10, 100, 1),
		policy("WH3", "SKU-1", 0, 100, 1),
		policy("WH2", "SKU-1", 0, 100, 10),
		policy("WH4", "SKU-1", 0, 100, 5),
	}
	lanes := []Lane{lane("WH1", "WH2", 1), lane("WH3", "WH2", 2), lane("WH1", "WH4", 1)}
	proposals, rejected := Planner{}.PlanFromSnapshot(snap, policies, lanes)
	if rejected != 0 {
		t.Fatalf("rejected = %d", rejected)
	}
	// WH1 headroom = 100-40 = 60, safety 10 → 50 donatable; WH2 needs 50,
	// WH4 needs 50. Highest score first: WH1→WH2 (50*10-50*1-1=449) takes
	// the whole WH1 budget; WH3→WH2 finds WH2's need exhausted; WH1→WH4
	// finds WH1's budget exhausted.
	if len(proposals) != 1 {
		t.Fatalf("proposals = %+v, want exactly WH1→WH2", proposals)
	}
	if p := proposals[0]; p.Origin != "WH1" || p.Destination != "WH2" || p.Quantity != 50 {
		t.Fatalf("proposal = %+v", p)
	}

	// With more WH1 headroom both WH1 lanes are served and WH3 tops up
	// nothing (WH2 is already full).
	snap.CapacityBySite["WH1"] = planning.PublishedCapacityPlan{SiteID: "WH1", CapacityOverWindow: 200}
	proposals, _ = Planner{}.PlanFromSnapshot(snap, policies, lanes)
	if len(proposals) != 2 {
		t.Fatalf("proposals = %+v, want WH1→WH2 and WH1→WH4", proposals)
	}
	got := map[SiteID]int{}
	for _, p := range proposals {
		got[p.Destination] += p.Quantity
	}
	if got["WH2"] != 50 || got["WH4"] != 50 {
		t.Fatalf("allocations = %+v", got)
	}
	if proposals[0].Score() < proposals[1].Score() {
		t.Fatalf("proposals must be score-ordered: %+v", proposals)
	}
}

// TestPlanFromSnapshot_IsDeterministic: equal input, equal output.
func TestPlanFromSnapshot_IsDeterministic(t *testing.T) {
	snap := scheduledSnapshot(map[string]float64{"WH1": 500, "WH2": 300, "WH3": 300},
		map[string]int{key("WH1", "SKU-1"): 40, key("WH2", "SKU-1"): 30, key("WH3", "SKU-1"): 30, key("WH2", "SKU-2"): 7, key("WH1", "SKU-2"): 1})
	policies := []Policy{policy("WH1", "SKU-1", 0, 100, 1), policy("WH2", "SKU-1", 0, 100, 3), policy("WH3", "SKU-1", 0, 100, 3),
		policy("WH1", "SKU-2", 0, 100, 1), policy("WH2", "SKU-2", 0, 100, 3)}
	lanes := []Lane{lane("WH1", "WH3", 1), lane("WH1", "WH2", 1)}
	first, _ := Planner{}.PlanFromSnapshot(snap, policies, lanes)
	for i := 0; i < 20; i++ {
		again, _ := Planner{}.PlanFromSnapshot(snap, policies, lanes)
		if len(again) != len(first) {
			t.Fatalf("run %d: %d proposals, want %d", i, len(again), len(first))
		}
		for j := range first {
			if again[j].Origin != first[j].Origin || again[j].Destination != first[j].Destination ||
				again[j].SKU != first[j].SKU || again[j].Quantity != first[j].Quantity {
				t.Fatalf("run %d differs at %d: %+v vs %+v", i, j, again[j], first[j])
			}
		}
	}
	if len(first) != 3 {
		t.Fatalf("proposals = %+v, want 3", first)
	}
}
