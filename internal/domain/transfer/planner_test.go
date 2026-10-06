package transfer

import (
	"testing"
	"time"
)

func TestPlannerGeneratesBoundedExplainableProposal(t *testing.T) {
	asOf := time.Date(2026, 10, 5, 15, 0, 0, 0, time.UTC)
	proposals := (Planner{}).Generate(
		[]Position{
			{Site: "GRU", SKU: "SKU-1", Available: 140, CustomerReservations: 10, CommittedOutbound: 10, AsOf: asOf},
			{Site: "REC", SKU: "SKU-1", Available: 10, ConfirmedInbound: 5, AsOf: asOf},
		},
		[]Policy{
			{Version: "policy-2026-10", Site: "GRU", SKU: "SKU-1", SafetyStock: 50, TargetStock: 80, UnitPriority: 2},
			{Version: "policy-2026-10", Site: "REC", SKU: "SKU-1", SafetyStock: 20, TargetStock: 70, UnitPriority: 6},
		},
		[]Lane{{Origin: "GRU", Destination: "REC", LeadTime: 24 * time.Hour, UnitHandlingCost: 1, Enabled: true}},
	)

	if len(proposals) != 1 {
		t.Fatalf("proposal count = %d, want 1", len(proposals))
	}
	proposal := proposals[0]
	if proposal.Quantity != 55 { // min(GRU usable 120 - safety 50, REC target 70 - (10 + 5))
		t.Errorf("quantity = %d, want 55", proposal.Quantity)
	}
	if proposal.Score() != 274 { // 55*6 - 55*1 - 1
		t.Errorf("score = %d, want 274", proposal.Score())
	}
	if !proposal.Valid() || proposal.PolicyVersion != "policy-2026-10" || proposal.PositionAsOf != asOf {
		t.Errorf("proposal should be auditable and valid: %+v", proposal)
	}
	if len(proposal.Reasons) != 3 {
		t.Errorf("reason count = %d, want 3", len(proposal.Reasons))
	}
}

func TestPlannerDoesNotBreachSourceSafetyStock(t *testing.T) {
	asOf := time.Date(2026, 10, 5, 15, 0, 0, 0, time.UTC)
	proposals := (Planner{}).Generate(
		[]Position{{Site: "A", SKU: "SKU", Available: 50, AsOf: asOf}, {Site: "B", SKU: "SKU", Available: 0, AsOf: asOf}},
		[]Policy{{Version: "v1", Site: "A", SKU: "SKU", SafetyStock: 50}, {Version: "v1", Site: "B", SKU: "SKU", TargetStock: 100, UnitPriority: 10}},
		[]Lane{{Origin: "A", Destination: "B", Enabled: true}},
	)
	if len(proposals) != 0 {
		t.Fatalf("proposal count = %d, want no proposal when surplus is zero", len(proposals))
	}
}

func TestPlannerRejectsStaleMixedSnapshot(t *testing.T) {
	asOf := time.Date(2026, 10, 5, 15, 0, 0, 0, time.UTC)
	proposals := (Planner{}).Generate(
		[]Position{{Site: "A", SKU: "SKU", Available: 100, AsOf: asOf}, {Site: "B", SKU: "SKU", Available: 0, AsOf: asOf.Add(-time.Minute)}},
		[]Policy{{Version: "v1", Site: "A", SKU: "SKU", SafetyStock: 10}, {Version: "v1", Site: "B", SKU: "SKU", TargetStock: 90, UnitPriority: 10}},
		[]Lane{{Origin: "A", Destination: "B", Enabled: true}},
	)
	if len(proposals) != 0 {
		t.Fatalf("proposal count = %d, want no proposal for mixed snapshot", len(proposals))
	}
}

func TestPlannerOrdersTiesDeterministically(t *testing.T) {
	asOf := time.Date(2026, 10, 5, 15, 0, 0, 0, time.UTC)
	proposals := (Planner{}).Generate(
		[]Position{{Site: "A", SKU: "B", Available: 20, AsOf: asOf}, {Site: "C", SKU: "B", Available: 0, AsOf: asOf}, {Site: "A", SKU: "A", Available: 20, AsOf: asOf}, {Site: "C", SKU: "A", Available: 0, AsOf: asOf}},
		[]Policy{{Version: "v1", Site: "A", SKU: "A", SafetyStock: 10}, {Version: "v1", Site: "C", SKU: "A", TargetStock: 10, UnitPriority: 1}, {Version: "v1", Site: "A", SKU: "B", SafetyStock: 10}, {Version: "v1", Site: "C", SKU: "B", TargetStock: 10, UnitPriority: 1}},
		[]Lane{{Origin: "A", Destination: "C", Enabled: true}},
	)
	if len(proposals) != 2 || proposals[0].SKU != "A" || proposals[1].SKU != "B" {
		t.Fatalf("proposals = %+v, want SKU A then B", proposals)
	}
}
