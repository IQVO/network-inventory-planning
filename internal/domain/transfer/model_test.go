package transfer

import (
	"testing"
	"time"
)

func TestPolicyDeficitAndLaneAllowance(t *testing.T) {
	policy := Policy{Site: "B", SKU: "SKU", TargetStock: 10}
	if got := policy.Deficit(Position{Available: 20}); got != 0 {
		t.Errorf("surplus deficit = %d, want 0", got)
	}
	if got := policy.Deficit(Position{Available: 1, ConfirmedInbound: 2}); got != 7 {
		t.Errorf("deficit = %d, want 7", got)
	}
	lane := Lane{Origin: "A", Destination: "B", Enabled: true}
	if !lane.Allows("A", "B") || lane.Allows("B", "A") {
		t.Fatal("lane allowance should be directed")
	}
}

func TestProposalValidityRejectsInvalidIdentityAndNonPositiveScore(t *testing.T) {
	asOf := time.Now().UTC()
	valid := Proposal{Origin: "A", Destination: "B", SKU: "SKU", Quantity: 1, PolicyVersion: "v1", PositionAsOf: asOf, ScoreBreakdown: ScoreBreakdown{PriorityBenefit: 2}}
	if !valid.Valid() {
		t.Fatal("baseline proposal should be valid")
	}
	for _, proposal := range []Proposal{
		{Origin: "", Destination: "B", SKU: "SKU", Quantity: 1, PolicyVersion: "v1", PositionAsOf: asOf, ScoreBreakdown: ScoreBreakdown{PriorityBenefit: 2}},
		{Origin: "A", Destination: "A", SKU: "SKU", Quantity: 1, PolicyVersion: "v1", PositionAsOf: asOf, ScoreBreakdown: ScoreBreakdown{PriorityBenefit: 2}},
		{Origin: "A", Destination: "B", SKU: "", Quantity: 1, PolicyVersion: "v1", PositionAsOf: asOf, ScoreBreakdown: ScoreBreakdown{PriorityBenefit: 2}},
		{Origin: "A", Destination: "B", SKU: "SKU", Quantity: 0, PolicyVersion: "v1", PositionAsOf: asOf, ScoreBreakdown: ScoreBreakdown{PriorityBenefit: 2}},
		{Origin: "A", Destination: "B", SKU: "SKU", Quantity: 1, PolicyVersion: "", PositionAsOf: asOf, ScoreBreakdown: ScoreBreakdown{PriorityBenefit: 2}},
		{Origin: "A", Destination: "B", SKU: "SKU", Quantity: 1, PolicyVersion: "v1", ScoreBreakdown: ScoreBreakdown{PriorityBenefit: 2}},
		{Origin: "A", Destination: "B", SKU: "SKU", Quantity: 1, PolicyVersion: "v1", PositionAsOf: asOf, ScoreBreakdown: ScoreBreakdown{}},
	} {
		if proposal.Valid() {
			t.Errorf("proposal should be invalid: %+v", proposal)
		}
	}
}

func TestPlannerSkipsDisabledAndIncompleteLanes(t *testing.T) {
	asOf := time.Date(2026, 10, 5, 15, 0, 0, 0, time.UTC)
	positions := []Position{{Site: "A", SKU: "SKU", Available: 100, AsOf: asOf}, {Site: "B", SKU: "SKU", Available: 0, AsOf: asOf}}
	policies := []Policy{{Version: "v1", Site: "A", SKU: "SKU", SafetyStock: 10}, {Version: "v1", Site: "B", SKU: "SKU", TargetStock: 50, UnitPriority: 1}}
	for _, lanes := range [][]Lane{
		{{Origin: "A", Destination: "B", Enabled: false}},
		{{Origin: "A", Destination: "C", Enabled: true}},
	} {
		if got := (Planner{}).Generate(positions, policies, lanes); len(got) != 0 {
			t.Errorf("proposals = %+v, want none", got)
		}
	}
}
