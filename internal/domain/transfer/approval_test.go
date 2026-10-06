package transfer

import (
	"errors"
	"testing"
	"time"

	"github.com/claudioed/network-inventory-planning/internal/domain/planning"
)

var approvalNow = time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)

func approvalSnapshot(t *testing.T) planning.PlanningSnapshot {
	t.Helper()
	now := approvalNow
	cap1, err := planning.NewSiteCapability("WH1", true, true, 3, now.Add(-time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	cap2, err := planning.NewSiteCapability("WH2", true, true, 3, now.Add(-time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	plan1, err := planning.NewPublishedCapacityPlan(planning.PublishedCapacityPlan{
		PlanID: "plan-1", SiteID: "WH1", Location: "PATH-ZONE-A", PathID: "pick-rebin-pack",
		WindowStart: now.Add(time.Hour), WindowEnd: now.Add(8 * time.Hour),
		AssignedDemand: 100, CapacityOverWindow: 500, PublishedAt: now.Add(-time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	plan2, err := planning.NewPublishedCapacityPlan(planning.PublishedCapacityPlan{
		PlanID: "plan-2", SiteID: "WH2", Location: "PATH-ZONE-B", PathID: "pick-rebin-pack",
		WindowStart: now.Add(time.Hour), WindowEnd: now.Add(8 * time.Hour),
		AssignedDemand: 10, CapacityOverWindow: 300, PublishedAt: now.Add(-time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	return planning.PlanningSnapshot{
		AsOf: now.Add(-time.Minute),
		Capabilities: map[string]planning.SiteCapability{
			"WH1": cap1, "WH2": cap2,
		},
		DemandBySiteSKU: map[string]int{
			"WH1\x00SKU-1": 40,
			"WH2\x00SKU-1": 10,
		},
		CapacityBySite: map[string]planning.PublishedCapacityPlan{
			"WH1": plan1, "WH2": plan2,
		},
	}
}

func approvalInput() ProposalInput {
	return ProposalInput{
		ID:                TransferID("trf-appr-1"),
		IdempotencyKey:    "idem-appr-1",
		OriginSiteID:      "WH1",
		DestinationSiteID: "WH2",
		SKU:               "SKU-1",
		Quantity:          5,
		PolicyVersion:     "policy-v3",
		OperatorReason:    "operator approved rebalance",
		ProposalAsOf:      approvalNow.Add(-5 * time.Minute),
		ExpiresAt:         approvalNow.Add(time.Hour),
		Now:               approvalNow,
	}
}

func TestValidateApprovalPassesCurrentFacts(t *testing.T) {
	err := ValidateApproval(approvalInput(), ApprovalFacts{Snapshot: approvalSnapshot(t), MaxStaleness: 10 * time.Minute})
	if err != nil {
		t.Fatalf("valid approval refused: %v", err)
	}
}

func TestValidateApprovalFailsClosed(t *testing.T) {
	cases := []struct {
		name   string
		mutS   func(snap *planning.PlanningSnapshot)
		mutIn  func(in *ProposalInput)
		hasSub string
	}{
		{
			name:   "empty snapshot",
			mutS:   func(snap *planning.PlanningSnapshot) { *snap = planning.PlanningSnapshot{} },
			hasSub: "no participating sites",
		},
		{
			name: "origin missing facts",
			mutS: func(snap *planning.PlanningSnapshot) {
				delete(snap.Capabilities, "WH1")
				delete(snap.CapacityBySite, "WH1")
			},
			hasSub: "origin site WH1 has no complete facts",
		},
		{
			name: "origin disabled",
			mutS: func(snap *planning.PlanningSnapshot) {
				cap := snap.Capabilities["WH1"]
				cap.TransferOriginEnabled = false
				snap.Capabilities["WH1"] = cap
			},
			hasSub: "not transfer-origin enabled",
		},
		{
			name: "destination missing facts",
			mutS: func(snap *planning.PlanningSnapshot) {
				delete(snap.Capabilities, "WH2")
			},
			hasSub: "destination site WH2 has no complete facts",
		},
		{
			name: "destination disabled",
			mutS: func(snap *planning.PlanningSnapshot) {
				cap := snap.Capabilities["WH2"]
				cap.TransferDestinationEnabled = false
				snap.Capabilities["WH2"] = cap
			},
			hasSub: "not transfer-destination enabled",
		},
		{
			name: "destination has no demand for sku",
			mutS: func(snap *planning.PlanningSnapshot) {
				delete(snap.DemandBySiteSKU, "WH2\x00SKU-1")
			},
			hasSub: "no in-window demand",
		},
		{
			name: "origin has no capacity plan",
			mutS: func(snap *planning.PlanningSnapshot) {
				delete(snap.CapacityBySite, "WH1")
			},
			hasSub: "no published capacity plan",
		},
		{
			name: "origin capacity cannot cover transfer",
			mutS: func(snap *planning.PlanningSnapshot) {
				plan := snap.CapacityBySite["WH1"]
				plan.CapacityOverWindow = 42 // demand 40 + transfer 5 > 42
				snap.CapacityBySite["WH1"] = plan
			},
			hasSub: "cannot cover",
		},
		{
			name: "unknown sku at destination",
			mutIn: func(in *ProposalInput) {
				in.SKU = "SKU-UNKNOWN"
			},
			hasSub: "no in-window demand",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			snap := approvalSnapshot(t)
			if tc.mutS != nil {
				tc.mutS(&snap)
			}
			in := approvalInput()
			if tc.mutIn != nil {
				tc.mutIn(&in)
			}
			err := ValidateApproval(in, ApprovalFacts{Snapshot: snap, MaxStaleness: 10 * time.Minute})
			if err == nil {
				t.Fatal("stale/missing fact must fail closed")
			}
			if !errors.Is(err, ErrFactsIncomplete) {
				t.Fatalf("err = %v, want ErrFactsIncomplete", err)
			}
			if tc.hasSub != "" && !containsSub(err.Error(), tc.hasSub) {
				t.Fatalf("err = %q, want substring %q", err.Error(), tc.hasSub)
			}
		})
	}
}

func containsSub(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || indexOfStr(s, sub) >= 0)
}

func indexOfStr(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
