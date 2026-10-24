package transfer

import (
	"strings"
	"testing"
	"time"
)

type roundCase struct {
	name                    string
	requested, units, avail int
	wantOK                  bool
	wantCases, wantRem      int
}

func TestRoundToCases(t *testing.T) {
	cases := []roundCase{
		{"exactly one case", 12, 12, 100, true, 1, 0},
		{"exactly two cases", 24, 12, 100, true, 2, 0},
		{"k*u+1 rounds down, remainder +1", 25, 12, 100, true, 2, 1},
		{"k*u-1 rounds up, remainder -1", 23, 12, 100, true, 2, -1},
		{"just under half a case rounds down", 17, 12, 100, true, 1, 5},
		{"exactly half a case ties UP", 18, 12, 100, true, 2, -6},
		{"just over half a case rounds up", 19, 12, 100, true, 2, -5},
		{"odd case size, below half rounds down", 19, 13, 100, true, 1, 6},
		{"odd case size, above half rounds up", 20, 13, 100, true, 2, -6},
		{"demand 1 still moves one case", 1, 12, 100, true, 1, -11},
		{"demand below half a case is lifted to one case", 5, 12, 100, true, 1, -7},
		{"half a case of 12 is exactly one case", 6, 12, 100, true, 1, -6},
		{"smallest legal case size", 1, 2, 100, true, 1, -1},
		{"demand 0 moves nothing", 0, 12, 100, false, 0, 0},
		{"negative demand moves nothing", -3, 12, 100, false, 0, 0},
		{"a case of 1 is not a case", 5, 1, 100, false, 0, 0},
		{"a case of 0 is not a case", 5, 0, 100, false, 0, 0},
		{"source caps the rounding", 36, 12, 30, true, 2, 12},
		{"source exactly covers the rounded cases", 25, 12, 24, true, 2, 1},
		{"source one eaches short of the rounded cases drops a case", 25, 12, 23, true, 1, 13},
		{"source exactly one case", 40, 12, 12, true, 1, 28},
		{"source one eaches short of one case moves nothing", 40, 12, 11, false, 0, 0},
		{"source empty moves nothing", 40, 12, 0, false, 0, 0},
		{"negative source moves nothing", 40, 12, -5, false, 0, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, tc.run)
	}
}

// run asserts one table row: the verdict, the exact report, the invariants
// every produced rounding keeps and the zero report of a refusal.
func (tc roundCase) run(t *testing.T) {
	got, ok := RoundToCases(tc.requested, tc.units, tc.avail)
	if ok != tc.wantOK {
		t.Fatalf("ok = %v, want %v (%+v)", ok, tc.wantOK, got)
	}
	if !ok {
		if got != (CaseRounding{}) {
			t.Fatalf("a refusal must carry the zero report, got %+v", got)
		}
		return
	}
	want := CaseRounding{
		CaseMultiple: tc.units, Cases: tc.wantCases, RequestedEaches: tc.requested,
		RoundedEaches: tc.wantCases * tc.units, RemainderEaches: tc.wantRem,
	}
	if got != want {
		t.Fatalf("got %+v, want %+v", got, want)
	}
	if got.RoundedEaches%tc.units != 0 || got.RoundedEaches > tc.avail {
		t.Fatalf("rounded %d must be a multiple of %d and within availability %d", got.RoundedEaches, tc.units, tc.avail)
	}
	if got.RequestedEaches != got.RoundedEaches+got.RemainderEaches {
		t.Fatalf("the demand total is not preserved: %+v", got)
	}
	if err := got.check(got.RoundedEaches); err != nil {
		t.Fatalf("a produced rounding must pass its own check: %v", err)
	}
}

// TestRoundToCasesIsIdempotent: re-rounding an already-rounded quantity
// changes nothing, so approving a proposal's own quantity is stable.
func TestRoundToCasesIsIdempotent(t *testing.T) {
	for _, units := range []int{2, 3, 12, 13, 48} {
		for requested := 1; requested <= 5*units; requested++ {
			first, ok := RoundToCases(requested, units, 1000)
			if !ok {
				t.Fatalf("units %d requested %d: refused with ample stock", units, requested)
			}
			again, ok := RoundToCases(first.RoundedEaches, units, 1000)
			if !ok || again.RoundedEaches != first.RoundedEaches || again.RemainderEaches != 0 {
				t.Fatalf("units %d requested %d: re-rounding %d gave %+v", units, requested, first.RoundedEaches, again)
			}
			if abs(first.RemainderEaches)*2 > units && first.Cases > 1 {
				t.Fatalf("units %d requested %d: rounded %d is not the nearest multiple", units, requested, first.RoundedEaches)
			}
		}
	}
}

func abs(n int) int {
	if n < 0 {
		return -n
	}
	return n
}

func TestCaseMultiplesOf(t *testing.T) {
	m := CaseMultiples{"A": 12, "B": 1, "C": 0, "D": 2}
	for sku, want := range map[SKU]struct {
		units int
		ok    bool
	}{
		"A": {12, true}, "B": {0, false}, "C": {0, false}, "D": {2, true}, "ABSENT": {0, false},
	} {
		units, ok := m.Of(sku)
		if units != want.units || ok != want.ok {
			t.Errorf("Of(%s) = %d,%v want %d,%v", sku, units, ok, want.units, want.ok)
		}
	}
	if _, ok := CaseMultiples(nil).Of("A"); ok {
		t.Error("a nil map has no multiples")
	}
}

func TestCaseRoundingCheckRefusesInconsistentReports(t *testing.T) {
	good := CaseRounding{CaseMultiple: 12, Cases: 2, RequestedEaches: 25, RoundedEaches: 24, RemainderEaches: 1}
	if err := good.check(24); err != nil {
		t.Fatalf("good report refused: %v", err)
	}
	bad := map[string]struct {
		mutate func(CaseRounding) CaseRounding
		qty    int
		want   string
	}{
		"multiple 1":        {func(r CaseRounding) CaseRounding { r.CaseMultiple = 1; return r }, 24, "case multiple"},
		"zero cases":        {func(r CaseRounding) CaseRounding { r.Cases = 0; return r }, 24, "cases must"},
		"rounded mismatch":  {func(r CaseRounding) CaseRounding { r.RoundedEaches = 25; return r }, 25, "not 2 cases"},
		"quantity mismatch": {func(r CaseRounding) CaseRounding { return r }, 25, "differs from the quantity"},
		"requested zero":    {func(r CaseRounding) CaseRounding { r.RequestedEaches = 0; r.RemainderEaches = -24; return r }, 24, "requested eaches"},
		"remainder wrong":   {func(r CaseRounding) CaseRounding { r.RemainderEaches = 2; return r }, 24, "remainder"},
	}
	for name, tc := range bad {
		err := tc.mutate(good).check(tc.qty)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: err = %v, want it to mention %q", name, err, tc.want)
		}
	}
	// the smallest valid report: 1 case, requested 1
	if err := (CaseRounding{CaseMultiple: 2, Cases: 1, RequestedEaches: 1, RoundedEaches: 2, RemainderEaches: -1}).check(2); err != nil {
		t.Errorf("boundary report refused: %v", err)
	}
	if err := (CaseRounding{CaseMultiple: 2, Cases: 1, RequestedEaches: 1, RoundedEaches: 2, RemainderEaches: -1}).check(2); err != nil {
		t.Errorf("boundary report refused: %v", err)
	}
}

func TestProposalValidChecksTheCaseReport(t *testing.T) {
	p := Proposal{
		Origin: "A", Destination: "B", SKU: "S", Quantity: 24, PolicyVersion: "v", PositionAsOf: time.Unix(1, 0),
		ScoreBreakdown: ScoreBreakdown{PriorityBenefit: 100},
		CaseRounding:   &CaseRounding{CaseMultiple: 12, Cases: 2, RequestedEaches: 25, RoundedEaches: 24, RemainderEaches: 1},
	}
	if !p.Valid() {
		t.Fatal("a consistent case report must be valid")
	}
	p.Quantity = 25
	if p.Valid() {
		t.Fatal("a quantity that is not the rounded eaches must be invalid")
	}
}

// ---- planner: Generate -------------------------------------------------

func caseGenerate(t *testing.T, multiples CaseMultiples, originAvailable, destAvailable, target int) []Proposal {
	t.Helper()
	asOf := time.Date(2026, 10, 5, 15, 0, 0, 0, time.UTC)
	return (Planner{}).WithCaseMultiples(multiples).Generate(
		[]Position{{Site: "A", SKU: "SKU-1", Available: originAvailable, AsOf: asOf}, {Site: "B", SKU: "SKU-1", Available: destAvailable, AsOf: asOf}},
		[]Policy{{Version: "v1", Site: "A", SKU: "SKU-1", SafetyStock: 10}, {Version: "v1", Site: "B", SKU: "SKU-1", TargetStock: target, UnitPriority: 10}},
		[]Lane{{Origin: "A", Destination: "B", Enabled: true}},
	)
}

func TestGenerateRoundsACaseSKUAndReportsTheRemainder(t *testing.T) {
	// deficit 25 (target 25, none on hand), source surplus 190: 25 -> 24, remainder 1.
	got := caseGenerate(t, CaseMultiples{"SKU-1": 12}, 200, 0, 25)
	if len(got) != 1 {
		t.Fatalf("proposals = %+v", got)
	}
	p := got[0]
	want := CaseRounding{CaseMultiple: 12, Cases: 2, RequestedEaches: 25, RoundedEaches: 24, RemainderEaches: 1}
	if p.Quantity != 24 || p.CaseRounding == nil || *p.CaseRounding != want {
		t.Fatalf("proposal = %+v rounding = %+v, want quantity 24 and %+v", p, p.CaseRounding, want)
	}
	if p.Score() != 240 { // 24 * priority 10, the score follows the moved quantity
		t.Errorf("score = %d, want 240", p.Score())
	}
}

func TestGenerateWithoutAnActiveCaseLevelIsUnchanged(t *testing.T) {
	plain := caseGenerate(t, nil, 200, 0, 25)
	for name, multiples := range map[string]CaseMultiples{
		"another sku has a level": {"SKU-9": 12},
		"a level of 1":            {"SKU-1": 1},
		"empty":                   {},
	} {
		got := caseGenerate(t, multiples, 200, 0, 25)
		if len(got) != 1 || got[0].Quantity != 25 || got[0].CaseRounding != nil {
			t.Errorf("%s: %+v", name, got)
		}
		if got[0].Score() != plain[0].Score() {
			t.Errorf("%s: score changed", name)
		}
	}
	if len(plain) != 1 || plain[0].Quantity != 25 || plain[0].CaseRounding != nil {
		t.Fatalf("plain = %+v", plain)
	}
}

func TestGenerateNeverExceedsTheSourceOrDropsBelowOneCase(t *testing.T) {
	// source surplus = 40 - safety 10 = 30; deficit 100: 100 rounds to 8 cases,
	// the source caps it to 2 (24) and the unserved 76 stay on the report.
	got := caseGenerate(t, CaseMultiples{"SKU-1": 12}, 40, 0, 100)
	if len(got) != 1 || got[0].Quantity != 24 || got[0].RemainderEaches != 76 || got[0].RequestedEaches != 100 {
		t.Fatalf("capped = %+v", got)
	}
	// surplus exactly one case.
	got = caseGenerate(t, CaseMultiples{"SKU-1": 12}, 22, 0, 100)
	if len(got) != 1 || got[0].Quantity != 12 || got[0].RemainderEaches != 88 {
		t.Fatalf("one case = %+v", got)
	}
	// surplus 11 < one case: no proposal (a loose partial case is not moved).
	if got = caseGenerate(t, CaseMultiples{"SKU-1": 12}, 21, 0, 100); len(got) != 0 {
		t.Fatalf("below one case = %+v", got)
	}
	// the same stock for a plain SKU still proposes the 11 loose eaches.
	if got = caseGenerate(t, nil, 21, 0, 100); len(got) != 1 || got[0].Quantity != 11 {
		t.Fatalf("plain = %+v", got)
	}
}

// ---- planner: PlanFromSnapshot -----------------------------------------

func caseSchedule(multiples CaseMultiples, capacity map[string]float64, demand map[string]int, policies []Policy) ([]Proposal, int) {
	return (Planner{}).WithCaseMultiples(multiples).PlanFromSnapshot(
		scheduledSnapshot(capacity, demand), policies, []Lane{lane("WH1", "WH2", 0)})
}

func TestPlanFromSnapshotRoundsCaseSKUsToCases(t *testing.T) {
	policies := []Policy{policy("WH1", "SKU-1", 0, 1000, 1), policy("WH2", "SKU-1", 0, 1000, 10)}
	capacity := map[string]float64{"WH1": 500, "WH2": 300}
	demand := map[string]int{key("WH1", "SKU-1"): 40, key("WH2", "SKU-1"): 25}
	got, rejected := caseSchedule(CaseMultiples{"SKU-1": 12}, capacity, demand, policies)
	if rejected != 0 || len(got) != 1 {
		t.Fatalf("got %+v rejected %d", got, rejected)
	}
	want := CaseRounding{CaseMultiple: 12, Cases: 2, RequestedEaches: 25, RoundedEaches: 24, RemainderEaches: 1}
	if got[0].Quantity != 24 || got[0].CaseRounding == nil || *got[0].CaseRounding != want {
		t.Fatalf("got %+v / %+v", got[0], got[0].CaseRounding)
	}
	plain, rejected := caseSchedule(nil, capacity, demand, policies)
	if rejected != 0 || len(plain) != 1 || plain[0].Quantity != 25 || plain[0].CaseRounding != nil {
		t.Fatalf("plain = %+v rejected %d", plain, rejected)
	}
}

func TestPlanFromSnapshotCountsASourceBelowOneCaseAsRejected(t *testing.T) {
	policies := []Policy{policy("WH1", "SKU-1", 0, 1000, 1), policy("WH2", "SKU-1", 0, 1000, 10)}
	// WH1 headroom = 50 - 40 = 10 < one case of 12; destination needs 25.
	capacity := map[string]float64{"WH1": 50, "WH2": 300}
	demand := map[string]int{key("WH1", "SKU-1"): 40, key("WH2", "SKU-1"): 25}
	got, rejected := caseSchedule(CaseMultiples{"SKU-1": 12}, capacity, demand, policies)
	if len(got) != 0 || rejected != 1 {
		t.Fatalf("got %+v rejected %d, want none and 1", got, rejected)
	}
	// a plain SKU moves the 10 loose eaches and rejects nothing.
	plain, rejected := caseSchedule(nil, capacity, demand, policies)
	if rejected != 0 || len(plain) != 1 || plain[0].Quantity != 10 {
		t.Fatalf("plain = %+v rejected %d", plain, rejected)
	}
	// exactly one case of headroom moves one case.
	capacity["WH1"] = 52
	got, rejected = caseSchedule(CaseMultiples{"SKU-1": 12}, capacity, demand, policies)
	if rejected != 0 || len(got) != 1 || got[0].Quantity != 12 || got[0].Cases != 1 || got[0].RemainderEaches != 13 {
		t.Fatalf("one case = %+v rejected %d", got, rejected)
	}
}

// A rounded-up proposal must stay approvable against the same snapshot.
func TestPlanFromSnapshotRoundedProposalIsApprovable(t *testing.T) {
	policies := []Policy{policy("WH1", "SKU-1", 0, 1000, 1), policy("WH2", "SKU-1", 0, 1000, 10)}
	capacity := map[string]float64{"WH1": 500, "WH2": 300}
	demand := map[string]int{key("WH1", "SKU-1"): 40, key("WH2", "SKU-1"): 23}
	snap := scheduledSnapshot(capacity, demand)
	got, _ := (Planner{}).WithCaseMultiples(CaseMultiples{"SKU-1": 12}).PlanFromSnapshot(snap, policies, []Lane{lane("WH1", "WH2", 0)})
	if len(got) != 1 || got[0].Quantity != 24 || got[0].RemainderEaches != -1 {
		t.Fatalf("got %+v", got)
	}
	in := ProposalInput{OriginSiteID: "WH1", DestinationSiteID: "WH2", SKU: "SKU-1", Quantity: got[0].Quantity}
	if err := ValidateApproval(in, ApprovalFacts{Snapshot: snap}); err != nil {
		t.Fatalf("rounded proposal not approvable: %v", err)
	}
}
