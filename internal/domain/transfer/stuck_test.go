package transfer

import (
	"testing"
	"time"
)

func TestParseStuckThresholdsDefaults(t *testing.T) {
	th, err := ParseStuckThresholds("")
	if err != nil {
		t.Fatalf("parse empty: %v", err)
	}
	if got := th.ThresholdFor(StateAllocating); got != time.Hour {
		t.Errorf("default ALLOCATING = %s, want 1h", got)
	}
	if got := th.ThresholdFor(StatePicked); got != 24*time.Hour {
		t.Errorf("default PICKED = %s, want 24h", got)
	}
	if got := th.ThresholdFor(StateInTransit); got != 72*time.Hour {
		t.Errorf("default IN_TRANSIT = %s, want 72h", got)
	}
	// Unlisted states fall back to the flat default.
	if got := th.ThresholdFor(StateApproved); got != DefaultStuckThreshold {
		t.Errorf("default APPROVED = %s, want %s", got, DefaultStuckThreshold)
	}
}

func TestParseStuckThresholdsPerStateOverride(t *testing.T) {
	th, err := ParseStuckThresholds("ALLOCATING=5m,PICKED=48h")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if got := th.ThresholdFor(StateAllocating); got != 5*time.Minute {
		t.Errorf("ALLOCATING override = %s, want 5m", got)
	}
	if got := th.ThresholdFor(StatePicked); got != 48*time.Hour {
		t.Errorf("PICKED override = %s, want 48h", got)
	}
	// IN_TRANSIT keeps its default despite the partial override.
	if got := th.ThresholdFor(StateInTransit); got != 72*time.Hour {
		t.Errorf("IN_TRANSIT after partial override = %s, want 72h", got)
	}
}

func TestParseStuckThresholdsRejectsGarbage(t *testing.T) {
	for _, raw := range []string{
		"ALLOCATING",          // no =
		"NOT_A_STATE=1h",      // unknown state
		"ALLOCATING=one-hour", // unparsable duration
		"ALLOCATING=0s",       // non-positive
		"ALLOCATING=-1h",      // negative
	} {
		if _, err := ParseStuckThresholds(raw); err == nil {
			t.Errorf("ParseStuckThresholds(%q) = nil error, want a refusal (fail-closed config)", raw)
		}
	}
}

func TestStuckCheckEvaluate(t *testing.T) {
	now := time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)
	check := StuckCheck{Thresholds: StuckThresholds{
		StateAllocating: time.Hour,
		StatePicked:     24 * time.Hour,
	}}
	views := []StuckView{
		{ID: "trf-ok", State: StateAllocating, UpdatedAt: now.Add(-10 * time.Minute)},     // fresh
		{ID: "trf-stuck", State: StateAllocating, UpdatedAt: now.Add(-2 * time.Hour)},     // > 1h
		{ID: "trf-picked-fresh", State: StatePicked, UpdatedAt: now.Add(-2 * time.Hour)},  // < 24h
		{ID: "trf-default", State: StateApproved, UpdatedAt: now.Add(-30 * time.Hour)},    // > flat 24h default
		{ID: "trf-terminal", State: StateReceived, UpdatedAt: now.Add(-1000 * time.Hour)}, // terminal: never
	}
	stuck := check.Evaluate(views, now)
	if len(stuck) != 2 {
		t.Fatalf("stuck = %+v, want 2", stuck)
	}
	if stuck[0].TransferID != "trf-stuck" || stuck[0].State != StateAllocating {
		t.Errorf("first = %+v, want trf-stuck ALLOCATING", stuck[0])
	}
	if stuck[0].ThresholdSeconds != 3600 {
		t.Errorf("threshold_seconds = %d, want 3600", stuck[0].ThresholdSeconds)
	}
	if stuck[0].AgeSeconds != 7200 {
		t.Errorf("age_seconds = %d, want 7200", stuck[0].AgeSeconds)
	}
	if stuck[1].TransferID != "trf-default" {
		t.Errorf("second = %+v, want trf-default (flat-default threshold)", stuck[1])
	}
}

func TestStuckCheckFutureUpdateNeverStuck(t *testing.T) {
	now := time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)
	check := StuckCheck{Thresholds: StuckThresholds{StateAllocating: time.Hour}}
	stuck := check.Evaluate([]StuckView{{ID: "trf-future", State: StateAllocating, UpdatedAt: now.Add(time.Hour)}}, now)
	if len(stuck) != 0 {
		t.Fatalf("future-updated transfer flagged stuck: %+v", stuck)
	}
}

func TestStateAdvancedSince(t *testing.T) {
	created := time.Date(2026, 10, 7, 10, 0, 0, 0, time.UTC)
	trf, err := ProposeTransfer(ProposalInput{
		ID:                "trf-a",
		IdempotencyKey:    "idem-a",
		OriginSiteID:      "WH1",
		DestinationSiteID: "WH2",
		SKU:               "SKU-1",
		Quantity:          5,
		PolicyVersion:     "v1",
		ProposalAsOf:      created.Add(-time.Minute),
		ExpiresAt:         created.Add(24 * time.Hour),
		Now:               created,
	})
	if err != nil {
		t.Fatalf("propose: %v", err)
	}
	// All 4 creation transitions (drafted/proposed/approved/allocation-
	// requested) after version 0.
	advanced := trf.StateAdvancedSince(0)
	if len(advanced) != 2 { // ProposeTransfer only appends drafted+proposed
		t.Fatalf("advanced since 0 = %d, want 2", len(advanced))
	}
	// The creation entry is DRAFT -> DRAFT (ProposeTransfer initialises
	// the aggregate in DRAFT, then records the drafted transition).
	if advanced[0].From != StateDraft || advanced[0].To != StateDraft {
		t.Errorf("first advanced = %+v, want DRAFT -> DRAFT", advanced[0])
	}
	if advanced[1].To != StateProposed {
		t.Errorf("second advanced = %+v, want -> PROPOSED", advanced[1])
	}
	if advanced[1].AgeSeconds != 0 {
		t.Errorf("age = %d, want 0 (same instant as creation)", advanced[1].AgeSeconds)
	}
	// Nothing new since version == len(audit).
	if got := trf.StateAdvancedSince(int64(len(trf.Audit()))); len(got) != 0 {
		t.Errorf("advanced since tip = %d, want 0", len(got))
	}
}

func TestNonTerminalStates(t *testing.T) {
	for _, s := range []TransferState{StateDraft, StateProposed, StateApproved, StateAllocating,
		StateAllocated, StatePicked, StateInTransit, StateArrived} {
		if !s.NonTerminal() {
			t.Errorf("%s should be non-terminal", s)
		}
	}
	for _, s := range []TransferState{StateReceived, StateUnfulfillable, StateCancelled} {
		if s.NonTerminal() {
			t.Errorf("%s should be terminal", s)
		}
	}
}

func TestNonTerminalTail(t *testing.T) {
	for _, s := range []TransferState{StatePicked, StateInTransit, StateArrived, StateReceived} {
		if !s.NonTerminalTail() {
			t.Errorf("%s must be in the picked tail", s)
		}
	}
	for _, s := range []TransferState{StateDraft, StateAllocating, StateAllocated, StateCancelled, StateUnfulfillable} {
		if s.NonTerminalTail() {
			t.Errorf("%s must NOT be in the picked tail", s)
		}
	}
}

func TestAnalyticsEventNames(t *testing.T) {
	// The EventName strings are the CloudEvents wire names: they must
	// stay exactly these (the encoder and the AsyncAPI agree on them).
	if (StateAdvanced{}).EventName() != "TransferStateAdvanced" {
		t.Fatal("StateAdvanced event name drifted")
	}
	if (StuckDetected{}).EventName() != "TransferStuckDetected" {
		t.Fatal("StuckDetected event name drifted")
	}
	if (RebalanceRunCompleted{}).EventName() != "RebalanceRunCompleted" {
		t.Fatal("RebalanceRunCompleted event name drifted")
	}
}
