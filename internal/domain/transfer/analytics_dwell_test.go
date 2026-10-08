package transfer

import (
	"testing"
	"time"
)

func proposeAt(t *testing.T, created time.Time) *InterWarehouseTransfer {
	t.Helper()
	trf, err := ProposeTransfer(ProposalInput{
		ID:                "trf-dwell",
		IdempotencyKey:    "idem-dwell",
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
	return trf
}

func dwellOf(t *testing.T, a StateAdvanced) int64 {
	t.Helper()
	if a.DwellSeconds == nil {
		t.Fatalf("%s -> %s: DwellSeconds = nil, want a value", a.From, a.To)
	}
	return *a.DwellSeconds
}

// A multi-hop trail: every transition's dwell is the time since the audit
// entry that moved the saga INTO `from`, not the age since creation.
func TestStateAdvancedDwellIsTimeInTheFromState(t *testing.T) {
	created := time.Date(2026, 10, 7, 10, 0, 0, 0, time.UTC)
	trf := proposeAt(t, created)
	if _, err := trf.Approve(created.Add(10 * time.Minute)); err != nil {
		t.Fatalf("approve: %v", err)
	}
	if _, err := trf.RequestAllocation(created.Add(25 * time.Minute)); err != nil {
		t.Fatalf("request allocation: %v", err)
	}
	if err := trf.Cancel("operator changed their mind", created.Add(85*time.Minute)); err != nil {
		t.Fatalf("cancel: %v", err)
	}

	adv := trf.StateAdvancedSince(0)
	if len(adv) != 5 {
		t.Fatalf("advanced = %d, want 5 (DRAFT creation, PROPOSED, APPROVED, ALLOCATING, CANCELLED)", len(adv))
	}

	// The creation entry: no earlier entry moved the saga into DRAFT, so the
	// dwell is unknown. It must be nil, never a false 0.
	if adv[0].DwellSeconds != nil {
		t.Errorf("creation entry DwellSeconds = %d, want nil (unknown, not zero)", *adv[0].DwellSeconds)
	}
	// DRAFT was entered by the creation entry at created and left at the same
	// instant: a genuine 0.
	if got := dwellOf(t, adv[1]); got != 0 {
		t.Errorf("DRAFT dwell = %d, want 0", got)
	}
	want := []struct {
		i     int
		from  TransferState
		dwell int64
		age   int64
	}{
		{2, StateProposed, 600, 600},     // PROPOSED at +0, left at +10m
		{3, StateApproved, 900, 1500},    // APPROVED at +10m, left at +25m
		{4, StateAllocating, 3600, 5100}, // ALLOCATING at +25m, left at +85m
	}
	for _, w := range want {
		a := adv[w.i]
		if a.From != w.from {
			t.Fatalf("advanced[%d].From = %s, want %s", w.i, a.From, w.from)
		}
		if got := dwellOf(t, a); got != w.dwell {
			t.Errorf("%s dwell = %d, want %d", w.from, got, w.dwell)
		}
		// age_seconds is unchanged: age since CREATION, so it differs from dwell.
		if a.AgeSeconds != w.age {
			t.Errorf("%s age_seconds = %d, want %d (unchanged semantics)", w.from, a.AgeSeconds, w.age)
		}
	}
}

// A use case loads the aggregate (version = persisted audit entries) and
// appends ONE transition: the dwell still reaches back to the persisted entry
// that moved the saga into `from`.
func TestStateAdvancedDwellReachesBackBeyondTheDelta(t *testing.T) {
	created := time.Date(2026, 10, 7, 10, 0, 0, 0, time.UTC)
	trf := proposeAt(t, created)
	loaded := int64(len(trf.Audit())) // persisted: DRAFT creation + PROPOSED
	if _, err := trf.Approve(created.Add(90 * time.Second)); err != nil {
		t.Fatalf("approve: %v", err)
	}
	adv := trf.StateAdvancedSince(loaded)
	if len(adv) != 1 {
		t.Fatalf("delta = %d, want 1", len(adv))
	}
	if adv[0].From != StateProposed || adv[0].To != StateApproved {
		t.Fatalf("delta = %+v, want PROPOSED -> APPROVED", adv[0])
	}
	if got := dwellOf(t, adv[0]); got != 90 {
		t.Errorf("PROPOSED dwell = %d, want 90 (entry time lives in the persisted, not the published, entries)", got)
	}
}
