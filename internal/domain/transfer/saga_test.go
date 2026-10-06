package transfer

import (
	"errors"
	"testing"
	"time"
)

var (
	sagaNow        = time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	sagaProposalAt = sagaNow.Add(-5 * time.Minute)
	sagaExpiry     = sagaNow.Add(time.Hour)
)

func validProposalInput() ProposalInput {
	return ProposalInput{
		ID:                TransferID("trf-0001"),
		IdempotencyKey:    "idem-1",
		OriginSiteID:      "WH1",
		DestinationSiteID: "WH2",
		SKU:               "SKU-1",
		Quantity:          10,
		PolicyVersion:     "policy-v3",
		OperatorReason:    "rebalance north region",
		ProposalAsOf:      sagaProposalAt,
		ExpiresAt:         sagaExpiry,
		Now:               sagaNow,
	}
}

func proposeForTest(t *testing.T) *InterWarehouseTransfer {
	t.Helper()
	trf, err := ProposeTransfer(validProposalInput())
	if err != nil {
		t.Fatalf("propose: %v", err)
	}
	return trf
}

func approveForTest(t *testing.T, trf *InterWarehouseTransfer) PlanApproved {
	t.Helper()
	approved, err := trf.Approve(sagaNow)
	if err != nil {
		t.Fatalf("approve: %v", err)
	}
	return approved
}

func requestAllocationForTest(t *testing.T, trf *InterWarehouseTransfer) AllocationRequested {
	t.Helper()
	cmd, err := trf.RequestAllocation(sagaNow)
	if err != nil {
		t.Fatalf("request allocation: %v", err)
	}
	return cmd
}

func TestProposeTransferValidatesSnapshot(t *testing.T) {
	cases := []struct {
		name string
		mut  func(*ProposalInput)
	}{
		{"no id", func(in *ProposalInput) { in.ID = "" }},
		{"no idempotency key", func(in *ProposalInput) { in.IdempotencyKey = " " }},
		{"no origin", func(in *ProposalInput) { in.OriginSiteID = "" }},
		{"no destination", func(in *ProposalInput) { in.DestinationSiteID = "" }},
		{"same site", func(in *ProposalInput) { in.DestinationSiteID = in.OriginSiteID }},
		{"no sku", func(in *ProposalInput) { in.SKU = "" }},
		{"zero quantity", func(in *ProposalInput) { in.Quantity = 0 }},
		{"negative quantity", func(in *ProposalInput) { in.Quantity = -3 }},
		{"no policy version", func(in *ProposalInput) { in.PolicyVersion = "" }},
		{"no proposal as-of", func(in *ProposalInput) { in.ProposalAsOf = time.Time{} }},
		{"no clock", func(in *ProposalInput) { in.Now = time.Time{} }},
		{"no expiry", func(in *ProposalInput) { in.ExpiresAt = time.Time{} }},
		{"expiry in the past", func(in *ProposalInput) { in.ExpiresAt = sagaNow.Add(-time.Second) }},
		{"expiry equals now", func(in *ProposalInput) { in.ExpiresAt = sagaNow }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := validProposalInput()
			tc.mut(&in)
			if _, err := ProposeTransfer(in); err == nil {
				t.Fatal("invalid proposal must be refused")
			}
		})
	}
}

func TestProposeTransferExpiredProposalIsTypedError(t *testing.T) {
	in := validProposalInput()
	in.ExpiresAt = sagaNow.Add(-time.Minute)
	_, err := ProposeTransfer(in)
	if !errors.Is(err, ErrProposalExpired) {
		t.Fatalf("err = %v, want ErrProposalExpired", err)
	}
}

func TestProposeTransferSeedsAuditTrail(t *testing.T) {
	trf := proposeForTest(t)
	if trf.State() != StateProposed {
		t.Fatalf("state = %s, want PROPOSED", trf.State())
	}
	audit := trf.Audit()
	if len(audit) != 2 {
		t.Fatalf("audit entries = %d, want 2 (drafted + proposed): %+v", len(audit), audit)
	}
	if audit[0].From != StateDraft || audit[0].To != StateDraft || audit[0].Event != "TransferDrafted" {
		t.Fatalf("creation entry = %+v", audit[0])
	}
	if audit[1].From != StateDraft || audit[1].To != StateProposed || audit[1].Event != "TransferProposed" {
		t.Fatalf("proposal entry = %+v", audit[1])
	}
	if trf.ID().LineID() != "trf-0001:1" {
		t.Fatalf("line id = %q, want trf-0001:1", trf.ID().LineID())
	}
	if trf.IdempotencyKey() != "idem-1" {
		t.Fatalf("idempotency key = %q", trf.IdempotencyKey())
	}
}

func TestLegalLifecycleReachesAllocated(t *testing.T) {
	trf := proposeForTest(t)

	approved := approveForTest(t, trf)
	assertApprovedEvent(t, approved)
	cmd := requestAllocationForTest(t, trf)
	assertAllocationCommand(t, cmd)
	reply := validAllocationReply()
	applyAllocationForTest(t, trf, reply)

	if trf.State() != StateAllocated {
		t.Fatalf("state = %s", trf.State())
	}
	if trf.ReservationID() != "res-9" || len(trf.Allocations()) != 2 {
		t.Fatalf("allocation state = %+v", trf)
	}
	if !trf.AllocationExpiresAt().Equal(reply.ExpiresAt) {
		t.Fatalf("allocation expiry = %v", trf.AllocationExpiresAt())
	}

	audit := trf.Audit()
	if len(audit) != 5 {
		t.Fatalf("audit entries = %d, want 5: %+v", len(audit), audit)
	}
	last := audit[len(audit)-1]
	if last.From != StateAllocating || last.To != StateAllocated || last.Event != "TransferStockAllocated" {
		t.Fatalf("allocation entry = %+v", last)
	}
}

func assertApprovedEvent(t *testing.T, approved PlanApproved) {
	t.Helper()
	if approved.TransferID != TransferID("trf-0001") || approved.Quantity != 10 || approved.PolicyVersion != "policy-v3" {
		t.Fatalf("PlanApproved = %+v", approved)
	}
}

func assertAllocationCommand(t *testing.T, cmd AllocationRequested) {
	t.Helper()
	if cmd.TransferLineID != "trf-0001:1" || cmd.OriginSiteID != "WH1" || cmd.SKU != "SKU-1" || cmd.Quantity != 10 {
		t.Fatalf("AllocationRequested = %+v", cmd)
	}
}

func validAllocationReply() StockAllocation {
	return StockAllocation{
		TransferLineID: "trf-0001:1",
		OriginSiteID:   "WH1",
		SKU:            "SKU-1",
		ReservationID:  "res-9",
		Quantity:       10,
		Allocations: []Allocation{
			{StockUnitID: "su-1", BinID: "BIN-A", Quantity: 6},
			{StockUnitID: "su-2", BinID: "BIN-B", Quantity: 4},
		},
		ExpiresAt: sagaNow.Add(24 * time.Hour),
	}
}

func applyAllocationForTest(t *testing.T, trf *InterWarehouseTransfer, reply StockAllocation) {
	t.Helper()
	if err := trf.MarkAllocated(reply, sagaNow.Add(time.Minute)); err != nil {
		t.Fatalf("mark allocated: %v", err)
	}
}

func TestMarkAllocatedValidatesReplyAgainstTransfer(t *testing.T) {
	base := StockAllocation{
		TransferLineID: "trf-0001:1",
		OriginSiteID:   "WH1",
		SKU:            "SKU-1",
		ReservationID:  "res-9",
		Quantity:       10,
		Allocations:    []Allocation{{StockUnitID: "su-1", BinID: "BIN-A", Quantity: 10}},
		ExpiresAt:      sagaNow.Add(time.Hour),
	}
	cases := []struct {
		name string
		mut  func(*StockAllocation)
	}{
		{"wrong line", func(a *StockAllocation) { a.TransferLineID = "trf-other:1" }},
		{"wrong origin", func(a *StockAllocation) { a.OriginSiteID = "WH9" }},
		{"wrong sku", func(a *StockAllocation) { a.SKU = "SKU-9" }},
		{"no reservation id", func(a *StockAllocation) { a.ReservationID = " " }},
		{"no allocations", func(a *StockAllocation) { a.Allocations = nil }},
		{"allocation without stock unit", func(a *StockAllocation) { a.Allocations[0].StockUnitID = "" }},
		{"allocation without bin", func(a *StockAllocation) { a.Allocations[0].BinID = " " }},
		{"non-positive allocation", func(a *StockAllocation) { a.Allocations[0].Quantity = 0 }},
		{"quantity sum below request", func(a *StockAllocation) { a.Allocations[0].Quantity = 9 }},
		{"quantity sum above request", func(a *StockAllocation) { a.Allocations[0].Quantity = 11 }},
		{"no expiry", func(a *StockAllocation) { a.ExpiresAt = time.Time{} }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			trf := proposeForTest(t)
			approveForTest(t, trf)
			requestAllocationForTest(t, trf)
			reply := base
			tc.mut(&reply)
			if err := trf.MarkAllocated(reply, sagaNow); err == nil {
				t.Fatal("mismatched allocation reply must be refused")
			}
			if trf.State() != StateAllocating {
				t.Fatalf("refused reply must leave state ALLOCATING, got %s", trf.State())
			}
		})
	}
}

func TestMarkUnfulfillableRequiresClosedReasonAndMatchingLine(t *testing.T) {
	trf := proposeForTest(t)
	approveForTest(t, trf)
	requestAllocationForTest(t, trf)

	if err := trf.MarkUnfulfillable("trf-0001:1", RejectionInsufficientUsable, sagaNow); err != nil {
		t.Fatalf("mark unfulfillable: %v", err)
	}
	if trf.State() != StateUnfulfillable || trf.RejectionReason() != RejectionInsufficientUsable {
		t.Fatalf("state = %s reason = %s", trf.State(), trf.RejectionReason())
	}

	other := proposeForTest(t)
	approveForTest(t, other)
	requestAllocationForTest(t, other)
	if err := other.MarkUnfulfillable("trf-other:1", RejectionInsufficientUsable, sagaNow); err == nil {
		t.Fatal("wrong line id must be refused")
	}
	if err := other.MarkUnfulfillable("trf-0001:1", RejectionReason("WEIRD"), sagaNow); err == nil {
		t.Fatal("unknown reason must be refused")
	}
	if other.State() != StateAllocating {
		t.Fatalf("state = %s, want ALLOCATING", other.State())
	}
}

func TestIllegalTransitionsAreRefused(t *testing.T) {
	draftish := func(name string, trf *InterWarehouseTransfer) func(t *testing.T) {
		return func(t *testing.T) {
			if _, err := trf.RequestAllocation(sagaNow); err == nil {
				t.Fatalf("%s: request allocation must be illegal", name)
			}
			var illegal *IllegalTransitionError
			if _, err := trf.RequestAllocation(sagaNow); !errors.As(err, &illegal) {
				t.Fatalf("%s: err = %v (%T), want IllegalTransitionError", name, err, err)
			}
		}
	}
	t.Run("from draft", draftish("draft", func() *InterWarehouseTransfer {
		trf := proposeForTest(t)
		trf.state = StateDraft
		return trf
	}()))
	t.Run("from proposed", draftish("proposed", proposeForTest(t)))
	t.Run("from allocated", draftish("allocated", func() *InterWarehouseTransfer {
		trf := proposeForTest(t)
		approveForTest(t, trf)
		requestAllocationForTest(t, trf)
		_ = trf.MarkAllocated(StockAllocation{
			TransferLineID: "trf-0001:1", OriginSiteID: "WH1", SKU: "SKU-1", ReservationID: "res-1",
			Allocations: []Allocation{{StockUnitID: "su-1", BinID: "BIN-A", Quantity: 10}},
			ExpiresAt:   sagaNow.Add(time.Hour),
		}, sagaNow)
		return trf
	}()))

	// Approve is legal ONLY from PROPOSED.
	fresh := proposeForTest(t)
	if _, err := fresh.Approve(sagaNow); err != nil {
		t.Fatalf("first approve: %v", err)
	}
	if _, err := fresh.Approve(sagaNow); err == nil {
		t.Fatal("second approve must be illegal")
	}
	// MarkAllocated is legal ONLY from ALLOCATING.
	if err := fresh.MarkAllocated(StockAllocation{
		TransferLineID: "trf-0001:1", OriginSiteID: "WH1", SKU: "SKU-1", ReservationID: "res-1",
		Allocations: []Allocation{{StockUnitID: "su-1", BinID: "BIN-A", Quantity: 10}},
		ExpiresAt:   sagaNow.Add(time.Hour),
	}, sagaNow); err == nil {
		t.Fatal("mark allocated from APPROVED must be illegal")
	}
}

func TestApproveRefusesExpiredProposal(t *testing.T) {
	trf := proposeForTest(t)
	if _, err := trf.Approve(sagaExpiry); !errors.Is(err, ErrProposalExpired) {
		t.Fatalf("err = %v, want ErrProposalExpired", err)
	}
	if trf.State() != StateProposed {
		t.Fatalf("expired approve must leave state PROPOSED, got %s", trf.State())
	}
}

func TestCancelOnlyBeforeRelease(t *testing.T) {
	states := map[string]*InterWarehouseTransfer{
		"proposed": proposeForTest(t),
		"approved": func() *InterWarehouseTransfer {
			trf := proposeForTest(t)
			approveForTest(t, trf)
			return trf
		}(),
		"allocating": func() *InterWarehouseTransfer {
			trf := proposeForTest(t)
			approveForTest(t, trf)
			requestAllocationForTest(t, trf)
			return trf
		}(),
		"allocated": func() *InterWarehouseTransfer {
			trf := proposeForTest(t)
			approveForTest(t, trf)
			requestAllocationForTest(t, trf)
			_ = trf.MarkAllocated(StockAllocation{
				TransferLineID: "trf-0001:1", OriginSiteID: "WH1", SKU: "SKU-1", ReservationID: "res-1",
				Allocations: []Allocation{{StockUnitID: "su-1", BinID: "BIN-A", Quantity: 10}},
				ExpiresAt:   sagaNow.Add(time.Hour),
			}, sagaNow)
			return trf
		}(),
		"unfulfillable": func() *InterWarehouseTransfer {
			trf := proposeForTest(t)
			approveForTest(t, trf)
			requestAllocationForTest(t, trf)
			_ = trf.MarkUnfulfillable("trf-0001:1", RejectionOriginSiteUnknown, sagaNow)
			return trf
		}(),
	}
	for name, trf := range states {
		if name == "allocated" || name == "unfulfillable" {
			if err := trf.Cancel("operator changed mind", sagaNow); err == nil {
				t.Fatalf("%s: cancel after settlement must be illegal", name)
			}
			continue
		}
		if err := trf.Cancel("operator changed mind", sagaNow); err != nil {
			t.Fatalf("%s: pre-release cancel must be legal: %v", name, err)
		}
		if trf.State() != StateCancelled {
			t.Fatalf("%s: state = %s, want CANCELLED", name, trf.State())
		}
		// Cancel requires a reason.
		other := proposeForTest(t)
		if err := other.Cancel("  ", sagaNow); err == nil {
			t.Fatalf("%s: empty reason must be refused", name)
		}
	}
}

func TestRehydrateRoundTripsSnapshot(t *testing.T) {
	trf := proposeForTest(t)
	approveForTest(t, trf)
	requestAllocationForTest(t, trf)
	_ = trf.MarkAllocated(StockAllocation{
		TransferLineID: "trf-0001:1", OriginSiteID: "WH1", SKU: "SKU-1", ReservationID: "res-1",
		Allocations: []Allocation{{StockUnitID: "su-1", BinID: "BIN-A", Quantity: 10}},
		ExpiresAt:   sagaNow.Add(time.Hour),
	}, sagaNow)

	snap := Snapshot{
		ID: trf.ID(), IdempotencyKey: trf.IdempotencyKey(),
		OriginSiteID: trf.OriginSiteID(), DestinationSiteID: trf.DestinationSiteID(),
		SKU: trf.SKU(), Quantity: trf.Quantity(), PolicyVersion: trf.PolicyVersion(),
		OperatorReason: trf.OperatorReason(), ProposalAsOf: trf.ProposalAsOf(), ExpiresAt: trf.ExpiresAt(),
		State: trf.State(), ReservationID: trf.ReservationID(), Allocations: trf.Allocations(),
		AllocationExpiresAt: trf.AllocationExpiresAt(), Audit: trf.Audit(),
		CreatedAt: trf.CreatedAt(), UpdatedAt: trf.UpdatedAt(), Version: 7,
	}
	clone := Rehydrate(snap)
	if clone.State() != StateAllocated || clone.ReservationID() != "res-1" || clone.Version() != 7 {
		t.Fatalf("rehydrated state=%s reservation=%s version=%d", clone.State(), clone.ReservationID(), clone.Version())
	}
	if len(clone.Audit()) != 5 || len(clone.Allocations()) != 1 {
		t.Fatalf("audit = %d allocations = %d", len(clone.Audit()), len(clone.Allocations()))
	}
	// Snapshot copies are defensive.
	clone.allocations[0].BinID = "MUTATED"
	if trf.Allocations()[0].BinID != "BIN-A" {
		t.Fatal("Allocations() must return a copy")
	}
}
