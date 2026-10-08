package usecases

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

// factTestApprovalInput is one valid approval request for the fact tests
// (approve_transfer_test.go owns the canonical baseApprovalInput).
func factTestApprovalInput() ApproveTransferInput {
	return ApproveTransferInput{
		IdempotencyKey:    "idem-1",
		OriginSiteID:      "WH1",
		DestinationSiteID: "WH2",
		SKU:               "SKU-1",
		Quantity:          5,
		PolicyVersion:     "policy-v3",
		OperatorReason:    "rebalance",
		ProposalAsOf:      approveNow.Add(-5 * time.Minute),
	}
}

func TestApproveFailsClosedWithoutPickPathConfig(t *testing.T) {
	// TRANSFER_PICK_PATH_ID unset: no approval may mint a transfer whose
	// pick demand could never be released (ADR 0005).
	repo := newFakeTransferRepo()
	uc := approveUseCase(repo, &fakeEventPublisher{}, approveFacts(), nil)
	uc.Release = WorkReleaseConfig{PickCPTOffset: time.Hour} // no PickPathID

	_, err := uc.Execute(context.Background(), factTestApprovalInput())
	if !errors.Is(err, ErrWorkReleaseNotConfigured) {
		t.Fatalf("err = %v, want ErrWorkReleaseNotConfigured", err)
	}
	if len(repo.rows) != 0 {
		t.Fatal("no transfer may be persisted on a config-incomplete refusal")
	}

	// A zero/negative CPT offset is equally incomplete.
	uc.Release = WorkReleaseConfig{PickPathID: "p"}
	if _, err := uc.Execute(context.Background(), factTestApprovalInput()); !errors.Is(err, ErrWorkReleaseNotConfigured) {
		t.Fatalf("err = %v, want ErrWorkReleaseNotConfigured for a zero CPT offset", err)
	}

	// The dispatch leg's config is NOT an approve-time gate: the dispatch
	// demand is released later, from the pick fact.
	uc.Release = WorkReleaseConfig{PickPathID: "p", PickCPTOffset: time.Hour}
	if _, err := uc.Execute(context.Background(), factTestApprovalInput()); err != nil {
		t.Fatalf("approve must pass with only the pick leg configured: %v", err)
	}
}

func TestAllocationReplyReleasesPickDemand(t *testing.T) {
	repo := newFakeTransferRepo()
	pub := &fakeEventPublisher{}
	uc := approveUseCase(repo, pub, approveFacts(), nil)
	result, err := uc.Execute(context.Background(), factTestApprovalInput())
	if err != nil {
		t.Fatalf("approve: %v", err)
	}

	alloc := ApplyTransferAllocation{Transfers: repo, Events: pub, UoW: passThroughUoW{}, Release: testReleaseConfig(), Now: func() time.Time { return approveNow }}
	replyAt := approveNow.Add(time.Minute)
	if err := alloc.Execute(context.Background(), ApplyAllocationInput{
		TransferID: result.TransferID,
		Allocation: transfer.StockAllocation{
			TransferLineID: result.TransferID.LineID(),
			OriginSiteID:   "WH1",
			SKU:            "SKU-1",
			ReservationID:  "res-77",
			Quantity:       5,
			Allocations:    []transfer.Allocation{{StockUnitID: "su-1", BinID: "BIN-A", Quantity: 5}},
			ExpiresAt:      approveNow.Add(24 * time.Hour),
		},
		OccurredAt: replyAt,
	}); err != nil {
		t.Fatalf("apply allocation: %v", err)
	}

	// The pick WorkDemandReleased was published in the same transaction.
	var demand transfer.DemandReleased
	found := false
	for _, e := range pub.events {
		if d, ok := e.(transfer.DemandReleased); ok {
			demand = d
			found = true
		}
	}
	if !found {
		t.Fatalf("no WorkDemandReleased published: %+v", pub.events)
	}
	if demand.DemandID != string(result.TransferID)+":pick" {
		t.Fatalf("demand id = %q", demand.DemandID)
	}
	if demand.WorkKind != transfer.WorkKindTransferPick || demand.TransferRef != result.TransferID {
		t.Fatalf("demand = %+v", demand)
	}
	if demand.PathID != "transfer-pick-path" || demand.SiteID != "WH1" {
		t.Fatalf("demand path/site = %q/%q", demand.PathID, demand.SiteID)
	}
	if demand.SKU != "SKU-1" || demand.Quantity != 5 {
		t.Fatalf("demand sku/qty = %q/%d", demand.SKU, demand.Quantity)
	}
	if !demand.CPT.Equal(replyAt.Add(2 * time.Hour)) {
		t.Fatalf("cpt = %v, want reply time + 2h", demand.CPT)
	}
	if !demand.OccurredAt.Equal(replyAt) {
		t.Fatalf("occurred at = %v, want the reply's CE time", demand.OccurredAt)
	}
}

// allocatedTransfer drives one transfer to ALLOCATED and returns the repo,
// publisher and id the fact tests need.
func allocatedTransfer(t *testing.T) (*fakeTransferRepo, *fakeEventPublisher, transfer.TransferID) {
	t.Helper()
	repo := newFakeTransferRepo()
	pub := &fakeEventPublisher{}
	uc := approveUseCase(repo, pub, approveFacts(), nil)
	result, err := uc.Execute(context.Background(), factTestApprovalInput())
	if err != nil {
		t.Fatalf("approve: %v", err)
	}
	alloc := ApplyTransferAllocation{Transfers: repo, Events: pub, UoW: passThroughUoW{}, Release: testReleaseConfig(), Now: func() time.Time { return approveNow }}
	if err := alloc.Execute(context.Background(), ApplyAllocationInput{
		TransferID: result.TransferID,
		Allocation: transfer.StockAllocation{
			TransferLineID: result.TransferID.LineID(),
			OriginSiteID:   "WH1",
			SKU:            "SKU-1",
			ReservationID:  "res-77",
			Quantity:       5,
			Allocations:    []transfer.Allocation{{StockUnitID: "su-1", BinID: "BIN-A", Quantity: 5}},
			ExpiresAt:      approveNow.Add(24 * time.Hour),
		},
		OccurredAt: approveNow.Add(time.Minute),
	}); err != nil {
		t.Fatalf("apply allocation: %v", err)
	}
	return repo, pub, result.TransferID
}

func TestPickFactRecordsShortPickAndReleasesDispatchWithPickedQuantity(t *testing.T) {
	repo, pub, id := allocatedTransfer(t) // allocated 5

	pick := ApplyTransferPick{Transfers: repo, Events: pub, UoW: passThroughUoW{}, Release: testReleaseConfig()}
	pickAt := approveNow.Add(10 * time.Minute)
	// A SHORT pick: 3 of the allocated 5.
	if err := pick.Execute(context.Background(), ApplyPickInput{
		TransferID: id,
		Picked:     transfer.Picked{PickedQuantity: 3},
		OccurredAt: pickAt,
	}); err != nil {
		t.Fatalf("short pick must be applied: %v", err)
	}

	loaded, err := repo.Load(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.State() != transfer.StatePicked || loaded.PickedQuantity() != 3 {
		t.Fatalf("state = %s picked = %d", loaded.State(), loaded.PickedQuantity())
	}

	// The dispatch demand carries the PICKED quantity (3), not the
	// allocated 5.
	var demand transfer.DemandReleased
	found := false
	for _, e := range pub.events {
		if d, ok := e.(transfer.DemandReleased); ok && d.WorkKind == transfer.WorkKindTransferDispatch {
			demand = d
			found = true
		}
	}
	if !found {
		t.Fatalf("no dispatch WorkDemandReleased published: %+v", pub.events)
	}
	if demand.DemandID != string(id)+":dispatch" || demand.PathID != "transfer-dispatch-path" || demand.SiteID != "WH1" {
		t.Fatalf("demand = %+v", demand)
	}
	if demand.Quantity != 3 {
		t.Fatalf("dispatch quantity = %d, want the picked 3", demand.Quantity)
	}
	if !demand.CPT.Equal(pickAt.Add(3 * time.Hour)) {
		t.Fatalf("cpt = %v", demand.CPT)
	}

	// An impossible pick (> allocated) is a deterministic fact refusal.
	err = pick.Execute(context.Background(), ApplyPickInput{TransferID: "trf-other", Picked: transfer.Picked{PickedQuantity: 99}, OccurredAt: pickAt})
	if !IsDeterministicFact(err) {
		t.Fatalf("unknown transfer pick err = %v, want deterministic", err)
	}
}

func TestDispatchAndArrivalFactsAdvanceSaga(t *testing.T) {
	repo, pub, id := allocatedTransfer(t)

	pick := ApplyTransferPick{Transfers: repo, Events: pub, UoW: passThroughUoW{}, Release: testReleaseConfig()}
	dispatched := ApplyTransferDispatched{Transfers: repo, UoW: passThroughUoW{}}
	arrival := ApplyTransferArrival{Transfers: repo, UoW: passThroughUoW{}}
	ctx := context.Background()

	if err := pick.Execute(ctx, ApplyPickInput{TransferID: id, Picked: transfer.Picked{PickedQuantity: 5}, OccurredAt: approveNow.Add(time.Minute)}); err != nil {
		t.Fatalf("pick: %v", err)
	}
	if err := dispatched.Execute(ctx, FactInput{TransferID: id, OccurredAt: approveNow.Add(2 * time.Minute)}); err != nil {
		t.Fatalf("dispatched: %v", err)
	}
	loaded, _ := repo.Load(ctx, id)
	if loaded.State() != transfer.StateInTransit {
		t.Fatalf("state = %s, want IN_TRANSIT", loaded.State())
	}

	if err := arrival.Execute(ctx, FactInput{TransferID: id, OccurredAt: approveNow.Add(3 * time.Minute)}); err != nil {
		t.Fatalf("arrival: %v", err)
	}
	loaded, _ = repo.Load(ctx, id)
	if loaded.State() != transfer.StateArrived {
		t.Fatalf("state = %s, want ARRIVED", loaded.State())
	}

	// Out-of-order facts (a replay of the arrival) are deterministic.
	err := arrival.Execute(ctx, FactInput{TransferID: id, OccurredAt: approveNow.Add(4 * time.Minute)})
	if !IsDeterministicFact(err) {
		t.Fatalf("replayed arrival err = %v, want deterministic", err)
	}
	// Unknown transfer is deterministic.
	err = dispatched.Execute(ctx, FactInput{TransferID: "trf-nope"})
	if !IsDeterministicFact(err) {
		t.Fatalf("unknown transfer dispatch err = %v, want deterministic", err)
	}
}

func TestReceiptStagedAndStowedFactsDriveReceivingTail(t *testing.T) {
	repo, pub, id := allocatedTransfer(t)
	ctx := context.Background()

	// Drive to IN_TRANSIT.
	pick := ApplyTransferPick{Transfers: repo, Events: pub, UoW: passThroughUoW{}, Release: testReleaseConfig()}
	dispatched := ApplyTransferDispatched{Transfers: repo, UoW: passThroughUoW{}}
	if err := pick.Execute(ctx, ApplyPickInput{TransferID: id, Picked: transfer.Picked{PickedQuantity: 5}, OccurredAt: approveNow.Add(time.Minute)}); err != nil {
		t.Fatalf("pick: %v", err)
	}
	if err := dispatched.Execute(ctx, FactInput{TransferID: id, OccurredAt: approveNow.Add(2 * time.Minute)}); err != nil {
		t.Fatalf("dispatched: %v", err)
	}

	staged := ApplyTransferReceiptStaged{Transfers: repo, UoW: passThroughUoW{}}
	if err := staged.Execute(ctx, ApplyReceiptStagedInput{
		TransferID: id, LineID: id.LineID(), OccurredAt: approveNow.Add(3 * time.Minute),
	}); err != nil {
		t.Fatalf("receipt staged: %v", err)
	}
	loaded, _ := repo.Load(ctx, id)
	if loaded.State() != transfer.StateArrived {
		t.Fatalf("state = %s, want ARRIVED", loaded.State())
	}

	stow := ApplyTransferStow{Transfers: repo, UoW: passThroughUoW{}}
	if err := stow.Execute(ctx, ApplyStowInput{
		TransferID: id,
		Stowed: transfer.Stowed{
			TransferLineID:   id.LineID(),
			DestinationSite:  "WH2",
			SKU:              "SKU-1",
			ReceivedQuantity: 5,
			StowedQuantity:   5,
			Allocations:      []transfer.StowAllocation{{StockUnitID: "su-1", BinID: "BIN-DEST", Quantity: 5}},
		},
		OccurredAt: approveNow.Add(4 * time.Minute),
	}); err != nil {
		t.Fatalf("stowed: %v", err)
	}
	loaded, _ = repo.Load(ctx, id)
	if loaded.State() != transfer.StateReceived {
		t.Fatalf("state = %s, want RECEIVED (terminal)", loaded.State())
	}
	if len(loaded.StowAllocations()) != 1 || loaded.StowAllocations()[0].BinID != "BIN-DEST" {
		t.Fatalf("stow allocations = %+v", loaded.StowAllocations())
	}

	// A mismatched stow fact is a deterministic refusal (ErrFactRefused).
	err := stow.Execute(ctx, ApplyStowInput{
		TransferID: id,
		Stowed: transfer.Stowed{
			TransferLineID:  id.LineID(),
			DestinationSite: "WH9", // wrong site
			SKU:             "SKU-1",
			StowedQuantity:  5,
			Allocations:     []transfer.StowAllocation{{StockUnitID: "su-1", BinID: "BIN-X", Quantity: 5}},
		},
		OccurredAt: approveNow.Add(5 * time.Minute),
	})
	if !IsDeterministicFact(err) {
		t.Fatalf("mismatched stow err = %v, want deterministic", err)
	}
}

// --- TransferStateAdvanced on transitions (ADR 0007) --------------------------

// allocatedTransfer seeds one transfer already driven to ALLOCATING, then
// applies an allocation reply: the use case must publish BOTH the pick
// WorkDemandReleased AND the TransferStateAdvanced occurrence for the
// ALLOCATING→ALLOCATED transition, in the same pass-through transaction.
func TestApplyTransferAllocationPublishesStateAdvanced(t *testing.T) {
	repo := newFakeTransferRepo()
	pub := &fakeEventPublisher{}
	uc := ApplyTransferAllocation{Transfers: repo, Events: pub, UoW: passThroughUoW{}, Release: testReleaseConfig(), Now: func() time.Time { return approveNow }}

	approved := seedAllocatingTransfer(t, repo)
	replyAt := approveNow.Add(10 * time.Minute)
	if err := uc.Execute(context.Background(), ApplyAllocationInput{
		TransferID: approved,
		Allocation: transfer.StockAllocation{
			TransferLineID: string(approved) + ":1",
			OriginSiteID:   "WH1",
			SKU:            "SKU-1",
			ReservationID:  "res-1",
			Quantity:       5,
			Allocations:    []transfer.Allocation{{StockUnitID: "su-1", BinID: "bin-1", Quantity: 5}},
			ExpiresAt:      replyAt.Add(time.Hour),
		},
		OccurredAt: replyAt,
	}); err != nil {
		t.Fatalf("apply allocation: %v", err)
	}

	var (
		gotDemand   bool
		gotAdvanced transfer.StateAdvanced
	)
	for _, e := range pub.events {
		switch evt := e.(type) {
		case transfer.DemandReleased:
			gotDemand = true
		case transfer.StateAdvanced:
			gotAdvanced = evt
		}
	}
	if !gotDemand {
		t.Fatal("the pick WorkDemandReleased must still be published")
	}
	if gotAdvanced.TransferID != approved {
		t.Fatalf("StateAdvanced = %+v, want transfer %s", gotAdvanced, approved)
	}
	if gotAdvanced.From != transfer.StateAllocating || gotAdvanced.To != transfer.StateAllocated {
		t.Fatalf("StateAdvanced = %+v, want ALLOCATING→ALLOCATED", gotAdvanced)
	}
	if gotAdvanced.AgeSeconds != 600 {
		t.Fatalf("age = %d seconds, want 600 (10 minutes after creation)", gotAdvanced.AgeSeconds)
	}
}

// TestApplyTransferPickPublishesStateAdvanced proves the same occurrence
// fires for the ALLOCATED→PICKED fact transition.
func TestApplyTransferPickPublishesStateAdvanced(t *testing.T) {
	repo := newFakeTransferRepo()
	pub := &fakeEventPublisher{}
	uc := ApplyTransferPick{Transfers: repo, Events: pub, UoW: passThroughUoW{}, Release: testReleaseConfig()}

	approved := seedAllocatedTransfer(t, repo)
	if err := uc.Execute(context.Background(), ApplyPickInput{
		TransferID: approved,
		Picked:     transfer.Picked{PickedQuantity: 4},
		OccurredAt: approveNow.Add(time.Hour),
	}); err != nil {
		t.Fatalf("apply pick: %v", err)
	}

	var advanced []transfer.StateAdvanced
	for _, e := range pub.events {
		if evt, ok := e.(transfer.StateAdvanced); ok {
			advanced = append(advanced, evt)
		}
	}
	if len(advanced) == 0 || advanced[len(advanced)-1].To != transfer.StatePicked {
		t.Fatalf("advanced = %+v, want the newest entry ALLOCATED→PICKED", advanced)
	}
}

// TestFactTransitionsPublishStateAdvanced drives a fact-only transition
// (no demand side effect) and proves the occurrence still fires.
func TestFactTransitionsPublishStateAdvanced(t *testing.T) {
	repo := newFakeTransferRepo()
	pub := &fakeEventPublisher{}
	dispatched := ApplyTransferDispatched{Transfers: repo, UoW: passThroughUoW{}, events: pub}

	id := seedPickedTransfer(t, repo)
	if err := dispatched.Execute(context.Background(), FactInput{TransferID: id, OccurredAt: approveNow.Add(2 * time.Hour)}); err != nil {
		t.Fatalf("apply dispatched: %v", err)
	}
	var last transfer.StateAdvanced
	saw := false
	for _, e := range pub.events {
		if evt, ok := e.(transfer.StateAdvanced); ok {
			last = evt
			saw = true
		}
	}
	if !saw || last.To != transfer.StateInTransit {
		t.Fatalf("events = %+v, want the newest StateAdvanced → IN_TRANSIT", pub.events)
	}
}

// seedAllocatingTransfer approves a transfer through the use case and
// leaves it in ALLOCATING.
func seedAllocatingTransfer(t *testing.T, repo *fakeTransferRepo) transfer.TransferID {
	t.Helper()
	uc := approveUseCase(repo, &fakeEventPublisher{}, approveFacts(), nil)
	result, err := uc.Execute(context.Background(), factTestApprovalInput())
	if err != nil {
		t.Fatalf("seed approve: %v", err)
	}
	return result.TransferID
}

// seedAllocatedTransfer drives a transfer to ALLOCATED.
func seedAllocatedTransfer(t *testing.T, repo *fakeTransferRepo) transfer.TransferID {
	t.Helper()
	id := seedAllocatingTransfer(t, repo)
	pub := &fakeEventPublisher{}
	uc := ApplyTransferAllocation{Transfers: repo, Events: pub, UoW: passThroughUoW{}, Release: testReleaseConfig(), Now: func() time.Time { return approveNow }}
	if err := uc.Execute(context.Background(), ApplyAllocationInput{
		TransferID: id,
		Allocation: transfer.StockAllocation{
			TransferLineID: string(id) + ":1",
			OriginSiteID:   "WH1",
			SKU:            "SKU-1",
			ReservationID:  "res-1",
			Quantity:       5,
			Allocations:    []transfer.Allocation{{StockUnitID: "su-1", BinID: "bin-1", Quantity: 5}},
			ExpiresAt:      approveNow.Add(time.Hour),
		},
		OccurredAt: approveNow,
	}); err != nil {
		t.Fatalf("seed allocate: %v", err)
	}
	return id
}

// seedPickedTransfer drives a transfer to PICKED.
func seedPickedTransfer(t *testing.T, repo *fakeTransferRepo) transfer.TransferID {
	t.Helper()
	id := seedAllocatedTransfer(t, repo)
	uc := ApplyTransferPick{Transfers: repo, Events: &fakeEventPublisher{}, UoW: passThroughUoW{}, Release: testReleaseConfig()}
	if err := uc.Execute(context.Background(), ApplyPickInput{
		TransferID: id,
		Picked:     transfer.Picked{PickedQuantity: 5},
		OccurredAt: approveNow,
	}); err != nil {
		t.Fatalf("seed pick: %v", err)
	}
	return id
}

// recordingUoW runs fn like passThroughUoW but remembers what fn returned: a
// real unit of work COMMITS only when fn returns nil and rolls everything back
// otherwise, so fnErr == nil is the assertion "this would have committed".
type recordingUoW struct{ fnErr error }

func (u *recordingUoW) Do(ctx context.Context, fn func(ctx context.Context) error) error {
	u.fnErr = fn(ctx)
	return u.fnErr
}

// TestPickCommitsWhenTheDispatchLegIsNotConfigured pins the behaviour the code
// always documented: a missing dispatch path must not strand the PICKED
// transition. The fact happened, so the transition (and its analytics
// occurrence) commits; only the dispatch demand is withheld, and the error is
// returned AFTER the commit as a deterministic failure so the Kafka consumer
// logs a warning and moves on instead of retrying the same message forever
// (which would block every later transfer fact on that partition).
func TestPickCommitsWhenTheDispatchLegIsNotConfigured(t *testing.T) {
	repo, pub, id := allocatedTransfer(t)

	release := testReleaseConfig()
	release.DispatchPathID = "" // TRANSFER_DISPATCH_PATH_ID unset
	uow := &recordingUoW{}
	pick := ApplyTransferPick{Transfers: repo, Events: pub, UoW: uow, Release: release}

	err := pick.Execute(context.Background(), ApplyPickInput{
		TransferID: id, Picked: transfer.Picked{PickedQuantity: 5}, OccurredAt: approveNow.Add(10 * time.Minute),
	})

	if !errors.Is(err, ErrWorkReleaseNotConfigured) {
		t.Fatalf("err = %v, want ErrWorkReleaseNotConfigured so the operator sees why no dispatch was released", err)
	}
	if uow.fnErr != nil {
		t.Fatalf("the transaction function returned %v: a real unit of work would ROLL BACK the PICKED transition", uow.fnErr)
	}
	if !IsDeterministicFact(err) {
		t.Fatalf("%v must be deterministic (log + skip), or the consumer retries it forever", err)
	}

	loaded, lerr := repo.Load(context.Background(), id)
	if lerr != nil {
		t.Fatal(lerr)
	}
	if loaded.State() != transfer.StatePicked {
		t.Fatalf("state = %s, want PICKED", loaded.State())
	}
	for _, e := range pub.events {
		if d, ok := e.(transfer.DemandReleased); ok && d.WorkKind == transfer.WorkKindTransferDispatch {
			t.Fatalf("a dispatch demand was published without a configured dispatch path: %+v", d)
		}
	}
}
