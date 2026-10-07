package usecases

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/claudioed/network-inventory-planning/internal/domain/planning"
	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

var approveNow = time.Date(2026, 10, 6, 15, 0, 0, 0, time.UTC)

// fakeTransferRepo implements ports.TransferRepository in memory.
type fakeTransferRepo struct {
	mu       sync.Mutex
	rows     map[string]*transfer.InterWarehouseTransfer
	byKey    map[string]string
	failNext int
	updates  int
}

func newFakeTransferRepo() *fakeTransferRepo {
	return &fakeTransferRepo{rows: map[string]*transfer.InterWarehouseTransfer{}, byKey: map[string]string{}}
}

func (f *fakeTransferRepo) Create(_ context.Context, t *transfer.InterWarehouseTransfer, key string) (*transfer.InterWarehouseTransfer, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failNext > 0 {
		f.failNext--
		return nil, fmt.Errorf("injected create failure")
	}
	if id, ok := f.byKey[key]; ok {
		return f.rows[id], nil
	}
	f.rows[string(t.ID())] = t
	f.byKey[key] = string(t.ID())
	t.SetVersion(1)
	return nil, nil
}

func (f *fakeTransferRepo) Load(_ context.Context, id transfer.TransferID) (*transfer.InterWarehouseTransfer, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	t, ok := f.rows[string(id)]
	if !ok {
		return nil, transfer.ErrTransferNotFound
	}
	return t, nil
}

func (f *fakeTransferRepo) UpdateState(_ context.Context, t *transfer.InterWarehouseTransfer) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.updates++
	f.rows[string(t.ID())] = t
	t.SetVersion(t.Version() + 1)
	return nil
}

// fakeEventPublisher records published events.
type fakeEventPublisher struct {
	mu     sync.Mutex
	events []transfer.DomainEvent
}

func (f *fakeEventPublisher) Publish(_ context.Context, events ...transfer.DomainEvent) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, events...)
	return nil
}

// passThroughUoW runs fn directly.
type passThroughUoW struct{}

func (passThroughUoW) Do(ctx context.Context, fn func(ctx context.Context) error) error {
	return fn(ctx)
}

// fakeApproveSnapshot implements ports.PlanningSnapshotRepository.
type fakeApproveSnapshot struct {
	facts planning.Facts
	err   error
}

func (f fakeApproveSnapshot) Load(ctx context.Context) (planning.Facts, error) { return f.facts, f.err }

func approveFacts() planning.Facts {
	due := approveNow.Add(2 * time.Hour)
	cap1, _ := planning.NewSiteCapability("WH1", true, true, 3, approveNow.Add(-time.Minute))
	cap2, _ := planning.NewSiteCapability("WH2", true, true, 3, approveNow.Add(-time.Minute))
	d1, _ := planning.NewSiteSkuDemand("ord-1", 1, "WH1", "SKU-1", 40, due, planning.DemandActive, "static-site-v1")
	d1.AsOf = approveNow.Add(-time.Minute)
	d2, _ := planning.NewSiteSkuDemand("ord-2", 1, "WH2", "SKU-1", 10, due, planning.DemandActive, "static-site-v1")
	d2.AsOf = approveNow.Add(-time.Minute)
	plan1, _ := planning.NewPublishedCapacityPlan(planning.PublishedCapacityPlan{
		PlanID: "plan-1", SiteID: "WH1", Location: "PATH-ZONE-A", PathID: "pick-rebin-pack",
		WindowStart: approveNow.Add(time.Hour), WindowEnd: approveNow.Add(8 * time.Hour),
		AssignedDemand: 100, CapacityOverWindow: 500, PublishedAt: approveNow.Add(-time.Minute),
	})
	plan2, _ := planning.NewPublishedCapacityPlan(planning.PublishedCapacityPlan{
		PlanID: "plan-2", SiteID: "WH2", Location: "PATH-ZONE-B", PathID: "pick-rebin-pack",
		WindowStart: approveNow.Add(time.Hour), WindowEnd: approveNow.Add(8 * time.Hour),
		AssignedDemand: 10, CapacityOverWindow: 300, PublishedAt: approveNow.Add(-time.Minute),
	})
	return planning.Facts{
		Capabilities: []planning.SiteCapability{cap1, cap2},
		Demands:      []planning.SiteSkuDemand{d1, d2},
		Plans:        []planning.PublishedCapacityPlan{plan1, plan2},
	}
}

// testReleaseConfig is a fully-configured WorkReleaseConfig for tests
// (production reads the TRANSFER_* env vars at the composition root).
func testReleaseConfig() WorkReleaseConfig {
	return WorkReleaseConfig{
		PickPathID:        "transfer-pick-path",
		PickCPTOffset:     2 * time.Hour,
		DispatchPathID:    "transfer-dispatch-path",
		DispatchCPTOffset: 3 * time.Hour,
	}
}

func approveUseCase(repo *fakeTransferRepo, pub *fakeEventPublisher, facts planning.Facts, loadErr error) ApproveTransfer {
	if pub == nil {
		pub = &fakeEventPublisher{}
	}
	return ApproveTransfer{
		Transfers:    repo,
		Events:       pub,
		Snapshot:     fakeApproveSnapshot{facts: facts, err: loadErr},
		UoW:          passThroughUoW{},
		MaxStaleness: 10 * time.Minute,
		Release:      testReleaseConfig(),
		Now:          func() time.Time { return approveNow },
	}
}

func baseApprovalInput() ApproveTransferInput {
	return ApproveTransferInput{
		IdempotencyKey:    "idem-1",
		OriginSiteID:      "WH1",
		DestinationSiteID: "WH2",
		SKU:               "SKU-1",
		Quantity:          5,
		PolicyVersion:     "policy-v3",
		OperatorReason:    "operator approved rebalance",
		ProposalAsOf:      approveNow.Add(-5 * time.Minute),
	}
}

func TestApproveTransferPersistsSagaAndEmitsBothEvents(t *testing.T) {
	repo := newFakeTransferRepo()
	pub := &fakeEventPublisher{}
	uc := approveUseCase(repo, pub, approveFacts(), nil)

	result, err := uc.Execute(context.Background(), baseApprovalInput())
	if err != nil {
		t.Fatalf("approve: %v", err)
	}
	if result.Replayed {
		t.Fatal("first approval must not be a replay")
	}
	if result.Transfer.State() != transfer.StateAllocating {
		t.Fatalf("state = %s, want ALLOCATING (awaiting inventory reply)", result.Transfer.State())
	}
	if result.TransferID.LineID() != string(result.TransferID)+":1" {
		t.Fatalf("line id = %q", result.TransferID.LineID())
	}
	if len(pub.events) != 2 {
		t.Fatalf("published events = %d, want 2", len(pub.events))
	}
	approved, ok := pub.events[0].(transfer.PlanApproved)
	if !ok || approved.TransferID != result.TransferID || approved.OperatorReason != "operator approved rebalance" {
		t.Fatalf("first event = %+v", pub.events[0])
	}
	cmd, ok := pub.events[1].(transfer.AllocationRequested)
	if !ok || cmd.TransferLineID != result.TransferID.LineID() || cmd.Quantity != 5 || cmd.OriginSiteID != "WH1" {
		t.Fatalf("second event = %+v", pub.events[1])
	}
	if len(repo.rows) != 1 {
		t.Fatalf("persisted transfers = %d, want 1", len(repo.rows))
	}
	audit := result.Transfer.Audit()
	if len(audit) != 4 {
		t.Fatalf("audit = %d entries, want 4 (drafted/proposed/approved/allocation-requested): %+v", len(audit), audit)
	}
}

func TestApproveTransferIsIdempotent(t *testing.T) {
	repo := newFakeTransferRepo()
	pub := &fakeEventPublisher{}
	uc := approveUseCase(repo, pub, approveFacts(), nil)

	first, err := uc.Execute(context.Background(), baseApprovalInput())
	if err != nil {
		t.Fatalf("first approve: %v", err)
	}

	// Same key, same payload: a REPLAY, no second aggregate, no second
	// event fan-out.
	second, err := uc.Execute(context.Background(), baseApprovalInput())
	if err != nil {
		t.Fatalf("replayed approve: %v", err)
	}
	if !second.Replayed {
		t.Fatal("second execution with the same key must be a replay")
	}
	if second.TransferID != first.TransferID {
		t.Fatalf("replay minted a new transfer: %s vs %s", second.TransferID, first.TransferID)
	}
	if len(repo.rows) != 1 {
		t.Fatalf("persisted transfers = %d, want 1 (idempotent)", len(repo.rows))
	}
	if len(pub.events) != 2 {
		t.Fatalf("published events = %d, want 2 (no re-fan-out on replay)", len(pub.events))
	}
}

func TestApproveTransferConflictingKeyReuse(t *testing.T) {
	repo := newFakeTransferRepo()
	uc := approveUseCase(repo, &fakeEventPublisher{}, approveFacts(), nil)
	if _, err := uc.Execute(context.Background(), baseApprovalInput()); err != nil {
		t.Fatalf("first approve: %v", err)
	}

	conflicting := baseApprovalInput()
	conflicting.PolicyVersion = "policy-v9" // same key, DIFFERENT payload
	if _, err := uc.Execute(context.Background(), conflicting); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("err = %v, want ErrIdempotencyConflict", err)
	}
}

func TestApproveTransferFailsClosedOnMissingAndStaleFacts(t *testing.T) {
	cases := []struct {
		name  string
		facts planning.Facts
	}{
		{"empty read models", planning.Facts{}},
		{"stale facts", agedApproveFacts(approveFacts(), -2*time.Hour)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			repo := newFakeTransferRepo()
			pub := &fakeEventPublisher{}
			uc := approveUseCase(repo, pub, tc.facts, nil)
			result, err := uc.Execute(context.Background(), baseApprovalInput())
			if err == nil {
				t.Fatal("fail-closed facts must refuse the approval")
			}
			if !errors.Is(err, transfer.ErrFactsIncomplete) {
				t.Fatalf("err = %v, want ErrFactsIncomplete", err)
			}
			if len(repo.rows) != 0 || len(pub.events) != 0 {
				t.Fatal("refused approval must persist and publish nothing")
			}
			_ = result
		})
	}
}

func agedApproveFacts(f planning.Facts, delta time.Duration) planning.Facts {
	for i := range f.Capabilities {
		f.Capabilities[i].AsOf = f.Capabilities[i].AsOf.Add(delta)
	}
	for i := range f.Demands {
		f.Demands[i].AsOf = f.Demands[i].AsOf.Add(delta)
	}
	for i := range f.Plans {
		f.Plans[i].PublishedAt = f.Plans[i].PublishedAt.Add(delta)
	}
	return f
}

func TestApproveTransferFailsClosedOnSiteMissingFacts(t *testing.T) {
	facts := approveFacts()
	facts.Capabilities = facts.Capabilities[:1] // WH2 loses its capability fact
	facts.Demands = facts.Demands[:1]
	facts.Plans = facts.Plans[:1]
	repo := newFakeTransferRepo()
	uc := approveUseCase(repo, &fakeEventPublisher{}, facts, nil)
	if _, err := uc.Execute(context.Background(), baseApprovalInput()); !errors.Is(err, transfer.ErrFactsIncomplete) {
		t.Fatalf("err = %v, want ErrFactsIncomplete", err)
	}
	if len(repo.rows) != 0 {
		t.Fatal("refused approval must persist nothing")
	}
}

func TestApproveTransferReadModelLoadError(t *testing.T) {
	uc := approveUseCase(newFakeTransferRepo(), &fakeEventPublisher{}, planning.Facts{}, fmt.Errorf("db down"))
	if _, err := uc.Execute(context.Background(), baseApprovalInput()); err == nil {
		t.Fatal("read-model load failure must refuse the approval")
	}
}

func TestApproveTransferRejectsInvalidInput(t *testing.T) {
	uc := approveUseCase(newFakeTransferRepo(), &fakeEventPublisher{}, approveFacts(), nil)
	cases := []struct {
		name string
		mut  func(*ApproveTransferInput)
	}{
		{"no key", func(in *ApproveTransferInput) { in.IdempotencyKey = "" }},
		{"no proposal as-of", func(in *ApproveTransferInput) { in.ProposalAsOf = time.Time{} }},
		{"same site", func(in *ApproveTransferInput) { in.DestinationSiteID = in.OriginSiteID }},
		{"zero quantity", func(in *ApproveTransferInput) { in.Quantity = 0 }},
		{"empty sku", func(in *ApproveTransferInput) { in.SKU = "" }},
		{"no policy version", func(in *ApproveTransferInput) { in.PolicyVersion = "" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := baseApprovalInput()
			tc.mut(&in)
			if _, err := uc.Execute(context.Background(), in); err == nil {
				t.Fatal("invalid input must be refused")
			}
		})
	}
	// The idempotency-key absence specifically maps to 422 via
	// ErrInvalidApproval.
	in := baseApprovalInput()
	in.IdempotencyKey = ""
	if _, err := uc.Execute(context.Background(), in); !errors.Is(err, ErrInvalidApproval) {
		t.Fatalf("err = %v, want ErrInvalidApproval", err)
	}
}

func TestApplyTransferAllocationAndRejection(t *testing.T) {
	repo := newFakeTransferRepo()
	uc := approveUseCase(repo, &fakeEventPublisher{}, approveFacts(), nil)
	result, err := uc.Execute(context.Background(), baseApprovalInput())
	if err != nil {
		t.Fatalf("approve: %v", err)
	}

	allocUc := ApplyTransferAllocation{Transfers: repo, Events: &fakeEventPublisher{}, UoW: passThroughUoW{}, Release: testReleaseConfig()}
	replyAt := approveNow.Add(time.Minute)
	err = allocUc.Execute(context.Background(), ApplyAllocationInput{
		TransferID: result.TransferID,
		Allocation: transfer.StockAllocation{
			TransferLineID: result.TransferID.LineID(),
			OriginSiteID:   "WH1",
			SKU:            "SKU-1",
			ReservationID:  "res-42",
			Quantity:       5,
			Allocations:    []transfer.Allocation{{StockUnitID: "su-1", BinID: "BIN-A", Quantity: 5}},
			ExpiresAt:      approveNow.Add(24 * time.Hour),
		},
		OccurredAt: replyAt,
	})
	if err != nil {
		t.Fatalf("apply allocation: %v", err)
	}
	loaded, err := repo.Load(context.Background(), result.TransferID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.State() != transfer.StateAllocated || loaded.ReservationID() != "res-42" {
		t.Fatalf("state = %s reservation = %s", loaded.State(), loaded.ReservationID())
	}
	if len(loaded.Audit()) != 5 {
		t.Fatalf("audit = %d, want 5", len(loaded.Audit()))
	}

	// A reply replay (already ALLOCATED) is deterministic, not transient.
	err = allocUc.Execute(context.Background(), ApplyAllocationInput{
		TransferID: result.TransferID,
		Allocation: transfer.StockAllocation{
			TransferLineID: result.TransferID.LineID(),
			OriginSiteID:   "WH1",
			SKU:            "SKU-1",
			ReservationID:  "res-42",
			Quantity:       5,
			Allocations:    []transfer.Allocation{{StockUnitID: "su-1", BinID: "BIN-A", Quantity: 5}},
			ExpiresAt:      approveNow.Add(24 * time.Hour),
		},
		OccurredAt: replyAt,
	})
	if !IsDeterministic(err) {
		t.Fatalf("replayed allocation reply must be deterministic, got %v", err)
	}

	// Unknown transfer is deterministic.
	err = allocUc.Execute(context.Background(), ApplyAllocationInput{TransferID: "trf-nope"})
	if !IsDeterministic(err) {
		t.Fatalf("unknown transfer must be deterministic, got %v", err)
	}
}

func TestApplyTransferRejection(t *testing.T) {
	repo := newFakeTransferRepo()
	uc := approveUseCase(repo, &fakeEventPublisher{}, approveFacts(), nil)
	result, err := uc.Execute(context.Background(), baseApprovalInput())
	if err != nil {
		t.Fatalf("approve: %v", err)
	}

	rejUc := ApplyTransferRejection{Transfers: repo, UoW: passThroughUoW{}}
	err = rejUc.Execute(context.Background(), ApplyRejectionInput{
		TransferID: result.TransferID,
		LineID:     result.TransferID.LineID(),
		Reason:     transfer.RejectionInsufficientUsable,
		OccurredAt: approveNow.Add(time.Minute),
	})
	if err != nil {
		t.Fatalf("apply rejection: %v", err)
	}
	loaded, err := repo.Load(context.Background(), result.TransferID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.State() != transfer.StateUnfulfillable || loaded.RejectionReason() != transfer.RejectionInsufficientUsable {
		t.Fatalf("state = %s reason = %s", loaded.State(), loaded.RejectionReason())
	}
}
