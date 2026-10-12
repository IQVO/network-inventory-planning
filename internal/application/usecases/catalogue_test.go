package usecases

import (
	"context"
	"errors"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

// fakePolicyRepo is an in-memory RebalancePolicyRepository with the same
// revision guard as Postgres.
type fakePolicyRepo struct {
	mu      sync.Mutex
	entries map[string]transfer.RebalancePolicy
	saveErr error
}

func newFakePolicyRepo() *fakePolicyRepo {
	return &fakePolicyRepo{entries: map[string]transfer.RebalancePolicy{}}
}

func (f *fakePolicyRepo) Get(_ context.Context, site transfer.SiteID, sku transfer.SKU) (transfer.RebalancePolicy, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	e, ok := f.entries[string(site)+"/"+string(sku)]
	return e, ok, nil
}

func (f *fakePolicyRepo) List(context.Context) ([]transfer.RebalancePolicy, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]transfer.RebalancePolicy, 0, len(f.entries))
	for _, e := range f.entries {
		out = append(out, e)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Policy.Site+out[i].Policy.Site < out[j].Policy.Site+out[j].Policy.Site
	})
	return out, nil
}

func (f *fakePolicyRepo) Save(_ context.Context, e transfer.RebalancePolicy, expected int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.saveErr != nil {
		return f.saveErr
	}
	key := string(e.Policy.Site) + "/" + string(e.Policy.SKU)
	stored, ok := f.entries[key]
	if (expected == 0 && ok) || (expected != 0 && (!ok || stored.Revision != expected)) {
		return transfer.ErrCatalogueConcurrentModification
	}
	f.entries[key] = e
	return nil
}

type fakeLaneRepo struct {
	mu      sync.Mutex
	entries map[string]transfer.TransferLane
}

func newFakeLaneRepo() *fakeLaneRepo {
	return &fakeLaneRepo{entries: map[string]transfer.TransferLane{}}
}

func (f *fakeLaneRepo) Get(_ context.Context, origin, destination transfer.SiteID) (transfer.TransferLane, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	e, ok := f.entries[string(origin)+">"+string(destination)]
	return e, ok, nil
}

func (f *fakeLaneRepo) List(context.Context) ([]transfer.TransferLane, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]transfer.TransferLane, 0, len(f.entries))
	for _, e := range f.entries {
		out = append(out, e)
	}
	return out, nil
}

func (f *fakeLaneRepo) Save(_ context.Context, e transfer.TransferLane, expected int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := string(e.Lane.Origin) + ">" + string(e.Lane.Destination)
	stored, ok := f.entries[key]
	if (expected == 0 && ok) || (expected != 0 && (!ok || stored.Revision != expected)) {
		return transfer.ErrCatalogueConcurrentModification
	}
	f.entries[key] = e
	return nil
}

var catNow = time.Date(2026, 10, 12, 9, 0, 0, 0, time.UTC)

func catPolicy(target int) transfer.Policy {
	return transfer.Policy{Version: "policy-v1", Site: "WH2", SKU: "SKU-1", SafetyStock: 5, TargetStock: target, UnitPriority: 10}
}

func rev(n int64) *int64 { return &n }

func TestPutRebalancePolicy_CreateReviseAndIdempotentReplay(t *testing.T) {
	repo := newFakePolicyRepo()
	uc := PutRebalancePolicy{Policies: repo, Now: func() time.Time { return catNow }}
	ctx := context.Background()

	created, err := uc.Execute(ctx, catPolicy(50), nil)
	if err != nil || created.Outcome != CatalogueCreated || created.Entry.Revision != 1 {
		t.Fatalf("create = %+v err=%v", created, err)
	}
	replay, err := uc.Execute(ctx, catPolicy(50), nil)
	if err != nil || replay.Outcome != CatalogueUnchanged || replay.Entry.Revision != 1 {
		t.Fatalf("identical PUT must be an idempotent no-op: %+v err=%v", replay, err)
	}
	updated, err := uc.Execute(ctx, catPolicy(80), rev(1))
	if err != nil || updated.Outcome != CatalogueUpdated || updated.Entry.Revision != 2 || updated.Entry.Policy.TargetStock != 80 {
		t.Fatalf("update = %+v err=%v", updated, err)
	}
}

func TestPutRebalancePolicy_WithoutIfMatchAppliesToTheCurrentRevision(t *testing.T) {
	repo := newFakePolicyRepo()
	uc := PutRebalancePolicy{Policies: repo, Now: func() time.Time { return catNow }}
	ctx := context.Background()
	for _, target := range []int{50, 80} {
		if _, err := uc.Execute(ctx, catPolicy(target), nil); err != nil {
			t.Fatal(err)
		}
	}
	unconditional, err := uc.Execute(ctx, catPolicy(90), nil)
	if err != nil || unconditional.Entry.Revision != 3 {
		t.Fatalf("a PUT without If-Match applies to the current revision: %+v err=%v", unconditional, err)
	}
	list, err := ListRebalancePolicies{Policies: repo}.Execute(ctx)
	if err != nil || len(list) != 1 || list[0].Policy.TargetStock != 90 {
		t.Fatalf("list = %+v err=%v", list, err)
	}
}

func TestPutRebalancePolicy_Refusals(t *testing.T) {
	ctx := context.Background()
	repo := newFakePolicyRepo()
	uc := PutRebalancePolicy{Policies: repo, Now: func() time.Time { return catNow }}
	if _, err := uc.Execute(ctx, catPolicy(50), nil); err != nil {
		t.Fatal(err)
	}

	if _, err := uc.Execute(ctx, catPolicy(60), rev(7)); !errors.Is(err, ErrCatalogueVersionMismatch) {
		t.Fatalf("stale If-Match: err = %v, want ErrCatalogueVersionMismatch", err)
	}
	fresh := catPolicy(60)
	fresh.SKU = "SKU-NEW"
	if _, err := uc.Execute(ctx, fresh, rev(1)); !errors.Is(err, ErrCatalogueVersionMismatch) {
		t.Fatalf("If-Match on an absent entry: err = %v, want ErrCatalogueVersionMismatch", err)
	}
	if _, err := uc.Execute(ctx, catPolicy(1), nil); !errors.Is(err, transfer.ErrInvalidCatalogueEntry) {
		t.Fatalf("invalid entry: err = %v", err)
	}
	if _, err := (PutRebalancePolicy{Policies: repo}).Execute(ctx, catPolicy(50), nil); err == nil {
		t.Fatal("a use case without a clock must refuse")
	}

	repo.saveErr = transfer.ErrCatalogueConcurrentModification
	if _, err := uc.Execute(ctx, catPolicy(70), nil); !errors.Is(err, transfer.ErrCatalogueConcurrentModification) {
		t.Fatalf("lost race: err = %v", err)
	}
	// An identical replay never writes, so it succeeds even while writes fail.
	if res, err := uc.Execute(ctx, catPolicy(50), rev(1)); err != nil || res.Outcome != CatalogueUnchanged {
		t.Fatalf("idempotent replay under write failure = %+v err=%v", res, err)
	}
}

func TestPutTransferLane_CreateReviseAndRefusals(t *testing.T) {
	ctx := context.Background()
	repo := newFakeLaneRepo()
	uc := PutTransferLane{Lanes: repo, Now: func() time.Time { return catNow }}
	lane := transfer.Lane{Origin: "WH1", Destination: "WH2", LeadTime: 24 * time.Hour, UnitHandlingCost: 1, Enabled: true}

	created, err := uc.Execute(ctx, lane, nil)
	if err != nil || created.Outcome != CatalogueCreated || created.Entry.Revision != 1 {
		t.Fatalf("create = %+v err=%v", created, err)
	}
	if res, err := uc.Execute(ctx, lane, rev(1)); err != nil || res.Outcome != CatalogueUnchanged {
		t.Fatalf("replay = %+v err=%v", res, err)
	}
	lane.Enabled = false
	if res, err := uc.Execute(ctx, lane, rev(1)); err != nil || res.Outcome != CatalogueUpdated || res.Entry.Revision != 2 {
		t.Fatalf("disable = %+v err=%v", res, err)
	}
	if _, err := uc.Execute(ctx, lane, rev(1)); !errors.Is(err, ErrCatalogueVersionMismatch) {
		t.Fatalf("stale If-Match: %v", err)
	}
	lane.Destination = lane.Origin
	if _, err := uc.Execute(ctx, lane, nil); !errors.Is(err, transfer.ErrInvalidCatalogueEntry) {
		t.Fatalf("invalid lane: %v", err)
	}
	if _, err := (PutTransferLane{Lanes: repo}).Execute(ctx, lane, nil); err == nil {
		t.Fatal("a use case without a clock must refuse")
	}
	list, err := ListTransferLanes{Lanes: repo}.Execute(ctx)
	if err != nil || len(list) != 1 || list[0].Lane.Enabled {
		t.Fatalf("list = %+v err=%v", list, err)
	}
}
