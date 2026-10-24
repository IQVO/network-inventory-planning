package memory

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

// policyEntry and laneEntry build one valid catalogue entry, so the store
// tests exercise the guarded-write rule exactly as the use cases drive it
// (NewRebalancePolicy mints revision 1; a Revise bumps it).
func policyEntry(t *testing.T, version string, target int) transfer.RebalancePolicy {
	t.Helper()
	e, err := transfer.NewRebalancePolicy(transfer.Policy{
		Version: version, Site: "WH1", SKU: "SKU-1",
		SafetyStock: 5, TargetStock: target, UnitPriority: 10,
	}, unitNow)
	if err != nil {
		t.Fatalf("NewRebalancePolicy: %v", err)
	}
	return e
}

func laneEntry(t *testing.T, origin transfer.SiteID, cost int) transfer.TransferLane {
	t.Helper()
	e, err := transfer.NewTransferLane(transfer.Lane{
		Origin: origin, Destination: "WH9",
		LeadTime: 24 * time.Hour, UnitHandlingCost: cost, Enabled: true,
	}, unitNow)
	if err != nil {
		t.Fatalf("NewTransferLane: %v", err)
	}
	return e
}

func TestRebalancePoliciesGetMissAndHit(t *testing.T) {
	r := NewRebalancePolicies()
	if _, ok, err := r.Get(context.Background(), "WH1", "SKU-1"); ok || err != nil {
		t.Fatalf("Get on empty catalogue = (%t, %v), want (false, nil)", ok, err)
	}
	entry := policyEntry(t, "pv-1", 50)
	if err := r.Save(context.Background(), entry, 0); err != nil {
		t.Fatalf("Save(create): %v", err)
	}
	got, ok, err := r.Get(context.Background(), "WH1", "SKU-1")
	if !ok || err != nil || got.Policy.Version != "pv-1" {
		t.Fatalf("Get = (%+v, %t, %v), want the saved entry", got, ok, err)
	}
	// A different SKU under the same site is a different key.
	if _, ok, _ := r.Get(context.Background(), "WH1", "SKU-2"); ok {
		t.Fatal("Get(WH1, SKU-2) hit; keys must include the SKU")
	}
}

func TestRebalancePoliciesListSortedBySiteThenSKU(t *testing.T) {
	r := NewRebalancePolicies()
	for _, tc := range []struct {
		site transfer.SiteID
		sku  transfer.SKU
	}{
		{"WH2", "SKU-2"}, {"WH1", "SKU-9"}, {"WH1", "SKU-1"}, {"WH10", "SKU-0"},
	} {
		e, err := transfer.NewRebalancePolicy(transfer.Policy{
			Version: "v", Site: tc.site, SKU: tc.sku,
			SafetyStock: 1, TargetStock: 10, UnitPriority: 1,
		}, unitNow)
		if err != nil {
			t.Fatalf("NewRebalancePolicy(%s/%s): %v", tc.site, tc.sku, err)
		}
		if err := r.Save(context.Background(), e, 0); err != nil {
			t.Fatalf("Save(%s/%s): %v", tc.site, tc.sku, err)
		}
	}
	entries, err := r.List(context.Background())
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	want := []string{"WH1/SKU-1", "WH1/SKU-9", "WH10/SKU-0", "WH2/SKU-2"} // byte order, like Postgres
	if len(entries) != len(want) {
		t.Fatalf("List returned %d entries, want %d", len(entries), len(want))
	}
	for i, key := range want {
		if got := string(entries[i].Policy.Site) + "/" + string(entries[i].Policy.SKU); got != key {
			t.Fatalf("entries[%d] = %s, want %s (full order %v)", i, got, key, entries)
		}
	}
}

// TestRebalancePoliciesSaveGuardedWrite proves the 409 contract the HTTP
// layer maps ErrCatalogueConcurrentModification onto: a Save is accepted
// only against the revision the writer read, and expected 0 means "must
// not exist yet".
func TestRebalancePoliciesSaveGuardedWrite(t *testing.T) {
	r := NewRebalancePolicies()
	first := policyEntry(t, "pv-1", 50)
	if err := r.Save(context.Background(), first, 0); err != nil {
		t.Fatalf("Save(create at expected 0): %v", err)
	}
	// A second create against the now-taken key must lose the race.
	if err := r.Save(context.Background(), first, 0); !errors.Is(err, transfer.ErrCatalogueConcurrentModification) {
		t.Fatalf("Save(create over existing) = %v, want ErrCatalogueConcurrentModification", err)
	}
	second, changed, err := first.Revise(transfer.Policy{
		Version: "pv-2", Site: "WH1", SKU: "SKU-1",
		SafetyStock: 5, TargetStock: 80, UnitPriority: 10,
	}, unitNow.Add(time.Minute))
	if err != nil || !changed {
		t.Fatalf("Revise = (%t, %v), want a changed entry", changed, err)
	}
	if err := r.Save(context.Background(), second, 1); err != nil {
		t.Fatalf("Save(update at expected 1): %v", err)
	}
	// Writing the SAME revision again (a replay racing a bumped store) loses.
	if err := r.Save(context.Background(), second, 1); !errors.Is(err, transfer.ErrCatalogueConcurrentModification) {
		t.Fatalf("Save(stale revision) = %v, want ErrCatalogueConcurrentModification", err)
	}
	// The store still holds the revision-2 entry.
	got, _, _ := r.Get(context.Background(), "WH1", "SKU-1")
	if got.Revision != 2 || got.Policy.TargetStock != 80 {
		t.Fatalf("stored entry = %+v, want revision 2 with target 80", got)
	}
}

func TestTransferLanesGetMissAndHit(t *testing.T) {
	l := NewTransferLanes()
	if _, ok, err := l.Get(context.Background(), "WH1", "WH9"); ok || err != nil {
		t.Fatalf("Get on empty catalogue = (%t, %v), want (false, nil)", ok, err)
	}
	entry := laneEntry(t, "WH1", 3)
	if err := l.Save(context.Background(), entry, 0); err != nil {
		t.Fatalf("Save(create): %v", err)
	}
	// Lanes are DIRECTED: the reverse direction is a different key.
	if _, ok, _ := l.Get(context.Background(), "WH9", "WH1"); ok {
		t.Fatal("Get(WH9, WH1) hit; lanes must be directed")
	}
	if got, ok, _ := l.Get(context.Background(), "WH1", "WH9"); !ok || got.Lane.Origin != "WH1" {
		t.Fatalf("Get(WH1, WH9) = (%+v, %t), want the saved lane", got, ok)
	}
}

func TestTransferLanesListSortedByOriginThenDestination(t *testing.T) {
	l := NewTransferLanes()
	for _, origin := range []transfer.SiteID{"WH2", "WH1", "WH10"} {
		e, err := transfer.NewTransferLane(transfer.Lane{
			Origin: origin, Destination: "WH9",
			LeadTime: time.Hour, UnitHandlingCost: 1, Enabled: true,
		}, unitNow)
		if err != nil {
			t.Fatalf("NewTransferLane(%s): %v", origin, err)
		}
		if err := l.Save(context.Background(), e, 0); err != nil {
			t.Fatalf("Save(%s): %v", origin, err)
		}
	}
	entries, err := l.List(context.Background())
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	want := []transfer.SiteID{"WH1", "WH10", "WH2"} // byte order, like Postgres
	for i, origin := range want {
		if entries[i].Lane.Origin != origin {
			t.Fatalf("entries[%d].Origin = %s, want %s (full order %v)", i, entries[i].Lane.Origin, origin, entries)
		}
	}
}

func TestTransferLanesSaveGuardedWrite(t *testing.T) {
	l := NewTransferLanes()
	first := laneEntry(t, "WH1", 3)
	if err := l.Save(context.Background(), first, 0); err != nil {
		t.Fatalf("Save(create at expected 0): %v", err)
	}
	if err := l.Save(context.Background(), first, 0); !errors.Is(err, transfer.ErrCatalogueConcurrentModification) {
		t.Fatalf("Save(create over existing) = %v, want ErrCatalogueConcurrentModification", err)
	}
	revised, changed, err := first.Revise(transfer.Lane{
		Origin: "WH1", Destination: "WH9",
		LeadTime: 48 * time.Hour, UnitHandlingCost: 4, Enabled: true,
	}, unitNow.Add(time.Minute))
	if err != nil || !changed {
		t.Fatalf("Revise = (%t, %v), want a changed lane", changed, err)
	}
	if err := l.Save(context.Background(), revised, 1); err != nil {
		t.Fatalf("Save(update at expected 1): %v", err)
	}
	if err := l.Save(context.Background(), revised, 1); !errors.Is(err, transfer.ErrCatalogueConcurrentModification) {
		t.Fatalf("Save(stale revision) = %v, want ErrCatalogueConcurrentModification", err)
	}
	got, _, _ := l.Get(context.Background(), "WH1", "WH9")
	if got.Revision != 2 || got.Lane.UnitHandlingCost != 4 {
		t.Fatalf("stored lane = %+v, want revision 2 with cost 4", got)
	}
}
