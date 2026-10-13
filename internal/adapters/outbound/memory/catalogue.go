package memory

import (
	"context"
	"sort"
	"sync"

	"github.com/claudioed/network-inventory-planning/internal/application/ports"
	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

var (
	_ ports.RebalancePolicyRepository = (*RebalancePolicies)(nil)
	_ ports.TransferLaneRepository    = (*TransferLanes)(nil)
)

// RebalancePolicies is the in-memory Rebalance Policy catalogue with the
// same revision guard as the Postgres adapter.
type RebalancePolicies struct {
	mu      sync.Mutex
	entries map[string]transfer.RebalancePolicy
}

// NewRebalancePolicies returns an empty catalogue.
func NewRebalancePolicies() *RebalancePolicies {
	return &RebalancePolicies{entries: map[string]transfer.RebalancePolicy{}}
}

func policyKey(site transfer.SiteID, sku transfer.SKU) string {
	return string(site) + "\x00" + string(sku)
}

// Get implements ports.RebalancePolicyRepository.
func (r *RebalancePolicies) Get(_ context.Context, site transfer.SiteID, sku transfer.SKU) (transfer.RebalancePolicy, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.entries[policyKey(site, sku)]
	return e, ok, nil
}

// List implements ports.RebalancePolicyRepository (site, then SKU).
func (r *RebalancePolicies) List(context.Context) ([]transfer.RebalancePolicy, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]transfer.RebalancePolicy, 0, len(r.entries))
	for _, e := range r.entries {
		out = append(out, e)
	}
	sort.Slice(out, func(i, j int) bool {
		return policyKey(out[i].Policy.Site, out[i].Policy.SKU) < policyKey(out[j].Policy.Site, out[j].Policy.SKU)
	})
	return out, nil
}

// Save implements ports.RebalancePolicyRepository.
func (r *RebalancePolicies) Save(_ context.Context, e transfer.RebalancePolicy, expectedRevision int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := policyKey(e.Policy.Site, e.Policy.SKU)
	if !revisionMatches(r.entries[key].Revision, expectedRevision) {
		return transfer.ErrCatalogueConcurrentModification
	}
	r.entries[key] = e
	return nil
}

// TransferLanes is the in-memory Transfer Lane catalogue.
type TransferLanes struct {
	mu      sync.Mutex
	entries map[string]transfer.TransferLane
}

// NewTransferLanes returns an empty catalogue.
func NewTransferLanes() *TransferLanes {
	return &TransferLanes{entries: map[string]transfer.TransferLane{}}
}

func laneKey(origin, destination transfer.SiteID) string {
	return string(origin) + "\x00" + string(destination)
}

// Get implements ports.TransferLaneRepository.
func (r *TransferLanes) Get(_ context.Context, origin, destination transfer.SiteID) (transfer.TransferLane, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.entries[laneKey(origin, destination)]
	return e, ok, nil
}

// List implements ports.TransferLaneRepository (origin, then destination).
func (r *TransferLanes) List(context.Context) ([]transfer.TransferLane, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]transfer.TransferLane, 0, len(r.entries))
	for _, e := range r.entries {
		out = append(out, e)
	}
	sort.Slice(out, func(i, j int) bool {
		return laneKey(out[i].Lane.Origin, out[i].Lane.Destination) < laneKey(out[j].Lane.Origin, out[j].Lane.Destination)
	})
	return out, nil
}

// Save implements ports.TransferLaneRepository.
func (r *TransferLanes) Save(_ context.Context, e transfer.TransferLane, expectedRevision int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := laneKey(e.Lane.Origin, e.Lane.Destination)
	if !revisionMatches(r.entries[key].Revision, expectedRevision) {
		return transfer.ErrCatalogueConcurrentModification
	}
	r.entries[key] = e
	return nil
}

// revisionMatches is the guarded-write rule: expected 0 means "must not
// exist yet" (a stored revision is never 0), N means "still at N".
func revisionMatches(stored, expected int64) bool {
	return stored == expected
}
