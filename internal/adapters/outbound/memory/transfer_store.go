package memory

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/claudioed/network-inventory-planning/internal/application/ports"
	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

var (
	_ ports.TransferRepository  = (*TransferStore)(nil)
	_ ports.TransferQuery       = (*TransferStore)(nil)
	_ ports.StuckTransferReader = (*TransferStore)(nil)
)

// TransferStore is the in-memory saga store. One type serves the write
// port (TransferRepository), the read-only query port (TransferQuery) and
// the observe-only StuckTransferReader, because they are three views of the
// same rows — exactly as the Postgres adapters read the same tables.
//
// Rows are kept as transfer.Snapshot values and rehydrated on every read, so
// a caller never aliases stored state.
type TransferStore struct {
	mu    sync.Mutex
	rows  map[transfer.TransferID]transfer.Snapshot
	byKey map[string]transfer.TransferID
}

// NewTransferStore returns an empty store.
func NewTransferStore() *TransferStore {
	return &TransferStore{
		rows:  map[transfer.TransferID]transfer.Snapshot{},
		byKey: map[string]transfer.TransferID{},
	}
}

// snapshotOf captures the persisted shape of an aggregate through its
// accessors (the aggregate's fields are unexported by design).
func snapshotOf(t *transfer.InterWarehouseTransfer) transfer.Snapshot {
	return transfer.Snapshot{
		ID:                  t.ID(),
		IdempotencyKey:      t.IdempotencyKey(),
		OriginSiteID:        t.OriginSiteID(),
		DestinationSiteID:   t.DestinationSiteID(),
		SKU:                 t.SKU(),
		Quantity:            t.Quantity(),
		PolicyVersion:       t.PolicyVersion(),
		OperatorReason:      t.OperatorReason(),
		ProposalAsOf:        t.ProposalAsOf(),
		ExpiresAt:           t.ExpiresAt(),
		State:               t.State(),
		ReservationID:       t.ReservationID(),
		Allocations:         t.Allocations(),
		AllocationExpiresAt: t.AllocationExpiresAt(),
		PickedQuantity:      t.PickedQuantity(),
		StowAllocations:     t.StowAllocations(),
		RejectionReason:     t.RejectionReason(),
		Audit:               t.Audit(),
		CreatedAt:           t.CreatedAt(),
		UpdatedAt:           t.UpdatedAt(),
		Version:             t.Version(),
	}
}

// numberAudit assigns per-transfer sequences starting at first.
func numberAudit(entries []transfer.AuditEntry, first int64) {
	for i := range entries {
		entries[i].Seq = first + int64(i)
	}
}

// Create implements ports.TransferRepository: the unique idempotency key
// makes a second Create return the FIRST transfer unchanged.
func (s *TransferStore) Create(_ context.Context, t *transfer.InterWarehouseTransfer, idempotencyKey string) (*transfer.InterWarehouseTransfer, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if id, ok := s.byKey[idempotencyKey]; ok {
		return transfer.Rehydrate(s.rows[id]), nil
	}
	snap := snapshotOf(t)
	snap.IdempotencyKey = idempotencyKey
	numberAudit(snap.Audit, 1)
	snap.Version = int64(len(snap.Audit))
	s.rows[snap.ID] = snap
	s.byKey[idempotencyKey] = snap.ID
	t.SetVersion(snap.Version)
	return nil, nil
}

// Load implements ports.TransferRepository.
func (s *TransferStore) Load(_ context.Context, id transfer.TransferID) (*transfer.InterWarehouseTransfer, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	snap, ok := s.rows[id]
	if !ok {
		return nil, transfer.ErrTransferNotFound
	}
	return transfer.Rehydrate(snap), nil
}

// UpdateState implements ports.TransferRepository with the same optimistic
// concurrency rule as the Postgres adapter: the update applies only against
// the version the aggregate was loaded at.
func (s *TransferStore) UpdateState(_ context.Context, t *transfer.InterWarehouseTransfer) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	stored, ok := s.rows[t.ID()]
	if !ok {
		return transfer.ErrTransferNotFound
	}
	if stored.Version != t.Version() {
		return fmt.Errorf("update transfer %s: optimistic concurrency conflict (loaded version %d, stored %d)", t.ID(), t.Version(), stored.Version)
	}
	snap := snapshotOf(t)
	loaded := len(stored.Audit)
	if loaded > len(snap.Audit) {
		return fmt.Errorf("update transfer %s: inconsistent audit length %d for stored trail %d", t.ID(), len(snap.Audit), loaded)
	}
	copy(snap.Audit, stored.Audit)
	numberAudit(snap.Audit[loaded:], int64(loaded)+1)
	snap.IdempotencyKey = stored.IdempotencyKey
	snap.Version = stored.Version + 1
	s.rows[snap.ID] = snap
	t.SetVersion(snap.Version)
	return nil
}

// Get implements ports.TransferQuery.
func (s *TransferStore) Get(ctx context.Context, id transfer.TransferID) (*transfer.InterWarehouseTransfer, error) {
	return s.Load(ctx, id)
}

// List implements ports.TransferQuery: filter, order, page, plus the total
// number of matches before paging.
func (s *TransferStore) List(_ context.Context, filter transfer.ListFilter) (transfer.TransferPage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var matches []transfer.Snapshot
	for _, snap := range s.rows {
		if matchesFilter(snap, filter) {
			matches = append(matches, snap)
		}
	}
	sortSnapshots(matches, filter.Order)
	total := len(matches)

	limit := filter.Limit
	if limit <= 0 {
		limit = transfer.DefaultListLimit
	}
	start := min(max(filter.Offset, 0), total)
	end := min(start+limit, total)
	items := make([]*transfer.InterWarehouseTransfer, 0, end-start)
	for _, snap := range matches[start:end] {
		items = append(items, transfer.Rehydrate(snap))
	}
	return transfer.TransferPage{Items: items, Total: total}, nil
}

// ListNonTerminal implements ports.StuckTransferReader: the narrow
// projection of every transfer still able to advance, oldest-updated first.
func (s *TransferStore) ListNonTerminal(_ context.Context, limit int) ([]transfer.StuckView, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if limit <= 0 {
		limit = 500
	}
	var open []transfer.Snapshot
	for _, snap := range s.rows {
		if snap.State.NonTerminal() {
			open = append(open, snap)
		}
	}
	sortSnapshots(open, transfer.OrderStalestFirst)
	if len(open) > limit {
		open = open[:limit]
	}
	views := make([]transfer.StuckView, 0, len(open))
	for _, snap := range open {
		views = append(views, transfer.StuckView{ID: snap.ID, State: snap.State, UpdatedAt: snap.UpdatedAt})
	}
	return views, nil
}

func matchesFilter(snap transfer.Snapshot, f transfer.ListFilter) bool {
	if len(f.States) > 0 && !containsState(f.States, snap.State) {
		return false
	}
	if f.OriginSiteID != "" && snap.OriginSiteID != f.OriginSiteID {
		return false
	}
	if f.DestinationSiteID != "" && snap.DestinationSiteID != f.DestinationSiteID {
		return false
	}
	if f.SiteID != "" && snap.OriginSiteID != f.SiteID && snap.DestinationSiteID != f.SiteID {
		return false
	}
	if !f.UpdatedBefore.IsZero() && !snap.UpdatedAt.Before(f.UpdatedBefore) {
		return false
	}
	return true
}

func containsState(states []transfer.TransferState, s transfer.TransferState) bool {
	for _, candidate := range states {
		if candidate == s {
			return true
		}
	}
	return false
}

// sortSnapshots mirrors transferOrderBy of the Postgres query adapter: a
// total order (the id tie-break) so paging is deterministic.
func sortSnapshots(snaps []transfer.Snapshot, order transfer.TransferOrder) {
	sort.Slice(snaps, func(i, j int) bool {
		a, b := snaps[i], snaps[j]
		if order == transfer.OrderStalestFirst {
			if !a.UpdatedAt.Equal(b.UpdatedAt) {
				return a.UpdatedAt.Before(b.UpdatedAt)
			}
			return a.ID < b.ID
		}
		if !a.CreatedAt.Equal(b.CreatedAt) {
			return a.CreatedAt.After(b.CreatedAt)
		}
		return a.ID < b.ID
	})
}
