package usecases

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/claudioed/network-inventory-planning/internal/application/ports"
	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

// ErrCatalogueVersionMismatch marks a catalogue PUT whose If-Match revision
// is not the stored entry's current revision (or names an entry that does
// not exist). The HTTP layer answers 412 version-mismatch: re-fetch, then
// retry with the current revision.
var ErrCatalogueVersionMismatch = errors.New("catalogue: if-match does not match the current revision")

// CatalogueOutcome is what a catalogue PUT did.
type CatalogueOutcome string

const (
	// CatalogueCreated: the entry did not exist and was created at
	// revision 1 (HTTP 201).
	CatalogueCreated CatalogueOutcome = "CREATED"
	// CatalogueUpdated: the entry changed and its revision was bumped
	// (HTTP 200).
	CatalogueUpdated CatalogueOutcome = "UPDATED"
	// CatalogueUnchanged: the PUT carried exactly the stored entry; nothing
	// was written (idempotent replay, HTTP 200).
	CatalogueUnchanged CatalogueOutcome = "UNCHANGED"
)

// PolicyPutResult is the outcome of PutRebalancePolicy.
type PolicyPutResult struct {
	Entry   transfer.RebalancePolicy
	Outcome CatalogueOutcome
}

// PutRebalancePolicy serves PUT /v1/rebalance-policies (ADR 0015): an
// idempotent upsert of one site + SKU policy with optimistic concurrency.
type PutRebalancePolicy struct {
	Policies ports.RebalancePolicyRepository
	// Now supplies the revision clock (never time.Now directly).
	Now func() time.Time
}

// Execute upserts p. ifMatch, when set, must equal the stored revision
// (ErrCatalogueVersionMismatch otherwise); without it the PUT applies to
// whatever revision is current. A lost write race surfaces as
// transfer.ErrCatalogueConcurrentModification.
func (u PutRebalancePolicy) Execute(ctx context.Context, p transfer.Policy, ifMatch *int64) (PolicyPutResult, error) {
	if u.Now == nil {
		return PolicyPutResult{}, fmt.Errorf("put rebalance policy: clock is required")
	}
	now := u.Now()
	entry, outcome, err := putEntry(ifMatch,
		func() (transfer.RebalancePolicy, bool, error) { return u.Policies.Get(ctx, p.Site, p.SKU) },
		func(e transfer.RebalancePolicy) int64 { return e.Revision },
		func() (transfer.RebalancePolicy, error) { return transfer.NewRebalancePolicy(p, now) },
		func(e transfer.RebalancePolicy) (transfer.RebalancePolicy, bool, error) { return e.Revise(p, now) },
		func(e transfer.RebalancePolicy, expected int64) error { return u.Policies.Save(ctx, e, expected) },
	)
	return PolicyPutResult{Entry: entry, Outcome: outcome}, err
}

// ListRebalancePolicies serves GET /v1/rebalance-policies.
type ListRebalancePolicies struct {
	Policies ports.RebalancePolicyRepository
}

// Execute lists every policy ordered by site, then SKU.
func (u ListRebalancePolicies) Execute(ctx context.Context) ([]transfer.RebalancePolicy, error) {
	return u.Policies.List(ctx)
}

// LanePutResult is the outcome of PutTransferLane.
type LanePutResult struct {
	Entry   transfer.TransferLane
	Outcome CatalogueOutcome
}

// PutTransferLane serves PUT /v1/transfer-lanes (ADR 0015): an idempotent
// upsert of one directed lane with optimistic concurrency.
type PutTransferLane struct {
	Lanes ports.TransferLaneRepository
	Now   func() time.Time
}

// Execute upserts l; see PutRebalancePolicy.Execute.
func (u PutTransferLane) Execute(ctx context.Context, l transfer.Lane, ifMatch *int64) (LanePutResult, error) {
	if u.Now == nil {
		return LanePutResult{}, fmt.Errorf("put transfer lane: clock is required")
	}
	now := u.Now()
	entry, outcome, err := putEntry(ifMatch,
		func() (transfer.TransferLane, bool, error) { return u.Lanes.Get(ctx, l.Origin, l.Destination) },
		func(e transfer.TransferLane) int64 { return e.Revision },
		func() (transfer.TransferLane, error) { return transfer.NewTransferLane(l, now) },
		func(e transfer.TransferLane) (transfer.TransferLane, bool, error) { return e.Revise(l, now) },
		func(e transfer.TransferLane, expected int64) error { return u.Lanes.Save(ctx, e, expected) },
	)
	return LanePutResult{Entry: entry, Outcome: outcome}, err
}

// ListTransferLanes serves GET /v1/transfer-lanes.
type ListTransferLanes struct {
	Lanes ports.TransferLaneRepository
}

// Execute lists every lane ordered by origin, then destination.
func (u ListTransferLanes) Execute(ctx context.Context) ([]transfer.TransferLane, error) {
	return u.Lanes.List(ctx)
}

// putEntry is the shared create-or-revise algorithm of both catalogues:
// read, check If-Match, create or revise in the domain, then write guarded
// by the revision that was read (0 for a create). An unchanged entry is
// never written.
func putEntry[E any](
	ifMatch *int64,
	get func() (E, bool, error),
	revision func(E) int64,
	create func() (E, error),
	revise func(E) (E, bool, error),
	save func(E, int64) error,
) (E, CatalogueOutcome, error) {
	var zero E
	stored, found, err := get()
	if err != nil {
		return zero, "", err
	}
	if ifMatch != nil && (!found || revision(stored) != *ifMatch) {
		return zero, "", ErrCatalogueVersionMismatch
	}
	if !found {
		entry, err := create()
		if err != nil {
			return zero, "", err
		}
		if err := save(entry, 0); err != nil {
			return zero, "", err
		}
		return entry, CatalogueCreated, nil
	}
	next, changed, err := revise(stored)
	if err != nil {
		return zero, "", err
	}
	if !changed {
		return stored, CatalogueUnchanged, nil
	}
	if err := save(next, revision(stored)); err != nil {
		return zero, "", err
	}
	return next, CatalogueUpdated, nil
}
