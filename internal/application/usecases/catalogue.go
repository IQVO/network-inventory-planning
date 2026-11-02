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
	return putCatalogued(u.Now, ifMatch, "put rebalance policy", policyPutResult,
		func() (transfer.RebalancePolicy, bool, error) { return u.Policies.Get(ctx, p.Site, p.SKU) },
		policyRevision,
		func(now time.Time) (transfer.RebalancePolicy, error) { return transfer.NewRebalancePolicy(p, now) },
		func(e transfer.RebalancePolicy, now time.Time) (transfer.RebalancePolicy, bool, error) {
			return e.Revise(p, now)
		},
		func(e transfer.RebalancePolicy, expected int64) error { return u.Policies.Save(ctx, e, expected) })
}

// ListRebalancePolicies serves GET /v1/rebalance-policies.
type ListRebalancePolicies struct {
	Policies ports.RebalancePolicyRepository
}

// Execute lists up to limit policies ordered by site, then SKU, after the
// keyset (nil is page one). The caller asks for one extra row to learn
// whether a next page exists.
func (u ListRebalancePolicies) Execute(ctx context.Context, after *transfer.PageAfter, limit int) ([]transfer.RebalancePolicy, error) {
	return u.Policies.ListPage(ctx, after, limit)
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
	return putCatalogued(u.Now, ifMatch, "put transfer lane", lanePutResult,
		func() (transfer.TransferLane, bool, error) { return u.Lanes.Get(ctx, l.Origin, l.Destination) },
		laneRevision,
		func(now time.Time) (transfer.TransferLane, error) { return transfer.NewTransferLane(l, now) },
		func(e transfer.TransferLane, now time.Time) (transfer.TransferLane, bool, error) {
			return e.Revise(l, now)
		},
		func(e transfer.TransferLane, expected int64) error { return u.Lanes.Save(ctx, e, expected) })
}

// ListTransferLanes serves GET /v1/transfer-lanes.
type ListTransferLanes struct {
	Lanes ports.TransferLaneRepository
}

// Execute lists up to limit lanes ordered by origin, then destination,
// after the keyset (nil is page one). The caller asks for one extra row to
// learn whether a next page exists.
func (u ListTransferLanes) Execute(ctx context.Context, after *transfer.PageAfter, limit int) ([]transfer.TransferLane, error) {
	return u.Lanes.ListPage(ctx, after, limit)
}

// policyPutResult and lanePutResult wrap putEntry's outcome into each use
// case's result type; policyRevision and laneRevision read the stored
// revision each catalogue checks If-Match against.
func policyPutResult(e transfer.RebalancePolicy, o CatalogueOutcome) PolicyPutResult {
	return PolicyPutResult{Entry: e, Outcome: o}
}

func policyRevision(e transfer.RebalancePolicy) int64 { return e.Revision }

func lanePutResult(e transfer.TransferLane, o CatalogueOutcome) LanePutResult {
	return LanePutResult{Entry: e, Outcome: o}
}

func laneRevision(e transfer.TransferLane) int64 { return e.Revision }

// putCatalogued runs the shared PUT skeleton of both catalogue use cases:
// the clock is mandatory and read exactly once (a nil clock is a wiring
// bug, so it fails closed naming the use case), then the entry flows
// through putEntry and the outcome is wrapped as R.
func putCatalogued[E any, R any](clock func() time.Time, ifMatch *int64, useCase string,
	wrap func(E, CatalogueOutcome) R,
	get func() (E, bool, error), revision func(E) int64,
	create func(now time.Time) (E, error),
	revise func(e E, now time.Time) (E, bool, error),
	save func(e E, expected int64) error,
) (R, error) {
	if clock == nil {
		var zero R
		return zero, fmt.Errorf("%s: clock is required", useCase)
	}
	now := clock()
	entry, outcome, err := putEntry(ifMatch, get, revision,
		func() (E, error) { return create(now) },
		func(e E) (E, bool, error) { return revise(e, now) },
		save)
	if err != nil {
		var zero R
		return zero, err
	}
	return wrap(entry, outcome), nil
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
