package transfer

import (
	"errors"
	"fmt"
)

// ErrBelowOneCase marks a transfer that cannot move even one whole case of
// a SKU that has an active CASE level: the source can supply fewer eaches
// than one case. The planner refuses it (counted as rejected) and the
// approval answers 422; the demand is reported, never silently shrunk to a
// loose partial case (ADR 0017).
var ErrBelowOneCase = errors.New("transfer: source availability is below one case")

// CaseMultiples maps a SKU to the units per pack (eaches) of its ACTIVE
// CASE level. A SKU absent from the map has no active CASE level and is
// planned exactly as before: the planner never rounds it. The application
// layer builds the map from the local pack-level view (ADR 0017); the
// domain only consumes it, so it stays free of I/O and clocks.
type CaseMultiples map[SKU]int

// Of returns the units per pack of sku's active CASE level. A level of
// fewer than 2 eaches is not a case (product-master rejects it; a corrupt
// view must not turn every quantity into a "multiple of 1"), so it counts
// as no level.
func (m CaseMultiples) Of(sku SKU) (unitsPerPack int, ok bool) {
	units, found := m[sku]
	if !found || units < 2 {
		return 0, false
	}
	return units, true
}

// CaseRounding reports how a requested quantity became a whole number of
// cases (ADR 0017). It rides on a Proposal, on the approved transfer and on
// the TransferPlanApproved event, so the demand total always stays visible:
//
//	RequestedEaches = RoundedEaches + RemainderEaches
//
// RemainderEaches is SIGNED: positive means that many requested eaches were
// not moved; negative means the rounding went up and moved that many eaches
// beyond the request. The JSON names are the OpenAPI names.
type CaseRounding struct {
	// CaseMultiple is the active CASE level's units per pack (eaches).
	CaseMultiple int `json:"caseMultiple"`
	// Cases is the number of whole cases the transfer moves (>= 1).
	Cases int `json:"cases"`
	// RequestedEaches is the quantity before rounding (the demand).
	RequestedEaches int `json:"requestedQtyEaches"`
	// RoundedEaches is the quantity the transfer moves: Cases * CaseMultiple.
	RoundedEaches int `json:"roundedQtyEaches"`
	// RemainderEaches is RequestedEaches - RoundedEaches (signed).
	RemainderEaches int `json:"remainderEaches"`
}

// RoundToCases applies the declared rounding rule (ADR 0017) to a requested
// quantity of eaches:
//
//  1. round to the NEAREST multiple of unitsPerPack, a tie going UP;
//  2. never fewer than one case while requested >= 1;
//  3. never more than the source can supply: capped at
//     floor(available / unitsPerPack) cases.
//
// ok is false when no whole case can move: requested < 1, unitsPerPack < 2,
// or available < unitsPerPack. The caller decides what that means (a
// rejected candidate in the planner, ErrBelowOneCase at approval).
func RoundToCases(requested, unitsPerPack, available int) (CaseRounding, bool) {
	if requested < 1 || unitsPerPack < 2 {
		return CaseRounding{}, false
	}
	cases := requested / unitsPerPack
	if 2*(requested%unitsPerPack) >= unitsPerPack {
		cases++
	}
	// At least one case (the demand is >= 1), at most what the source can
	// supply: the cap wins, so a source below one case moves nothing.
	cases = min(max(cases, 1), available/unitsPerPack)
	if cases < 1 {
		return CaseRounding{}, false
	}
	rounded := cases * unitsPerPack
	return CaseRounding{
		CaseMultiple:    unitsPerPack,
		Cases:           cases,
		RequestedEaches: requested,
		RoundedEaches:   rounded,
		RemainderEaches: requested - rounded,
	}, true
}

// check proves the rounding is internally consistent and describes a
// transfer of exactly quantity eaches. It is the invariant ProposeTransfer
// and Proposal.Valid enforce so a hand-built or corrupted report can never
// be persisted or advertised.
func (r CaseRounding) check(quantity int) error {
	switch {
	case r.CaseMultiple < 2:
		return fmt.Errorf("case multiple must be at least 2, got %d", r.CaseMultiple)
	case r.Cases < 1:
		return fmt.Errorf("cases must be at least 1, got %d", r.Cases)
	case r.RoundedEaches != r.Cases*r.CaseMultiple:
		return fmt.Errorf("rounded eaches %d is not %d cases of %d", r.RoundedEaches, r.Cases, r.CaseMultiple)
	case r.RoundedEaches != quantity:
		return fmt.Errorf("rounded eaches %d differs from the quantity %d", r.RoundedEaches, quantity)
	case r.RequestedEaches < 1:
		return fmt.Errorf("requested eaches must be at least 1, got %d", r.RequestedEaches)
	case r.RemainderEaches != r.RequestedEaches-r.RoundedEaches:
		return fmt.Errorf("remainder %d is not requested %d minus rounded %d", r.RemainderEaches, r.RequestedEaches, r.RoundedEaches)
	}
	return nil
}
