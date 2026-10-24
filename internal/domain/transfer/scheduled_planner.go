package transfer

import (
	"sort"

	"github.com/claudioed/network-inventory-planning/internal/domain/planning"
)

// PlanFromSnapshot is the scheduled rebalance planner (ADR 0015): it turns
// the operator-managed Rebalance Policy and Transfer Lane catalogues into
// advisory proposals over the fail-closed planning snapshot.
//
// NIP holds no per-site stock position (no sibling publishes one), so the
// scheduled run plans on the SAME site-level approximation ValidateApproval
// enforces (ADR 0003), which makes every proposal approvable as generated:
//
//   - a lane is usable only when it is enabled and site_capability lets its
//     origin send and its destination receive (a site without complete
//     facts has no capability in the snapshot, so its lanes are unusable);
//   - the destination's need for a SKU is its in-window ACTIVE demand,
//     capped by its policy's target stock (never propose more than the
//     target; a target of 0 means "do not stock here");
//   - the origin donates from its capacity headroom (capacity over window
//     minus its own in-window demand) and keeps its policy's safety stock
//     of that headroom back (donor protection); the origin must carry a
//     policy for the SKU too — an unmanaged origin never donates;
//   - candidates are served greedily, highest score first, each bounded by
//     the destination's remaining need and the origin's remaining headroom,
//     so one pass never promises the same need or headroom twice.
//
// rejected counts candidates the proposal's own validity check refused
// (for example a lane whose handling and lead-time cost outweigh the
// priority benefit); an exhausted budget is not a rejection.
func (pl Planner) PlanFromSnapshot(snap planning.PlanningSnapshot, policies []Policy, lanes []Lane) (proposals []Proposal, rejected int) {
	byKey := indexPolicies(policies)
	candidates := make([]scheduledCandidate, 0)
	for _, l := range usableLanes(snap, lanes) {
		for _, destination := range policiesAt(policies, l.Destination) {
			origin, ok := byKey[siteSKU(l.Origin, destination.SKU)]
			if !ok {
				continue
			}
			c := scheduledCandidate{lane: l, destination: destination, origin: origin}
			p, ok, belowCase := c.propose(snap, pl.caseMultiples, need(snap, destination), headroom(snap, l.Origin)-origin.SafetyStock)
			if belowCase {
				rejected++
				continue
			}
			if !ok {
				continue
			}
			if !p.Valid() {
				rejected++
				continue
			}
			c.proposal = p
			candidates = append(candidates, c)
		}
	}
	accepted, refused := allocate(snap, pl.caseMultiples, candidates)
	sortProposals(accepted)
	return accepted, rejected + refused
}

// scheduledCandidate is one (lane, SKU) the planner may propose.
type scheduledCandidate struct {
	lane        Lane
	destination Policy
	origin      Policy
	proposal    Proposal
}

// propose builds the candidate's proposal for at most min(need, donor)
// units, rounded to whole cases when the SKU has an active CASE level
// (donor is the case rounding's source availability). ok is false when
// there is nothing to move; belowCase is true when there IS something to
// move but the donor cannot supply one whole case (ADR 0017), which the
// caller counts as a rejection, not as an exhausted budget.
func (c scheduledCandidate) propose(snap planning.PlanningSnapshot, multiples CaseMultiples, need, donor int) (p Proposal, ok, belowCase bool) {
	quantity := min(need, donor)
	if quantity <= 0 {
		return Proposal{}, false, false
	}
	rounding, ok := roundForSKU(multiples, c.destination.SKU, need, donor)
	if !ok {
		return Proposal{}, false, true
	}
	if rounding != nil {
		quantity = rounding.RoundedEaches
	}
	return Proposal{
		Origin:        c.lane.Origin,
		Destination:   c.lane.Destination,
		SKU:           c.destination.SKU,
		Quantity:      quantity,
		PolicyVersion: c.destination.Version,
		PositionAsOf:  snap.AsOf,
		Reasons:       []ReasonCode{ReasonDestinationBelowTarget, ReasonOriginAboveSafety, ReasonApprovedLane},
		ScoreBreakdown: ScoreBreakdown{
			PriorityBenefit: quantity * c.destination.UnitPriority,
			HandlingPenalty: quantity * c.lane.UnitHandlingCost,
			LeadTimePenalty: int(c.lane.LeadTime.Hours() / 24),
		},
		CaseRounding: rounding,
	}, true, false
}

// allocate serves the candidates highest score first against the shared
// budgets: remaining need per destination+SKU and remaining headroom per
// origin site.
func allocate(snap planning.PlanningSnapshot, multiples CaseMultiples, candidates []scheduledCandidate) (accepted []Proposal, refused int) {
	sort.SliceStable(candidates, func(i, j int) bool {
		return lessProposal(candidates[i].proposal, candidates[j].proposal)
	})
	needLeft := map[SiteID]int{}
	headroomLeft := map[SiteID]int{}
	accepted = make([]Proposal, 0, len(candidates))
	for _, c := range candidates {
		needKey := siteSKU(c.lane.Destination, c.destination.SKU)
		if _, seen := needLeft[needKey]; !seen {
			needLeft[needKey] = need(snap, c.destination)
		}
		if _, seen := headroomLeft[c.lane.Origin]; !seen {
			headroomLeft[c.lane.Origin] = headroom(snap, c.lane.Origin)
		}
		p, ok, belowCase := c.propose(snap, multiples, needLeft[needKey], headroomLeft[c.lane.Origin]-c.origin.SafetyStock)
		if belowCase {
			refused++
			continue
		}
		if !ok {
			continue // budget exhausted by a better candidate
		}
		if !p.Valid() {
			refused++
			continue
		}
		needLeft[needKey] -= p.Quantity
		headroomLeft[c.lane.Origin] -= p.Quantity
		accepted = append(accepted, p)
	}
	return accepted, refused
}

// usableLanes keeps the enabled lanes whose origin may send and whose
// destination may receive per site_capability, in a stable order.
func usableLanes(snap planning.PlanningSnapshot, lanes []Lane) []Lane {
	out := make([]Lane, 0, len(lanes))
	for _, l := range lanes {
		origin, hasOrigin := snap.Capabilities[string(l.Origin)]
		destination, hasDestination := snap.Capabilities[string(l.Destination)]
		if l.Enabled && hasOrigin && hasDestination && origin.OriginAllowed() && destination.DestinationAllowed() && l.Origin != l.Destination {
			out = append(out, l)
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Origin != out[j].Origin {
			return out[i].Origin < out[j].Origin
		}
		return out[i].Destination < out[j].Destination
	})
	return out
}

// policiesAt returns the policies held at site, ordered by SKU.
func policiesAt(policies []Policy, site SiteID) []Policy {
	out := make([]Policy, 0)
	for _, p := range policies {
		if p.Site == site {
			out = append(out, p)
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].SKU < out[j].SKU })
	return out
}

// need is the destination's in-window ACTIVE demand for the policy's SKU,
// capped by the policy's target stock.
func need(snap planning.PlanningSnapshot, destination Policy) int {
	return min(snap.DemandBySiteSKU[string(destination.Site)+"\x00"+string(destination.SKU)], destination.TargetStock)
}

// headroom is the site's published capacity over the window minus its own
// in-window demand (the same arithmetic as ValidateApproval); 0 when the site
// has no published capacity plan.
func headroom(snap planning.PlanningSnapshot, site SiteID) int {
	room, _ := OriginHeadroom(snap, string(site))
	return room
}

// OriginHeadroom is the most a site can send over the snapshot window: its
// published capacity minus its own in-window demand. ValidateApproval
// refuses a transfer above it, so it is also the availability the case
// rounding caps at (ADR 0017). ok is false when the site has no published
// capacity plan (then ValidateApproval reports the precise fact error).
func OriginHeadroom(snap planning.PlanningSnapshot, site string) (room int, ok bool) {
	plan, found := snap.CapacityBySite[site]
	if !found {
		return 0, false
	}
	demand := 0
	prefix := site + "\x00"
	for key, units := range snap.DemandBySiteSKU {
		if len(key) > len(prefix) && key[:len(prefix)] == prefix {
			demand += units
		}
	}
	return int(plan.CapacityOverWindow) - demand, true
}

// lessProposal is the planner's total order: score descending, then SKU,
// origin, destination ascending.
func lessProposal(a, b Proposal) bool {
	if a.Score() != b.Score() {
		return a.Score() > b.Score()
	}
	if a.SKU != b.SKU {
		return a.SKU < b.SKU
	}
	if a.Origin != b.Origin {
		return a.Origin < b.Origin
	}
	return a.Destination < b.Destination
}
