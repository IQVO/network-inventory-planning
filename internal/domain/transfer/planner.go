package transfer

import "sort"

// Planner generates explainable transfer proposals from one coherent network
// snapshot. It is intentionally deterministic: equal input yields equal output.
//
// The zero Planner rounds nothing. WithCaseMultiples returns a copy that
// rounds the SKUs with an active CASE level to whole cases (ADR 0017).
type Planner struct {
	caseMultiples CaseMultiples
}

// WithCaseMultiples returns a Planner that rounds every proposal of a SKU
// in multiples to a whole number of cases, never above the source's
// availability. SKUs absent from multiples are planned exactly as before.
func (p Planner) WithCaseMultiples(multiples CaseMultiples) Planner {
	p.caseMultiples = multiples
	return p
}

// Generate evaluates every directed lane for matching SKU positions and policies.
// A source retains its safety stock; a destination gets no more than its deficit.
func (p Planner) Generate(positions []Position, policies []Policy, lanes []Lane) []Proposal {
	positionsBySiteSKU := indexPositions(positions)
	policiesBySiteSKU := indexPolicies(policies)
	proposals := make([]Proposal, 0)

	for _, lane := range lanes {
		if !lane.Enabled {
			continue
		}
		proposals = append(proposals, proposalsForLane(lane, policies, positionsBySiteSKU, policiesBySiteSKU, p.caseMultiples)...)
	}

	sortProposals(proposals)
	return proposals
}

func indexPositions(positions []Position) map[SiteID]Position {
	indexed := make(map[SiteID]Position, len(positions))
	for _, position := range positions {
		if position.Site != "" && position.SKU != "" {
			indexed[siteSKU(position.Site, position.SKU)] = position
		}
	}
	return indexed
}

func indexPolicies(policies []Policy) map[SiteID]Policy {
	indexed := make(map[SiteID]Policy, len(policies))
	for _, policy := range policies {
		if policy.Site != "" && policy.SKU != "" {
			indexed[siteSKU(policy.Site, policy.SKU)] = policy
		}
	}
	return indexed
}

func proposalsForLane(lane Lane, policies []Policy, positions map[SiteID]Position, policiesBySiteSKU map[SiteID]Policy, multiples CaseMultiples) []Proposal {
	proposals := make([]Proposal, 0)
	for _, destinationPolicy := range policies {
		if destinationPolicy.Site != lane.Destination {
			continue
		}
		proposal, ok := proposalForDestination(lane, destinationPolicy, positions, policiesBySiteSKU, multiples)
		if ok {
			proposals = append(proposals, proposal)
		}
	}
	return proposals
}

func proposalForDestination(lane Lane, destinationPolicy Policy, positions map[SiteID]Position, policies map[SiteID]Policy, multiples CaseMultiples) (Proposal, bool) {
	originKey := siteSKU(lane.Origin, destinationPolicy.SKU)
	originPosition, hasOrigin := positions[originKey]
	originPolicy, hasOriginPolicy := policies[originKey]
	destinationPosition, hasDestination := positions[siteSKU(lane.Destination, destinationPolicy.SKU)]
	if !hasOrigin || !hasOriginPolicy || !hasDestination || !sameAsOf(originPosition, destinationPosition) {
		return Proposal{}, false
	}

	available := originPosition.Usable() - originPolicy.SafetyStock
	deficit := destinationPolicy.Deficit(destinationPosition)
	quantity := min(available, deficit)
	if quantity <= 0 {
		return Proposal{}, false
	}
	// A case SKU rounds the destination's whole deficit (the demand) to
	// cases and caps the result at what the source can supply, so the
	// report shows the true unserved demand, short source included.
	rounding, ok := roundForSKU(multiples, destinationPolicy.SKU, deficit, available)
	if !ok {
		return Proposal{}, false
	}
	if rounding != nil {
		quantity = rounding.RoundedEaches
	}

	proposal := Proposal{
		Origin:        lane.Origin,
		Destination:   lane.Destination,
		SKU:           destinationPolicy.SKU,
		Quantity:      quantity,
		PolicyVersion: destinationPolicy.Version,
		PositionAsOf:  originPosition.AsOf,
		Reasons:       []ReasonCode{ReasonDestinationBelowTarget, ReasonOriginAboveSafety, ReasonApprovedLane},
		ScoreBreakdown: ScoreBreakdown{
			PriorityBenefit: quantity * destinationPolicy.UnitPriority,
			HandlingPenalty: quantity * lane.UnitHandlingCost,
			LeadTimePenalty: int(lane.LeadTime.Hours() / 24),
		},
		CaseRounding: rounding,
	}
	return proposal, proposal.Valid()
}

func siteSKU(site SiteID, sku SKU) SiteID {
	return site + "\x00" + SiteID(sku)
}

func sortProposals(proposals []Proposal) {
	sort.Slice(proposals, func(i, j int) bool {
		if proposals[i].Score() != proposals[j].Score() {
			return proposals[i].Score() > proposals[j].Score()
		}
		if proposals[i].SKU != proposals[j].SKU {
			return proposals[i].SKU < proposals[j].SKU
		}
		if proposals[i].Origin != proposals[j].Origin {
			return proposals[i].Origin < proposals[j].Origin
		}
		return proposals[i].Destination < proposals[j].Destination
	})
}

func sameAsOf(left, right Position) bool {
	return left.AsOf.Equal(right.AsOf)
}

func min(left, right int) int {
	if left < right {
		return left
	}
	return right
}

// roundForSKU applies the case rule to one candidate: demand is what the
// destination needs, available what the source can give. A SKU without an
// active CASE level yields (nil, true): the quantity is untouched. A SKU
// with one yields its rounding, or (nil, false) when the source cannot
// supply even one whole case.
func roundForSKU(multiples CaseMultiples, sku SKU, demand, available int) (*CaseRounding, bool) {
	units, ok := multiples.Of(sku)
	if !ok {
		return nil, true
	}
	rounding, ok := RoundToCases(demand, units, available)
	if !ok {
		return nil, false
	}
	return &rounding, true
}
