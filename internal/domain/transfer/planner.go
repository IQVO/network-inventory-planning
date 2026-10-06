package transfer

import "sort"

// Planner generates explainable transfer proposals from one coherent network
// snapshot. It is intentionally deterministic: equal input yields equal output.
type Planner struct{}

// Generate evaluates every directed lane for matching SKU positions and policies.
// A source retains its safety stock; a destination gets no more than its deficit.
func (Planner) Generate(positions []Position, policies []Policy, lanes []Lane) []Proposal {
	positionsBySiteSKU := indexPositions(positions)
	policiesBySiteSKU := indexPolicies(policies)
	proposals := make([]Proposal, 0)

	for _, lane := range lanes {
		if !lane.Enabled {
			continue
		}
		proposals = append(proposals, proposalsForLane(lane, policies, positionsBySiteSKU, policiesBySiteSKU)...)
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

func proposalsForLane(lane Lane, policies []Policy, positions map[SiteID]Position, policiesBySiteSKU map[SiteID]Policy) []Proposal {
	proposals := make([]Proposal, 0)
	for _, destinationPolicy := range policies {
		if destinationPolicy.Site != lane.Destination {
			continue
		}
		proposal, ok := proposalForDestination(lane, destinationPolicy, positions, policiesBySiteSKU)
		if ok {
			proposals = append(proposals, proposal)
		}
	}
	return proposals
}

func proposalForDestination(lane Lane, destinationPolicy Policy, positions map[SiteID]Position, policies map[SiteID]Policy) (Proposal, bool) {
	originKey := siteSKU(lane.Origin, destinationPolicy.SKU)
	originPosition, hasOrigin := positions[originKey]
	originPolicy, hasOriginPolicy := policies[originKey]
	destinationPosition, hasDestination := positions[siteSKU(lane.Destination, destinationPolicy.SKU)]
	if !hasOrigin || !hasOriginPolicy || !hasDestination || !sameAsOf(originPosition, destinationPosition) {
		return Proposal{}, false
	}

	quantity := min(originPosition.Usable()-originPolicy.SafetyStock, destinationPolicy.Deficit(destinationPosition))
	if quantity <= 0 {
		return Proposal{}, false
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
