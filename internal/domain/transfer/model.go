package transfer

import (
	"strings"
	"time"
)

// SiteID identifies one warehouse in the fulfillment network.
type SiteID string

// SKU identifies a sellable stock keeping unit.
type SKU string

// Position is the planner's point-in-time view of one SKU at one warehouse.
// It is deliberately not an inventory ledger; Inventory Storage is authoritative
// for physical quantities and reservations.
type Position struct {
	Site                 SiteID
	SKU                  SKU
	Available            int
	CustomerReservations int
	ConfirmedInbound     int
	CommittedOutbound    int
	AsOf                 time.Time
}

// Usable returns stock that planning may consider before a new network transfer.
func (p Position) Usable() int {
	return p.Available - p.CustomerReservations - p.CommittedOutbound
}

// Policy holds versioned guardrails for a SKU at one destination site.
type Policy struct {
	Version      string
	Site         SiteID
	SKU          SKU
	SafetyStock  int
	TargetStock  int
	UnitPriority int
}

// Deficit returns the units needed to reach target stock, after confirmed inbound.
func (p Policy) Deficit(position Position) int {
	deficit := p.TargetStock - (position.Usable() + position.ConfirmedInbound)
	if deficit < 0 {
		return 0
	}
	return deficit
}

// Lane is an approved directed route for an internal inventory transfer.
type Lane struct {
	Origin           SiteID
	Destination      SiteID
	LeadTime         time.Duration
	UnitHandlingCost int
	Enabled          bool
}

// Allows reports whether the lane can carry the proposed directed transfer.
func (l Lane) Allows(origin, destination SiteID) bool {
	return l.Enabled && l.Origin == origin && l.Destination == destination
}

// ReasonCode explains a recommendation without exposing an opaque optimizer.
type ReasonCode string

const (
	ReasonDestinationBelowTarget ReasonCode = "DESTINATION_BELOW_TARGET"
	ReasonOriginAboveSafety      ReasonCode = "ORIGIN_ABOVE_SAFETY_STOCK"
	ReasonApprovedLane           ReasonCode = "APPROVED_LANE"
)

// ScoreBreakdown makes a planner's choice inspectable and reproducible.
type ScoreBreakdown struct {
	PriorityBenefit int
	HandlingPenalty int
	LeadTimePenalty int
}

// Total returns the final positive score used for ordering proposals.
func (s ScoreBreakdown) Total() int {
	return s.PriorityBenefit - s.HandlingPenalty - s.LeadTimePenalty
}

// Proposal is an advisory recommendation. It does not reserve or move stock.
type Proposal struct {
	Origin         SiteID
	Destination    SiteID
	SKU            SKU
	Quantity       int
	PolicyVersion  string
	PositionAsOf   time.Time
	Reasons        []ReasonCode
	ScoreBreakdown ScoreBreakdown
}

// Score returns the proposal's deterministic final score.
func (p Proposal) Score() int {
	return p.ScoreBreakdown.Total()
}

// Valid returns true only when an advisory proposal is meaningful and auditable.
func (p Proposal) Valid() bool {
	if strings.TrimSpace(string(p.Origin)) == "" || strings.TrimSpace(string(p.Destination)) == "" || p.Origin == p.Destination {
		return false
	}
	if strings.TrimSpace(string(p.SKU)) == "" || strings.TrimSpace(p.PolicyVersion) == "" || p.Quantity <= 0 || p.PositionAsOf.IsZero() {
		return false
	}
	return p.Score() > 0
}
