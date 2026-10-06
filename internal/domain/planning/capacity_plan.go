package planning

import (
	"fmt"
	"strings"
	"time"
)

// PublishedCapacityPlan mirrors warehouse-planning's CapacityPlanPublished
// fact into this context's local read model. The window is the producer's
// [window_start, window_end) planning window; PublishedAt is the CE `time`
// of the publication event and the last-write-wins tiebreaker.
//
// LEGACY plans: a CapacityPlanPublished WITHOUT site_id (published before
// the producer added the field) is EXCLUDED from this read model — the
// site can never be inferred from warehouse_id. That exclusion happens in
// the consumer (the fact is simply not applied), not by defaulting here.
type PublishedCapacityPlan struct {
	PlanID             string
	SiteID             string
	Location           string
	PathID             string
	WindowStart        time.Time
	WindowEnd          time.Time
	AssignedDemand     float64
	CapacityOverWindow float64
	Shortage           float64
	// PublishedAt is the CloudEvents `time` of the applied publication.
	PublishedAt time.Time
}

// NewPublishedCapacityPlan validates and constructs a PublishedCapacityPlan.
func NewPublishedCapacityPlan(p PublishedCapacityPlan) (PublishedCapacityPlan, error) {
	if strings.TrimSpace(p.PlanID) == "" {
		return PublishedCapacityPlan{}, fmt.Errorf("published capacity plan: plan id is required")
	}
	if strings.TrimSpace(p.SiteID) == "" {
		return PublishedCapacityPlan{}, fmt.Errorf("published capacity plan %s: site id is required", p.PlanID)
	}
	if strings.TrimSpace(p.Location) == "" {
		return PublishedCapacityPlan{}, fmt.Errorf("published capacity plan %s: location is required", p.PlanID)
	}
	if strings.TrimSpace(p.PathID) == "" {
		return PublishedCapacityPlan{}, fmt.Errorf("published capacity plan %s: path id is required", p.PlanID)
	}
	if !p.WindowEnd.After(p.WindowStart) {
		return PublishedCapacityPlan{}, fmt.Errorf("published capacity plan %s: window end must be after window start", p.PlanID)
	}
	if p.AssignedDemand < 0 {
		return PublishedCapacityPlan{}, fmt.Errorf("published capacity plan %s: assigned demand must not be negative", p.PlanID)
	}
	if p.CapacityOverWindow < 0 {
		return PublishedCapacityPlan{}, fmt.Errorf("published capacity plan %s: capacity over window must not be negative", p.PlanID)
	}
	if p.Shortage < 0 {
		return PublishedCapacityPlan{}, fmt.Errorf("published capacity plan %s: shortage must not be negative", p.PlanID)
	}
	if p.PublishedAt.IsZero() {
		return PublishedCapacityPlan{}, fmt.Errorf("published capacity plan %s: published at is required", p.PlanID)
	}
	return p, nil
}

// Covers reports whether dueAt falls in the plan's half-open capacity
// window [window_start, window_end).
func (p PublishedCapacityPlan) Covers(dueAt time.Time) bool {
	return !dueAt.Before(p.WindowStart) && dueAt.Before(p.WindowEnd)
}

// Supersedes reports whether p is the newer fact for the same plan_id:
// a strictly later CE time wins (LWW on CE time per the consumer contract).
func (p PublishedCapacityPlan) Supersedes(other PublishedCapacityPlan) bool {
	return p.PublishedAt.After(other.PublishedAt)
}
