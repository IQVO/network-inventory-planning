package planning

import (
	"fmt"
	"strings"
	"time"
)

// DemandState mirrors order-management's SiteSkuDemandState vocabulary
// exactly (ACTIVE / REMOVED). Anything else is a contract break, not a
// demand fact.
type DemandState string

const (
	DemandActive  DemandState = "ACTIVE"
	DemandRemoved DemandState = "REMOVED"
)

// SiteSkuDemand is one source order line's demand at a site, mirrored from
// order-management's SiteSkuDemandChanged. Keyed by (source_order_id,
// line_no): a later event for the same key replaces the row whatever its
// state; REMOVED means the line's demand no longer constrains planning.
type SiteSkuDemand struct {
	SourceOrderID string
	LineNo        int
	Site          string
	SKU           string
	DemandedUnits int
	DueAt         time.Time
	State         DemandState
	// AssignmentVersion identifies order-management's site-assignment
	// policy that produced the fact ("static-site-v1" today).
	AssignmentVersion string
	// AsOf is the CloudEvents `time` of the applied event.
	AsOf time.Time
}

// NewSiteSkuDemand validates and constructs a SiteSkuDemand.
func NewSiteSkuDemand(sourceOrderID string, lineNo int, site, sku string, demandedUnits int, dueAt time.Time, state DemandState, assignmentVersion string) (SiteSkuDemand, error) {
	if strings.TrimSpace(sourceOrderID) == "" {
		return SiteSkuDemand{}, fmt.Errorf("site sku demand: source order id is required")
	}
	if lineNo <= 0 {
		return SiteSkuDemand{}, fmt.Errorf("site sku demand %s: line no must be positive, got %d", sourceOrderID, lineNo)
	}
	if strings.TrimSpace(site) == "" {
		return SiteSkuDemand{}, fmt.Errorf("site sku demand %s/%d: site is required", sourceOrderID, lineNo)
	}
	if strings.TrimSpace(sku) == "" {
		return SiteSkuDemand{}, fmt.Errorf("site sku demand %s/%d: sku is required", sourceOrderID, lineNo)
	}
	if demandedUnits < 0 {
		return SiteSkuDemand{}, fmt.Errorf("site sku demand %s/%d: demanded units must not be negative, got %d", sourceOrderID, lineNo, demandedUnits)
	}
	if demandedUnits == 0 && state == DemandActive {
		return SiteSkuDemand{}, fmt.Errorf("site sku demand %s/%d: ACTIVE demand must carry positive units", sourceOrderID, lineNo)
	}
	if state != DemandActive && state != DemandRemoved {
		return SiteSkuDemand{}, fmt.Errorf("site sku demand %s/%d: unknown state %q", sourceOrderID, lineNo, state)
	}
	if strings.TrimSpace(assignmentVersion) == "" {
		return SiteSkuDemand{}, fmt.Errorf("site sku demand %s/%d: assignment version is required", sourceOrderID, lineNo)
	}
	return SiteSkuDemand{
		SourceOrderID:     sourceOrderID,
		LineNo:            lineNo,
		Site:              site,
		SKU:               sku,
		DemandedUnits:     demandedUnits,
		DueAt:             dueAt.UTC(),
		State:             state,
		AssignmentVersion: assignmentVersion,
		AsOf:              time.Time{}.UTC(),
	}, nil
}

// SourceKey is the natural key of the demand row: source order + line.
func (d SiteSkuDemand) SourceKey() string {
	return fmt.Sprintf("%s#%d", d.SourceOrderID, d.LineNo)
}

// Active reports whether the demand currently constrains planning.
func (d SiteSkuDemand) Active() bool {
	return d.State == DemandActive
}

// DueIn reports whether due_at falls in the half-open window
// [start, end) — the same interval semantics as capacity windows.
func (d SiteSkuDemand) DueIn(start, end time.Time) bool {
	return !d.DueAt.Before(start) && d.DueAt.Before(end)
}
