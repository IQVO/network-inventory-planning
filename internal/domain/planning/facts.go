package planning

import "time"

// Facts is the raw content of the three Phase-1 read models, as loaded by
// ports.PlanningSnapshotRepository and consumed by BuildSnapshot. Each
// fact carries its own watermark; assembly and the fail-closed rules are
// the domain's job.
type Facts struct {
	Capabilities []SiteCapability
	Demands      []SiteSkuDemand
	Plans        []PublishedCapacityPlan
}

// Watermark returns the newest fact time across all three read models —
// the moment the read side was last known current.
func (f Facts) Watermark() time.Time {
	var newest time.Time
	for _, c := range f.Capabilities {
		if c.AsOf.After(newest) {
			newest = c.AsOf
		}
	}
	for _, d := range f.Demands {
		if d.AsOf.After(newest) {
			newest = d.AsOf
		}
	}
	for _, p := range f.Plans {
		if p.PublishedAt.After(newest) {
			newest = p.PublishedAt
		}
	}
	return newest
}
