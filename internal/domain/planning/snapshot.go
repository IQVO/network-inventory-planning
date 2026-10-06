package planning

import (
	"fmt"
	"time"
)

// SnapshotInput is everything the fail-closed snapshot is built from. All
// four inputs are mandatory: watermarks (AsOf on each fact) freshness,
// capabilities gate direction, demands gate need, plans gate capacity.
type SnapshotInput struct {
	Capabilities []SiteCapability
	Demands      []SiteSkuDemand
	Plans        []PublishedCapacityPlan
	// MaxStaleness bounds how old each fact's watermark may be relative
	// to Now before the snapshot refuses to be built.
	MaxStaleness time.Duration
	// Now supplies the planning clock (never time.Now directly).
	Now func() time.Time
}

// PlanningSnapshot is the coherent, fail-closed view of the local read
// models a simulation runs on. Every participating site has all three
// facts (capability, demand, published plan) fresh within MaxStaleness;
// anything missing, stale or direction-disabled excludes that site — or
// refuses the whole snapshot when nothing survivable remains.
type PlanningSnapshot struct {
	// AsOf is the OLDEST watermark across the included facts (the
	// snapshot is only as fresh as its stalest input).
	AsOf time.Time
	// Capabilities of the participating sites, keyed by site.
	Capabilities map[string]SiteCapability
	// DemandBySiteSKU aggregates ACTIVE demand units by site+SKU, for
	// demands whose due_at falls in the site's capacity window.
	DemandBySiteSKU map[string]int
	// CapacityBySite maps a participating site to its newest published
	// capacity plan covering the site's demand window.
	CapacityBySite map[string]PublishedCapacityPlan
}

// BuildSnapshot applies the fail-closed rules and derives the per-site
// aggregates. It NEVER infers: a site missing any fact is dropped, not
// zero-filled; a globally empty input is an error, not an empty snapshot.
func BuildSnapshot(in SnapshotInput) (PlanningSnapshot, error) {
	if err := in.validate(); err != nil {
		return PlanningSnapshot{}, err
	}
	now := in.Now()

	capsBySite, err := indexCapabilities(in.Capabilities, now, in.MaxStaleness)
	if err != nil {
		return PlanningSnapshot{}, err
	}
	if err := checkDemandFreshness(in.Demands, now, in.MaxStaleness); err != nil {
		return PlanningSnapshot{}, err
	}
	planBySite, err := indexPlans(in.Plans, now, in.MaxStaleness)
	if err != nil {
		return PlanningSnapshot{}, err
	}

	// A site participates only with all three facts; a REMOVED demand
	// row still counts as "the site has demand facts" (its units simply
	// contribute nothing).
	snapshot, oldest, err := participatingSites(capsBySite, planBySite, siteSet(in.Demands), now)
	if err != nil {
		return PlanningSnapshot{}, err
	}
	oldest = aggregateDemand(snapshot, in.Demands, oldest)
	snapshot.AsOf = oldest
	return snapshot, nil
}

// validate refuses structurally empty inputs and a missing clock.
func (in SnapshotInput) validate() error {
	if len(in.Capabilities) == 0 {
		return fmt.Errorf("planning snapshot: no site capability facts; refusing to plan from an empty read model")
	}
	if len(in.Demands) == 0 {
		return fmt.Errorf("planning snapshot: no site sku demand facts; refusing to plan from an empty read model")
	}
	if len(in.Plans) == 0 {
		return fmt.Errorf("planning snapshot: no published capacity plan facts; refusing to plan from an empty read model")
	}
	if in.MaxStaleness <= 0 {
		return fmt.Errorf("planning snapshot: max staleness must be positive")
	}
	return nil
}

// indexCapabilities keeps the newest capability per site and refuses any
// stale one (fail-closed: staleness is an error, never a skip).
func indexCapabilities(caps []SiteCapability, now time.Time, maxStaleness time.Duration) (map[string]SiteCapability, error) {
	indexed := make(map[string]SiteCapability, len(caps))
	for _, c := range caps {
		if now.Sub(c.AsOf) > maxStaleness {
			return nil, fmt.Errorf("planning snapshot: stale site capability for %s (as of %s, older than %s)", c.Site, c.AsOf.UTC().Format(time.RFC3339), maxStaleness)
		}
		if prev, ok := indexed[c.Site]; ok && !c.Supersedes(prev) {
			continue
		}
		indexed[c.Site] = c
	}
	return indexed, nil
}

// checkDemandFreshness refuses any stale demand fact.
func checkDemandFreshness(demands []SiteSkuDemand, now time.Time, maxStaleness time.Duration) error {
	for _, d := range demands {
		if !d.AsOf.IsZero() && now.Sub(d.AsOf) > maxStaleness {
			return fmt.Errorf("planning snapshot: stale site sku demand %s (as of %s, older than %s)", d.SourceKey(), d.AsOf.UTC().Format(time.RFC3339), maxStaleness)
		}
	}
	return nil
}

// indexPlans keeps the newest plan per site and refuses any stale one.
func indexPlans(plans []PublishedCapacityPlan, now time.Time, maxStaleness time.Duration) (map[string]PublishedCapacityPlan, error) {
	indexed := make(map[string]PublishedCapacityPlan)
	for _, p := range plans {
		if now.Sub(p.PublishedAt) > maxStaleness {
			return nil, fmt.Errorf("planning snapshot: stale published capacity plan %s (as of %s, older than %s)", p.PlanID, p.PublishedAt.UTC().Format(time.RFC3339), maxStaleness)
		}
		if prev, ok := indexed[p.SiteID]; ok && !p.Supersedes(prev) {
			continue
		}
		indexed[p.SiteID] = p
	}
	return indexed, nil
}

// siteSet returns the set of sites that have demand facts.
func siteSet(demands []SiteSkuDemand) map[string]bool {
	sites := make(map[string]bool, len(demands))
	for _, d := range demands {
		sites[d.Site] = true
	}
	return sites
}

// participatingSites assembles the sites carrying all three facts. A site
// disabled for either transfer direction on a site that otherwise
// participates is a hard error (fail-closed), and so is zero survivors.
func participatingSites(
	capsBySite map[string]SiteCapability,
	planBySite map[string]PublishedCapacityPlan,
	demandSites map[string]bool,
	now time.Time,
) (PlanningSnapshot, time.Time, error) {
	snapshot := PlanningSnapshot{
		Capabilities:    make(map[string]SiteCapability),
		DemandBySiteSKU: make(map[string]int),
		CapacityBySite:  make(map[string]PublishedCapacityPlan),
	}
	oldest := now
	for site, capability := range capsBySite {
		plan, hasPlan := planBySite[site]
		if !demandSites[site] || !hasPlan {
			continue // excluded: missing fact, never zero-filled
		}
		if !capability.OriginAllowed() {
			return PlanningSnapshot{}, now, fmt.Errorf("planning snapshot: site %s is disabled as a transfer origin", site)
		}
		if !capability.DestinationAllowed() {
			return PlanningSnapshot{}, now, fmt.Errorf("planning snapshot: site %s is disabled as a transfer destination", site)
		}
		snapshot.Capabilities[site] = capability
		snapshot.CapacityBySite[site] = plan
		oldest = earlier(oldest, capability.AsOf)
		oldest = earlier(oldest, plan.PublishedAt)
	}
	if len(snapshot.Capabilities) == 0 {
		return PlanningSnapshot{}, now, fmt.Errorf("planning snapshot: no site has capability, demand and capacity facts together")
	}
	return snapshot, oldest, nil
}

// aggregateDemand adds each ACTIVE demand whose due_at falls in its site's
// capacity window, tracking the oldest included watermark.
func aggregateDemand(snapshot PlanningSnapshot, demands []SiteSkuDemand, oldest time.Time) time.Time {
	for _, d := range demands {
		plan, participating := snapshot.CapacityBySite[d.Site]
		if !participating || !d.Active() {
			continue
		}
		if !plan.Covers(d.DueAt) {
			continue // due_at outside the site's capacity window
		}
		snapshot.DemandBySiteSKU[siteSKUKey(d.Site, d.SKU)] += d.DemandedUnits
		oldest = earlier(oldest, d.AsOf)
	}
	return oldest
}

// earlier returns the lesser of two times (zero times ignored).
func earlier(a, b time.Time) time.Time {
	if b.IsZero() || (!a.IsZero() && a.Before(b)) {
		return a
	}
	if a.IsZero() {
		return b
	}
	return b
}

// siteSKUKey builds the composite map key for site+SKU aggregation.
func siteSKUKey(site, sku string) string {
	return site + "\x00" + sku
}
