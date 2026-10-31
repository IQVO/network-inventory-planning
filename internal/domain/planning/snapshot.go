package planning

import (
	"errors"
	"fmt"
	"sort"
	"strings"
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
	// to Now: a fact exactly MaxStaleness old is still fresh, one
	// nanosecond older is stale and excludes ITS site (ADR 0018).
	MaxStaleness time.Duration
	// Now supplies the planning clock (never time.Now directly).
	Now func() time.Time
}

// PlanningSnapshot is the coherent, fail-closed view of the local read
// models a simulation runs on. Every participating site has all three
// facts (capability, demand, published plan) fresh within MaxStaleness;
// anything missing or stale excludes that site, and the snapshot is
// refused only when no site survives. A site that forbids one transfer
// direction still participates: the planner and the approval validation
// gate each direction on the capability itself.
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
	// StaleSites maps each site excluded for a STALE fact to the reason
	// (which fact, as of when), so a refusal can name it. A site missing
	// a fact altogether is simply absent from the snapshot.
	StaleSites map[string]string
}

// BuildSnapshot applies the fail-closed rules and derives the per-site
// aggregates. It NEVER infers: a site missing any fact, or holding a stale
// one, is dropped rather than zero-filled or refusing the other sites; a
// globally empty input or an input where no site survives is an error.
func BuildSnapshot(in SnapshotInput) (PlanningSnapshot, error) {
	if err := in.validate(); err != nil {
		return PlanningSnapshot{}, err
	}
	now := in.Now()
	stale := staleFacts{}

	capsBySite := indexCapabilities(in.Capabilities, now, in.MaxStaleness, stale)
	markStaleDemands(in.Demands, now, in.MaxStaleness, stale)
	planBySite := indexPlans(in.Plans, now, in.MaxStaleness, stale)

	// A site participates only with all three facts; a REMOVED demand
	// row still counts as "the site has demand facts" (its units simply
	// contribute nothing).
	snapshot, oldest, err := participatingSites(capsBySite, planBySite, siteSet(in.Demands), stale, now)
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

// staleFacts records, per site, why it is excluded: the first stale fact of
// each kind (capability, demand, plan).
type staleFacts map[string]map[string]string

// mark records a stale fact of the given kind for the site, keeping the first.
func (s staleFacts) mark(site, kind, reason string) {
	if s[site] == nil {
		s[site] = map[string]string{}
	}
	if _, seen := s[site][kind]; !seen {
		s[site][kind] = reason
	}
}

// reasons flattens the per-kind reasons into one stable string per site.
func (s staleFacts) reasons() map[string]string {
	out := make(map[string]string, len(s))
	for site, byKind := range s {
		parts := make([]string, 0, len(byKind))
		for _, kind := range []string{kindCapability, kindDemand, kindPlan} {
			if reason, ok := byKind[kind]; ok {
				parts = append(parts, reason)
			}
		}
		out[site] = strings.Join(parts, "; ")
	}
	return out
}

const (
	kindCapability = "capability"
	kindDemand     = "demand"
	kindPlan       = "plan"
)

// isStale reports whether a watermark is older than the freshness budget:
// exactly maxStaleness old is still fresh.
func isStale(asOf, now time.Time, maxStaleness time.Duration) bool {
	return now.Sub(asOf) > maxStaleness
}

// staleReason words the stale fact for a refusal message.
func staleReason(fact string, asOf time.Time, maxStaleness time.Duration) string {
	return fmt.Sprintf("stale %s (as of %s, older than %s)", fact, asOf.UTC().Format(time.RFC3339), maxStaleness)
}

// indexCapabilities keeps the newest capability per site and drops (and
// records) a site whose newest capability is stale.
func indexCapabilities(caps []SiteCapability, now time.Time, maxStaleness time.Duration, stale staleFacts) map[string]SiteCapability {
	newest := make(map[string]SiteCapability, len(caps))
	for _, c := range caps {
		if prev, ok := newest[c.Site]; ok && !c.Supersedes(prev) {
			continue
		}
		newest[c.Site] = c
	}
	for site, c := range newest {
		if isStale(c.AsOf, now, maxStaleness) {
			stale.mark(site, kindCapability, staleReason("site capability", c.AsOf, maxStaleness))
			delete(newest, site)
		}
	}
	return newest
}

// markStaleDemands records every site holding a stale demand fact (a zero
// watermark is "never applied by a consumer", not stale).
func markStaleDemands(demands []SiteSkuDemand, now time.Time, maxStaleness time.Duration, stale staleFacts) {
	for _, d := range demands {
		if !d.AsOf.IsZero() && isStale(d.AsOf, now, maxStaleness) {
			stale.mark(d.Site, kindDemand, staleReason("site sku demand "+d.SourceKey(), d.AsOf, maxStaleness))
		}
	}
}

// indexPlans keeps the newest plan per site and drops (and records) a site
// whose newest plan is stale.
func indexPlans(plans []PublishedCapacityPlan, now time.Time, maxStaleness time.Duration, stale staleFacts) map[string]PublishedCapacityPlan {
	newest := make(map[string]PublishedCapacityPlan)
	for _, p := range plans {
		if prev, ok := newest[p.SiteID]; ok && !p.Supersedes(prev) {
			continue
		}
		newest[p.SiteID] = p
	}
	for site, p := range newest {
		if isStale(p.PublishedAt, now, maxStaleness) {
			stale.mark(site, kindPlan, staleReason("published capacity plan "+p.PlanID, p.PublishedAt, maxStaleness))
			delete(newest, site)
		}
	}
	return newest
}

// siteSet returns the set of sites that have demand facts.
func siteSet(demands []SiteSkuDemand) map[string]bool {
	sites := make(map[string]bool, len(demands))
	for _, d := range demands {
		sites[d.Site] = true
	}
	return sites
}

// participatingSites assembles the sites carrying all three fresh facts. A
// site with a missing or stale fact is excluded; zero survivors is an
// error that names every stale exclusion. A site disabled for one transfer
// direction participates: the direction is gated where it is used.
func participatingSites(
	capsBySite map[string]SiteCapability,
	planBySite map[string]PublishedCapacityPlan,
	demandSites map[string]bool,
	stale staleFacts,
	now time.Time,
) (PlanningSnapshot, time.Time, error) {
	snapshot := PlanningSnapshot{
		Capabilities:    make(map[string]SiteCapability),
		DemandBySiteSKU: make(map[string]int),
		CapacityBySite:  make(map[string]PublishedCapacityPlan),
		StaleSites:      stale.reasons(),
	}
	oldest := now
	for site, capability := range capsBySite {
		plan, hasPlan := planBySite[site]
		if !demandSites[site] || !hasPlan {
			continue // excluded: missing fact, never zero-filled
		}
		if _, excluded := stale[site]; excluded {
			continue // excluded: a stale demand fact
		}
		snapshot.Capabilities[site] = capability
		snapshot.CapacityBySite[site] = plan
		oldest = earlier(oldest, capability.AsOf)
		oldest = earlier(oldest, plan.PublishedAt)
	}
	if len(snapshot.Capabilities) == 0 {
		return PlanningSnapshot{}, now, noSurvivorError(snapshot.StaleSites)
	}
	return snapshot, oldest, nil
}

// noSurvivorError is the refusal when no site is left, naming each stale
// exclusion in site order.
func noSurvivorError(staleSites map[string]string) error {
	const base = "planning snapshot: no site has capability, demand and capacity facts together"
	if len(staleSites) == 0 {
		return errors.New(base)
	}
	sites := make([]string, 0, len(staleSites))
	for site := range staleSites {
		sites = append(sites, site)
	}
	sort.Strings(sites)
	parts := make([]string, 0, len(sites))
	for _, site := range sites {
		parts = append(parts, site+": "+staleSites[site])
	}
	return fmt.Errorf("%s; excluded: %s", base, strings.Join(parts, "; "))
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
