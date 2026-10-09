package main_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/cucumber/godog"

	"github.com/claudioed/network-inventory-planning/internal/domain/planning"
)

// declaredFacts is the content of the three local read models (site
// capability, site/SKU demand, published capacity plan) as the scenario has
// declared it. Facts are local and declarative: nothing here is fetched
// from a sibling context.
type declaredFacts = planning.Facts

// planningWindowStart and planningWindowLength place every declared capacity
// plan one hour after "now" and seven hours long, so an hour-old demand is
// outside the window and a demand due in two hours is inside it.
const (
	planningWindowStart  = time.Hour
	planningWindowLength = 7 * time.Hour
)

// ------------------------------------------------- declared planning facts --

// siteDeclared declares ALL three facts a site needs to participate in the
// fail-closed snapshot: a capability (may send and receive), a published
// capacity plan, and active in-window demand for one SKU.
func (w *world) siteDeclared(site string, capacity int, units int, sku string) error {
	asOf := w.clock.Now().Add(-time.Minute)
	now := w.clock.Now()

	capability, err := planning.NewSiteCapability(site, true, true, 1, asOf)
	if err != nil {
		return err
	}
	plan, err := planning.NewPublishedCapacityPlan(planning.PublishedCapacityPlan{
		PlanID: "plan-" + site, SiteID: site, Location: "PATH-ZONE-" + site, PathID: "pick-rebin-pack",
		WindowStart: now.Add(planningWindowStart), WindowEnd: now.Add(planningWindowStart + planningWindowLength),
		AssignedDemand: float64(units), CapacityOverWindow: float64(capacity), PublishedAt: asOf,
	})
	if err != nil {
		return err
	}
	w.declared.Capabilities = append(w.declared.Capabilities, capability)
	w.declared.Plans = append(w.declared.Plans, plan)
	w.declared.Demands = append(w.declared.Demands, w.demand(site, sku, "in-window", units, planning.DemandActive, now.Add(2*time.Hour)))
	w.facts.Set(w.declared)
	return nil
}

// demand builds one demand fact; the domain constructor zeroes the
// watermark, so scenarios set it directly like the consumer does.
func (w *world) demand(site, sku, order string, units int, state planning.DemandState, due time.Time) planning.SiteSkuDemand {
	return planning.SiteSkuDemand{
		SourceOrderID: fmt.Sprintf("ord-%s-%s-%d", site, order, len(w.declared.Demands)+1), LineNo: 1,
		Site: site, SKU: sku, DemandedUnits: units, DueAt: due.UTC(), State: state,
		AssignmentVersion: "static-site-v1", AsOf: w.clock.Now().Add(-time.Minute),
	}
}

func (w *world) siteAlsoHasDemand(site string, units int, sku string) error {
	w.declared.Demands = append(w.declared.Demands, w.demand(site, sku, "more", units, planning.DemandActive, w.clock.Now().Add(3*time.Hour)))
	w.facts.Set(w.declared)
	return nil
}

func (w *world) siteHasDemandOutsideWindow(site string, units int, sku string) error {
	// Due now; the capacity window opens one hour from now.
	w.declared.Demands = append(w.declared.Demands, w.demand(site, sku, "early", units, planning.DemandActive, w.clock.Now()))
	w.facts.Set(w.declared)
	return nil
}

func (w *world) siteHasRemovedDemand(site string, units int, sku string) error {
	w.declared.Demands = append(w.declared.Demands, w.demand(site, sku, "gone", units, planning.DemandRemoved, w.clock.Now().Add(2*time.Hour)))
	w.facts.Set(w.declared)
	return nil
}

func (w *world) siteCannot(site, direction string) error {
	for i := range w.declared.Capabilities {
		if w.declared.Capabilities[i].Site != site {
			continue
		}
		if direction == "send" {
			w.declared.Capabilities[i].TransferOriginEnabled = false
		} else {
			w.declared.Capabilities[i].TransferDestinationEnabled = false
		}
	}
	w.facts.Set(w.declared)
	return nil
}

func (w *world) siteHasNoCapacityPlan(site string) error {
	kept := w.declared.Plans[:0:0]
	for _, plan := range w.declared.Plans {
		if plan.SiteID != site {
			kept = append(kept, plan)
		}
	}
	w.declared.Plans = kept
	w.facts.Set(w.declared)
	return nil
}

func (w *world) noPlanningFacts() error {
	w.declared = declaredFacts{}
	w.facts.Set(w.declared)
	return nil
}

// planningFactsRefreshed re-stamps every declared fact with a fresh
// watermark, as the Kafka consumers do when the producers publish again.
func (w *world) planningFactsRefreshed() error {
	asOf := w.clock.Now().Add(-time.Minute)
	for i := range w.declared.Capabilities {
		w.declared.Capabilities[i].AsOf = asOf
	}
	for i := range w.declared.Plans {
		w.declared.Plans[i].PublishedAt = asOf
	}
	for i := range w.declared.Demands {
		w.declared.Demands[i].AsOf = asOf
	}
	w.facts.Set(w.declared)
	return nil
}

// ------------------------------------------------- explicit planning snapshot

// proposalSnapshot is the body of POST /v1/transfer-proposals:generate under
// construction: a caller-provided, coherent planning snapshot.
type proposalSnapshot struct {
	asOf      string
	positions []map[string]any
	policies  []map[string]any
	lanes     []map[string]any
}

func (w *world) aPlanningSnapshotAsOf(asOf string) error {
	w.snapshot = proposalSnapshot{asOf: asOf}
	return nil
}

// position returns the snapshot's position of site+sku, creating it as of
// the snapshot's asOf.
func (s *proposalSnapshot) position(site, sku string) map[string]any {
	for _, p := range s.positions {
		if p["Site"] == site && p["SKU"] == sku {
			return p
		}
	}
	p := map[string]any{
		"Site": site, "SKU": sku, "Available": 0, "CustomerReservations": 0,
		"ConfirmedInbound": 0, "CommittedOutbound": 0, "AsOf": s.asOf,
	}
	s.positions = append(s.positions, p)
	return p
}

func (w *world) sitePositionHas(site string, units int, field, sku string) error {
	keys := map[string]string{
		"available units":          "Available",
		"customer reservations":    "CustomerReservations",
		"customer reservation":     "CustomerReservations",
		"confirmed inbound units":  "ConfirmedInbound",
		"committed outbound units": "CommittedOutbound",
	}
	key, ok := keys[field]
	if !ok {
		return fmt.Errorf("unknown position field %q", field)
	}
	w.snapshot.position(site, sku)[key] = units
	return nil
}

func (w *world) sitePositionAsOf(site, sku, asOf string) error {
	w.snapshot.position(site, sku)["AsOf"] = asOf
	return nil
}

func (w *world) siteHoldsPolicy(site, version, sku string, safety, target, priority int) error {
	w.snapshot.policies = append(w.snapshot.policies, map[string]any{
		"Version": version, "Site": site, "SKU": sku,
		"SafetyStock": safety, "TargetStock": target, "UnitPriority": priority,
	})
	return nil
}

func (w *world) aLane(state, origin, destination string, hours, cost int) error {
	w.snapshot.lanes = append(w.snapshot.lanes, map[string]any{
		"Origin": origin, "Destination": destination,
		"LeadTime":         int64(hours) * int64(time.Hour),
		"UnitHandlingCost": cost, "Enabled": state == "enabled",
	})
	return nil
}

func (w *world) iGenerateTransferProposals(ctx context.Context) error {
	body, err := json.Marshal(map[string]any{
		"asOf":      w.snapshot.asOf,
		"positions": nonNil(w.snapshot.positions),
		"policies":  nonNil(w.snapshot.policies),
		"lanes":     nonNil(w.snapshot.lanes),
	})
	if err != nil {
		return err
	}
	return w.record(ctx, request{server: w.oltp, method: http.MethodPost, path: "/v1/transfer-proposals:generate", body: body})
}

func nonNil(items []map[string]any) []map[string]any {
	if items == nil {
		return []map[string]any{}
	}
	return items
}

// theGenerateResponsesAreIdentical re-sends the same snapshot and compares
// the two answers byte for byte: proposals are deterministic.
func (w *world) iGenerateTheSameProposalsAgain(ctx context.Context) error {
	first := string(w.body)
	if err := w.iGenerateTransferProposals(ctx); err != nil {
		return err
	}
	if string(w.body) != first {
		return fmt.Errorf("the same snapshot produced different proposals:\n%s\n%s", first, string(w.body))
	}
	return nil
}

func (w *world) registerFactSteps(sc *godog.ScenarioContext) {
	sc.Step(`^site "([^"]*)" is declared with capacity (\d+) and (\d+) units of demand for SKU "([^"]*)"$`, w.siteDeclared)
	sc.Step(`^site "([^"]*)" also has (\d+) units of demand for SKU "([^"]*)" inside its capacity window$`, w.siteAlsoHasDemand)
	sc.Step(`^site "([^"]*)" also has (\d+) units of demand for SKU "([^"]*)" due before its capacity window$`, w.siteHasDemandOutsideWindow)
	sc.Step(`^site "([^"]*)" also has (\d+) units of removed demand for SKU "([^"]*)"$`, w.siteHasRemovedDemand)
	sc.Step(`^site "([^"]*)" cannot (send|receive) transfers$`, w.siteCannot)
	sc.Step(`^site "([^"]*)" has no published capacity plan$`, w.siteHasNoCapacityPlan)
	sc.Step(`^no planning facts are declared$`, w.noPlanningFacts)
	sc.Step(`^the planning facts are refreshed$`, w.planningFactsRefreshed)

	sc.Step(`^a planning snapshot as of "([^"]*)"$`, w.aPlanningSnapshotAsOf)
	sc.Step(`^site "([^"]*)" has (\d+) (available units|customer reservations?|confirmed inbound units|committed outbound units) of SKU "([^"]*)"$`, w.sitePositionHas)
	sc.Step(`^site "([^"]*)" has its position of SKU "([^"]*)" as of "([^"]*)"$`, w.sitePositionAsOf)
	sc.Step(`^site "([^"]*)" holds policy "([^"]*)" for SKU "([^"]*)" with safety stock (\d+), target stock (\d+) and unit priority (\d+)$`, w.siteHoldsPolicy)
	sc.Step(`^an (enabled|disabled) lane from "([^"]*)" to "([^"]*)" with lead time (\d+) hours and unit handling cost (\d+)$`, w.aLane)
	sc.Step(`^I generate transfer proposals$`, w.iGenerateTransferProposals)
	sc.Step(`^I generate the same transfer proposals again and get an identical answer$`, w.iGenerateTheSameProposalsAgain)
}
