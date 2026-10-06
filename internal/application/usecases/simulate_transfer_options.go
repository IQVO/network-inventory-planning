package usecases

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/claudioed/network-inventory-planning/internal/application/ports"
	"github.com/claudioed/network-inventory-planning/internal/domain/planning"
)

// SimulateTransferOptions serves an ADVISORY-ONLY simulation from the local
// read models (site capability, site/SKU demand, published capacity). It
// never reserves, moves or promises stock: it answers "what would the
// network look like" from fail-closed inputs. The explicit-snapshot
// POST /v1/transfer-proposals:generate endpoint stays as the diagnostic
// path; this use case is the read-model path.
type SimulateTransferOptions struct {
	Snapshot ports.PlanningSnapshotRepository
	// MaxStaleness bounds fact freshness before the snapshot refuses.
	MaxStaleness time.Duration
	// Now supplies the planning clock (never time.Now directly).
	Now func() time.Time
}

// SiteSimulation is one participating site's advisory view.
type SiteSimulation struct {
	Site               string
	OriginEnabled      bool
	DestinationEnabled bool
	TotalDemand        int
	CapacityOverWindow float64
	// CapacityHeadroom is CapacityOverWindow minus the site's in-window
	// demand — negative means the published plan is already short.
	CapacityHeadroom int
	WindowStart      time.Time
	WindowEnd        time.Time
}

// SimulationResult is the advisory-only answer.
type SimulationResult struct {
	Advisory bool
	AsOf     time.Time
	Sites    []SiteSimulation
}

// Execute loads the local read models, assembles the fail-closed snapshot
// and projects it into the advisory simulation. Any missing/stale fact or
// direction-disabled site is an ERROR (fail-closed), never a silently
// degraded simulation.
func (u SimulateTransferOptions) Execute(ctx context.Context) (SimulationResult, error) {
	if u.Now == nil {
		return SimulationResult{}, fmt.Errorf("simulate transfer options: clock is required")
	}
	if u.MaxStaleness <= 0 {
		return SimulationResult{}, fmt.Errorf("simulate transfer options: max staleness must be positive")
	}
	facts, err := u.Snapshot.Load(ctx)
	if err != nil {
		return SimulationResult{}, fmt.Errorf("simulate transfer options: load read models: %w", err)
	}
	snap, err := planning.BuildSnapshot(planning.SnapshotInput{
		Capabilities: facts.Capabilities,
		Demands:      facts.Demands,
		Plans:        facts.Plans,
		MaxStaleness: u.MaxStaleness,
		Now:          u.Now,
	})
	if err != nil {
		return SimulationResult{}, err
	}

	// Per-site projection over the fail-closed snapshot.
	result := SimulationResult{Advisory: true, AsOf: snap.AsOf, Sites: make([]SiteSimulation, 0, len(snap.Capabilities))}
	for site, cap := range snap.Capabilities {
		plan := snap.CapacityBySite[site]
		result.Sites = append(result.Sites, SiteSimulation{
			Site:               site,
			OriginEnabled:      cap.OriginAllowed(),
			DestinationEnabled: cap.DestinationAllowed(),
			TotalDemand:        demandBySiteTotal(snap, site),
			CapacityOverWindow: plan.CapacityOverWindow,
			CapacityHeadroom:   int(plan.CapacityOverWindow) - demandBySiteTotal(snap, site),
			WindowStart:        plan.WindowStart,
			WindowEnd:          plan.WindowEnd,
		})
	}
	sort.Slice(result.Sites, func(i, j int) bool { return result.Sites[i].Site < result.Sites[j].Site })
	return result, nil
}

// demandBySiteTotal sums the window-filtered demand units of one site.
func demandBySiteTotal(snap planning.PlanningSnapshot, site string) int {
	total := 0
	for key, units := range snap.DemandBySiteSKU {
		if strings.HasPrefix(key, site+"\x00") {
			total += units
		}
	}
	return total
}
