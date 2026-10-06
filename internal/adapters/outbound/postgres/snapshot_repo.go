package postgres

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/network-inventory-planning/internal/domain/planning"
)

// SnapshotRepo is a pgxpool-backed ports.PlanningSnapshotRepository: the
// plain read side of the three Phase-1 read models. It performs NO
// fail-closed logic itself (that is planning.BuildSnapshot's job) and
// never degrades a missing fact into a zero value.
type SnapshotRepo struct {
	pool *pgxpool.Pool
}

// NewSnapshotRepo constructs a SnapshotRepo over pool.
func NewSnapshotRepo(pool *pgxpool.Pool) *SnapshotRepo {
	return &SnapshotRepo{pool: pool}
}

// Load implements ports.PlanningSnapshotRepository. An empty read model is
// returned as empty slices, never an error.
func (r *SnapshotRepo) Load(ctx context.Context) (planning.Facts, error) {
	q := queryFor(ctx, r.pool)

	capRows, err := q.Query(ctx, `
		SELECT site_id, transfer_origin_enabled, transfer_destination_enabled, capability_revision, as_of
		FROM site_capability
	`)
	if err != nil {
		return planning.Facts{}, err
	}
	defer capRows.Close()
	facts := planning.Facts{Capabilities: []planning.SiteCapability{}}
	for capRows.Next() {
		var c planning.SiteCapability
		if err := capRows.Scan(&c.Site, &c.TransferOriginEnabled, &c.TransferDestinationEnabled, &c.Revision, &c.AsOf); err != nil {
			return planning.Facts{}, err
		}
		facts.Capabilities = append(facts.Capabilities, c)
	}
	if err := capRows.Err(); err != nil {
		return planning.Facts{}, err
	}

	demandRows, err := q.Query(ctx, `
		SELECT source_order_id, line_no, site_id, sku, demanded_units, due_at, state, assignment_version, as_of
		FROM site_sku_demand
	`)
	if err != nil {
		return planning.Facts{}, err
	}
	defer demandRows.Close()
	facts.Demands = []planning.SiteSkuDemand{}
	for demandRows.Next() {
		var d planning.SiteSkuDemand
		var state string
		if err := demandRows.Scan(&d.SourceOrderID, &d.LineNo, &d.Site, &d.SKU, &d.DemandedUnits, &d.DueAt, &state, &d.AssignmentVersion, &d.AsOf); err != nil {
			return planning.Facts{}, err
		}
		d.State = planning.DemandState(state)
		facts.Demands = append(facts.Demands, d)
	}
	if err := demandRows.Err(); err != nil {
		return planning.Facts{}, err
	}

	planRows, err := q.Query(ctx, `
		SELECT plan_id, site_id, location, path_id, window_start, window_end, assigned_demand, capacity_over_window, shortage, published_at
		FROM published_capacity_plan
	`)
	if err != nil {
		return planning.Facts{}, err
	}
	defer planRows.Close()
	facts.Plans = []planning.PublishedCapacityPlan{}
	for planRows.Next() {
		var p planning.PublishedCapacityPlan
		if err := planRows.Scan(&p.PlanID, &p.SiteID, &p.Location, &p.PathID, &p.WindowStart, &p.WindowEnd, &p.AssignedDemand, &p.CapacityOverWindow, &p.Shortage, &p.PublishedAt); err != nil {
			return planning.Facts{}, err
		}
		facts.Plans = append(facts.Plans, p)
	}
	if err := planRows.Err(); err != nil {
		return planning.Facts{}, err
	}
	return facts, nil
}
