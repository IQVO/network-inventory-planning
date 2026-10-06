package postgres

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/network-inventory-planning/internal/domain/planning"
)

// ProcessedEventRepo is a pgxpool-backed ports.ProcessedEventRepository
// (inbound Kafka consumer idempotency).
type ProcessedEventRepo struct {
	pool *pgxpool.Pool
}

// NewProcessedEventRepo constructs a ProcessedEventRepo over pool.
func NewProcessedEventRepo(pool *pgxpool.Pool) *ProcessedEventRepo {
	return &ProcessedEventRepo{pool: pool}
}

// Claim records (consumer, eventID) as processed, returning false if it
// was already recorded by an earlier call. Called inside a
// ports.UnitOfWork (the consumers' case) the INSERT is part of the SAME
// transaction as the event's side effects, so a rollback un-claims it and
// a redelivery is processed, not skipped.
func (r *ProcessedEventRepo) Claim(ctx context.Context, consumer, eventID string) (bool, error) {
	tag, err := queryFor(ctx, r.pool).Exec(ctx, `
		INSERT INTO processed_events (consumer, event_id)
		VALUES ($1, $2)
		ON CONFLICT (consumer, event_id) DO NOTHING
	`, consumer, eventID)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

// SiteCapabilityRepo is a pgxpool-backed ports.SiteCapabilityRepository
// over the site_capability table.
type SiteCapabilityRepo struct {
	pool *pgxpool.Pool
}

// NewSiteCapabilityRepo constructs a SiteCapabilityRepo over pool.
func NewSiteCapabilityRepo(pool *pgxpool.Pool) *SiteCapabilityRepo {
	return &SiteCapabilityRepo{pool: pool}
}

// Upsert applies c only when its revision supersedes the stored row (or
// the site is new): last-write-wins on capability_revision.
func (r *SiteCapabilityRepo) Upsert(ctx context.Context, c planning.SiteCapability) (bool, error) {
	tag, err := queryFor(ctx, r.pool).Exec(ctx, `
		INSERT INTO site_capability (site_id, transfer_origin_enabled, transfer_destination_enabled, capability_revision, as_of)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (site_id) DO UPDATE SET
			transfer_origin_enabled      = EXCLUDED.transfer_origin_enabled,
			transfer_destination_enabled = EXCLUDED.transfer_destination_enabled,
			capability_revision          = EXCLUDED.capability_revision,
			as_of                        = EXCLUDED.as_of
		WHERE EXCLUDED.capability_revision > site_capability.capability_revision
	`, c.Site, c.TransferOriginEnabled, c.TransferDestinationEnabled, c.Revision, c.AsOf)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

// SiteSkuDemandRepo is a pgxpool-backed ports.SiteSkuDemandRepository over
// the site_sku_demand table.
type SiteSkuDemandRepo struct {
	pool *pgxpool.Pool
}

// NewSiteSkuDemandRepo constructs a SiteSkuDemandRepo over pool.
func NewSiteSkuDemandRepo(pool *pgxpool.Pool) *SiteSkuDemandRepo {
	return &SiteSkuDemandRepo{pool: pool}
}

// Upsert replaces the row keyed by d's source order + line whatever the
// state: a REMOVED fact tombstones the line's demand in place.
func (r *SiteSkuDemandRepo) Upsert(ctx context.Context, d planning.SiteSkuDemand) (bool, error) {
	tag, err := queryFor(ctx, r.pool).Exec(ctx, `
		INSERT INTO site_sku_demand (source_order_id, line_no, site_id, sku, demanded_units, due_at, state, assignment_version, as_of)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (source_order_id, line_no) DO UPDATE SET
			site_id            = EXCLUDED.site_id,
			sku                = EXCLUDED.sku,
			demanded_units     = EXCLUDED.demanded_units,
			due_at             = EXCLUDED.due_at,
			state              = EXCLUDED.state,
			assignment_version = EXCLUDED.assignment_version,
			as_of              = EXCLUDED.as_of
	`, d.SourceOrderID, d.LineNo, d.Site, d.SKU, d.DemandedUnits, d.DueAt, string(d.State), d.AssignmentVersion, d.AsOf)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

// PublishedCapacityPlanRepo is a pgxpool-backed
// ports.PublishedCapacityPlanRepository over the published_capacity_plan
// table.
type PublishedCapacityPlanRepo struct {
	pool *pgxpool.Pool
}

// NewPublishedCapacityPlanRepo constructs a PublishedCapacityPlanRepo over pool.
func NewPublishedCapacityPlanRepo(pool *pgxpool.Pool) *PublishedCapacityPlanRepo {
	return &PublishedCapacityPlanRepo{pool: pool}
}

// Upsert applies p only when its CE time supersedes the stored row (or the
// plan is new): last-write-wins on the CloudEvents time.
func (r *PublishedCapacityPlanRepo) Upsert(ctx context.Context, p planning.PublishedCapacityPlan) (bool, error) {
	tag, err := queryFor(ctx, r.pool).Exec(ctx, `
		INSERT INTO published_capacity_plan (plan_id, site_id, location, path_id, window_start, window_end, assigned_demand, capacity_over_window, shortage, published_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT (plan_id) DO UPDATE SET
			site_id              = EXCLUDED.site_id,
			location             = EXCLUDED.location,
			path_id              = EXCLUDED.path_id,
			window_start         = EXCLUDED.window_start,
			window_end           = EXCLUDED.window_end,
			assigned_demand      = EXCLUDED.assigned_demand,
			capacity_over_window = EXCLUDED.capacity_over_window,
			shortage             = EXCLUDED.shortage,
			published_at         = EXCLUDED.published_at
		WHERE EXCLUDED.published_at > published_capacity_plan.published_at
	`, p.PlanID, p.SiteID, p.Location, p.PathID, p.WindowStart, p.WindowEnd, p.AssignedDemand, p.CapacityOverWindow, p.Shortage, p.PublishedAt)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}
