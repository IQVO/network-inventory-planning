package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/network-inventory-planning/internal/application/ports"
	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

// Compile-time assertions that the catalogue repos satisfy their ports.
var (
	_ ports.RebalancePolicyRepository = (*RebalancePolicyRepo)(nil)
	_ ports.TransferLaneRepository    = (*TransferLaneRepo)(nil)
)

// RebalancePolicyRepo is the pgx-backed Rebalance Policy catalogue
// (migration 0005, ADR 0015).
type RebalancePolicyRepo struct {
	pool *pgxpool.Pool
}

// NewRebalancePolicyRepo constructs a RebalancePolicyRepo over pool.
func NewRebalancePolicyRepo(pool *pgxpool.Pool) *RebalancePolicyRepo {
	return &RebalancePolicyRepo{pool: pool}
}

const policyColumns = `site_id, sku, policy_version, safety_stock, target_stock, unit_priority, revision, updated_at`

func scanPolicy(row pgx.Row) (transfer.RebalancePolicy, error) {
	var (
		e         transfer.RebalancePolicy
		site, sku string
	)
	err := row.Scan(&site, &sku, &e.Policy.Version, &e.Policy.SafetyStock, &e.Policy.TargetStock,
		&e.Policy.UnitPriority, &e.Revision, &e.UpdatedAt)
	e.Policy.Site, e.Policy.SKU = transfer.SiteID(site), transfer.SKU(sku)
	e.UpdatedAt = e.UpdatedAt.UTC()
	return e, err
}

// Get implements ports.RebalancePolicyRepository.
func (r *RebalancePolicyRepo) Get(ctx context.Context, site transfer.SiteID, sku transfer.SKU) (transfer.RebalancePolicy, bool, error) {
	e, err := scanPolicy(queryFor(ctx, r.pool).QueryRow(ctx,
		`SELECT `+policyColumns+` FROM rebalance_policies WHERE site_id = $1 AND sku = $2`, string(site), string(sku)))
	if errors.Is(err, pgx.ErrNoRows) {
		return transfer.RebalancePolicy{}, false, nil
	}
	if err != nil {
		return transfer.RebalancePolicy{}, false, fmt.Errorf("get rebalance policy: %w", err)
	}
	return e, true, nil
}

// List implements ports.RebalancePolicyRepository.
func (r *RebalancePolicyRepo) List(ctx context.Context) ([]transfer.RebalancePolicy, error) {
	rows, err := queryFor(ctx, r.pool).Query(ctx, `SELECT `+policyColumns+` FROM rebalance_policies ORDER BY site_id, sku`)
	if err != nil {
		return nil, fmt.Errorf("list rebalance policies: %w", err)
	}
	defer rows.Close()
	out := []transfer.RebalancePolicy{}
	for rows.Next() {
		e, err := scanPolicy(rows)
		if err != nil {
			return nil, fmt.Errorf("list rebalance policies: %w", err)
		}
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list rebalance policies: %w", err)
	}
	return out, nil
}

// Save implements ports.RebalancePolicyRepository: expectedRevision 0
// inserts (ON CONFLICT DO NOTHING), N updates WHERE revision = N; zero rows
// affected is a lost race.
func (r *RebalancePolicyRepo) Save(ctx context.Context, e transfer.RebalancePolicy, expectedRevision int64) error {
	p := e.Policy
	var (
		sql  string
		args = []any{string(p.Site), string(p.SKU), p.Version, p.SafetyStock, p.TargetStock, p.UnitPriority, e.Revision, e.UpdatedAt}
	)
	if expectedRevision == 0 {
		sql = `INSERT INTO rebalance_policies (` + policyColumns + `)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
			ON CONFLICT (site_id, sku) DO NOTHING`
	} else {
		sql = `UPDATE rebalance_policies SET policy_version = $3, safety_stock = $4, target_stock = $5,
				unit_priority = $6, revision = $7, updated_at = $8
			WHERE site_id = $1 AND sku = $2 AND revision = $9`
		args = append(args, expectedRevision)
	}
	return guardedWrite(ctx, queryFor(ctx, r.pool), "save rebalance policy", sql, args...)
}

// TransferLaneRepo is the pgx-backed Transfer Lane catalogue (migration
// 0005, ADR 0015).
type TransferLaneRepo struct {
	pool *pgxpool.Pool
}

// NewTransferLaneRepo constructs a TransferLaneRepo over pool.
func NewTransferLaneRepo(pool *pgxpool.Pool) *TransferLaneRepo {
	return &TransferLaneRepo{pool: pool}
}

const laneColumns = `origin_site_id, destination_site_id, lead_time_ns, unit_handling_cost, enabled, revision, updated_at`

func scanLane(row pgx.Row) (transfer.TransferLane, error) {
	var (
		e                   transfer.TransferLane
		origin, destination string
		leadTimeNS          int64
	)
	err := row.Scan(&origin, &destination, &leadTimeNS, &e.Lane.UnitHandlingCost, &e.Lane.Enabled, &e.Revision, &e.UpdatedAt)
	e.Lane.Origin, e.Lane.Destination = transfer.SiteID(origin), transfer.SiteID(destination)
	e.Lane.LeadTime = time.Duration(leadTimeNS)
	e.UpdatedAt = e.UpdatedAt.UTC()
	return e, err
}

// Get implements ports.TransferLaneRepository.
func (r *TransferLaneRepo) Get(ctx context.Context, origin, destination transfer.SiteID) (transfer.TransferLane, bool, error) {
	e, err := scanLane(queryFor(ctx, r.pool).QueryRow(ctx,
		`SELECT `+laneColumns+` FROM transfer_lanes WHERE origin_site_id = $1 AND destination_site_id = $2`, string(origin), string(destination)))
	if errors.Is(err, pgx.ErrNoRows) {
		return transfer.TransferLane{}, false, nil
	}
	if err != nil {
		return transfer.TransferLane{}, false, fmt.Errorf("get transfer lane: %w", err)
	}
	return e, true, nil
}

// List implements ports.TransferLaneRepository.
func (r *TransferLaneRepo) List(ctx context.Context) ([]transfer.TransferLane, error) {
	rows, err := queryFor(ctx, r.pool).Query(ctx, `SELECT `+laneColumns+` FROM transfer_lanes ORDER BY origin_site_id, destination_site_id`)
	if err != nil {
		return nil, fmt.Errorf("list transfer lanes: %w", err)
	}
	defer rows.Close()
	out := []transfer.TransferLane{}
	for rows.Next() {
		e, err := scanLane(rows)
		if err != nil {
			return nil, fmt.Errorf("list transfer lanes: %w", err)
		}
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list transfer lanes: %w", err)
	}
	return out, nil
}

// Save implements ports.TransferLaneRepository (see RebalancePolicyRepo.Save).
func (r *TransferLaneRepo) Save(ctx context.Context, e transfer.TransferLane, expectedRevision int64) error {
	l := e.Lane
	var (
		sql  string
		args = []any{string(l.Origin), string(l.Destination), int64(l.LeadTime), l.UnitHandlingCost, l.Enabled, e.Revision, e.UpdatedAt}
	)
	if expectedRevision == 0 {
		sql = `INSERT INTO transfer_lanes (` + laneColumns + `)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
			ON CONFLICT (origin_site_id, destination_site_id) DO NOTHING`
	} else {
		sql = `UPDATE transfer_lanes SET lead_time_ns = $3, unit_handling_cost = $4, enabled = $5,
				revision = $6, updated_at = $7
			WHERE origin_site_id = $1 AND destination_site_id = $2 AND revision = $8`
		args = append(args, expectedRevision)
	}
	return guardedWrite(ctx, queryFor(ctx, r.pool), "save transfer lane", sql, args...)
}

// guardedWrite runs one revision-guarded INSERT/UPDATE and maps zero rows
// affected to transfer.ErrCatalogueConcurrentModification.
func guardedWrite(ctx context.Context, q querier, what, sql string, args ...any) error {
	tag, err := q.Exec(ctx, sql, args...)
	if err != nil {
		return fmt.Errorf("%s: %w", what, err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("%s: %w", what, transfer.ErrCatalogueConcurrentModification)
	}
	return nil
}
