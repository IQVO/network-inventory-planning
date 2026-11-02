package postgres

import (
	"context"
	"fmt"

	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

// ListPage implements ports.RebalancePolicyRepository: (site, SKU) keyset.
func (r *RebalancePolicyRepo) ListPage(ctx context.Context, after *transfer.PageAfter, limit int) ([]transfer.RebalancePolicy, error) {
	site, sku := keysetArgs(after)
	rows, err := queryFor(ctx, r.pool).Query(ctx, `SELECT `+policyColumns+` FROM rebalance_policies
		WHERE $1::text IS NULL OR (site_id, sku) > ($1::text, $2::text)
		ORDER BY site_id, sku LIMIT $3`, site, sku, limit)
	if err != nil {
		return nil, fmt.Errorf("list rebalance policies page: %w", err)
	}
	defer rows.Close()
	out := []transfer.RebalancePolicy{}
	for rows.Next() {
		e, err := scanPolicy(rows)
		if err != nil {
			return nil, fmt.Errorf("list rebalance policies page: %w", err)
		}
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list rebalance policies page: %w", err)
	}
	return out, nil
}

// ListPage implements ports.TransferLaneRepository: (origin, destination) keyset.
func (r *TransferLaneRepo) ListPage(ctx context.Context, after *transfer.PageAfter, limit int) ([]transfer.TransferLane, error) {
	origin, destination := keysetArgs(after)
	rows, err := queryFor(ctx, r.pool).Query(ctx, `SELECT `+laneColumns+` FROM transfer_lanes
		WHERE $1::text IS NULL OR (origin_site_id, destination_site_id) > ($1::text, $2::text)
		ORDER BY origin_site_id, destination_site_id LIMIT $3`, origin, destination, limit)
	if err != nil {
		return nil, fmt.Errorf("list transfer lanes page: %w", err)
	}
	defer rows.Close()
	out := []transfer.TransferLane{}
	for rows.Next() {
		e, err := scanLane(rows)
		if err != nil {
			return nil, fmt.Errorf("list transfer lanes page: %w", err)
		}
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list transfer lanes page: %w", err)
	}
	return out, nil
}

// keysetArgs maps an optional keyset onto the two nullable SQL arguments
// (NULL, NULL means page one).
func keysetArgs(after *transfer.PageAfter) (any, any) {
	if after == nil {
		return nil, nil
	}
	return after.Sort, after.ID
}
