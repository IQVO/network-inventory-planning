package memory

import (
	"context"
	"sort"
	"strconv"
	"time"

	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

// ListPage implements ports.RebalancePolicyRepository: (site, SKU) keyset,
// the same order and continuation rule as the Postgres adapter.
func (r *RebalancePolicies) ListPage(ctx context.Context, after *transfer.PageAfter, limit int) ([]transfer.RebalancePolicy, error) {
	all, _ := r.List(ctx)
	out := []transfer.RebalancePolicy{}
	for _, e := range all {
		if after == nil || tupleAfter(string(e.Policy.Site), string(e.Policy.SKU), after) {
			out = append(out, e)
		}
	}
	return out[:min(limit, len(out))], nil
}

// ListPage implements ports.TransferLaneRepository: (origin, destination) keyset.
func (r *TransferLanes) ListPage(ctx context.Context, after *transfer.PageAfter, limit int) ([]transfer.TransferLane, error) {
	all, _ := r.List(ctx)
	out := []transfer.TransferLane{}
	for _, e := range all {
		if after == nil || tupleAfter(string(e.Lane.Origin), string(e.Lane.Destination), after) {
			out = append(out, e)
		}
	}
	return out[:min(limit, len(out))], nil
}

// tupleAfter reports whether (a, b) sorts strictly after the keyset.
func tupleAfter(a, b string, after *transfer.PageAfter) bool {
	return a > after.Sort || (a == after.Sort && b > after.ID)
}

// ListPage implements ports.RebalanceRunRepository: (started_at DESC,
// id DESC) keyset.
func (r *RebalanceRuns) ListPage(ctx context.Context, after *transfer.PageAfter, limit int) ([]transfer.RebalanceRun, error) {
	var (
		at time.Time
		id int64
	)
	if after != nil {
		var err, idErr error
		at, err = time.Parse(time.RFC3339Nano, after.Sort)
		id, idErr = strconv.ParseInt(after.ID, 10, 64)
		if err != nil || idErr != nil {
			return nil, transfer.ErrInvalidPageAfter
		}
	}
	all, _ := r.List(ctx, 1<<30)
	sort.SliceStable(all, func(i, j int) bool {
		if !all[i].StartedAt.Equal(all[j].StartedAt) {
			return all[i].StartedAt.After(all[j].StartedAt)
		}
		return all[i].ID > all[j].ID
	})
	out := []transfer.RebalanceRun{}
	for _, run := range all {
		if after == nil || run.StartedAt.Before(at) || (run.StartedAt.Equal(at) && run.ID < id) {
			out = append(out, run)
		}
	}
	return out[:min(limit, len(out))], nil
}
