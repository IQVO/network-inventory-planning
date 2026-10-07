package postgres

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

// TransferQueryRepo is the pgx-backed, READ-ONLY ports.TransferQuery over
// inter_warehouse_transfer + transfer_audit. It issues SELECTs only and is
// separate from TransferRepo so the write port is never widened.
type TransferQueryRepo struct {
	pool *pgxpool.Pool
}

// NewTransferQueryRepo constructs a TransferQueryRepo over pool.
func NewTransferQueryRepo(pool *pgxpool.Pool) *TransferQueryRepo {
	return &TransferQueryRepo{pool: pool}
}

// Get returns one transfer with its audit trail, or
// transfer.ErrTransferNotFound.
func (r *TransferQueryRepo) Get(ctx context.Context, id transfer.TransferID) (*transfer.InterWarehouseTransfer, error) {
	return NewTransferRepo(r.pool).Load(ctx, id)
}

// List returns one page of transfers matching the filter, each with its
// audit trail (loaded in ONE batched query), plus the unpaged total.
func (r *TransferQueryRepo) List(ctx context.Context, filter transfer.ListFilter) (transfer.TransferPage, error) {
	q := queryFor(ctx, r.pool)
	where, args := transferWhere(filter)

	var total int
	if err := q.QueryRow(ctx, `SELECT count(*) FROM inter_warehouse_transfer`+where, args...).Scan(&total); err != nil {
		return transfer.TransferPage{}, fmt.Errorf("count transfers: %w", err)
	}

	limit := filter.Limit
	if limit <= 0 {
		limit = transfer.DefaultListLimit
	}
	pageArgs := append(append([]any(nil), args...), limit, filter.Offset)
	query := fmt.Sprintf(`SELECT %s FROM inter_warehouse_transfer%s ORDER BY %s LIMIT $%d OFFSET $%d`,
		transferColumns, where, transferOrderBy(filter.Order), len(args)+1, len(args)+2)
	rows, err := q.Query(ctx, query, pageArgs...)
	if err != nil {
		return transfer.TransferPage{}, fmt.Errorf("list transfers: %w", err)
	}
	snaps := []transfer.Snapshot{}
	for rows.Next() {
		snap, scanErr := scanTransfer(rows)
		if scanErr != nil {
			rows.Close()
			return transfer.TransferPage{}, fmt.Errorf("list transfers: scan: %w", scanErr)
		}
		snaps = append(snaps, snap)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return transfer.TransferPage{}, fmt.Errorf("list transfers: %w", err)
	}

	audits, err := loadAudits(ctx, q, snaps)
	if err != nil {
		return transfer.TransferPage{}, err
	}
	items := make([]*transfer.InterWarehouseTransfer, 0, len(snaps))
	for _, snap := range snaps {
		snap.Audit = audits[string(snap.ID)]
		items = append(items, transfer.Rehydrate(snap))
	}
	return transfer.TransferPage{Items: items, Total: total}, nil
}

// transferWhere builds the parametrised WHERE clause of a filter (with a
// leading space, or empty when nothing filters) and its arguments. Only
// placeholders are interpolated, never values.
func transferWhere(f transfer.ListFilter) (string, []any) {
	var (
		conds []string
		args  []any
	)
	add := func(cond string, value any) {
		args = append(args, value)
		conds = append(conds, fmt.Sprintf(cond, len(args)))
	}
	if len(f.States) > 0 {
		states := make([]string, 0, len(f.States))
		for _, s := range f.States {
			states = append(states, string(s))
		}
		add("state = ANY($%d)", states)
	}
	if f.OriginSiteID != "" {
		add("origin_site_id = $%d", f.OriginSiteID)
	}
	if f.DestinationSiteID != "" {
		add("destination_site_id = $%d", f.DestinationSiteID)
	}
	if f.SiteID != "" {
		add("(origin_site_id = $%[1]d OR destination_site_id = $%[1]d)", f.SiteID)
	}
	if !f.UpdatedBefore.IsZero() {
		add("updated_at < $%d", f.UpdatedBefore.UTC())
	}
	if len(conds) == 0 {
		return "", nil
	}
	return " WHERE " + strings.Join(conds, " AND "), args
}

// transferOrderBy maps an order onto a total, stable ORDER BY (the id
// tie-break makes paging deterministic).
func transferOrderBy(order transfer.TransferOrder) string {
	if order == transfer.OrderStalestFirst {
		return "updated_at ASC, transfer_id ASC"
	}
	return "created_at DESC, transfer_id ASC"
}

// loadAudits returns the audit trails of snaps keyed by transfer id, each
// in seq order, with one query.
func loadAudits(ctx context.Context, q querier, snaps []transfer.Snapshot) (map[string][]transfer.AuditEntry, error) {
	out := make(map[string][]transfer.AuditEntry, len(snaps))
	if len(snaps) == 0 {
		return out, nil
	}
	ids := make([]string, 0, len(snaps))
	for _, s := range snaps {
		ids = append(ids, string(s.ID))
		out[string(s.ID)] = []transfer.AuditEntry{}
	}
	rows, err := q.Query(ctx, `
		SELECT transfer_id, seq, from_state, to_state, event, reason, occurred_at
		FROM transfer_audit
		WHERE transfer_id = ANY($1)
		ORDER BY transfer_id, seq ASC
	`, ids)
	if err != nil {
		return nil, fmt.Errorf("load audits: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var (
			id   string
			from string
			e    transfer.AuditEntry
		)
		if err := rows.Scan(&id, &e.Seq, &from, &e.To, &e.Event, &e.Reason, &e.OccurredAt); err != nil {
			return nil, fmt.Errorf("load audits: %w", err)
		}
		e.From = transfer.TransferState(from)
		out[id] = append(out[id], e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("load audits: %w", err)
	}
	return out, nil
}
