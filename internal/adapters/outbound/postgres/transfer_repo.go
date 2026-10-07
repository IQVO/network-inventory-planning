package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

// TransferRepo is the pgx-backed ports.TransferRepository for the
// InterWarehouseTransfer saga aggregate: one row in
// inter_warehouse_transfer plus the append-only transfer_audit trail.
// All writes go through queryFor, so inside a UnitOfWork they join the
// caller's transaction.
type TransferRepo struct {
	pool *pgxpool.Pool
}

// NewTransferRepo constructs a TransferRepo over pool.
func NewTransferRepo(pool *pgxpool.Pool) *TransferRepo {
	return &TransferRepo{pool: pool}
}

// Create inserts the new aggregate with its full initial audit trail. The
// UNIQUE idempotency_key makes a replay of the same logical approval
// return the FIRST transfer unchanged (idempotent approve) — never a
// duplicate aggregate, never a second outbox fan-out.
func (r *TransferRepo) Create(ctx context.Context, t *transfer.InterWarehouseTransfer, idempotencyKey string) (*transfer.InterWarehouseTransfer, error) {
	q := queryFor(ctx, r.pool)

	// ON CONFLICT on the unique idempotency key is a NO-OP insert: the
	// exists check below then re-loads and returns the FIRST transfer.
	// version is initialised to the audit-entry count: the invariant
	// UpdateState relies on (version == persisted audit entries).
	tag, err := q.Exec(ctx, `
		INSERT INTO inter_warehouse_transfer
			(transfer_id, idempotency_key, origin_site_id, destination_site_id, sku, quantity,
			 policy_version, operator_reason, proposal_as_of, expires_at, state,
			 created_at, updated_at, version)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
		ON CONFLICT (idempotency_key) DO NOTHING
	`, string(t.ID()), idempotencyKey, t.OriginSiteID(), t.DestinationSiteID(), t.SKU(), t.Quantity(),
		t.PolicyVersion(), t.OperatorReason(), t.ProposalAsOf(), t.ExpiresAt(), string(t.State()),
		t.CreatedAt(), t.UpdatedAt(), len(t.Audit()))
	if err != nil {
		return nil, fmt.Errorf("create transfer: %w", err)
	}
	if tag.RowsAffected() != 1 {
		// The key already existed: return the ORIGINAL transfer so the
		// use case can distinguish a replay from a conflict.
		existing, loadErr := r.Load(ctx, transfer.TransferID(r.idempotencyKeyLookup(ctx, idempotencyKey)))
		if loadErr != nil {
			return nil, fmt.Errorf("create transfer: load existing for idempotency key: %w", loadErr)
		}
		return existing, nil
	}
	if err := appendAudit(ctx, q, t.ID(), t.Audit()); err != nil {
		return nil, err
	}
	t.SetVersion(int64(len(t.Audit())))
	return nil, nil
}

// idempotencyKeyLookup resolves the transfer id a key already belongs to.
func (r *TransferRepo) idempotencyKeyLookup(ctx context.Context, key string) string {
	q := queryFor(ctx, r.pool)
	var id string
	if err := q.QueryRow(ctx, `SELECT transfer_id FROM inter_warehouse_transfer WHERE idempotency_key = $1`, key).Scan(&id); err != nil {
		return ""
	}
	return id
}

// Load rehydrates the aggregate (row + audit trail) by transfer id.
func (r *TransferRepo) Load(ctx context.Context, id transfer.TransferID) (*transfer.InterWarehouseTransfer, error) {
	q := queryFor(ctx, r.pool)
	snap, err := scanTransfer(q.QueryRow(ctx, `SELECT `+transferColumns+` FROM inter_warehouse_transfer WHERE transfer_id = $1`, string(id)))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, transfer.ErrTransferNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("load transfer %s: %w", id, err)
	}
	audit, err := loadAudit(ctx, q, id)
	if err != nil {
		return nil, err
	}
	snap.Audit = audit
	return transfer.Rehydrate(snap), nil
}

// transferColumns is the column list scanTransfer reads, in order.
const transferColumns = `transfer_id, idempotency_key, origin_site_id, destination_site_id, sku, quantity,
		       policy_version, operator_reason, proposal_as_of, expires_at, state,
		       reservation_id, allocations, allocation_expires_at, rejection_reason,
		       picked_quantity, stow_allocations,
		       created_at, updated_at, version`

// rowScanner is the Scan half of pgx.Row / pgx.Rows.
type rowScanner interface {
	Scan(dest ...any) error
}

// scanTransfer decodes one inter_warehouse_transfer row (columns in
// transferColumns order) into a Snapshot WITHOUT its audit trail. pgx
// errors (including pgx.ErrNoRows) are returned unwrapped.
func scanTransfer(row rowScanner) (transfer.Snapshot, error) {
	var (
		snap              transfer.Snapshot
		allocationsRaw    []byte
		stowRaw           []byte
		state             string
		rejection         *string
		reservation       *string
		allocationExpires *time.Time
		pickedQty         *int
	)
	if err := row.Scan(
		&snap.ID, &snap.IdempotencyKey, &snap.OriginSiteID, &snap.DestinationSiteID, &snap.SKU, &snap.Quantity,
		&snap.PolicyVersion, &snap.OperatorReason, &snap.ProposalAsOf, &snap.ExpiresAt, &state,
		&reservation, &allocationsRaw, &allocationExpires, &rejection,
		&pickedQty, &stowRaw,
		&snap.CreatedAt, &snap.UpdatedAt, &snap.Version,
	); err != nil {
		return transfer.Snapshot{}, err
	}
	snap.State = transfer.TransferState(state)
	if reservation != nil {
		snap.ReservationID = *reservation
	}
	if allocationExpires != nil {
		snap.AllocationExpiresAt = *allocationExpires
	}
	if rejection != nil {
		snap.RejectionReason = transfer.RejectionReason(*rejection)
	}
	if pickedQty != nil {
		snap.PickedQuantity = *pickedQty
	}
	if len(allocationsRaw) > 0 {
		if err := json.Unmarshal(allocationsRaw, &snap.Allocations); err != nil {
			return transfer.Snapshot{}, fmt.Errorf("decode allocations: %w", err)
		}
	}
	if len(stowRaw) > 0 {
		if err := json.Unmarshal(stowRaw, &snap.StowAllocations); err != nil {
			return transfer.Snapshot{}, fmt.Errorf("decode stow allocations: %w", err)
		}
	}
	return snap, nil
}

// UpdateState persists state, reservation fields and the audit entries
// appended since the loaded version (optimistic concurrency: a concurrent
// writer bumps version first and this update refuses).
func (r *TransferRepo) UpdateState(ctx context.Context, t *transfer.InterWarehouseTransfer) error {
	q := queryFor(ctx, r.pool)
	allocations, err := json.Marshal(t.Allocations())
	if err != nil {
		return fmt.Errorf("update transfer %s: encode allocations: %w", t.ID(), err)
	}
	stow, err := json.Marshal(t.StowAllocations())
	if err != nil {
		return fmt.Errorf("update transfer %s: encode stow allocations: %w", t.ID(), err)
	}
	var rejection any
	if t.RejectionReason() != "" {
		rejection = string(t.RejectionReason())
	}
	var pickedQty any
	if t.PickedQuantity() > 0 {
		pickedQty = t.PickedQuantity()
	}

	tag, err := q.Exec(ctx, `
		UPDATE inter_warehouse_transfer SET
			state = $2, reservation_id = $3, allocations = $4, allocation_expires_at = $5,
			rejection_reason = $6, updated_at = $7, version = version + 1,
			picked_quantity = $9, stow_allocations = $10
		WHERE transfer_id = $1 AND version = $8
	`, string(t.ID()), string(t.State()), t.ReservationID(), allocations, t.AllocationExpiresAt(),
		rejection, t.UpdatedAt(), t.Version(), pickedQty, stow)
	if err != nil {
		return fmt.Errorf("update transfer %s: %w", t.ID(), err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("update transfer %s: optimistic concurrency conflict (loaded version %d)", t.ID(), t.Version())
	}

	audit := t.Audit()
	// Invariant: version == the number of persisted audit entries. The
	// trail already holds the first Version entries; only the ones
	// appended since the Load are new.
	loaded := int(t.Version())
	if loaded < 0 || loaded > len(audit) {
		return fmt.Errorf("update transfer %s: inconsistent audit length %d for version %d", t.ID(), len(audit), t.Version())
	}
	newEntries := audit[loaded:]
	if err := appendAudit(ctx, q, t.ID(), newEntries); err != nil {
		return err
	}
	t.SetVersion(t.Version() + 1)
	return nil
}

// appendAudit inserts audit entries with per-transfer sequential seqs
// derived from what is already persisted.
func appendAudit(ctx context.Context, q querier, id transfer.TransferID, entries []transfer.AuditEntry) error {
	if len(entries) == 0 {
		return nil
	}
	var nextSeq int64
	if err := q.QueryRow(ctx, `SELECT COALESCE(MAX(seq), 0) + 1 FROM transfer_audit WHERE transfer_id = $1`, string(id)).Scan(&nextSeq); err != nil {
		return fmt.Errorf("append audit for %s: next seq: %w", id, err)
	}
	for _, e := range entries {
		if _, err := q.Exec(ctx, `
			INSERT INTO transfer_audit (transfer_id, seq, from_state, to_state, event, reason, occurred_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
		`, string(id), nextSeq, string(e.From), string(e.To), e.Event, e.Reason, e.OccurredAt); err != nil {
			return fmt.Errorf("append audit for %s seq %d: %w", id, nextSeq, err)
		}
		nextSeq++
	}
	return nil
}

// loadAudit returns the persisted audit trail in seq order.
func loadAudit(ctx context.Context, q querier, id transfer.TransferID) ([]transfer.AuditEntry, error) {
	rows, err := q.Query(ctx, `
		SELECT seq, from_state, to_state, event, reason, occurred_at
		FROM transfer_audit
		WHERE transfer_id = $1
		ORDER BY seq ASC
	`, string(id))
	if err != nil {
		return nil, fmt.Errorf("load audit for %s: %w", id, err)
	}
	defer rows.Close()
	audit := []transfer.AuditEntry{}
	for rows.Next() {
		var e transfer.AuditEntry
		var from string
		if err := rows.Scan(&e.Seq, &from, &e.To, &e.Event, &e.Reason, &e.OccurredAt); err != nil {
			return nil, fmt.Errorf("load audit for %s: %w", id, err)
		}
		e.From = transfer.TransferState(from)
		audit = append(audit, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("load audit for %s: %w", id, err)
	}
	return audit, nil
}
