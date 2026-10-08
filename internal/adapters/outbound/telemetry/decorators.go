package telemetry

import (
	"context"

	outboundkafka "github.com/claudioed/network-inventory-planning/internal/adapters/outbound/kafka"
	"github.com/claudioed/network-inventory-planning/internal/application/ports"
	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

// The decorators in this file are how the Tier-2 counters that have no
// single use-case choke point are recorded WITHOUT touching the use cases
// or the Postgres adapters: every saga transition is persisted through
// ports.TransferRepository (Create / UpdateState) and every outbox row is
// sent through the relay sink, so those two seams see each event exactly
// once. Both are composed in the composition root only.

// meteredTransferRepository records network_inventory_planning.transfers.
// state_advanced{to} for every audit entry (= saga transition) a
// successful Create or UpdateState persists.
//
// Counting at the repository, not at the analytics-event publisher, is
// deliberate: it sees EVERY transition including those whose analytics
// occurrence is not published (the fact use cases without an events
// publisher). The counter is incremented inside the caller's UnitOfWork,
// just before commit: a later rollback (e.g. an outbox insert failing
// after the state write) would over-count by that attempt's transitions;
// ADR 0011 documents this as accepted -- the counter is a health signal,
// not a ledger (the audit table is the ledger).
type meteredTransferRepository struct {
	ports.TransferRepository
	metrics ports.TransferMetrics
}

// NewMeteredTransferRepository decorates inner with the state-advanced
// counter. A nil metrics returns inner unchanged.
func NewMeteredTransferRepository(inner ports.TransferRepository, metrics ports.TransferMetrics) ports.TransferRepository {
	if metrics == nil {
		return inner
	}
	return &meteredTransferRepository{TransferRepository: inner, metrics: metrics}
}

// Create implements ports.TransferRepository. A replayed idempotency key
// (existing != nil) persisted nothing and counts nothing.
func (r *meteredTransferRepository) Create(ctx context.Context, t *transfer.InterWarehouseTransfer, idempotencyKey string) (*transfer.InterWarehouseTransfer, error) {
	existing, err := r.TransferRepository.Create(ctx, t, idempotencyKey)
	if err == nil && existing == nil {
		r.record(ctx, t.Audit())
	}
	return existing, err
}

// UpdateState implements ports.TransferRepository. The aggregate's version
// is the number of persisted audit entries (the repository invariant), so
// the entries past it are the transitions this call persists.
func (r *meteredTransferRepository) UpdateState(ctx context.Context, t *transfer.InterWarehouseTransfer) error {
	loaded := int(t.Version())
	err := r.TransferRepository.UpdateState(ctx, t)
	if err != nil {
		return err
	}
	if audit := t.Audit(); loaded >= 0 && loaded <= len(audit) {
		r.record(ctx, audit[loaded:])
	}
	return nil
}

func (r *meteredTransferRepository) record(ctx context.Context, entries []transfer.AuditEntry) {
	for _, e := range entries {
		r.metrics.TransferStateAdvanced(ctx, string(e.To))
	}
}

// RelaySink is the send seam of the outbox relay (postgres.RelaySink has
// the identical method set).
type RelaySink interface {
	Send(ctx context.Context, msg outboundkafka.Encoded) error
}

// meteredRelaySink records network_inventory_planning.outbox.relayed
// {outcome} for each send the relay attempts.
type meteredRelaySink struct {
	inner   RelaySink
	metrics ports.TransferMetrics
}

// NewMeteredRelaySink decorates inner with the relayed counter. A nil
// metrics returns inner unchanged.
func NewMeteredRelaySink(inner RelaySink, metrics ports.TransferMetrics) RelaySink {
	if metrics == nil {
		return inner
	}
	return &meteredRelaySink{inner: inner, metrics: metrics}
}

// Send implements RelaySink.
func (s *meteredRelaySink) Send(ctx context.Context, msg outboundkafka.Encoded) error {
	err := s.inner.Send(ctx, msg)
	if err != nil {
		s.metrics.OutboxRelayed(ctx, ports.RelayFailed)
		return err
	}
	s.metrics.OutboxRelayed(ctx, ports.RelayPublished)
	return nil
}
