package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	kafkago "github.com/segmentio/kafka-go"

	outboundkafka "github.com/claudioed/network-inventory-planning/internal/adapters/outbound/kafka"
	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

// OutboxWriter is the pgx-backed ports.TransferEventPublisher: it encodes
// each saga domain event via the outbound Kafka encoder and inserts one
// outbox_events row per message INSIDE the caller's UnitOfWork. The state
// change and the events commit together or not at all; the relay drains
// them afterwards (ADR 0003).
type OutboxWriter struct {
	pool     *pgxpool.Pool
	encoders []outboundkafka.Encoder
}

// NewOutboxWriter builds an OutboxWriter fanning events out to encoders.
func NewOutboxWriter(pool *pgxpool.Pool, encoders ...outboundkafka.Encoder) *OutboxWriter {
	return &OutboxWriter{pool: pool, encoders: encoders}
}

// outboxHeader is the JSON shape headers are stored as in outbox_events.
type outboxHeader struct {
	Key   string `json:"key"`
	Value []byte `json:"value"`
}

// encodeOutboxHeaders marshals kafka-go headers for storage.
func encodeOutboxHeaders(headers []kafkago.Header) ([]byte, error) {
	stored := make([]outboxHeader, 0, len(headers))
	for _, h := range headers {
		stored = append(stored, outboxHeader{Key: h.Key, Value: h.Value})
	}
	return json.Marshal(stored)
}

// decodeOutboxHeaders is the inverse of encodeOutboxHeaders.
func decodeOutboxHeaders(raw []byte) ([]kafkago.Header, error) {
	var stored []outboxHeader
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &stored); err != nil {
			return nil, err
		}
	}
	out := make([]kafkago.Header, len(stored))
	for i, h := range stored {
		out[i] = kafkago.Header{Key: h.Key, Value: h.Value}
	}
	return out, nil
}

// Publish implements ports.TransferEventPublisher.
func (w *OutboxWriter) Publish(ctx context.Context, events ...transfer.DomainEvent) error {
	q := queryFor(ctx, w.pool)
	for _, event := range events {
		for _, enc := range w.encoders {
			messages, err := enc.Encode(ctx, event)
			if err != nil {
				return fmt.Errorf("outbox: encode %s: %w", event.EventName(), err)
			}
			for _, msg := range messages {
				headersJSON, err := encodeOutboxHeaders(msg.Headers)
				if err != nil {
					return fmt.Errorf("outbox: marshal headers for %s: %w", event.EventName(), err)
				}
				if _, err := q.Exec(ctx, `
					INSERT INTO outbox_events (topic, event_type, key, value, headers)
					VALUES ($1, $2, $3, $4, $5)
				`, msg.Topic, msg.EventType, msg.Key, msg.Value, headersJSON); err != nil {
					return fmt.Errorf("outbox: insert row for %s: %w", event.EventName(), err)
				}
			}
		}
	}
	return nil
}

// OutboxMessage is one claimed, already-encoded outbox row.
type OutboxMessage struct {
	ID      int64
	Encoded outboundkafka.Encoded
}

// RelaySink sends one already-encoded message to Kafka. The production
// implementation is outboundkafka.RelaySink; tests substitute a fake.
type RelaySink interface {
	Send(ctx context.Context, msg outboundkafka.Encoded) error
}

// OutboxRelay polls outbox_events for unpublished rows and sends them to
// sink ONE AT A TIME in id order, marking each published as it succeeds —
// the same pattern as inventory-storage's relay. A failure stops the pass at
// the failed row, so a later row never overtakes an earlier one that is
// still being retried. Publish only ever inserts rows; this is the only path
// by which a row reaches Kafka.
//
// With WithMaxAttempts(n), n > 0 (ADR 0016; cmd/network-inventory-planning
// passes OUTBOX_MAX_ATTEMPTS, default 20), a row that has failed n times is
// excluded from claims and logged at ERROR: later rows, including rows with
// the same key, then overtake it. That trade is deliberate — one poison row
// must not stop the whole relay forever — and the excluded row stays in
// outbox_events (attempts, last_error) until an operator re-arms it.
type OutboxRelay struct {
	pool        *pgxpool.Pool
	sink        RelaySink
	logger      *slog.Logger
	interval    time.Duration
	batchSize   int
	maxAttempts int
}

// OutboxRelayOption configures an OutboxRelay.
type OutboxRelayOption func(*OutboxRelay)

// DefaultOutboxMaxAttempts is the attempt cap the composition root uses when
// OUTBOX_MAX_ATTEMPTS is unset (fleet convention, ADR 0016). The relay's own
// zero value stays 0 (unlimited) so the option is always an explicit choice.
const DefaultOutboxMaxAttempts = 20

// WithRelayInterval overrides the default 1s poll interval.
func WithRelayInterval(d time.Duration) OutboxRelayOption {
	return func(r *OutboxRelay) { r.interval = d }
}

// WithBatchSize overrides the default 100-row claim batch.
func WithBatchSize(n int) OutboxRelayOption { return func(r *OutboxRelay) { r.batchSize = n } }

// WithRelayLogger overrides the default slog.Default().
func WithRelayLogger(l *slog.Logger) OutboxRelayOption {
	return func(r *OutboxRelay) { r.logger = l }
}

// WithMaxAttempts overrides the default 0 (unlimited retries): a row that
// failed this many times is excluded from future claims but stays visible
// in outbox_events for manual inspection, and the failure that reaches the
// cap is logged at ERROR with the row's id, topic, type and key (ADR 0016).
// 0 keeps retrying without bound. Every cmd/* that builds a relay must pass
// it (architecture fitness test TestOutboxRelaysPassAttemptCap).
func WithMaxAttempts(n int) OutboxRelayOption { return func(r *OutboxRelay) { r.maxAttempts = n } }

// NewOutboxRelay builds an OutboxRelay draining pool's outbox_events onto
// sink.
func NewOutboxRelay(pool *pgxpool.Pool, sink RelaySink, opts ...OutboxRelayOption) *OutboxRelay {
	r := &OutboxRelay{pool: pool, sink: sink, logger: slog.Default(), interval: time.Second, batchSize: 100}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// Run polls until ctx is cancelled. A failed pass (including a sink send
// failure) is retried on the next tick; the relay itself never returns an
// error short of cancellation.
func (r *OutboxRelay) Run(ctx context.Context) error {
	for {
		if _, err := r.RelayOnce(ctx); err != nil && !errors.Is(err, context.Canceled) && ctx.Err() == nil {
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(r.interval):
			}
			continue
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(r.interval):
		}
	}
}

// RelayOnce runs exactly one claim-and-send pass and returns the number of
// rows successfully published. Tests drive it deterministically.
func (r *OutboxRelay) RelayOnce(ctx context.Context) (int, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	rows, err := r.claimBatch(ctx, tx)
	if err != nil {
		return 0, err
	}
	published := 0
	for _, row := range rows {
		if sendErr := r.sink.Send(ctx, row.Encoded); sendErr != nil {
			return published, r.recordFailure(ctx, tx, row, sendErr)
		}
		if _, uerr := tx.Exec(ctx, `UPDATE outbox_events SET published_at = now() WHERE id = $1`, row.ID); uerr != nil {
			return published, uerr
		}
		published++
	}
	if err := tx.Commit(ctx); err != nil {
		return published, err
	}
	return published, nil
}

// recordFailure stages the failed row's attempts/last_error, commits the
// pass (every row published so far in it has its UPDATE staged in the same
// tx) and stops there, so a later row never overtakes a row that is still
// being retried. It returns sendErr itself so RelayOnce's caller can assert
// on the exact failure. The log is written only after the commit, so an
// ERROR saying a row reached the attempt cap is never emitted for an
// attempt that was rolled back.
func (r *OutboxRelay) recordFailure(ctx context.Context, tx pgx.Tx, row OutboxMessage, sendErr error) error {
	var attempts int
	if err := tx.QueryRow(ctx, `
		UPDATE outbox_events SET attempts = attempts + 1, last_error = $2
		WHERE id = $1
		RETURNING attempts
	`, row.ID, sendErr.Error()).Scan(&attempts); err != nil {
		return errors.Join(sendErr, err)
	}
	if commitErr := tx.Commit(ctx); commitErr != nil {
		return errors.Join(sendErr, commitErr)
	}
	r.logFailure(row, attempts, sendErr)
	return sendErr
}

// logFailure logs a failed send at WARN while the row will be retried, and
// at ERROR once the row has reached the attempt cap (ADR 0016): from then on
// claimBatch excludes it, later rows overtake it (including rows with the
// same key) and it stays in outbox_events until an operator re-arms it
// (`UPDATE outbox_events SET attempts = 0 WHERE id = ...`).
func (r *OutboxRelay) logFailure(row OutboxMessage, attempts int, sendErr error) {
	if r.maxAttempts > 0 && attempts >= r.maxAttempts {
		r.logger.Error("outbox relay: row reached the attempt cap and is excluded from future claims; re-arm it with attempts = 0 after fixing the cause",
			"outbox_id", row.ID, "topic", row.Encoded.Topic, "event_type", row.Encoded.EventType,
			"key", string(row.Encoded.Key), "attempts", attempts, "max_attempts", r.maxAttempts, "error", sendErr)
		return
	}
	r.logger.Warn("outbox relay: send failed, stopping this pass at the failed row",
		"outbox_id", row.ID, "topic", row.Encoded.Topic, "attempts", attempts, "error", sendErr)
}

// claimBatch selects and locks up to batchSize unpublished rows, oldest
// first, within tx (FOR UPDATE SKIP LOCKED so a second relay instance
// claims a disjoint set rather than racing this one).
func (r *OutboxRelay) claimBatch(ctx context.Context, tx pgx.Tx) ([]OutboxMessage, error) {
	attemptsFilter := ""
	args := []any{r.batchSize}
	if r.maxAttempts > 0 {
		attemptsFilter = "AND attempts < $2"
		args = append(args, r.maxAttempts)
	}

	sqlRows, err := tx.Query(ctx, `
		SELECT id, topic, event_type, key, value, headers
		FROM outbox_events
		WHERE published_at IS NULL `+attemptsFilter+`
		ORDER BY id ASC
		LIMIT $1
		FOR UPDATE SKIP LOCKED
	`, args...)
	if err != nil {
		return nil, err
	}
	defer sqlRows.Close()

	var out []OutboxMessage
	for sqlRows.Next() {
		var row OutboxMessage
		var headersRaw []byte
		if err := sqlRows.Scan(&row.ID, &row.Encoded.Topic, &row.Encoded.EventType, &row.Encoded.Key, &row.Encoded.Value, &headersRaw); err != nil {
			return nil, err
		}
		headers, err := decodeOutboxHeaders(headersRaw)
		if err != nil {
			return nil, err
		}
		row.Encoded.Headers = headers
		out = append(out, row)
	}
	if err := sqlRows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}
