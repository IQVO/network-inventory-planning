package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
// sink ONE AT A TIME in id order, marking each published as it succeeds.
// Ordering is guaranteed per (topic, key): when a row fails, the pass
// records the failure and carries on, skipping only later rows of the SAME
// (topic, key) so none overtakes it — rows of other keys/topics are never
// held behind a poison row. Publish only ever inserts rows; this is the
// only path by which a row reaches Kafka.
type OutboxRelay struct {
	pool        *pgxpool.Pool
	sink        RelaySink
	interval    time.Duration
	batchSize   int
	maxAttempts int
}

// OutboxRelayOption configures an OutboxRelay.
type OutboxRelayOption func(*OutboxRelay)

// WithRelayInterval overrides the default 1s poll interval.
func WithRelayInterval(d time.Duration) OutboxRelayOption {
	return func(r *OutboxRelay) { r.interval = d }
}

// WithBatchSize overrides the default 100-row claim batch.
func WithBatchSize(n int) OutboxRelayOption { return func(r *OutboxRelay) { r.batchSize = n } }

// WithMaxAttempts overrides the default 0 (unlimited retries): a row that
// failed this many times is excluded from future claims but stays visible
// in outbox_events for manual inspection.
func WithMaxAttempts(n int) OutboxRelayOption { return func(r *OutboxRelay) { r.maxAttempts = n } }

// NewOutboxRelay builds an OutboxRelay draining pool's outbox_events onto
// sink.
func NewOutboxRelay(pool *pgxpool.Pool, sink RelaySink, opts ...OutboxRelayOption) *OutboxRelay {
	r := &OutboxRelay{pool: pool, sink: sink, interval: time.Second, batchSize: 100}
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
	// blocked holds every (topic,key) that already failed in this pass:
	// its later rows are skipped so they never overtake the failed one
	// (per-key order), while rows of every OTHER (topic,key) keep flowing
	// — one poison row must not hold the whole outbox hostage.
	blocked := map[relayStream]struct{}{}
	var sendErrs []error
	for _, row := range rows {
		stream := relayStream{topic: row.Encoded.Topic, key: string(row.Encoded.Key)}
		if _, skip := blocked[stream]; skip {
			continue
		}
		if sendErr := r.sink.Send(ctx, row.Encoded); sendErr != nil {
			if _, uerr := tx.Exec(ctx, `
				UPDATE outbox_events SET attempts = attempts + 1, last_error = $2 WHERE id = $1
			`, row.ID, sendErr.Error()); uerr != nil {
				return published, uerr
			}
			blocked[stream] = struct{}{}
			sendErrs = append(sendErrs, fmt.Errorf("outbox row %d: %w", row.ID, sendErr))
			continue
		}
		if _, uerr := tx.Exec(ctx, `UPDATE outbox_events SET published_at = now() WHERE id = $1`, row.ID); uerr != nil {
			return published, uerr
		}
		published++
	}
	// Commit what published plus every failure record.
	if err := tx.Commit(ctx); err != nil {
		return published, err
	}
	return published, errors.Join(sendErrs...)
}

// relayStream identifies one ordering domain of the outbox: rows with the
// same topic AND key must be delivered in id order.
type relayStream struct {
	topic string
	key   string
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
