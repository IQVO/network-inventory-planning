// Package kafka holds this service's OUTBOUND Kafka adapter (Phase 2):
// the transfer-events encoder that turns saga domain events into their
// CloudEvents 1.0 wire form on warehouse.network-inventory-planning.events,
// and RelaySink, the production Sink the outbox relay drains through.
// Nothing here sends directly from a request handler — the transactional
// outbox (postgres.OutboxWriter + postgres.OutboxRelay) is the only
// publication path (ADR 0003).
package kafka

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	kafkago "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"

	"github.com/claudioed/network-inventory-planning/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

// TransferTopic is this service's own integration topic.
const TransferTopic = "warehouse.network-inventory-planning.events"

// entityTransfer is the `type` entity segment of both published types.
const entityTransfer = "transfer"

// Encoded is one already-encoded, wire-ready Kafka message. The CloudEvents
// id was minted once at Encode time, persisted with the outbox row, and is
// carried unchanged on every relay redelivery.
type Encoded struct {
	Topic     string
	EventType string
	Key       []byte
	Value     []byte
	Headers   []kafkago.Header
}

// idMinter mints the CloudEvents id once per event occurrence. A field so
// tests can make it deterministic.
type idMinter func() string

func uuidMinter() string { return uuid.NewString() }

// Encoder turns one saga domain event into its Kafka wire form without
// sending it. postgres.OutboxWriter calls it inside the approval
// transaction.
type Encoder interface {
	Encode(ctx context.Context, event transfer.DomainEvent) ([]Encoded, error)
}

// TransferEncoder encodes the published types:
//
//   - transfer.PlanApproved → TransferPlanApproved (subject/key transfer id)
//   - transfer.AllocationRequested → TransferAllocationRequested
//     (subject/key transfer_line_id, payload exactly inventory-storage's
//     consumed contract {transfer_id, transfer_line_id, origin_site_id,
//     sku, quantity})
//   - transfer.DemandReleased → WorkDemandReleased (subject/key
//     demand_id, payload exactly WES's consumed contract {demand_id,
//     work_kind, transfer_ref, path_id, site_id, cpt, sku, quantity} —
//     ADR 0005)
//
// Any other domain event encodes to zero messages (skipped, not an error).
type TransferEncoder struct {
	mintID idMinter
}

// NewTransferEncoder constructs the production encoder.
func NewTransferEncoder() *TransferEncoder { return &TransferEncoder{mintID: uuidMinter} }

// Encode implements Encoder.
func (e *TransferEncoder) Encode(ctx context.Context, event transfer.DomainEvent) ([]Encoded, error) {
	switch evt := event.(type) {
	case transfer.PlanApproved:
		return e.encodePlanApproved(ctx, evt)
	case transfer.AllocationRequested:
		return e.encodeAllocationRequested(ctx, evt)
	case transfer.DemandReleased:
		return e.encodeDemandReleased(ctx, evt)
	default:
		return nil, nil
	}
}

// entityWorkDemand is the `type` entity segment of WorkDemandReleased —
// WES's consumed contract names the entity `workdemand`, not `transfer`.
const entityWorkDemand = "workdemand"

// tracedHeaders builds the Kafka headers every encoded message carries:
// the fleet content-type header plus W3C trace headers injected from
// whatever span is active on ctx (ADR 0007). The injected traceparent
// rides along with the outbox row, so the relay's eventual Kafka write
// carries the trace of the use case that raised the event even though it
// happens later and in another goroutine.
//
// With no live span the propagator writes nothing (a no-op tracer
// produces no valid traceparent), so an un-instrumented deployment ships
// clean headers instead of an all-zero traceparent a consumer would try
// to parent onto.
func tracedHeaders(ctx context.Context) []kafkago.Header {
	headers := []kafkago.Header{cloudevents.ContentTypeHeader()}
	otel.GetTextMapPropagator().Inject(ctx, headerCarrier{headers: &headers})
	return headers
}

func (e *TransferEncoder) encodeDemandReleased(ctx context.Context, evt transfer.DemandReleased) ([]Encoded, error) {
	// EXACTLY WES's consumed contract (mirrored from its
	// apis/asyncapi.yaml on origin/develop): data {demand_id, work_kind,
	// transfer_ref, path_id, site_id, cpt, sku, quantity}, subject/key
	// demand_id, every field required by the producer.
	payload := map[string]any{
		"demand_id":    evt.DemandID,
		"work_kind":    string(evt.WorkKind),
		"transfer_ref": string(evt.TransferRef),
		"path_id":      evt.PathID,
		"site_id":      evt.SiteID,
		"cpt":          evt.CPT.UTC().Format(time.RFC3339),
		"sku":          evt.SKU,
		"quantity":     evt.Quantity,
	}
	value, err := cloudevents.New(cloudevents.Spec{
		ID:        e.mintID(),
		Entity:    entityWorkDemand,
		EventName: "WorkDemandReleased",
		Subject:   evt.DemandID,
		Time:      evt.OccurredAt,
		Stream:    cloudevents.StreamEvents,
		Version:   1,
		Data:      payload,
	})
	if err != nil {
		return nil, fmt.Errorf("encode WorkDemandReleased: %w", err)
	}
	return []Encoded{{
		Topic:     TransferTopic,
		EventType: cloudevents.Type(entityWorkDemand, "WorkDemandReleased"),
		Key:       []byte(evt.DemandID),
		Value:     value,
		Headers:   tracedHeaders(ctx),
	}}, nil
}

func (e *TransferEncoder) encodePlanApproved(ctx context.Context, evt transfer.PlanApproved) ([]Encoded, error) {
	payload := map[string]any{
		"transfer_id":         string(evt.TransferID),
		"origin_site_id":      evt.OriginSiteID,
		"destination_site_id": evt.DestinationSiteID,
		"sku":                 evt.SKU,
		"quantity":            evt.Quantity,
		"policy_version":      evt.PolicyVersion,
		"operator_reason":     evt.OperatorReason,
		"proposal_as_of":      evt.ProposalAsOf.UTC().Format(time.RFC3339),
	}
	value, err := cloudevents.New(cloudevents.Spec{
		ID:        e.mintID(),
		Entity:    entityTransfer,
		EventName: "TransferPlanApproved",
		Subject:   string(evt.TransferID),
		Time:      evt.OccurredAt,
		Stream:    cloudevents.StreamEvents,
		Version:   1,
		Data:      payload,
	})
	if err != nil {
		return nil, fmt.Errorf("encode TransferPlanApproved: %w", err)
	}
	return []Encoded{{
		Topic:     TransferTopic,
		EventType: cloudevents.Type(entityTransfer, "TransferPlanApproved"),
		Key:       []byte(evt.TransferID),
		Value:     value,
		Headers:   tracedHeaders(ctx),
	}}, nil
}

func (e *TransferEncoder) encodeAllocationRequested(ctx context.Context, evt transfer.AllocationRequested) ([]Encoded, error) {
	// EXACTLY inventory-storage's consumed contract (read from its
	// apis/asyncapi.yaml on origin/develop): data {transfer_id,
	// transfer_line_id, origin_site_id, sku, quantity}, subject/key
	// transfer_line_id.
	payload := map[string]any{
		"transfer_id":      string(evt.TransferID),
		"transfer_line_id": evt.TransferLineID,
		"origin_site_id":   evt.OriginSiteID,
		"sku":              evt.SKU,
		"quantity":         evt.Quantity,
	}
	value, err := cloudevents.New(cloudevents.Spec{
		ID:        e.mintID(),
		Entity:    entityTransfer,
		EventName: "TransferAllocationRequested",
		Subject:   evt.TransferLineID,
		Time:      evt.OccurredAt,
		Stream:    cloudevents.StreamEvents,
		Version:   1,
		Data:      payload,
	})
	if err != nil {
		return nil, fmt.Errorf("encode TransferAllocationRequested: %w", err)
	}
	return []Encoded{{
		Topic:     TransferTopic,
		EventType: cloudevents.Type(entityTransfer, "TransferAllocationRequested"),
		Key:       []byte(evt.TransferLineID),
		Value:     value,
		Headers:   tracedHeaders(ctx),
	}}, nil
}

// RelaySink is the production Sink: ONE shared *kafkago.Writer with no
// fixed topic (outbox rows may span topics) and synchronous writes, so
// RelayOnce's return reflects real send outcomes. The headers on each
// Encoded row are forwarded VERBATIM: the traceparent injected at Encode
// time is the one that reaches the broker (ADR 0007).
type RelaySink struct {
	writer *kafkago.Writer
	once   sync.Once
	// send is the single-message write, split out so tests intercept
	// sends without a broker. nil in production (writes go to writer).
	send func(ctx context.Context, msg kafkago.Message) error
}

// NewRelaySink constructs the RelaySink writing to brokers.
func NewRelaySink(brokers []string) *RelaySink {
	return &RelaySink{writer: kafkago.NewWriter(kafkago.WriterConfig{
		Brokers:   brokers,
		Balancer:  &kafkago.Hash{},
		BatchSize: 1, // one message per Send: relay ordering, not throughput
	})}
}

// Send publishes one encoded message.
func (s *RelaySink) Send(ctx context.Context, msg Encoded) error {
	if s.send != nil {
		return s.send(ctx, kafkago.Message{
			Topic:   msg.Topic,
			Key:     msg.Key,
			Value:   msg.Value,
			Headers: msg.Headers,
		})
	}
	return s.writer.WriteMessages(ctx, kafkago.Message{
		Topic:   msg.Topic,
		Key:     msg.Key,
		Value:   msg.Value,
		Headers: msg.Headers,
	})
}

// Close releases the underlying writer.
func (s *RelaySink) Close() error {
	var err error
	s.once.Do(func() { err = s.writer.Close() })
	return err
}
