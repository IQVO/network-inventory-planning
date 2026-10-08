package kafka

import (
	"context"
	"fmt"

	"github.com/claudioed/network-inventory-planning/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

// AnalyticsTopic is this context's dedicated analytics stream (ADR 0007):
// saga-health occurrences (TransferStateAdvanced, TransferStuckDetected,
// RebalanceRunCompleted) published as CloudEvents 1.0 analytics
// occurrences, separate from the integration topic so the OLTP contract
// and the health signal evolve independently — the same separation as
// inventory-storage's warehouse.inventory.analytics.
const AnalyticsTopic = "warehouse.network-inventory-planning.analytics"

// analyticsSchemaVersion is the v<N> of every analytics dataschema.
const analyticsSchemaVersion = 1

// AnalyticsEncoder encodes the saga-health occurrences onto
// AnalyticsTopic (ADR 0007). It satisfies the same Encoder port as
// TransferEncoder, so the transactional outbox fans every domain event to
// both encoders and each keeps only its own types — an event outside the
// analytics contract (e.g. PlanApproved on the integration topic)
// encodes to zero messages, never an error.
type AnalyticsEncoder struct {
	mintID idMinter
}

// NewAnalyticsEncoder constructs the production analytics encoder.
func NewAnalyticsEncoder() *AnalyticsEncoder { return &AnalyticsEncoder{mintID: uuidMinter} }

// entitySaga is the `type` entity segment of the saga-health
// occurrences: they describe the transfer saga's health, not one
// integration command.
const entitySaga = "saga"

// Encode implements Encoder.
func (e *AnalyticsEncoder) Encode(ctx context.Context, event transfer.DomainEvent) ([]Encoded, error) {
	switch evt := event.(type) {
	case transfer.StateAdvanced:
		return e.encodeStateAdvanced(ctx, evt)
	case transfer.StuckDetected:
		return e.encodeStuckDetected(ctx, evt)
	case transfer.RebalanceRunCompleted:
		return e.encodeRebalanceRun(ctx, evt)
	default:
		return nil, nil
	}
}

func (e *AnalyticsEncoder) encodeStateAdvanced(ctx context.Context, evt transfer.StateAdvanced) ([]Encoded, error) {
	payload := map[string]any{
		"transfer_id": string(evt.TransferID),
		"from":        string(evt.From),
		"to":          string(evt.To),
		"age_seconds": evt.AgeSeconds,
	}
	// dwell_seconds is additive within v1 (ADR 0009 amendment): omitted —
	// never sent as 0 — when the entry time of `from` is unknown.
	if evt.DwellSeconds != nil {
		payload["dwell_seconds"] = *evt.DwellSeconds
	}
	value, err := cloudevents.New(cloudevents.Spec{
		ID:        e.mintID(),
		Entity:    entitySaga,
		EventName: "TransferStateAdvanced",
		Subject:   string(evt.TransferID),
		Time:      evt.OccurredAt,
		Stream:    cloudevents.StreamAnalytics,
		Version:   analyticsSchemaVersion,
		Data:      payload,
	})
	if err != nil {
		return nil, fmt.Errorf("encode TransferStateAdvanced: %w", err)
	}
	return []Encoded{{
		Topic:     AnalyticsTopic,
		EventType: cloudevents.Type(entitySaga, "TransferStateAdvanced"),
		Key:       []byte(evt.TransferID),
		Value:     value,
		Headers:   tracedHeaders(ctx),
	}}, nil
}

func (e *AnalyticsEncoder) encodeStuckDetected(ctx context.Context, evt transfer.StuckDetected) ([]Encoded, error) {
	payload := map[string]any{
		"transfer_id":       string(evt.TransferID),
		"state":             string(evt.State),
		"age_seconds":       evt.AgeSeconds,
		"threshold_seconds": evt.ThresholdSeconds,
	}
	value, err := cloudevents.New(cloudevents.Spec{
		ID:        e.mintID(),
		Entity:    entitySaga,
		EventName: "TransferStuckDetected",
		Subject:   string(evt.TransferID),
		Time:      evt.OccurredAt,
		Stream:    cloudevents.StreamAnalytics,
		Version:   analyticsSchemaVersion,
		Data:      payload,
	})
	if err != nil {
		return nil, fmt.Errorf("encode TransferStuckDetected: %w", err)
	}
	return []Encoded{{
		Topic:     AnalyticsTopic,
		EventType: cloudevents.Type(entitySaga, "TransferStuckDetected"),
		Key:       []byte(evt.TransferID),
		Value:     value,
		Headers:   tracedHeaders(ctx),
	}}, nil
}

func (e *AnalyticsEncoder) encodeRebalanceRun(ctx context.Context, evt transfer.RebalanceRunCompleted) ([]Encoded, error) {
	payload := map[string]any{
		"run_id":         evt.RunID,
		"proposal_count": evt.ProposalCount,
		"rejected_count": evt.RejectedCount,
		"stale_facts":    evt.StaleFacts,
	}
	value, err := cloudevents.New(cloudevents.Spec{
		ID:        e.mintID(),
		Entity:    entitySaga,
		EventName: "RebalanceRunCompleted",
		// Subject is the run, not one transfer: the occurrence describes
		// a planning pass over the whole network.
		Subject: evt.RunID,
		Time:    evt.CompletedAt,
		Stream:  cloudevents.StreamAnalytics,
		Version: analyticsSchemaVersion,
		Data:    payload,
	})
	if err != nil {
		return nil, fmt.Errorf("encode RebalanceRunCompleted: %w", err)
	}
	return []Encoded{{
		Topic:     AnalyticsTopic,
		EventType: cloudevents.Type(entitySaga, "RebalanceRunCompleted"),
		Key:       []byte(evt.RunID),
		Value:     value,
		Headers:   tracedHeaders(ctx),
	}}, nil
}
