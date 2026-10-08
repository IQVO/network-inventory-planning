package telemetry

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/claudioed/network-inventory-planning/internal/application/ports"
)

// meterName scopes this service's own instruments, keeping them distinct
// from the ones otelhttp and the runtime collector register.
const meterName = "github.com/claudioed/network-inventory-planning"

// Tier-2 instrument names (fleet standard-metrics convention,
// <context>.<aggregate>.<verb>; Prometheus: dots->underscores plus
// `_total` on counters). ADR 0011 documents them and the derived names.
const (
	transfersApprovedCounterName = "network_inventory_planning.transfers.approved"
	stateAdvancedCounterName     = "network_inventory_planning.transfers.state_advanced"
	outboxRelayedCounterName     = "network_inventory_planning.outbox.relayed"
)

const (
	outcomeKey = attribute.Key("outcome")
	toKey      = attribute.Key("to")

	// otherValue collapses any value outside a counter's closed
	// vocabulary, so a programming slip can never mint a new series.
	otherValue = "other"
)

// TransferMetrics implements ports.TransferMetrics against the global
// MeterProvider. Until Setup installs a real provider the global one is a
// no-op, so recording is cheap and safe in tests and local runs. Its
// methods are nil-receiver safe: a nil *TransferMetrics is a documented
// no-op, matching the fleet's PlanMetrics / PathMetrics convention.
type TransferMetrics struct {
	approved      metric.Int64Counter
	stateAdvanced metric.Int64Counter
	relayed       metric.Int64Counter
}

var _ ports.TransferMetrics = (*TransferMetrics)(nil)

// NewTransferMetrics registers the three counters on the global
// MeterProvider. It only fails if an instrument name is invalid (a
// programming error); a caller that prefers to run un-instrumented may
// ignore the error and use the nil *TransferMetrics.
func NewTransferMetrics() (*TransferMetrics, error) {
	return newTransferMetrics(otel.Meter(meterName))
}

func newTransferMetrics(meter metric.Meter) (*TransferMetrics, error) {
	approved, err := meter.Int64Counter(transfersApprovedCounterName,
		metric.WithDescription("POST /v1/transfers:approve outcomes: approved (new transfer created), replayed (idempotent re-send of an existing key) or refused (any error). A refused share climbing means operators approve proposals the fail-closed facts or inputs reject; replayed climbing means clients are retrying."),
		metric.WithUnit("{approval}"),
	)
	if err != nil {
		return nil, err
	}
	stateAdvanced, err := meter.Int64Counter(stateAdvancedCounterName,
		metric.WithDescription("Saga transitions, by the state entered (to). The shape of the funnel: a state that is entered but whose successor stops being entered is where transfers are stalling."),
		metric.WithUnit("{transition}"),
	)
	if err != nil {
		return nil, err
	}
	relayed, err := meter.Int64Counter(outboxRelayedCounterName,
		metric.WithDescription("Outbox rows the relay tried to send to Kafka, by outcome (published or failed). Failed with no matching published means the broker (or an encoder) is down and the saga is silently not progressing."),
		metric.WithUnit("{message}"),
	)
	if err != nil {
		return nil, err
	}
	return &TransferMetrics{approved: approved, stateAdvanced: stateAdvanced, relayed: relayed}, nil
}

// TransferApproved implements ports.TransferMetrics.
func (m *TransferMetrics) TransferApproved(ctx context.Context, outcome string) {
	if m == nil {
		return
	}
	switch outcome {
	case ports.ApprovalApproved, ports.ApprovalReplayed, ports.ApprovalRefused:
	default:
		outcome = otherValue
	}
	m.approved.Add(ctx, 1, metric.WithAttributes(outcomeKey.String(outcome)))
}

// TransferStateAdvanced implements ports.TransferMetrics. `to` is passed
// through: the caller supplies a domain TransferState (a closed enum), and
// an empty value is collapsed to "other".
func (m *TransferMetrics) TransferStateAdvanced(ctx context.Context, to string) {
	if m == nil {
		return
	}
	if to == "" {
		to = otherValue
	}
	m.stateAdvanced.Add(ctx, 1, metric.WithAttributes(toKey.String(to)))
}

// OutboxRelayed implements ports.TransferMetrics.
func (m *TransferMetrics) OutboxRelayed(ctx context.Context, outcome string) {
	if m == nil {
		return
	}
	switch outcome {
	case ports.RelayPublished, ports.RelayFailed:
	default:
		outcome = otherValue
	}
	m.relayed.Add(ctx, 1, metric.WithAttributes(outcomeKey.String(outcome)))
}
