package main_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/cucumber/godog"
	kafkago "github.com/segmentio/kafka-go"

	inboundkafka "github.com/claudioed/network-inventory-planning/internal/adapters/inbound/kafka"
	outboundkafka "github.com/claudioed/network-inventory-planning/internal/adapters/outbound/kafka"
	"github.com/claudioed/network-inventory-planning/internal/application/usecases"
	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

// ----------------------------------------------- scheduled and ticked work --

// theScheduledRebalanceRuns runs one NIP_REBALANCE_SCHEDULE tick: the same
// use case cmd's ticker calls, over the declared read models.
func (w *world) theScheduledRebalanceRuns(ctx context.Context) error {
	run := usecases.RunScheduledRebalance{
		Snapshot:     w.facts,
		Planner:      transfer.Planner{},
		Runs:         w.runs,
		Events:       w.bus,
		MaxStaleness: maxStaleness,
		Now:          w.clock.Now,
	}
	_, err := run.Execute(ctx)
	return err
}

// theSagaHealthCheckRuns runs one NIP_HEALTH_CHECK_INTERVAL tick with the
// default per-state stuck thresholds.
func (w *world) theSagaHealthCheckRuns(ctx context.Context) error {
	check := usecases.CheckStuckTransfers{
		Reader: w.transfers,
		Events: w.bus,
		Check:  transfer.StuckCheck{Thresholds: transfer.StuckThresholds(transfer.DefaultStuckThresholds())},
		Limit:  500,
		Now:    w.clock.Now,
	}
	n, err := check.Execute(ctx)
	w.stuckDetected = n
	return err
}

func (w *world) theHealthCheckDetected(expected int) error {
	if w.stuckDetected != expected {
		return fmt.Errorf("expected the health check to detect %d stuck transfer(s), got %d", expected, w.stuckDetected)
	}
	return nil
}

// noTransferExists lists transfers through the public API, out of band so
// the recorded response is left alone.
func (w *world) noTransferExists(ctx context.Context) error {
	status, body, _, err := w.call(ctx, request{server: w.oltp, method: http.MethodGet, path: "/v1/transfers"})
	if err != nil {
		return err
	}
	var page struct {
		Total int `json:"total"`
	}
	if status != http.StatusOK || json.Unmarshal(body, &page) != nil {
		return fmt.Errorf("listing transfers: status %d: %s", status, string(body))
	}
	if page.Total != 0 {
		return fmt.Errorf("expected no transfer to exist, found %d", page.Total)
	}
	return nil
}

// ------------------------------------------------------ analytics projector --

// theProjectorConsumes feeds every analytics message published since the
// last call to the PRODUCTION analytics consumer, which decodes the
// CloudEvents envelope and applies it to the analytical store the reports
// read — the publish → project hop, minus the broker.
func (w *world) theProjectorConsumes(ctx context.Context) error {
	_, messages := w.bus.snapshot()
	if err := w.project(ctx, messages[w.projected:]); err != nil {
		return err
	}
	w.projected = len(messages)
	return nil
}

// theTopicRedelivers hands every analytics message to the consumer again, as
// Kafka does after a consumer restart (at-least-once delivery).
func (w *world) theTopicRedelivers(ctx context.Context) error {
	_, messages := w.bus.snapshot()
	return w.project(ctx, messages)
}

func (w *world) project(ctx context.Context, messages []outboundkafka.Encoded) error {
	consumer := &inboundkafka.AnalyticsConsumer{Projection: w.analytics}
	for _, m := range messages {
		if m.Topic != outboundkafka.AnalyticsTopic {
			continue
		}
		if err := consumer.HandleMessage(ctx, kafkago.Message{Topic: m.Topic, Key: m.Key, Value: m.Value, Headers: m.Headers}); err != nil {
			return fmt.Errorf("projecting %s: %w", m.EventType, err)
		}
	}
	return nil
}

func (w *world) registerReportSteps(sc *godog.ScenarioContext) {
	sc.Step(`^the scheduled rebalance runs$`, w.theScheduledRebalanceRuns)
	sc.Step(`^the saga health check runs$`, w.theSagaHealthCheckRuns)
	sc.Step(`^the saga health check has detected (\d+) stuck transfers?$`, w.theHealthCheckDetected)
	sc.Step(`^no transfer exists$`, w.noTransferExists)
	sc.Step(`^the analytics projector has consumed the analytics topic$`, w.theProjectorConsumes)
	sc.Step(`^the analytics topic redelivers every message$`, w.theTopicRedelivers)
}
