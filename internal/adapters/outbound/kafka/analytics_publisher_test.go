package kafka

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

var analyticsNow = time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)

// analyticsEnvelope is the decoded view the analytics goldens assert on.
type analyticsEnvelope struct {
	SpecVersion string         `json:"specversion"`
	ID          string         `json:"id"`
	Source      string         `json:"source"`
	Type        string         `json:"type"`
	Subject     string         `json:"subject"`
	Time        string         `json:"time"`
	DataSchema  string         `json:"dataschema"`
	Data        map[string]any `json:"data"`
}

func decodeAnalytics(t *testing.T, raw []byte) analyticsEnvelope {
	t.Helper()
	var e analyticsEnvelope
	if err := json.Unmarshal(raw, &e); err != nil {
		t.Fatalf("decode analytics envelope: %v", err)
	}
	return e
}

func TestAnalyticsEncoderEncodesStateAdvanced(t *testing.T) {
	e := &AnalyticsEncoder{mintID: func() string { return "ce-an-1" }}
	msgs, err := e.Encode(context.Background(), transfer.StateAdvanced{
		TransferID: transfer.TransferID("trf-1"),
		From:       transfer.StateAllocating,
		To:         transfer.StateAllocated,
		AgeSeconds: 3600,
		OccurredAt: analyticsNow,
	})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("messages = %d, want 1", len(msgs))
	}
	m := msgs[0]
	if m.Topic != AnalyticsTopic {
		t.Fatalf("topic = %q, want %q", m.Topic, AnalyticsTopic)
	}
	if string(m.Key) != "trf-1" {
		t.Fatalf("key = %q, want the transfer id", m.Key)
	}
	env := decodeAnalytics(t, m.Value)
	if env.Type != "com.warehouse.wes.network-inventory-planning.saga.TransferStateAdvanced" {
		t.Fatalf("type = %q", env.Type)
	}
	if env.DataSchema != "urn:warehouse:network-inventory-planning:analytics:TransferStateAdvanced:v1" {
		t.Fatalf("dataschema = %q (analytics stream, not events)", env.DataSchema)
	}
	if env.Subject != "trf-1" {
		t.Fatalf("subject = %q", env.Subject)
	}
	if len(env.Data) != 4 ||
		env.Data["transfer_id"] != "trf-1" ||
		env.Data["from"] != "ALLOCATING" ||
		env.Data["to"] != "ALLOCATED" ||
		env.Data["age_seconds"].(float64) != 3600 {
		t.Fatalf("data = %+v, want exactly {transfer_id, from, to, age_seconds}", env.Data)
	}
	if !hasHeader(m.Headers, "content-type") {
		t.Fatalf("headers = %+v, want the content-type header", m.Headers)
	}
}

func TestAnalyticsEncoderEncodesStuckDetected(t *testing.T) {
	e := &AnalyticsEncoder{mintID: func() string { return "ce-an-2" }}
	msgs, err := e.Encode(context.Background(), transfer.StuckDetected{
		TransferID:       transfer.TransferID("trf-2"),
		State:            transfer.StateInTransit,
		AgeSeconds:       90000,
		ThresholdSeconds: 259200,
		OccurredAt:       analyticsNow,
	})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("messages = %d, want 1", len(msgs))
	}
	m := msgs[0]
	if m.Topic != AnalyticsTopic {
		t.Fatalf("topic = %q", m.Topic)
	}
	env := decodeAnalytics(t, m.Value)
	if env.Type != "com.warehouse.wes.network-inventory-planning.saga.TransferStuckDetected" {
		t.Fatalf("type = %q", env.Type)
	}
	if env.DataSchema != "urn:warehouse:network-inventory-planning:analytics:TransferStuckDetected:v1" {
		t.Fatalf("dataschema = %q", env.DataSchema)
	}
	if len(env.Data) != 4 ||
		env.Data["state"] != "IN_TRANSIT" ||
		env.Data["age_seconds"].(float64) != 90000 ||
		env.Data["threshold_seconds"].(float64) != 259200 {
		t.Fatalf("data = %+v, want exactly {transfer_id, state, age_seconds, threshold_seconds}", env.Data)
	}
}

func TestAnalyticsEncoderEncodesRebalanceRunCompleted(t *testing.T) {
	e := &AnalyticsEncoder{mintID: func() string { return "ce-an-3" }}
	msgs, err := e.Encode(context.Background(), transfer.RebalanceRunCompleted{
		RunID:         "rebal-1-20261007T120000Z",
		ProposalCount: 3,
		RejectedCount: 1,
		StaleFacts:    0,
		CompletedAt:   analyticsNow,
	})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("messages = %d, want 1", len(msgs))
	}
	m := msgs[0]
	if m.Topic != AnalyticsTopic {
		t.Fatalf("topic = %q", m.Topic)
	}
	if string(m.Key) != "rebal-1-20261007T120000Z" {
		t.Fatalf("key = %q, want the run id", m.Key)
	}
	env := decodeAnalytics(t, m.Value)
	if env.Type != "com.warehouse.wes.network-inventory-planning.saga.RebalanceRunCompleted" {
		t.Fatalf("type = %q", env.Type)
	}
	if env.DataSchema != "urn:warehouse:network-inventory-planning:analytics:RebalanceRunCompleted:v1" {
		t.Fatalf("dataschema = %q", env.DataSchema)
	}
	if env.Subject != "rebal-1-20261007T120000Z" {
		t.Fatalf("subject = %q, want the run id", env.Subject)
	}
	if len(env.Data) != 4 ||
		env.Data["proposal_count"].(float64) != 3 ||
		env.Data["rejected_count"].(float64) != 1 ||
		env.Data["stale_facts"].(float64) != 0 {
		t.Fatalf("data = %+v, want exactly {run_id, proposal_count, rejected_count, stale_facts}", env.Data)
	}
}

func TestAnalyticsEncoderSkipsIntegrationEvents(t *testing.T) {
	e := NewAnalyticsEncoder()
	// Integration contract events are NOT analytics occurrences.
	msgs, err := e.Encode(context.Background(), transfer.PlanApproved{
		TransferID:        transfer.TransferID("trf-1"),
		OriginSiteID:      "WH1",
		DestinationSiteID: "WH2",
		SKU:               "SKU-1",
		Quantity:          10,
		PolicyVersion:     "v1",
		OccurredAt:        analyticsNow,
	})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if len(msgs) != 0 {
		t.Fatalf("messages = %d, want 0 (integration event skipped by the analytics encoder)", len(msgs))
	}
}

// TestAnalyticsEncoderInjectsTraceparent proves the analytics occurrences
// carry the same W3C propagation as the integration events (ADR 0007).
// PRESENCE only — the value is run-specific.
func TestAnalyticsEncoderInjectsTraceparent(t *testing.T) {
	installPropagator(t)
	e := NewAnalyticsEncoder()
	msgs, err := e.Encode(spanContextOn(context.Background()), transfer.StuckDetected{
		TransferID:       transfer.TransferID("trf-9"),
		State:            transfer.StatePicked,
		AgeSeconds:       100000,
		ThresholdSeconds: 86400,
		OccurredAt:       analyticsNow,
	})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if len(msgs) != 1 || !hasHeader(msgs[0].Headers, "traceparent") {
		t.Fatalf("headers = %+v, want a traceparent on the analytics occurrence", msgs)
	}
}
