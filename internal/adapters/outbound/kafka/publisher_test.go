package kafka

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

var encNow = time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)

func TestTransferEncoderEncodesPlanApproved(t *testing.T) {
	e := &TransferEncoder{mintID: func() string { return "ce-id-1" }}
	msgs, err := e.Encode(context.Background(), transfer.PlanApproved{
		TransferID:        transfer.TransferID("trf-1"),
		OriginSiteID:      "WH1",
		DestinationSiteID: "WH2",
		SKU:               "SKU-1",
		Quantity:          10,
		PolicyVersion:     "policy-v3",
		OperatorReason:    "rebalance",
		ProposalAsOf:      encNow.Add(-5 * time.Minute),
		OccurredAt:        encNow,
	})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("messages = %d, want 1", len(msgs))
	}
	m := msgs[0]
	if m.Topic != TransferTopic {
		t.Fatalf("topic = %q", m.Topic)
	}
	if string(m.Key) != "trf-1" {
		t.Fatalf("key = %q, want the transfer id", m.Key)
	}
	if m.EventType != "com.warehouse.wes.network-inventory-planning.transfer.TransferPlanApproved" {
		t.Fatalf("event type = %q", m.EventType)
	}

	var envelope struct {
		SpecVersion string         `json:"specversion"`
		ID          string         `json:"id"`
		Source      string         `json:"source"`
		Type        string         `json:"type"`
		Subject     string         `json:"subject"`
		Time        string         `json:"time"`
		DataSchema  string         `json:"dataschema"`
		Data        map[string]any `json:"data"`
	}
	if err := json.Unmarshal(m.Value, &envelope); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}
	assertPlanApprovedEnvelope(t, envelope, m.EventType)
	if len(m.Headers) == 0 || m.Headers[0].Key != "content-type" || !strings.Contains(string(m.Headers[0].Value), "application/cloudevents+json") {
		t.Fatalf("headers = %+v", m.Headers)
	}
}

func assertPlanApprovedEnvelope(t *testing.T, envelope struct {
	SpecVersion string         `json:"specversion"`
	ID          string         `json:"id"`
	Source      string         `json:"source"`
	Type        string         `json:"type"`
	Subject     string         `json:"subject"`
	Time        string         `json:"time"`
	DataSchema  string         `json:"dataschema"`
	Data        map[string]any `json:"data"`
}, wantType string) {
	t.Helper()
	if envelope.SpecVersion != "1.0" || envelope.ID != "ce-id-1" {
		t.Fatalf("envelope = %+v", envelope)
	}
	if envelope.Source != "/warehouse/network-inventory-planning" {
		t.Fatalf("source = %q", envelope.Source)
	}
	if envelope.Type != wantType {
		t.Fatalf("envelope type = %q, want %q", envelope.Type, wantType)
	}
	if envelope.Subject != "trf-1" {
		t.Fatalf("subject = %q", envelope.Subject)
	}
	if envelope.DataSchema != "urn:warehouse:network-inventory-planning:events:TransferPlanApproved:v1" {
		t.Fatalf("dataschema = %q", envelope.DataSchema)
	}
	if envelope.Data["transfer_id"] != "trf-1" || envelope.Data["quantity"].(float64) != 10 {
		t.Fatalf("data = %+v", envelope.Data)
	}
	if envelope.Data["destination_site_id"] != "WH2" || envelope.Data["policy_version"] != "policy-v3" {
		t.Fatalf("data = %+v", envelope.Data)
	}
}

func TestTransferEncoderEncodesAllocationRequested(t *testing.T) {
	e := &TransferEncoder{mintID: func() string { return "ce-id-2" }}
	msgs, err := e.Encode(context.Background(), transfer.AllocationRequested{
		TransferID:     transfer.TransferID("trf-1"),
		TransferLineID: "trf-1:1",
		OriginSiteID:   "WH1",
		SKU:            "SKU-1",
		Quantity:       10,
		OccurredAt:     encNow,
	})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("messages = %d, want 1", len(msgs))
	}
	m := msgs[0]
	if m.Topic != TransferTopic {
		t.Fatalf("topic = %q", m.Topic)
	}
	// inventory-storage keys and subjects the command on transfer_line_id.
	if string(m.Key) != "trf-1:1" {
		t.Fatalf("key = %q, want transfer_line_id", m.Key)
	}
	if m.EventType != "com.warehouse.wes.network-inventory-planning.transfer.TransferAllocationRequested" {
		t.Fatalf("event type = %q", m.EventType)
	}

	var envelope struct {
		Type    string         `json:"type"`
		Subject string         `json:"subject"`
		Data    map[string]any `json:"data"`
	}
	if err := json.Unmarshal(m.Value, &envelope); err != nil {
		t.Fatal(err)
	}
	// EXACTLY inventory-storage's consumed contract: five fields, no more.
	if len(envelope.Data) != 5 {
		t.Fatalf("data fields = %d (%+v), want exactly 5", len(envelope.Data), envelope.Data)
	}
	if envelope.Data["transfer_id"] != "trf-1" ||
		envelope.Data["transfer_line_id"] != "trf-1:1" ||
		envelope.Data["origin_site_id"] != "WH1" ||
		envelope.Data["sku"] != "SKU-1" ||
		envelope.Data["quantity"].(float64) != 10 {
		t.Fatalf("data = %+v", envelope.Data)
	}
	if envelope.Subject != "trf-1:1" {
		t.Fatalf("subject = %q", envelope.Subject)
	}
}

func TestTransferEncoderSkipsUnknownEvent(t *testing.T) {
	e := NewTransferEncoder()
	msgs, err := e.Encode(context.Background(), fakeDomainEvent{})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if len(msgs) != 0 {
		t.Fatalf("messages = %d, want 0 (unknown event skipped)", len(msgs))
	}
}

func TestTransferEncoderEncodesWorkDemandReleased(t *testing.T) {
	e := &TransferEncoder{mintID: func() string { return "ce-id-3" }}
	msgs, err := e.Encode(context.Background(), transfer.DemandReleased{
		DemandID:    "trf-1:pick",
		WorkKind:    transfer.WorkKindTransferPick,
		TransferRef: transfer.TransferID("trf-1"),
		PathID:      "transfer-pick-path",
		SiteID:      "WH1",
		CPT:         encNow.Add(2 * time.Hour),
		SKU:         "SKU-1",
		Quantity:    10,
		OccurredAt:  encNow,
	})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("messages = %d, want 1", len(msgs))
	}
	m := msgs[0]
	if m.Topic != TransferTopic {
		t.Fatalf("topic = %q", m.Topic)
	}
	// WES's consumed contract: type names the workdemand entity, key and
	// subject are the demand_id.
	if m.EventType != "com.warehouse.wes.network-inventory-planning.workdemand.WorkDemandReleased" {
		t.Fatalf("event type = %q", m.EventType)
	}
	if string(m.Key) != "trf-1:pick" {
		t.Fatalf("key = %q, want demand_id", m.Key)
	}

	var envelope struct {
		Type       string         `json:"type"`
		Subject    string         `json:"subject"`
		DataSchema string         `json:"dataschema"`
		Data       map[string]any `json:"data"`
	}
	if err := json.Unmarshal(m.Value, &envelope); err != nil {
		t.Fatal(err)
	}
	assertWorkDemandEnvelope(t, envelope)
	assertWorkDemandData(t, envelope.Data)
}

func assertWorkDemandEnvelope(t *testing.T, envelope struct {
	Type       string         `json:"type"`
	Subject    string         `json:"subject"`
	DataSchema string         `json:"dataschema"`
	Data       map[string]any `json:"data"`
}) {
	t.Helper()
	if envelope.Type != "com.warehouse.wes.network-inventory-planning.workdemand.WorkDemandReleased" {
		t.Fatalf("type = %q", envelope.Type)
	}
	if envelope.Subject != "trf-1:pick" {
		t.Fatalf("subject = %q", envelope.Subject)
	}
	if envelope.DataSchema != "urn:warehouse:network-inventory-planning:events:WorkDemandReleased:v1" {
		t.Fatalf("dataschema = %q", envelope.DataSchema)
	}
}

// assertWorkDemandData proves the payload is EXACTLY WES's consumed
// contract: the six required fields plus sku and quantity, no more.
func assertWorkDemandData(t *testing.T, data map[string]any) {
	t.Helper()
	if len(data) != 8 {
		t.Fatalf("data fields = %d (%+v), want exactly 8", len(data), data)
	}
	if data["demand_id"] != "trf-1:pick" ||
		data["work_kind"] != "TRANSFER_PICK" ||
		data["transfer_ref"] != "trf-1" ||
		data["path_id"] != "transfer-pick-path" ||
		data["site_id"] != "WH1" ||
		data["cpt"] != encNow.Add(2*time.Hour).Format(time.RFC3339) ||
		data["sku"] != "SKU-1" ||
		data["quantity"].(float64) != 10 {
		t.Fatalf("data = %+v", data)
	}
}

type fakeDomainEvent struct{}

func (fakeDomainEvent) EventName() string { return "SomethingElse" }
