//go:build integration

// End-to-end proof over REAL infrastructure (testcontainers Postgres +
// Kafka, the repo's own recipe — no pgtest/kafkatest helpers exist in this
// repo): the REAL run() boots over a MIGRATED database, the six consumers
// ingest fleet-sibling CloudEvents into the read models, the documented REST
// surface drives the transfer saga (proposals -> approval -> allocation
// reply -> cancel), every state is asserted through REST reads, and the
// documented CloudEvents are read back off the documented topics, bounded by
// deadlines.
//
// Every assertion traces to the docs: the REST contracts in
// docs/docs/api-reference/rest/*.mdx, the use-case invariants and outbox
// events in docs/docs/ddd/use-cases.md, the saga in ADR 0003, the
// CloudEvents catalogue in ADR 0004, work-demand release in ADR 0005, the
// analytics fan-out in ADR 0007, and the cancel compensation in ADR
// 0022/0023. Fixture events are produced the way this repo's own
// internal/adapters/inbound/kafka/kafka_integration tests do.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"

	inboundkafka "github.com/claudioed/network-inventory-planning/internal/adapters/inbound/kafka"
	"github.com/claudioed/network-inventory-planning/internal/adapters/kafka/cloudevents"
	outboundkafka "github.com/claudioed/network-inventory-planning/internal/adapters/outbound/kafka"
	"github.com/claudioed/network-inventory-planning/internal/adapters/outbound/postgres"
)

// The fleet fitness function TestKafkaIntegrationTestsUseTestcontainers
// requires every Kafka-touching integration test file to run its broker via
// testcontainers-go/modules/kafka (the shared itKafka in the boot file does).
var _ = tckafka.Run

// TestTransferLifecycleEndToEndOverRealInfrastructure walks the documented
// happy path AND the documented cancel compensation of the shipped binary:
// read-model ingestion (consumers) -> simulation -> advisory proposal
// generation -> approval (saga + outbox) -> allocation reply -> cancel after
// allocation (RELEASING -> CANCELLED), with Kafka read-backs at each step.
func TestTransferLifecycleEndToEndOverRealInfrastructure(t *testing.T) {
	dsn := itPostgres(t)
	brokers := itKafka(t)
	// A migrated database, exactly like the repo's own integration suites
	// prepare one; the binary's boot-time migration pass is idempotent on
	// top of it.
	if err := postgres.RunMigrations(dsn, itMigrationsDir(t)); err != nil {
		t.Fatalf("run migrations: %v", err)
	}
	for _, topic := range itAllTopics() {
		itEnsureTopic(t, brokers, topic)
	}

	// The three planning facts as the fleet siblings emit them (the exact
	// fixture shapes of the repo's own planning read-models suite): WH1
	// (origin-capable, capacity 500, demand 40) and WH2 (destination,
	// demand 10 due inside the plan window).
	occurred := time.Now().UTC().Truncate(time.Microsecond)
	windowStart := occurred.Add(time.Hour).Truncate(time.Microsecond)
	windowEnd := occurred.Add(8 * time.Hour).Truncate(time.Microsecond)
	dueAt := occurred.Add(2 * time.Hour).Truncate(time.Microsecond)
	itProduceCloudEvent(t, brokers, inboundkafka.FacilityTopic, "WH1", "com.warehouse.wms.facility-layout.site.SiteCapabilityChanged", occurred,
		map[string]any{"site_code": "WH1", "transfer_origin_enabled": true, "transfer_destination_enabled": true, "capability_revision": 4})
	itProduceCloudEvent(t, brokers, inboundkafka.FacilityTopic, "WH2", "com.warehouse.wms.facility-layout.site.SiteCapabilityChanged", occurred,
		map[string]any{"site_code": "WH2", "transfer_origin_enabled": true, "transfer_destination_enabled": true, "capability_revision": 2})
	itProduceCloudEvent(t, brokers, inboundkafka.OrderTopic, "ord-e2e-1/line/1", "com.warehouse.wes.order-management.siteskudemand.SiteSkuDemandChanged", occurred,
		map[string]any{"source_order_id": "ord-e2e-1", "line_no": 1, "site_id": "WH1", "sku": "SKU-1", "demanded_units": 40,
			"due_at": dueAt.Format(time.RFC3339), "state": "ACTIVE", "assignment_version": "static-site-v1"})
	itProduceCloudEvent(t, brokers, inboundkafka.OrderTopic, "ord-e2e-2/line/1", "com.warehouse.wes.order-management.siteskudemand.SiteSkuDemandChanged", occurred,
		map[string]any{"source_order_id": "ord-e2e-2", "line_no": 1, "site_id": "WH2", "sku": "SKU-1", "demanded_units": 10,
			"due_at": dueAt.Format(time.RFC3339), "state": "ACTIVE", "assignment_version": "static-site-v1"})
	itProduceCloudEvent(t, brokers, inboundkafka.PlanningTopic, "plan-e2e-1", "com.warehouse.wes.warehouse-planning.capacityplan.CapacityPlanPublished", occurred,
		map[string]any{"plan_id": "plan-e2e-1", "site_id": "WH1", "warehouse_id": "WH-1", "location": "PATH-ZONE-A", "path_id": "pick-rebin-pack",
			"window_start": windowStart.Format(time.RFC3339), "window_end": windowEnd.Format(time.RFC3339),
			"assigned_demand": 100, "path_capacity": 125, "capacity_over_window": 500, "shortage": 0,
			"bottleneck_step": "REBIN", "published_at": occurred.Format(time.RFC3339)})
	itProduceCloudEvent(t, brokers, inboundkafka.PlanningTopic, "plan-e2e-2", "com.warehouse.wes.warehouse-planning.capacityplan.CapacityPlanPublished", occurred,
		map[string]any{"plan_id": "plan-e2e-2", "site_id": "WH2", "warehouse_id": "WH-2", "location": "PATH-ZONE-B", "path_id": "pick-rebin-pack",
			"window_start": windowStart.Format(time.RFC3339), "window_end": windowEnd.Format(time.RFC3339),
			"assigned_demand": 10, "path_capacity": 125, "capacity_over_window": 300, "shortage": 0,
			"bottleneck_step": "REBIN", "published_at": occurred.Format(time.RFC3339)})

	s := itBootService(t, itServiceEnv(dsn, brokers))

	// 1. The consumers ingested the facts and the fail-closed snapshot
	//    builds: the simulation answers 200 with BOTH sites (a stale or
	//    missing fact excludes the site; no site at all answers 503 —
	//    simulate-transfer-options.mdx, use-cases.md).
	var sim map[string]any
	waitUntil(t, "the simulation to answer 200 over the ingested read models", func() bool {
		r := s.call(t, http.MethodGet, "/v1/transfer-simulations", "", "")
		if r.status != http.StatusOK {
			return false
		}
		sim = r.json(t)
		return len(sim["sites"].([]any)) == 2
	})
	site := func(name string) map[string]any {
		t.Helper()
		for _, raw := range sim["sites"].([]any) {
			m := raw.(map[string]any)
			if m["site"] == name {
				return m
			}
		}
		t.Fatalf("simulation has no site %s: %v", name, sim)
		return nil
	}
	if got := site("WH1"); got["totalDemand"] != float64(40) || got["capacityOverWindow"] != float64(500) || got["originEnabled"] != true {
		t.Fatalf("WH1 advisory view = %v, want demand 40 over capacity 500, origin enabled", got)
	}

	// 2. The advisory proposal run over an explicit snapshot: deterministic
	//    planner invariants (use-cases.md): destination deficit caps the
	//    quantity (60 - 5 = 55), the origin keeps its safety stock, and a
	//    proposal survives only with a positive score. Idempotent per the
	//    fleet key contract (generate-transfer-proposals.mdx).
	asOf := occurred.Format(time.RFC3339)
	genBody := fmt.Sprintf(`{"asOf":%q,"positions":[
		{"Site":"WH1","SKU":"SKU-1","Available":100,"AsOf":%q},
		{"Site":"WH2","SKU":"SKU-1","Available":5,"AsOf":%q}],
		"policies":[
		{"Version":"p-e2e","Site":"WH1","SKU":"SKU-1","SafetyStock":10,"TargetStock":20,"UnitPriority":7},
		{"Version":"p-e2e","Site":"WH2","SKU":"SKU-1","SafetyStock":10,"TargetStock":60,"UnitPriority":7}],
		"lanes":[{"Origin":"WH1","Destination":"WH2","Enabled":true,"LeadTime":86400000000000,"UnitHandlingCost":1}]}`, asOf, asOf, asOf)
	gen := s.call(t, http.MethodPost, "/v1/transfer-proposal-runs", "e2e-gen-1", genBody)
	if gen.status != http.StatusOK {
		t.Fatalf("proposal run = %d %s, want 200", gen.status, gen.body)
	}
	proposals := gen.json(t)["proposals"].([]any)
	if len(proposals) != 1 {
		t.Fatalf("proposals = %v, want exactly the WH1->WH2 one", proposals)
	}
	p := proposals[0].(map[string]any)
	if p["Origin"] != "WH1" || p["Destination"] != "WH2" || p["Quantity"] != float64(55) || p["PolicyVersion"] != "p-e2e" {
		t.Fatalf("proposal = %v, want WH1->WH2 SKU-1 qty 55 (destination deficit 60-5, origin keeps safety stock)", p)
	}
	reasons := map[string]bool{}
	for _, r := range p["Reasons"].([]any) {
		reasons[r.(string)] = true
	}
	if !reasons["DESTINATION_BELOW_TARGET"] || !reasons["ORIGIN_ABOVE_SAFETY_STOCK"] || !reasons["APPROVED_LANE"] {
		t.Fatalf("proposal reasons = %v, want all three documented codes", reasons)
	}
	if _, ok := p["ScoreBreakdown"].(map[string]any); !ok {
		t.Fatalf("proposal has no score breakdown: %v", p)
	}
	if replay := s.call(t, http.MethodPost, "/v1/transfer-proposal-runs", "e2e-gen-1", genBody); replay.header.Get("Idempotency-Replayed") != "true" {
		t.Fatalf("replayed proposal run header = %q, want Idempotency-Replayed: true", replay.header.Get("Idempotency-Replayed"))
	}

	// 3. Approval starts the saga: 200 with state ALLOCATING and the v1
	//    transfer line id (approve-transfer.mdx). A replay of the same key
	//    answers the ORIGINAL transfer.
	approveBody := fmt.Sprintf(`{"originSiteId":"WH1","destinationSiteId":"WH2","sku":"SKU-1","quantity":5,
		"policyVersion":"policy-v3","operatorReason":"e2e rebalance","proposalAsOf":%q}`,
		occurred.Add(-5*time.Minute).Format(time.RFC3339))
	approved := s.call(t, http.MethodPost, "/v1/transfer-approvals", "e2e-appr-1", approveBody)
	if approved.status != http.StatusOK {
		t.Fatalf("approve = %d %s, want 200", approved.status, approved.body)
	}
	ap := approved.json(t)
	transferID := ap["transferId"].(string)
	lineID := transferID + ":1"
	if ap["state"] != "ALLOCATING" || ap["transferLineId"] != lineID || ap["expiresAt"] == "" {
		t.Fatalf("approval = %v, want ALLOCATING with line %s and an expiry", ap, lineID)
	}
	if replay := s.call(t, http.MethodPost, "/v1/transfer-approvals", "e2e-appr-1", approveBody); replay.json(t)["transferId"] != transferID || replay.header.Get("Idempotency-Replayed") != "true" {
		t.Fatalf("replayed approval = %d %s (header %q), want the same transfer replayed", replay.status, replay.body, replay.header.Get("Idempotency-Replayed"))
	}

	// 4. The outbox relay drained the approval's integration CloudEvents onto
	//    the documented topic: TransferPlanApproved + the
	//    TransferAllocationRequested command keyed by the transfer line
	//    (use-cases.md, approve-transfer.mdx). The four TransferStateAdvanced
	//    occurrences of the approval ride the ANALYTICS encoder instead
	//    (ADR 0007's fan-out), asserted below on their own topic.
	type envelope struct {
		Specversion string         `json:"specversion"`
		Type        string         `json:"type"`
		Subject     string         `json:"subject"`
		Data        map[string]any `json:"data"`
	}
	var approvedEvt, requestedEvt, pickDemand *envelope
	itFetchUntil(t, brokers, outboundkafka.TransferTopic, "the approval's CloudEvents on the events topic", func(msgs []kafkago.Message) bool {
		approvedEvt, requestedEvt = nil, nil
		for _, m := range msgs {
			var e envelope
			if err := json.Unmarshal(m.Value, &e); err != nil {
				t.Fatalf("decode %s: %v", m.Key, err)
			}
			if e.Specversion != "1.0" {
				t.Fatalf("event %s is not CloudEvents 1.0: %s", e.Type, m.Value)
			}
			switch {
			case e.Type == "com.warehouse.wes.network-inventory-planning.transfer.TransferPlanApproved":
				approvedEvt = &e
			case e.Type == "com.warehouse.wes.network-inventory-planning.transfer.TransferAllocationRequested":
				requestedEvt = &e
				if string(m.Key) != lineID {
					t.Fatalf("command key = %q, want the transfer line %q (AGENTS.md: Kafka key = aggregate id)", m.Key, lineID)
				}
			}
		}
		return approvedEvt != nil && requestedEvt != nil
	})
	if approvedEvt.Data["transfer_id"] != transferID {
		t.Fatalf("TransferPlanApproved payload = %v", approvedEvt.Data)
	}
	if requestedEvt.Data["origin_site_id"] != "WH1" || requestedEvt.Data["sku"] != "SKU-1" || requestedEvt.Data["quantity"] != float64(5) {
		t.Fatalf("TransferAllocationRequested payload = %v", requestedEvt.Data)
	}
	advanced := 0
	itFetchUntil(t, brokers, outboundkafka.AnalyticsTopic, "the approval's TransferStateAdvanced occurrences (ADR 0007)", func(msgs []kafkago.Message) bool {
		advanced = 0
		for _, m := range msgs {
			var e envelope
			if err := json.Unmarshal(m.Value, &e); err != nil {
				continue
			}
			if e.Type == "com.warehouse.wes.network-inventory-planning.saga.TransferStateAdvanced" && e.Subject == transferID {
				advanced++
			}
		}
		return advanced >= 4 // ->DRAFT, ->PROPOSED, ->APPROVED, ->ALLOCATING (use-cases.md)
	})

	// 5. inventory-storage's reply (the repo's own synthetic wire shape)
	//    drives the saga to ALLOCATED: the REST read side shows the
	//    reservation, and the pick WorkDemandReleased reached the topic
	//    (use-cases.md, ADR 0005).
	itProduceRaw(t, brokers, inboundkafka.InventoryTopic, lineID, itSyntheticAllocationReply(t, transferID, time.Now().UTC()))
	var detail map[string]any
	waitUntil(t, "the transfer to reach ALLOCATED through the reply consumer", func() bool {
		r := s.call(t, http.MethodGet, "/v1/transfers/"+transferID, "", "")
		if r.status != http.StatusOK {
			return false
		}
		detail = r.json(t)
		return detail["state"] == "ALLOCATED"
	})
	if detail["reservationId"] != "res-e2e-1" {
		t.Fatalf("ALLOCATED transfer = %v, want reservation res-e2e-1", detail)
	}
	if len(detail["audit"].([]any)) < 5 {
		t.Fatalf("audit = %v, want the full trail DRAFT->...->ALLOCATED", detail["audit"])
	}
	itFetchUntil(t, brokers, outboundkafka.TransferTopic, "the pick WorkDemandReleased (ADR 0005)", func(msgs []kafkago.Message) bool {
		for _, m := range msgs {
			var e envelope
			if err := json.Unmarshal(m.Value, &e); err != nil {
				continue
			}
			if e.Type == "com.warehouse.wes.network-inventory-planning.workdemand.WorkDemandReleased" && e.Subject == transferID+":pick" {
				pickDemand = &e
			}
		}
		return pickDemand != nil
	})

	// 6. Cancel AFTER allocation (ADR 0022): 202 RELEASING — the hold is
	//    released asynchronously; inventory-storage's released fact settles
	//    CANCELLED and the transfer holds no reservation anymore (ADR 0023).
	cancelled := s.call(t, http.MethodPost, "/v1/transfers/"+transferID+"/cancellation", "e2e-cancel-1", `{"reason":"wrong lane selected"}`)
	if cancelled.status != http.StatusAccepted || cancelled.json(t)["state"] != "RELEASING" {
		t.Fatalf("cancel an ALLOCATED transfer = %d %s, want 202 RELEASING", cancelled.status, cancelled.body)
	}
	itProduceRaw(t, brokers, inboundkafka.InventoryTopic, lineID, itSyntheticAllocationReleased(t, transferID, time.Now().UTC()))
	waitUntil(t, "the released fact to settle the cancel to CANCELLED", func() bool {
		r := s.call(t, http.MethodGet, "/v1/transfers/"+transferID, "", "")
		if r.status != http.StatusOK {
			return false
		}
		detail = r.json(t)
		return detail["state"] == "CANCELLED"
	})
	if detail["reservationId"] != nil {
		t.Fatalf("a CANCELLED transfer must hold no reservation (ADR 0023), got %v", detail["reservationId"])
	}
	itFetchUntil(t, brokers, outboundkafka.TransferTopic, "the TransferAllocationReleaseRequested command (ADR 0022)", func(msgs []kafkago.Message) bool {
		for _, m := range msgs {
			var e envelope
			if err := json.Unmarshal(m.Value, &e); err != nil {
				continue
			}
			if e.Type == "com.warehouse.wes.network-inventory-planning.transfer.TransferAllocationReleaseRequested" && e.Data["transfer_id"] == transferID {
				return true
			}
		}
		return false
	})

	// 7. The read side: the list answers the settled transfer (newest
	//    first, list-transfers.mdx) and an unknown id is the documented
	//    404 problem+json (get-transfer.mdx).
	listed := s.call(t, http.MethodGet, "/v1/transfers?state=CANCELLED", "", "")
	if listed.status != http.StatusOK || !strings.Contains(listed.body, transferID) {
		t.Fatalf("list CANCELLED transfers = %d %s, want the settled %s", listed.status, listed.body, transferID)
	}
	if missing := s.call(t, http.MethodGet, "/v1/transfers/nope", "", ""); missing.status != http.StatusNotFound || !strings.Contains(missing.body, "\"status\":404") {
		t.Fatalf("unknown transfer = %d %s, want a 404 problem+json", missing.status, missing.body)
	}

	// 8. The whole binary drains cleanly after the full lifecycle (ADR 0019;
	//    see itAssertCleanDrain for the one known, reported deviation).
	itAssertCleanDrain(t, s.stop())
}

// itProduceCloudEvent publishes one producer-shaped CloudEvent onto topic
// (the recipe of the repo's own kafka_integration harness: the shared
// encoder mints the envelope, the type is replaced with the producer's
// exact full string).
func itProduceCloudEvent(t *testing.T, brokers []string, topic, key, fullType string, occurredAt time.Time, data any) {
	t.Helper()
	value, err := cloudevents.New(cloudevents.Spec{
		ID: fmt.Sprintf("e2e-%s-%d", key, time.Now().UnixNano()), Entity: "entity", EventName: "Synthetic",
		Subject: key, Time: occurredAt, Stream: cloudevents.StreamEvents, Version: 1, Data: data,
	})
	if err != nil {
		t.Fatalf("encode event: %v", err)
	}
	value = []byte(strings.ReplaceAll(string(value),
		`"com.warehouse.wes.network-inventory-planning.entity.Synthetic"`, `"`+fullType+`"`))
	itProduceRaw(t, brokers, topic, key, value)
}

// itProduceRaw publishes a raw message with the content-type header the
// real producers set.
func itProduceRaw(t *testing.T, brokers []string, topic, key string, value []byte) {
	t.Helper()
	writer := kafkago.NewWriter(kafkago.WriterConfig{Brokers: brokers, Topic: topic, Balancer: &kafkago.Hash{}})
	defer func() { _ = writer.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	msg := kafkago.Message{Key: []byte(key), Value: value, Headers: []kafkago.Header{cloudevents.ContentTypeHeader()}}
	if err := writer.WriteMessages(ctx, msg); err != nil {
		t.Fatalf("produce message: %v", err)
	}
}

// itFetchUntil reads topic with a FRESH consumer group (replays the whole
// topic from the beginning) until need is satisfied, bounded by a deadline.
func itFetchUntil(t *testing.T, brokers []string, topic, what string, need func([]kafkago.Message) bool) {
	t.Helper()
	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers: brokers, Topic: topic, GroupID: itUniqueGroup("e2e-read"),
		MinBytes: 1, MaxBytes: 10e6,
	})
	defer func() { _ = reader.Close() }()
	var msgs []kafkago.Message
	for deadline := time.Now().Add(90 * time.Second); time.Now().Before(deadline); time.Sleep(200 * time.Millisecond) {
		fetchCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		for {
			msg, err := reader.FetchMessage(fetchCtx)
			if err != nil {
				break
			}
			msgs = append(msgs, msg)
		}
		cancel()
		if need(msgs) {
			return
		}
	}
	t.Fatalf("timed out waiting for %s (%d messages seen)", what, len(msgs))
}

// itSyntheticAllocationReply builds inventory-storage's
// TransferStockAllocated wire shape, exactly as the repo's own saga suite
// mirrors it from the asyncapi contract.
func itSyntheticAllocationReply(t *testing.T, transferID string, occurredAt time.Time) []byte {
	t.Helper()
	value, err := json.Marshal(map[string]any{
		"specversion": "1.0", "id": "e2e-reply-" + transferID, "source": "/warehouse/inventory-storage",
		"type": "com.warehouse.wms.inventory-storage.reservation.TransferStockAllocated", "subject": "res-e2e-1",
		"time": occurredAt.UTC().Format(time.RFC3339Nano), "datacontenttype": "application/json",
		"dataschema": "urn:warehouse:inventory-storage:events:TransferStockAllocated:v1",
		"data": map[string]any{
			"transfer_id": transferID, "transfer_line_id": transferID + ":1", "origin_site_id": "WH1",
			"reservation_id": "res-e2e-1", "sku": "SKU-1", "quantity": 5,
			"allocations": []map[string]any{{"stock_unit_id": "su-e2e-1", "bin_id": "BIN-E2E", "quantity": 5}},
			"expires_at":  occurredAt.Add(24 * time.Hour).UTC().Format(time.RFC3339),
		},
	})
	if err != nil {
		t.Fatalf("marshal reply: %v", err)
	}
	return value
}

// itSyntheticAllocationReleased builds inventory-storage's
// TransferStockAllocationReleased wire shape (ADR 0022) that settles a
// RELEASING cancel to CANCELLED.
func itSyntheticAllocationReleased(t *testing.T, transferID string, occurredAt time.Time) []byte {
	t.Helper()
	value, err := json.Marshal(map[string]any{
		"specversion": "1.0", "id": "e2e-released-" + transferID, "source": "/warehouse/inventory-storage",
		"type": "com.warehouse.wms.inventory-storage.reservation.TransferStockAllocationReleased", "subject": transferID + ":1",
		"time": occurredAt.UTC().Format(time.RFC3339Nano), "datacontenttype": "application/json",
		"dataschema": "urn:warehouse:inventory-storage:events:TransferStockAllocationReleased:v1",
		"data": map[string]any{
			"transfer_id": transferID, "transfer_line_id": transferID + ":1", "reservation_id": "res-e2e-1",
			"origin_site_id": "WH1", "sku": "SKU-1", "quantity": 5,
			"outcome": "RELEASED", "reason": "revoked for cancelled transfer",
		},
	})
	if err != nil {
		t.Fatalf("marshal released fact: %v", err)
	}
	return value
}

// httpReply is one REST exchange with the booted binary.
type httpReply struct {
	status int
	body   string
	header http.Header
}

func (r httpReply) json(t *testing.T) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal([]byte(r.body), &m); err != nil {
		t.Fatalf("not JSON: %s", r.body)
	}
	return m
}

// call drives the booted binary's documented REST surface only.
func (s *service) call(t *testing.T, method, path, key, body string) httpReply {
	t.Helper()
	req, err := http.NewRequest(method, s.base+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	return httpReply{status: resp.StatusCode, body: string(b), header: resp.Header}
}
