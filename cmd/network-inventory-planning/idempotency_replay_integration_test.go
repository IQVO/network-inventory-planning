//go:build integration

// Replay-class proof over the REAL router and Postgres (fleet rule,
// .claude/rules/fleet/idempotency-and-outbox.md; ADR 0024): a RETRYABLE
// refusal is answered but NOT stored — the key row is absent, so the retry
// under the same key executes again and, once the state allows it, succeeds
// and is then stored and replayed.
//
// Two real RETRYABLE problem types of the keyed routes drive it:
//
//   - facts-incomplete (422, POST /v1/transfer-approvals): the planning read
//     models are fed by Kafka; before the facts arrive the approval is
//     refused. After the facts arrive, the SAME key with the SAME body must
//     execute (not replay the refusal), answer 200 and then be replayed.
//   - transfer-not-found (404, POST /v1/transfers/{id}/cancellation): the
//     transfer arrives by another request (the approval), so the 404 depends
//     on state that can change; the retry under the same key must execute
//     again (still 404, never a replayed one).
//
// concurrent-modification (409) is RETRYABLE too, but it only answers the
// unkeyed catalogue PUTs, so its class is proven by the replay-class tests
// in internal/adapters/inbound/http (ADR 0024).
package main

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	inboundkafka "github.com/claudioed/network-inventory-planning/internal/adapters/inbound/kafka"
	"github.com/claudioed/network-inventory-planning/internal/adapters/outbound/postgres"
)

// The topics the planning facts arrive on (the boot file's shared constants).
const (
	facilityTopicForFacts = inboundkafka.FacilityTopic
	orderTopicForFacts    = inboundkafka.OrderTopic
	planningTopicForFacts = inboundkafka.PlanningTopic
)

// TestRetryableRefusalsAreNotStoredAndTheRetryExecutes boots the real binary
// over an empty migrated database, refuses under keys whose causes are
// state-dependent, and proves the release: no idempotency_keys row, the
// retry executes, the later success is stored and replayed.
func TestRetryableRefusalsAreNotStoredAndTheRetryExecutes(t *testing.T) {
	dsn := itPostgres(t)
	brokers := itKafka(t)
	if err := postgres.RunMigrations(dsn, itMigrationsDir(t)); err != nil {
		t.Fatalf("run migrations: %v", err)
	}
	for _, topic := range itAllTopics() {
		itEnsureTopic(t, brokers, topic)
	}
	s := itBootService(t, itServiceEnv(dsn, brokers))

	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("connect pool: %v", err)
	}
	t.Cleanup(pool.Close)
	keyRows := func(key string) int {
		t.Helper()
		var n int
		if err := pool.QueryRow(context.Background(),
			`SELECT count(*) FROM idempotency_keys WHERE idempotency_key = $1`, key).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	problemType := func(r httpReply) string {
		t.Helper()
		var p struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal([]byte(r.body), &p); err != nil {
			t.Fatalf("not a problem body: %s", r.body)
		}
		return p.Type
	}

	t.Run("facts-incomplete is released and the retry executes and is stored", func(t *testing.T) {
		const key = "replay-facts-incomplete-1"
		approveBody := `{"originSiteId":"WH1","destinationSiteId":"WH2","sku":"SKU-1","quantity":5,` +
			`"policyVersion":"policy-v3","operatorReason":"replay-class proof",` +
			`"proposalAsOf":"` + time.Now().UTC().Add(-5*time.Minute).Format(time.RFC3339) + `"}`

		// The read models are still empty: the approval is refused with the
		// fail-closed facts-incomplete problem (422).
		refused := s.call(t, http.MethodPost, "/v1/transfer-approvals", key, approveBody)
		if refused.status != http.StatusUnprocessableEntity {
			t.Fatalf("approval over empty read models = %d %s, want 422", refused.status, refused.body)
		}
		if got := problemType(refused); got != "https://warehouse.example/problems/facts-incomplete" {
			t.Fatalf("problem type = %s, want facts-incomplete", got)
		}
		if refused.header.Get("Idempotency-Replayed") != "" {
			t.Fatalf("the first answer carries Idempotency-Replayed: %q", refused.header.Get("Idempotency-Replayed"))
		}
		if got := keyRows(key); got != 0 {
			t.Fatalf("idempotency_keys rows after the facts-incomplete refusal = %d, want 0: a RETRYABLE refusal must not be stored", got)
		}

		// The facts arrive (the same fixture shapes as the e2e suite): the
		// cause of the refusal clears.
		occurred := time.Now().UTC().Truncate(time.Microsecond)
		windowStart := occurred.Add(time.Hour).Truncate(time.Microsecond)
		windowEnd := occurred.Add(8 * time.Hour).Truncate(time.Microsecond)
		dueAt := occurred.Add(2 * time.Hour).Truncate(time.Microsecond)
		itProduceCloudEvent(t, brokers, facilityTopicForFacts, "WH1", "com.warehouse.wms.facility-layout.site.SiteCapabilityChanged", occurred,
			map[string]any{"site_code": "WH1", "transfer_origin_enabled": true, "transfer_destination_enabled": true, "capability_revision": 4})
		itProduceCloudEvent(t, brokers, facilityTopicForFacts, "WH2", "com.warehouse.wms.facility-layout.site.SiteCapabilityChanged", occurred,
			map[string]any{"site_code": "WH2", "transfer_origin_enabled": true, "transfer_destination_enabled": true, "capability_revision": 2})
		itProduceCloudEvent(t, brokers, orderTopicForFacts, "ord-replay-1/line/1", "com.warehouse.wes.order-management.siteskudemand.SiteSkuDemandChanged", occurred,
			map[string]any{"source_order_id": "ord-replay-1", "line_no": 1, "site_id": "WH1", "sku": "SKU-1", "demanded_units": 40,
				"due_at": dueAt.Format(time.RFC3339), "state": "ACTIVE", "assignment_version": "static-site-v1"})
		itProduceCloudEvent(t, brokers, orderTopicForFacts, "ord-replay-2/line/1", "com.warehouse.wes.order-management.siteskudemand.SiteSkuDemandChanged", occurred,
			map[string]any{"source_order_id": "ord-replay-2", "line_no": 1, "site_id": "WH2", "sku": "SKU-1", "demanded_units": 10,
				"due_at": dueAt.Format(time.RFC3339), "state": "ACTIVE", "assignment_version": "static-site-v1"})
		itProduceCloudEvent(t, brokers, planningTopicForFacts, "plan-replay-1", "com.warehouse.wes.warehouse-planning.capacityplan.CapacityPlanPublished", occurred,
			map[string]any{"plan_id": "plan-replay-1", "site_id": "WH1", "warehouse_id": "WH-1", "location": "PATH-ZONE-A", "path_id": "pick-rebin-pack",
				"window_start": windowStart.Format(time.RFC3339), "window_end": windowEnd.Format(time.RFC3339),
				"assigned_demand": 100, "path_capacity": 125, "capacity_over_window": 500, "shortage": 0,
				"bottleneck_step": "REBIN", "published_at": occurred.Format(time.RFC3339)})
		itProduceCloudEvent(t, brokers, planningTopicForFacts, "plan-replay-2", "com.warehouse.wes.warehouse-planning.capacityplan.CapacityPlanPublished", occurred,
			map[string]any{"plan_id": "plan-replay-2", "site_id": "WH2", "warehouse_id": "WH-2", "location": "PATH-ZONE-B", "path_id": "pick-rebin-pack",
				"window_start": windowStart.Format(time.RFC3339), "window_end": windowEnd.Format(time.RFC3339),
				"assigned_demand": 10, "path_capacity": 125, "capacity_over_window": 300, "shortage": 0,
				"bottleneck_step": "REBIN", "published_at": occurred.Format(time.RFC3339)})
		waitUntil(t, "the facts to reach the read models (simulation 200 with both sites)", func() bool {
			r := s.call(t, http.MethodGet, "/v1/transfer-simulations", "", "")
			if r.status != http.StatusOK {
				return false
			}
			var sim struct {
				Sites []map[string]any `json:"sites"`
			}
			return json.Unmarshal([]byte(r.body), &sim) == nil && len(sim.Sites) == 2
		})

		// The retry under the SAME key executes (the refusal was never
		// stored) and succeeds.
		retry := s.call(t, http.MethodPost, "/v1/transfer-approvals", key, approveBody)
		if retry.status != http.StatusOK {
			t.Fatalf("retry after the facts recovered = %d %s, want 200: the key must have been released", retry.status, retry.body)
		}
		if retry.header.Get("Idempotency-Replayed") != "" {
			t.Fatalf("the retry carries Idempotency-Replayed: %q, want none: it must execute", retry.header.Get("Idempotency-Replayed"))
		}
		var approved struct {
			State string `json:"state"`
		}
		if err := json.Unmarshal([]byte(retry.body), &approved); err != nil || approved.State != "ALLOCATING" {
			t.Fatalf("retry body = %s, want an ALLOCATING transfer", retry.body)
		}
		if got := keyRows(key); got != 1 {
			t.Fatalf("idempotency_keys rows after the successful retry = %d, want 1: a 2xx is stored", got)
		}

		// The success is replayed from the store from now on.
		again := s.call(t, http.MethodPost, "/v1/transfer-approvals", key, approveBody)
		if again.status != http.StatusOK || again.header.Get("Idempotency-Replayed") != "true" || again.body != retry.body {
			t.Fatalf("request after the stored success = %d (replayed %q), want the stored 200 replayed byte for byte",
				again.status, again.header.Get("Idempotency-Replayed"))
		}
		if got := keyRows(key); got != 1 {
			t.Fatalf("idempotency_keys rows after the replay = %d, want 1: a replay records nothing new", got)
		}
	})

	t.Run("transfer-not-found is released and the retry executes again", func(t *testing.T) {
		const key = "replay-transfer-not-found-1"
		const path = "/v1/transfers/trf-replay-unknown/cancellation"
		body := `{"reason":"replay-class proof"}`

		first := s.call(t, http.MethodPost, path, key, body)
		if first.status != http.StatusNotFound {
			t.Fatalf("cancel of an unknown transfer = %d %s, want 404", first.status, first.body)
		}
		if got := problemType(first); got != "https://warehouse.example/problems/transfer-not-found" {
			t.Fatalf("problem type = %s, want transfer-not-found", got)
		}
		if first.header.Get("Idempotency-Replayed") != "" {
			t.Fatalf("the first answer carries Idempotency-Replayed: %q", first.header.Get("Idempotency-Replayed"))
		}
		if got := keyRows(key); got != 0 {
			t.Fatalf("idempotency_keys rows after the transfer-not-found refusal = %d, want 0: a RETRYABLE refusal must not be stored", got)
		}

		second := s.call(t, http.MethodPost, path, key, body)
		if second.status != http.StatusNotFound {
			t.Fatalf("retry = %d %s, want 404 again", second.status, second.body)
		}
		if second.header.Get("Idempotency-Replayed") != "" {
			t.Fatalf("the retry carries Idempotency-Replayed: %q, want none: the 404 depends on state and must re-execute", second.header.Get("Idempotency-Replayed"))
		}
		if got := keyRows(key); got != 0 {
			t.Fatalf("idempotency_keys rows after the retry = %d, want 0", got)
		}
	})
}
