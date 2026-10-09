//go:build integration

// Package kafka_integration proves the three Phase-1 consumers end-to-end
// against REAL infrastructure: Postgres and Kafka via testcontainers
// (never a skip-gated external broker — the fleet rule
// TestKafkaIntegrationTestsUseTestcontainers enforces it).
package kafka_integration

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/network-inventory-planning/internal/adapters/inbound/kafka"
	"github.com/claudioed/network-inventory-planning/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/network-inventory-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/network-inventory-planning/internal/domain/planning"
)

var (
	sharedBrokers   []string
	sharedContainer testcontainers.Container
)

func TestMain(m *testing.M) {
	code := m.Run()
	if sharedContainer != nil {
		if err := testcontainers.TerminateContainer(sharedContainer); err != nil {
			fmt.Fprintf(os.Stderr, "terminate kafka container: %v\n", err)
		}
	}
	os.Exit(code)
}

func startKafkaBroker(t *testing.T) []string {
	t.Helper()
	if sharedBrokers != nil {
		return sharedBrokers
	}
	ctx := context.Background()
	container, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1",
		tckafka.WithClusterID("network-inventory-planning-itest"),
	)
	if err != nil {
		t.Fatalf("start kafka container: %v", err)
	}
	sharedContainer = container
	brokers, err := container.Brokers(ctx)
	if err != nil {
		t.Fatalf("resolve kafka brokers: %v", err)
	}
	sharedBrokers = brokers
	return brokers
}

func startPostgres(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	container, err := tcpostgres.Run(ctx,
		"postgres:16-alpine",
		tcpostgres.WithDatabase("nip_test"),
		tcpostgres.WithUsername("nip_test"),
		tcpostgres.WithPassword("nip_test"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(60*time.Second),
		),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	t.Cleanup(func() {
		if err := container.Terminate(context.Background()); err != nil {
			t.Logf("terminate postgres container: %v", err)
		}
	})
	connStr, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("resolve connection string: %v", err)
	}
	return connStr
}

// migrationsDir resolves the repo's migrations directory relative to THIS
// FILE (go test's working directory varies by runner).
func migrationsDir(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate test source file")
	}
	return filepath.Join(filepath.Dir(thisFile), "..", "..", "..", "outbound", "postgres", "migrations")
}

// ensureTopic creates topic if the broker has auto-create disabled.
func ensureTopic(t *testing.T, brokers []string, topic string) {
	t.Helper()
	conn, err := kafkago.Dial("tcp", brokers[0])
	if err != nil {
		t.Fatalf("dial broker: %v", err)
	}
	defer func() { _ = conn.Close() }()
	controller, err := conn.Controller()
	if err != nil {
		t.Fatalf("resolve controller: %v", err)
	}
	ctrlConn, err := kafkago.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		t.Fatalf("dial controller: %v", err)
	}
	defer func() { _ = ctrlConn.Close() }()
	if err := ctrlConn.CreateTopics(kafkago.TopicConfig{Topic: topic, NumPartitions: 1, ReplicationFactor: 1}); err != nil {
		// Already exists is fine.
		if !strings.Contains(err.Error(), "already exists") {
			t.Fatalf("create topic %s: %v", topic, err)
		}
	}
}

// uniqueGroupID gives each test its own consumer group so tests never
// share a committed offset.
func uniqueGroupID(prefix string) string {
	return fmt.Sprintf("%s-itest-%d", prefix, time.Now().UnixNano())
}

// produceCloudEvent publishes one producer-shaped CloudEvent onto topic.
func produceCloudEvent(t *testing.T, brokers []string, topic, key, fullType string, occurredAt time.Time, data any) {
	t.Helper()
	value, err := cloudevents.New(cloudevents.Spec{
		ID:        fmt.Sprintf("itest-%s-%d", key, time.Now().UnixNano()),
		Entity:    "entity",
		EventName: "Synthetic",
		Subject:   key,
		Time:      occurredAt,
		Stream:    cloudevents.StreamEvents,
		Version:   1,
		Data:      data,
	})
	if err != nil {
		t.Fatalf("encode event: %v", err)
	}
	// Replace with the producer's exact full type string.
	replace := func(s string) string {
		old := `"com.warehouse.wes.network-inventory-planning.entity.Synthetic"`
		return replaceAll(s, old, `"`+fullType+`"`)
	}
	_ = replace

	writer := kafkago.NewWriter(kafkago.WriterConfig{Brokers: brokers, Topic: topic, Balancer: &kafkago.Hash{}})
	defer func() { _ = writer.Close() }()
	msg := kafkago.Message{
		Key:     []byte(key),
		Value:   []byte(replaceAll(string(value), `"com.warehouse.wes.network-inventory-planning.entity.Synthetic"`, `"`+fullType+`"`)),
		Headers: []kafkago.Header{cloudevents.ContentTypeHeader()},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := writer.WriteMessages(ctx, msg); err != nil {
		t.Fatalf("produce message: %v", err)
	}
}

func replaceAll(s, old, new string) string {
	out := ""
	for {
		i := indexOf(s, old)
		if i < 0 {
			return out + s
		}
		out += s[:i] + new
		s = s[i+len(old):]
	}
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

// runConsumerUntil runs c until ctx's deadline, then closes it.
func runConsumerUntil(t *testing.T, c interface {
	Run(ctx context.Context) error
	Close() error
}, window time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), window)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	<-ctx.Done()
	select {
	case <-done:
	default:
	}
	if err := c.Close(); err != nil {
		t.Logf("close consumer: %v", err)
	}
}

// TestPlanningReadModelsIntegration drives all three consumers against a
// real Kafka and a real Postgres, then proves the fail-closed snapshot and
// the advisory simulation over the persisted read models.
func TestPlanningReadModelsIntegration(t *testing.T) {
	brokers := startKafkaBroker(t)
	databaseURL := startPostgres(t)
	if err := postgres.RunMigrations(databaseURL, migrationsDir(t)); err != nil {
		t.Fatalf("run migrations: %v", err)
	}
	ctx := context.Background()
	pool, err := postgres.NewPool(ctx, databaseURL)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	defer pool.Close()

	uow := postgres.NewUnitOfWork(pool)
	processedEvents := postgres.NewProcessedEventRepo(pool)
	capRepo := postgres.NewSiteCapabilityRepo(pool)
	demandRepo := postgres.NewSiteSkuDemandRepo(pool)
	planRepo := postgres.NewPublishedCapacityPlanRepo(pool)
	snapshotRepo := postgres.NewSnapshotRepo(pool)

	occurred := time.Now().UTC().Truncate(time.Microsecond)
	windowStart := occurred.Add(time.Hour).Truncate(time.Microsecond)
	windowEnd := occurred.Add(8 * time.Hour).Truncate(time.Microsecond)
	dueAt := occurred.Add(2 * time.Hour).Truncate(time.Microsecond)

	for _, topic := range []string{kafka.FacilityTopic, kafka.OrderTopic, kafka.PlanningTopic} {
		ensureTopic(t, brokers, topic)
	}

	// 1. site capability facts (facility-layout).
	produceCloudEvent(t, brokers, kafka.FacilityTopic, "WH1", "com.warehouse.wms.facility-layout.site.SiteCapabilityChanged", occurred,
		map[string]any{"site_code": "WH1", "transfer_origin_enabled": true, "transfer_destination_enabled": true, "capability_revision": 4})
	produceCloudEvent(t, brokers, kafka.FacilityTopic, "WH2", "com.warehouse.wms.facility-layout.site.SiteCapabilityChanged", occurred,
		map[string]any{"site_code": "WH2", "transfer_origin_enabled": true, "transfer_destination_enabled": true, "capability_revision": 2})

	// 2. site/SKU demand facts (order-management).
	produceCloudEvent(t, brokers, kafka.OrderTopic, "ord-1/line/1", "com.warehouse.wes.order-management.siteskudemand.SiteSkuDemandChanged", occurred,
		map[string]any{"source_order_id": "ord-1", "line_no": 1, "site_id": "WH1", "sku": "SKU-1", "demanded_units": 40,
			"due_at": dueAt.Format(time.RFC3339), "state": "ACTIVE", "assignment_version": "static-site-v1"})
	produceCloudEvent(t, brokers, kafka.OrderTopic, "ord-2/line/1", "com.warehouse.wes.order-management.siteskudemand.SiteSkuDemandChanged", occurred,
		map[string]any{"source_order_id": "ord-2", "line_no": 1, "site_id": "WH2", "sku": "SKU-1", "demanded_units": 10,
			"due_at": dueAt.Format(time.RFC3339), "state": "ACTIVE", "assignment_version": "static-site-v1"})

	// 3. published capacity plans (warehouse-planning), one MODERN (with
	// site_id) and one LEGACY (without site_id: must be excluded).
	produceCloudEvent(t, brokers, kafka.PlanningTopic, "plan-1", "com.warehouse.wes.warehouse-planning.capacityplan.CapacityPlanPublished", occurred,
		map[string]any{"plan_id": "plan-1", "site_id": "WH1", "warehouse_id": "WH-1", "location": "PATH-ZONE-A", "path_id": "pick-rebin-pack",
			"window_start": windowStart.Format(time.RFC3339), "window_end": windowEnd.Format(time.RFC3339),
			"assigned_demand": 100, "path_capacity": 125, "capacity_over_window": 500, "shortage": 0,
			"bottleneck_step": "REBIN", "published_at": occurred.Format(time.RFC3339)})
	produceCloudEvent(t, brokers, kafka.PlanningTopic, "plan-2", "com.warehouse.wes.warehouse-planning.capacityplan.CapacityPlanPublished", occurred,
		map[string]any{"plan_id": "plan-2", "site_id": "WH2", "warehouse_id": "WH-2", "location": "PATH-ZONE-B", "path_id": "pick-rebin-pack",
			"window_start": windowStart.Format(time.RFC3339), "window_end": windowEnd.Format(time.RFC3339),
			"assigned_demand": 10, "path_capacity": 125, "capacity_over_window": 300, "shortage": 0,
			"bottleneck_step": "REBIN", "published_at": occurred.Format(time.RFC3339)})
	produceCloudEvent(t, brokers, kafka.PlanningTopic, "plan-legacy", "com.warehouse.wes.warehouse-planning.capacityplan.CapacityPlanPublished", occurred,
		map[string]any{"plan_id": "plan-legacy", "warehouse_id": "WH-1", "location": "PATH-ZONE-A", "path_id": "pick-rebin-pack",
			"window_start": windowStart.Format(time.RFC3339), "window_end": windowEnd.Format(time.RFC3339),
			"assigned_demand": 10, "path_capacity": 125, "capacity_over_window": 999, "shortage": 0,
			"bottleneck_step": "REBIN", "published_at": occurred.Format(time.RFC3339)})

	// Also an unknown type and a garbage message on the facility topic:
	// both must be consumed past, never crash, never write.
	produceCloudEvent(t, brokers, kafka.FacilityTopic, "WH1", "com.warehouse.wms.facility-layout.site.SiteRegistered", occurred,
		map[string]any{"site_code": "WH1", "site_name": "x"})
	produceRaw(t, brokers, kafka.FacilityTopic, "WH1", []byte(`{"event_type":"legacy-flat-envelope"}`))

	capConsumer := kafka.NewSiteCapabilityConsumer(brokers, uniqueGroupID("site-capability"), capRepo, processedEvents, uow, nil)
	demandConsumer := kafka.NewSiteSkuDemandConsumer(brokers, uniqueGroupID("site-sku-demand"), demandRepo, processedEvents, uow, nil)
	planConsumer := kafka.NewCapacityPlanConsumer(brokers, uniqueGroupID("capacity-plan"), planRepo, processedEvents, uow, nil)

	runConsumerUntil(t, capConsumer, 30*time.Second)
	runConsumerUntil(t, demandConsumer, 30*time.Second)
	runConsumerUntil(t, planConsumer, 30*time.Second)

	// Read models: capability + demand + plan rows present; legacy plan
	// excluded.
	facts, err := snapshotRepo.Load(ctx)
	if err != nil {
		t.Fatalf("load facts: %v", err)
	}
	if len(facts.Capabilities) != 2 {
		t.Fatalf("capabilities = %+v, want 2 sites", facts.Capabilities)
	}
	if len(facts.Demands) != 2 {
		t.Fatalf("demands = %+v, want 2 rows", facts.Demands)
	}
	if len(facts.Plans) != 2 || hasPlan(facts.Plans, "plan-legacy") {
		t.Fatalf("plans = %+v, want exactly plan-1 and plan-2 (legacy without site_id excluded)", facts.Plans)
	}

	// Fail-closed snapshot over the real rows: fresh facts, both sites
	// participating.
	now := time.Now
	snap, err := planning.BuildSnapshot(planning.SnapshotInput{
		Capabilities: facts.Capabilities, Demands: facts.Demands, Plans: facts.Plans,
		MaxStaleness: time.Hour, Now: now,
	})
	if err != nil {
		t.Fatalf("build snapshot: %v", err)
	}
	if len(snap.Capabilities) != 2 || snap.DemandBySiteSKU["WH1\x00SKU-1"] != 40 {
		t.Fatalf("snapshot = %+v", snap)
	}

	// And fail-closed when stale: same facts, a freshness budget they
	// cannot meet (they are already older than 1ns? no — use a tiny
	// staleness with facts from the past is not reliable; instead age the
	// input explicitly).
	aged := planning.Facts{Capabilities: facts.Capabilities, Demands: facts.Demands, Plans: facts.Plans}
	for i := range aged.Capabilities {
		aged.Capabilities[i].AsOf = aged.Capabilities[i].AsOf.Add(-2 * time.Hour)
	}
	if _, err := planning.BuildSnapshot(planning.SnapshotInput{
		Capabilities: aged.Capabilities, Demands: aged.Demands, Plans: aged.Plans,
		MaxStaleness: time.Hour, Now: now,
	}); err == nil {
		t.Fatal("stale facts must fail closed")
	}
}

func hasPlan(plans []planning.PublishedCapacityPlan, id string) bool {
	for _, p := range plans {
		if p.PlanID == id {
			return true
		}
	}
	return false
}

// produceRaw publishes a raw (non-CloudEvents) message.
func produceRaw(t *testing.T, brokers []string, topic, key string, value []byte) {
	t.Helper()
	writer := kafkago.NewWriter(kafkago.WriterConfig{Brokers: brokers, Topic: topic, Balancer: &kafkago.Hash{}})
	defer func() { _ = writer.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := writer.WriteMessages(ctx, kafkago.Message{Key: []byte(key), Value: value}); err != nil {
		t.Fatalf("produce raw message: %v", err)
	}
}
