//go:build integration

// Boot-level end-to-end proof of the REAL entry point of
// cmd/network-inventory-planning on testcontainers: an EMPTY Postgres (the
// binary runs its own migrations), a real Kafka (the binary consumes), the
// documented probe answers, and the documented shutdown path (SIGTERM —
// run() installs its own signal.NotifyContext) drains cleanly.
//
// Doc sources: docs/docs/operations/runbook.md ("api: there is no /readyz;
// GET /healthz answers 204 as soon as the HTTP server listens, and the
// server only starts listening after the migrations applied and the pool
// pinged"), docs/docs/operations/configuration.md (DATABASE_URL set =
// migrate + pool + ping or refuse to boot; the consumer-group env vars;
// PACK_LEVEL_CONSUMER_GROUP required when KAFKA_BROKERS is set).
package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	inboundkafka "github.com/claudioed/network-inventory-planning/internal/adapters/inbound/kafka"
	outboundkafka "github.com/claudioed/network-inventory-planning/internal/adapters/outbound/kafka"
)

// itMigrationsPathValue is the absolute path of the repo's migrations
// directory (the binary's default MIGRATIONS_PATH is CWD-relative, which a
// go test run cannot rely on).
var itMigrationsPathValue = func() string {
	_, thisFile, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(thisFile), "..", "..", "internal", "adapters", "outbound", "postgres", "migrations")
}()

// One shared Kafka container per package (the recipe of the repo's own
// kafka_integration harness); Postgres is per-test so every test starts from
// a database state it owns.
var (
	itBrokers   []string
	itKafkaOnce sync.Once
	itKafkaErr  error
	itKafkaCtr  *tckafka.KafkaContainer
)

// TestMain holds the package-level SIGTERM registration for the whole test
// run: the real run() installs (and on return RESTORES) its own signal
// handler, so a SIGTERM arriving outside a live run() would otherwise kill
// the test binary itself. Registering a sink here makes signal-driven
// shutdown of run() always safe. It then terminates the shared Kafka
// container after m.Run().
func TestMain(m *testing.M) {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	code := m.Run()
	if itKafkaCtr != nil {
		if err := testcontainers.TerminateContainer(itKafkaCtr); err != nil {
			fmt.Fprintf(os.Stderr, "terminate kafka container: %v\n", err)
		}
	}
	os.Exit(code)
}

// itKafka starts the shared broker once and returns its address.
func itKafka(t *testing.T) []string {
	t.Helper()
	itKafkaOnce.Do(func() {
		ctx := context.Background()
		container, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1",
			tckafka.WithClusterID("network-inventory-planning-e2e-itest"),
		)
		if err != nil {
			itKafkaErr = err
			return
		}
		itKafkaCtr = container
		itBrokers, err = container.Brokers(ctx)
		if err != nil {
			itKafkaErr = err
		}
	})
	if itKafkaErr != nil {
		t.Fatalf("start kafka container: %v", itKafkaErr)
	}
	return itBrokers
}

// itPostgres starts a throwaway Postgres and returns its DSN. The caller
// decides whether to pre-apply migrations (the boot test does NOT: it boots
// the real binary against the empty database and lets it migrate itself).
func itPostgres(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	container, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("nip_e2e_test"),
		tcpostgres.WithUsername("nip_e2e"),
		tcpostgres.WithPassword("nip_e2e"),
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
	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("resolve connection string: %v", err)
	}
	return dsn
}

// itMigrationsDir resolves the repo's migrations directory relative to THIS
// FILE (go test's working directory varies by runner), like the repo's own
// kafka_integration harness does.
func itMigrationsDir(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate test source file")
	}
	return filepath.Join(filepath.Dir(thisFile), "..", "..", "internal", "adapters", "outbound", "postgres", "migrations")
}

// itEnsureTopic creates topic if the broker has auto-create disabled
// (verbatim recipe of the repo's kafka_integration harness).
func itEnsureTopic(t *testing.T, brokers []string, topic string) {
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
		if !strings.Contains(err.Error(), "already exists") {
			t.Fatalf("create topic %s: %v", topic, err)
		}
	}
}

// itUniqueGroup gives each boot its own consumer group ids so a rerun never
// shares a committed offset (AGENTS.md: no default group is safe).
func itUniqueGroup(prefix string) string {
	return fmt.Sprintf("%s-e2e-%d", prefix, time.Now().UnixNano())
}

// service is one booted instance of the real run().
type service struct {
	base string
	done <-chan error

	stopOnce sync.Once
	stopErr  error
}

// itBootService sets the environment a deployment would set, starts the REAL
// run() in-process, waits for the documented probe (/healthz 204 — the
// runbook: the server listens only after migrations applied and the pool
// pinged), and registers a cleanup that stops it.
func itBootService(t *testing.T, env map[string]string) *service {
	t.Helper()
	for k, v := range env {
		t.Setenv(k, v)
	}
	addr := freeAddr(t)
	t.Setenv("HTTP_ADDR", addr)
	done := make(chan error, 1)
	go func() { done <- run() }()
	s := &service{base: "http://" + addr, done: done}
	t.Cleanup(func() { itAssertCleanDrain(t, s.stop()) })
	waitUntil(t, "the documented probe /healthz to answer 204", func() bool {
		resp, err := http.Get(s.base + "/healthz")
		if err != nil {
			return false
		}
		_ = resp.Body.Close()
		return resp.StatusCode == http.StatusNoContent
	})
	return s
}

// stop drives the binary's real shutdown path. run() takes no context from
// its caller — it installs signal.NotifyContext(os.Interrupt, SIGTERM)
// itself, which is exactly the kubelet's path; the SIGTERM is safe because
// TestMain keeps a package-level signal sink registered.
func (s *service) stop() error {
	s.stopOnce.Do(func() {
		select {
		case s.stopErr = <-s.done: // already stopped on its own
		default:
		}
		if s.stopErr == nil {
			if killErr := syscall.Kill(syscall.Getpid(), syscall.SIGTERM); killErr != nil {
				s.stopErr = killErr
				return
			}
			select {
			case s.stopErr = <-s.done:
			case <-time.After(60 * time.Second):
				s.stopErr = fmt.Errorf("the service did not stop")
			}
		}
	})
	return s.stopErr
}

// itAssertCleanDrain checks run()'s shutdown outcome. ADR 0019's contract:
// a stop caused by the context's cancellation is a clean shutdown and run()
// returns nil. Since the runner contexts derive from run()'s root signal
// context (no private signal.NotifyContext anywhere), a SIGTERM reaches the
// root first and every runner stop reads as clean — the earlier accepted
// deviation ("returned without an error while the service was still
// running" on a clean stop) is gone and must stay gone.
func itAssertCleanDrain(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("the binary must drain cleanly on SIGTERM, got %v", err)
	}
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	return l.Addr().String()
}

func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(90 * time.Second); time.Now().Before(deadline); time.Sleep(200 * time.Millisecond) {
		if cond() {
			return
		}
	}
	t.Fatalf("timed out waiting for %s", what)
}

// itServiceEnv is the environment a full deployment sets (main.go's env
// names, never guessed): DATABASE_URL over the (empty or migrated) database,
// MIGRATIONS_PATH absolute (the binary's default is CWD-relative), every
// consumer group its own unique id (AGENTS.md runtime rule: an EMPTY value
// means that consumer does not run, and the pack-level group is MANDATORY
// once KAFKA_BROKERS is set, ADR 0017), the outbox relay on (the saga's
// events then reach Kafka), the ADR 0005 release legs configured, and the
// ADR 0007 health ticker off (documented 0=off) so no stuck-check occurrence
// pollutes the topic assertions.
func itServiceEnv(dsn string, brokers []string) map[string]string {
	return map[string]string{
		"DATABASE_URL":                   dsn,
		"MIGRATIONS_PATH":                itMigrationsPathValue,
		"KAFKA_BROKERS":                  strings.Join(brokers, ","),
		"SITE_CAPABILITY_CONSUMER_GROUP": itUniqueGroup("site-capability"),
		"SITE_SKU_DEMAND_CONSUMER_GROUP": itUniqueGroup("site-sku-demand"),
		"CAPACITY_PLAN_CONSUMER_GROUP":   itUniqueGroup("capacity-plan"),
		"TRANSFER_REPLY_CONSUMER_GROUP":  itUniqueGroup("transfer-reply"),
		"TRANSFER_FACT_CONSUMER_GROUP":   itUniqueGroup("transfer-fact"),
		"PACK_LEVEL_CONSUMER_GROUP":      itUniqueGroup("pack-level"),
		"OUTBOX_RELAY_ENABLED":           "1",
		"TRANSFER_PICK_PATH_ID":          "transfer-pick-path",
		"TRANSFER_DISPATCH_PATH_ID":      "transfer-dispatch-path",
		"PLANNING_MAX_STALENESS":         "30m",
		"NIP_HEALTH_CHECK_INTERVAL":      "0",
		"LOG_LEVEL":                      "error",
	}
}

// itAllTopics returns every topic the booted binary touches: the six the
// consumers subscribe to plus the integration and analytics topics the
// relay drains the outbox onto (wireOutbox fans out over both encoders,
// ADR 0007).
func itAllTopics() []string {
	return []string{
		inboundkafka.FacilityTopic,
		inboundkafka.OrderTopic,
		inboundkafka.PlanningTopic,
		inboundkafka.InventoryTopic,
		inboundkafka.FulfillmentTopic,
		inboundkafka.ProductMasterTopic,
		outboundkafka.TransferTopic,
		outboundkafka.AnalyticsTopic,
	}
}

// TestBootMigratesServesHealthzAndDrainsCleanly proves the boot contract of
// the shipped binary on an EMPTY database: env only -> the binary migrates
// itself -> the documented probe answers -> SIGTERM drains cleanly (nil
// error, no goroutine leak taking the process down, ADR 0019's clean-stop
// contract).
func TestBootMigratesServesHealthzAndDrainsCleanly(t *testing.T) {
	dsn := itPostgres(t) // empty: the binary owns the migrations
	brokers := itKafka(t)

	// Every topic any of the six consumers subscribes to, plus the two the
	// relay drains to: a missing topic with auto-create off would kill the
	// consumer (and with it the process, ADR 0019) for the wrong reason.
	for _, topic := range itAllTopics() {
		itEnsureTopic(t, brokers, topic)
	}

	env := itServiceEnv(dsn, brokers)
	s := itBootService(t, env)

	// The binary migrated the empty database itself: golang-migrate's
	// bookkeeping table exists AND the saga schema landed (outbox_events).
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("open postgres pool: %v", err)
	}
	defer pool.Close()
	var applied int
	if err := pool.QueryRow(context.Background(), "SELECT count(*) FROM schema_migrations").Scan(&applied); err != nil {
		t.Fatalf("schema_migrations after boot: %v (the binary must migrate the empty database itself)", err)
	}
	if applied == 0 {
		t.Fatal("the binary booted without applying any migration")
	}
	var hasOutbox bool
	if err := pool.QueryRow(context.Background(), "SELECT to_regclass('public.outbox_events') IS NOT NULL").Scan(&hasOutbox); err != nil || !hasOutbox {
		t.Fatalf("outbox_events exists = %v (err %v): the saga schema did not land", hasOutbox, err)
	}

	// The documented probe still answers after the schema checks (the
	// consumers and the relay did not take the process down, ADR 0019).
	resp, err := http.Get(s.base + "/healthz")
	if err != nil {
		t.Fatalf("healthz: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("healthz = %d, want 204 (the runbook's api probe)", resp.StatusCode)
	}

	// SIGTERM (the kubelet's path) drains cleanly per ADR 0019 (see
	// itAssertCleanDrain for the one known, reported deviation).
	itAssertCleanDrain(t, s.stop())
}
