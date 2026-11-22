//go:build integration

// End-to-end proof of the REAL entry point of cmd/nip-projector on
// testcontainers (the recipe of the repo's own
// cmd/network-inventory-planning boot test): an EMPTY analytical Postgres
// (the binary migrates it itself), a real Kafka (the binary consumes the
// analytics topic), the documented admin probe answers, the projector
// folds the repo's own encoder events into the facts tables (a replay of
// the same CloudEvents id dedupes), and SIGTERM drains cleanly.
//
// Doc sources: docs/docs/adr/0009-analytics-read-side.md (cmd/nip-projector
// is the ONLY writer of the separate analytical database;
// ANALYTICS_DATABASE_URL / ANALYTICS_MIGRATIONS_PATH / KAFKA_BROKERS /
// ANALYTICS_CONSUMER_GROUP / ADMIN_ADDR), .claude/rules/analytics.md (the
// projector's env table), analytics/migrations/0001_saga_facts.up.sql (the
// facts tables asserted below).
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

	"github.com/claudioed/network-inventory-planning/internal/adapters/outbound/kafka"
	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

// itMigrationsPathValue resolves the repo's ANALYTICAL migrations directory
// (analytics/migrations) relative to this file: the binary's default
// ANALYTICS_MIGRATIONS_PATH is CWD-relative, which a go test run cannot
// rely on.
var itMigrationsPathValue = func() string {
	_, thisFile, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(thisFile), "..", "..", "analytics", "migrations")
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
			tckafka.WithClusterID("nip-projector-itest"),
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

// itPostgres starts a throwaway EMPTY analytical Postgres and returns its
// DSN: the projector's boot step owns the migrations.
func itPostgres(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	container, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("nip_analytics_itest"),
		tcpostgres.WithUsername("nip"),
		tcpostgres.WithPassword("nip"),
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
	ctrlConn, err := kafkago.Dial("tcp", net.JoinHostPort(controller.Host, fmt.Sprint(controller.Port)))
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

// itProduce writes already-encoded messages onto the broker.
func itProduce(t *testing.T, brokers []string, msgs []kafka.Encoded) {
	t.Helper()
	w := &kafkago.Writer{Addr: kafkago.TCP(brokers...), Balancer: &kafkago.Hash{}}
	defer func() { _ = w.Close() }()
	kmsgs := make([]kafkago.Message, 0, len(msgs))
	for _, m := range msgs {
		kmsgs = append(kmsgs, kafkago.Message{Topic: m.Topic, Key: m.Key, Value: m.Value, Headers: m.Headers})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := w.WriteMessages(ctx, kmsgs...); err != nil {
		t.Fatalf("produce %d messages: %v", len(kmsgs), err)
	}
}

// service is one booted instance of the real run().
type service struct {
	addr     string
	done     <-chan error
	stopOnce sync.Once
	stopErr  error
}

// itBootProjector sets the environment a deployment sets, starts the REAL
// run() in-process and waits for the documented admin probe /readyz.
func itBootProjector(t *testing.T, env map[string]string) *service {
	t.Helper()
	for k, v := range env {
		t.Setenv(k, v)
	}
	addr := freeAddr(t)
	t.Setenv("ADMIN_ADDR", addr)
	done := make(chan error, 1)
	go func() { done <- run() }()
	s := &service{addr: addr, done: done}
	t.Cleanup(func() { itAssertCleanDrain(t, s.stop()) })
	waitUntil(t, "/readyz 200", func() bool {
		resp, err := http.Get("http://" + addr + "/readyz")
		if err != nil {
			return false
		}
		_ = resp.Body.Close()
		return resp.StatusCode == http.StatusOK
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
				s.stopErr = fmt.Errorf("the projector did not stop")
			}
		}
	})
	return s.stopErr
}

// itAssertCleanDrain checks run()'s shutdown outcome: a SIGTERM-driven stop
// is a clean shutdown and run() returns nil.
func itAssertCleanDrain(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("the projector must drain cleanly on SIGTERM, got %v", err)
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

// itAnalyticsPool opens a plain pool over the analytical DSN for the fact
// assertions.
func itAnalyticsPool(t *testing.T, dsn string) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("open analytics pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// itCount answers a SELECT count(*) query.
func itCount(t *testing.T, pool *pgxpool.Pool, query string, args ...any) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), query, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return n
}

// TestProjectorBootsMigratesProjectsAndStopsCleanly proves the shipped
// binary's contract on real infrastructure: env only -> the analytical
// database is migrated by the binary itself -> /readyz answers -> the three
// documented saga-health events (encoded by the repo's own AnalyticsEncoder)
// are folded into the facts tables -> a replay of the same CloudEvents id
// does not double count -> SIGTERM drains cleanly.
func TestProjectorBootsMigratesProjectsAndStopsCleanly(t *testing.T) {
	dsn := itPostgres(t) // empty: the projector owns the analytical migrations
	brokers := itKafka(t)
	itEnsureTopic(t, brokers, kafka.AnalyticsTopic)

	s := itBootProjector(t, map[string]string{
		"ANALYTICS_DATABASE_URL":    dsn,
		"ANALYTICS_MIGRATIONS_PATH": itMigrationsPathValue,
		"KAFKA_BROKERS":             strings.Join(brokers, ","),
		// A unique group id per run (the fleet rule for locally run
		// consumers): a rerun must never share a committed offset.
		"ANALYTICS_CONSUMER_GROUP": fmt.Sprintf("nip-projector-itest-%d", time.Now().UnixNano()),
		"LOG_LEVEL":                "error",
	})

	// The binary migrated the empty analytical database itself.
	pool := itAnalyticsPool(t, dsn)
	if applied := itCount(t, pool, "SELECT count(*) FROM schema_migrations"); applied == 0 {
		t.Fatal("the projector booted without applying any analytical migration")
	}

	// The repo's real encoder builds the wire form of the three documented
	// saga-health events (ADR 0009 / internal/domain/transfer/analytics.go).
	at := time.Now().UTC().Truncate(24 * time.Hour).Add(-24 * time.Hour)
	encoder := kafka.NewAnalyticsEncoder()
	advanced, err := encoder.Encode(context.Background(), transfer.StateAdvanced{
		TransferID: "TRF-PROJECTOR-1", From: transfer.StateProposed, To: transfer.StateAllocated,
		AgeSeconds: 240, OccurredAt: at.Add(8 * time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	stuck, err := encoder.Encode(context.Background(), transfer.StuckDetected{
		TransferID: "TRF-PROJECTOR-1", State: transfer.StateAllocated,
		AgeSeconds: 5400, ThresholdSeconds: 3600, OccurredAt: at.Add(9 * time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	rebalance, err := encoder.Encode(context.Background(), transfer.RebalanceRunCompleted{
		RunID: "RUN-PROJECTOR-1", ProposalCount: 7, RejectedCount: 3, StaleFacts: 2, CompletedAt: at.Add(10 * time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	// The same encoded bytes twice: the CloudEvents id is minted at Encode
	// time, so this is a true redelivery and must dedupe.
	itProduce(t, brokers, advanced)
	itProduce(t, brokers, stuck)
	itProduce(t, brokers, rebalance)
	itProduce(t, brokers, advanced)

	// All three facts land, and the replayed TransferStateAdvanced did not
	// double count (ADR 0009: the id claim and the fact insert are ONE
	// transaction).
	waitUntil(t, "the three saga-health facts projected", func() bool {
		return itCount(t, pool, `SELECT count(*) FROM transfer_state_advances
			WHERE transfer_id = 'TRF-PROJECTOR-1' AND to_state = 'ALLOCATED' AND age_seconds = 240`) == 1 &&
			itCount(t, pool, `SELECT count(*) FROM transfer_stuck_detections
			WHERE transfer_id = 'TRF-PROJECTOR-1' AND state = 'ALLOCATED' AND age_seconds = 5400 AND threshold_seconds = 3600`) == 1 &&
			itCount(t, pool, `SELECT count(*) FROM rebalance_run_facts
			WHERE run_id = 'RUN-PROJECTOR-1' AND proposal_count = 7 AND rejected_count = 3 AND stale_facts = 2`) == 1 &&
			itCount(t, pool, "SELECT count(*) FROM analytics_processed_events") == 3
	})

	itAssertCleanDrain(t, s.stop())
}
