//go:build integration

package postgres_integration

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	outboundkafka "github.com/claudioed/network-inventory-planning/internal/adapters/outbound/kafka"
	"github.com/claudioed/network-inventory-planning/internal/adapters/outbound/postgres"
)

// scriptedSink is a postgres.RelaySink with per-message failure injection:
// a message whose value is in failing is refused, everything else is
// recorded in send order.
type scriptedSink struct {
	mu      sync.Mutex
	failing map[string]bool
	sent    []string
}

func (s *scriptedSink) Send(_ context.Context, msg outboundkafka.Encoded) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	v := string(msg.Value)
	if s.failing[v] {
		return fmt.Errorf("injected send failure for %s", v)
	}
	s.sent = append(s.sent, v)
	return nil
}

func (s *scriptedSink) sentValues() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.sent...)
}

func (s *scriptedSink) heal() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failing = map[string]bool{}
}

func seedOutbox(t *testing.T, ctx context.Context, pool *pgxpool.Pool, topic, key, value string) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		INSERT INTO outbox_events (topic, event_type, key, value) VALUES ($1, 'T', $2, $3)
	`, topic, []byte(key), []byte(value)); err != nil {
		t.Fatalf("seed outbox row %s: %v", value, err)
	}
}

type outboxRow struct {
	published bool
	attempts  int
	lastError string
}

func outboxState(t *testing.T, ctx context.Context, pool *pgxpool.Pool) map[string]outboxRow {
	t.Helper()
	rows, err := pool.Query(ctx, `SELECT convert_from(value, 'UTF8'), published_at IS NOT NULL, attempts, COALESCE(last_error, '') FROM outbox_events`)
	if err != nil {
		t.Fatalf("read outbox: %v", err)
	}
	defer rows.Close()
	out := map[string]outboxRow{}
	for rows.Next() {
		var value string
		var r outboxRow
		if err := rows.Scan(&value, &r.published, &r.attempts, &r.lastError); err != nil {
			t.Fatalf("scan outbox: %v", err)
		}
		out[value] = r
	}
	return out
}

// TestRelayOnePoisonRowDoesNotBlockOtherKeys is the sensor for outbox
// head-of-line blocking: one row that keeps failing (id order first)
// used to stop the whole pass, so every later row of EVERY key waited
// behind it. Per-(topic,key) order must still hold.
func TestRelayOnePoisonRowDoesNotBlockOtherKeys(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	databaseURL := startPostgres(t)
	if err := postgres.RunMigrations(databaseURL, rebalanceMigrationsDir(t)); err != nil {
		t.Fatalf("run migrations: %v", err)
	}
	ctx := context.Background()
	pool, err := postgres.NewPool(ctx, databaseURL)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	defer pool.Close()

	// id order: A1 (poison), A2 (same topic+key as A1), B1 (other key),
	// OT-A1 (same key bytes, OTHER topic).
	seedOutbox(t, ctx, pool, "topic-1", "key-A", "A1")
	seedOutbox(t, ctx, pool, "topic-1", "key-A", "A2")
	seedOutbox(t, ctx, pool, "topic-1", "key-B", "B1")
	seedOutbox(t, ctx, pool, "topic-2", "key-A", "OT-A1")

	sink := &scriptedSink{failing: map[string]bool{"A1": true}}
	relay := postgres.NewOutboxRelay(pool, sink)

	// (a) the failing key-A row must not block key B or the other topic.
	published, err := relay.RelayOnce(ctx)
	if err == nil {
		t.Fatal("a pass with a failing row must report the send error")
	}
	if published != 2 {
		t.Fatalf("published = %d, want 2 (B1 and OT-A1 are not behind the poison row)", published)
	}
	// (b) A2 shares topic+key with the failed A1: it must NOT overtake it.
	if got, want := sink.sentValues(), []string{"B1", "OT-A1"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("sent = %v, want %v (A2 must not be sent ahead of A1)", got, want)
	}
	state := outboxState(t, ctx, pool)
	if a1 := state["A1"]; a1.published || a1.attempts != 1 || a1.lastError == "" {
		t.Fatalf("A1 = %+v, want unpublished with 1 attempt and its error recorded", a1)
	}
	if a2 := state["A2"]; a2.published || a2.attempts != 0 {
		t.Fatalf("A2 = %+v, want untouched (skipped, not attempted)", a2)
	}
	if !state["B1"].published || !state["OT-A1"].published {
		t.Fatalf("B1/OT-A1 must be marked published: %+v", state)
	}

	// (c) once the sink heals the failed row is retried, then its successor.
	sink.heal()
	published, err = relay.RelayOnce(ctx)
	if err != nil || published != 2 {
		t.Fatalf("retry pass = %d, %v; want 2, nil", published, err)
	}
	if got, want := sink.sentValues(), []string{"B1", "OT-A1", "A1", "A2"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("sent = %v, want %v (A1 retried before A2)", got, want)
	}
	for value, row := range outboxState(t, ctx, pool) {
		if !row.published {
			t.Fatalf("%s still unpublished after the retry pass", value)
		}
	}
}

// TestRelayOnceReportsEveryFailureAndKeepsCounting proves two independent
// poison rows are both recorded in one pass and the error is combined.
func TestRelayOnceReportsEveryFailureAndKeepsCounting(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	databaseURL := startPostgres(t)
	if err := postgres.RunMigrations(databaseURL, rebalanceMigrationsDir(t)); err != nil {
		t.Fatalf("run migrations: %v", err)
	}
	ctx := context.Background()
	pool, err := postgres.NewPool(ctx, databaseURL)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	defer pool.Close()

	seedOutbox(t, ctx, pool, "topic-1", "key-A", "A1")
	seedOutbox(t, ctx, pool, "topic-1", "key-B", "B1")
	seedOutbox(t, ctx, pool, "topic-1", "key-C", "C1")

	sink := &scriptedSink{failing: map[string]bool{"A1": true, "B1": true}}
	relay := postgres.NewOutboxRelay(pool, sink)
	published, err := relay.RelayOnce(ctx)
	if published != 1 {
		t.Fatalf("published = %d, want 1 (only C1 is sendable)", published)
	}
	if err == nil {
		t.Fatal("want a combined error")
	}
	for _, want := range []string{"injected send failure for A1", "injected send failure for B1"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("combined error %q is missing %q", err.Error(), want)
		}
	}
	state := outboxState(t, ctx, pool)
	if state["A1"].attempts != 1 || state["B1"].attempts != 1 || !state["C1"].published {
		t.Fatalf("state = %+v, want A1/B1 one attempt each and C1 published", state)
	}
}
