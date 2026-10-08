package kafka

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/network-inventory-planning/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/network-inventory-planning/internal/analytics/report"
)

var anAt = time.Date(2026, 10, 5, 8, 10, 0, 0, time.UTC)

// anWire builds a producer-shaped analytics CloudEvent of the given event
// name (entity "saga") under the given id and subject.
func anWire(t *testing.T, id, eventName, subject string, at time.Time, data any) []byte {
	t.Helper()
	value, err := cloudevents.New(cloudevents.Spec{
		ID: id, Entity: analyticsEntity, EventName: eventName, Subject: subject,
		Time: at, Stream: cloudevents.StreamAnalytics, Version: 1, Data: data,
	})
	if err != nil {
		t.Fatalf("encode %s: %v", eventName, err)
	}
	return value
}

type anFakeDLQ struct {
	mu   sync.Mutex
	msgs []kafkago.Message
	fail int // fail the next n writes
}

func (f *anFakeDLQ) WriteMessages(_ context.Context, msgs ...kafkago.Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail > 0 {
		f.fail--
		return errors.New("dlq broker down")
	}
	f.msgs = append(f.msgs, msgs...)
	return nil
}
func (f *anFakeDLQ) Close() error { return nil }
func (f *anFakeDLQ) written() []kafkago.Message {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]kafkago.Message(nil), f.msgs...)
}

// anRecorder records every report.Event and can fail on demand.
type anRecorder struct {
	mu     sync.Mutex
	events []report.Event
	errs   []error // returned in order, one per call, then nil
	calls  int
}

func (r *anRecorder) Apply(_ context.Context, e report.Event) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	if len(r.errs) > 0 {
		err := r.errs[0]
		r.errs = r.errs[1:]
		if err != nil {
			return false, err
		}
	}
	r.events = append(r.events, e)
	return true, nil
}

type anLockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *anLockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *anLockedBuffer) lines(substr string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, line := range strings.Split(l.b.String(), "\n") {
		if line != "" && strings.Contains(line, substr) {
			n++
		}
	}
	return n
}

func anConsumer(p report.Projection, dlq DeadLetterWriter, w io.Writer) *AnalyticsConsumer {
	return &AnalyticsConsumer{
		Reader: &fakeReader{}, Projection: p, DLQ: dlq, DLQTopic: "t.dlq",
		Logger: slog.New(slog.NewJSONHandler(w, nil)),
		sleep:  func(context.Context, time.Duration) error { return nil },
	}
}

func TestAnalyticsConsumer_ProjectsEachKnownTypeFromItsFullCloudEventsType(t *testing.T) {
	cases := []struct {
		name  string
		value []byte
		want  report.Event
	}{
		{"state advanced", anWire(t, "e1", "TransferStateAdvanced", "tr-1", anAt,
			map[string]any{"transfer_id": "tr-1", "from": "PROPOSED", "to": "APPROVED", "age_seconds": 42}),
			report.Event{Kind: report.KindStateAdvanced, EventID: "e1", At: anAt, TransferID: "tr-1", From: "PROPOSED", To: "APPROVED", AgeSeconds: 42}},
		{"state advanced, creation entry has an empty from", anWire(t, "e2", "TransferStateAdvanced", "tr-2", anAt,
			map[string]any{"transfer_id": "tr-2", "from": "", "to": "PROPOSED", "age_seconds": 0}),
			report.Event{Kind: report.KindStateAdvanced, EventID: "e2", At: anAt, TransferID: "tr-2", To: "PROPOSED"}},
		{"stuck detected", anWire(t, "e3", "TransferStuckDetected", "tr-3", anAt,
			map[string]any{"transfer_id": "tr-3", "state": "ALLOCATING", "age_seconds": 700, "threshold_seconds": 600}),
			report.Event{Kind: report.KindStuckDetected, EventID: "e3", At: anAt, TransferID: "tr-3", State: "ALLOCATING", AgeSeconds: 700, ThresholdSeconds: 600}},
		{"rebalance run completed", anWire(t, "e4", "RebalanceRunCompleted", "run-9", anAt,
			map[string]any{"run_id": "run-9", "proposal_count": 7, "rejected_count": 2, "stale_facts": 1}),
			report.Event{Kind: report.KindRebalanceRunCompleted, EventID: "e4", At: anAt, RunID: "run-9", ProposalCount: 7, RejectedCount: 2, StaleFacts: 1}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rec, dlq := &anRecorder{}, &anFakeDLQ{}
			if err := anConsumer(rec, dlq, io.Discard).HandleMessage(context.Background(), kafkago.Message{Value: c.value}); err != nil {
				t.Fatalf("HandleMessage = %v", err)
			}
			if !reflect.DeepEqual(rec.events, []report.Event{c.want}) {
				t.Fatalf("applied %+v\nwant    %+v", rec.events, c.want)
			}
			if len(dlq.written()) != 0 {
				t.Fatal("a valid event must not be dead-lettered")
			}
		})
	}
}

func TestAnalyticsConsumer_ASameIdDeliveredTwiceIsAppliedByTheProjectionOnceNotByTheConsumer(t *testing.T) {
	// The consumer is stateless: idempotency is the Projection's contract
	// (same-transaction claim). It must hand both deliveries over.
	rec := &anRecorder{}
	value := anWire(t, "dup", "TransferStateAdvanced", "tr-1", anAt,
		map[string]any{"transfer_id": "tr-1", "from": "A", "to": "B", "age_seconds": 1})
	c := anConsumer(rec, &anFakeDLQ{}, io.Discard)
	for i := 0; i < 2; i++ {
		if err := c.HandleMessage(context.Background(), kafkago.Message{Value: value}); err != nil {
			t.Fatal(err)
		}
	}
	if rec.calls != 2 {
		t.Fatalf("Apply calls = %d, want 2 (dedupe belongs to the store)", rec.calls)
	}
}

func TestAnalyticsConsumer_AnUnknownTypeIsSkippedAndLoggedNeverFatal(t *testing.T) {
	rec, dlq, logs := &anRecorder{}, &anFakeDLQ{}, &anLockedBuffer{}
	value := anWire(t, "e9", "TransferTeleported", "tr-1", anAt, map[string]any{"transfer_id": "tr-1"})
	if err := anConsumer(rec, dlq, logs).HandleMessage(context.Background(), kafkago.Message{Value: value}); err != nil {
		t.Fatalf("an unknown type must not be an error, got %v", err)
	}
	if rec.calls != 0 || len(dlq.written()) != 0 {
		t.Fatalf("unknown type reached the projection (%d) or the DLQ (%d)", rec.calls, len(dlq.written()))
	}
	if logs.lines("unknown type") != 1 {
		t.Fatalf("want one log line naming the skip, got %d", logs.lines("unknown type"))
	}
}

func TestAnalyticsConsumer_AnOtherContextWithTheSameEventNameIsNotProjected(t *testing.T) {
	// Dispatch is on the FULL type: the last segment alone must not match.
	value, err := cloudevents.New(cloudevents.Spec{
		ID: "x", Entity: "other", EventName: "TransferStateAdvanced", Subject: "tr-1",
		Time: anAt, Stream: cloudevents.StreamAnalytics, Version: 1,
		Data: map[string]any{"transfer_id": "tr-1", "to": "B", "age_seconds": 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	rec := &anRecorder{}
	if err := anConsumer(rec, &anFakeDLQ{}, io.Discard).HandleMessage(context.Background(), kafkago.Message{Value: value}); err != nil || rec.calls != 0 {
		t.Fatalf("HandleMessage = %v, applied %d; want a skip", err, rec.calls)
	}
}

func TestAnalyticsConsumer_NonCloudEventsAreSkippedWithASampledWarn(t *testing.T) {
	rec, logs := &anRecorder{}, &anLockedBuffer{}
	c := anConsumer(rec, &anFakeDLQ{}, logs)
	now := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	c.Now = func() time.Time { return now }
	for i := 0; i < 5; i++ {
		if err := c.HandleMessage(context.Background(), kafkago.Message{Value: []byte(`{"flat":"envelope"}`)}); err != nil {
			t.Fatal(err)
		}
	}
	if got := logs.lines("not a CloudEvents 1.0 event"); got != 1 {
		t.Fatalf("WARN lines in the first interval = %d, want 1", got)
	}
	now = now.Add(skipWarnEvery)
	if err := c.HandleMessage(context.Background(), kafkago.Message{Value: []byte(`garbage`)}); err != nil {
		t.Fatal(err)
	}
	if got := logs.lines("not a CloudEvents 1.0 event"); got != 2 {
		t.Fatalf("WARN lines after the interval = %d, want 2", got)
	}
	if !strings.Contains(logs.b.String(), `"suppressed_since_last_warning":4`) {
		t.Fatalf("the second WARN must carry the suppressed count (4); logs:\n%s", logs.b.String())
	}
	if rec.calls != 0 {
		t.Fatal("non-CloudEvents messages must never reach the projection")
	}
}

func TestAnalyticsConsumer_UnusablePayloadsAreDeadLetteredAtOnce(t *testing.T) {
	cases := map[string][]byte{
		"subject is not the transfer": anWire(t, "p1", "TransferStateAdvanced", "other", anAt,
			map[string]any{"transfer_id": "tr-1", "from": "A", "to": "B", "age_seconds": 1}),
		"missing to": anWire(t, "p2", "TransferStateAdvanced", "tr-1", anAt,
			map[string]any{"transfer_id": "tr-1", "from": "A", "age_seconds": 1}),
		"missing age": anWire(t, "p3", "TransferStateAdvanced", "tr-1", anAt,
			map[string]any{"transfer_id": "tr-1", "from": "A", "to": "B"}),
		"negative age": anWire(t, "p4", "TransferStateAdvanced", "tr-1", anAt,
			map[string]any{"transfer_id": "tr-1", "from": "A", "to": "B", "age_seconds": -5}),
		"stuck without state": anWire(t, "p5", "TransferStuckDetected", "tr-1", anAt,
			map[string]any{"transfer_id": "tr-1", "age_seconds": 5, "threshold_seconds": 1}),
		"stuck negative threshold": anWire(t, "p6", "TransferStuckDetected", "tr-1", anAt,
			map[string]any{"transfer_id": "tr-1", "state": "S", "age_seconds": 5, "threshold_seconds": -1}),
		"run id is not the subject": anWire(t, "p7", "RebalanceRunCompleted", "run-1", anAt,
			map[string]any{"run_id": "run-2", "proposal_count": 1, "rejected_count": 0}),
		"run without counts": anWire(t, "p8", "RebalanceRunCompleted", "run-1", anAt,
			map[string]any{"run_id": "run-1"}),
		"run negative rejected": anWire(t, "p9", "RebalanceRunCompleted", "run-1", anAt,
			map[string]any{"run_id": "run-1", "proposal_count": 1, "rejected_count": -1}),
		"run negative stale facts": anWire(t, "p10", "RebalanceRunCompleted", "run-1", anAt,
			map[string]any{"run_id": "run-1", "proposal_count": 1, "rejected_count": 0, "stale_facts": -3}),
		"data is not an object": anWire(t, "p11", "TransferStateAdvanced", "tr-1", anAt, "oops"),
		"no time":               anWire(t, "p12", "TransferStateAdvanced", "tr-1", time.Time{}, map[string]any{"transfer_id": "tr-1", "to": "B", "age_seconds": 1}),
	}
	for name, value := range cases {
		t.Run(name, func(t *testing.T) {
			rec, dlq := &anRecorder{}, &anFakeDLQ{}
			msg := kafkago.Message{Topic: "src", Key: []byte("k"), Value: value, Headers: []kafkago.Header{{Key: "h", Value: []byte("v")}}}
			if err := anConsumer(rec, dlq, io.Discard).HandleMessage(context.Background(), msg); err != nil {
				t.Fatalf("poison must be dead-lettered and committed past, got %v", err)
			}
			if rec.calls != 0 {
				t.Fatal("poison reached the projection")
			}
			got := dlq.written()
			if len(got) != 1 || !bytes.Equal(got[0].Value, value) || !bytes.Equal(got[0].Key, []byte("k")) {
				t.Fatalf("DLQ = %+v; want the raw message once", got)
			}
			headers := map[string]string{}
			for _, h := range got[0].Headers {
				headers[h.Key] = string(h.Value)
			}
			if headers["h"] != "v" || headers["x-dlq-source-topic"] != "src" || headers["x-dlq-error"] == "" || headers["x-dlq-failed-at"] == "" {
				t.Fatalf("DLQ headers = %v", headers)
			}
		})
	}
}

func TestAnalyticsConsumer_AStoreRejectionIsDeadLetteredButATransientFailureIsRetried(t *testing.T) {
	value := anWire(t, "r1", "TransferStateAdvanced", "tr-1", anAt,
		map[string]any{"transfer_id": "tr-1", "from": "A", "to": "B", "age_seconds": 1})

	dlq := &anFakeDLQ{}
	rejecting := &anRecorder{errs: []error{report.ErrRejected}}
	if err := anConsumer(rejecting, dlq, io.Discard).HandleMessage(context.Background(), kafkago.Message{Value: value}); err != nil {
		t.Fatalf("a rejection is poison: dead-letter and commit past, got %v", err)
	}
	if len(dlq.written()) != 1 {
		t.Fatalf("DLQ = %d messages, want 1", len(dlq.written()))
	}

	dlq = &anFakeDLQ{}
	down := &anRecorder{errs: []error{errors.New("connection refused")}}
	if err := anConsumer(down, dlq, io.Discard).HandleMessage(context.Background(), kafkago.Message{Value: value}); err == nil {
		t.Fatal("a transient failure must be returned so the loop retries the same message")
	}
	if len(dlq.written()) != 0 {
		t.Fatal("a transient failure must NEVER be dead-lettered")
	}
}

func TestAnalyticsConsumer_AFailedDeadLetterWriteIsReturnedSoPoisonIsNeverLost(t *testing.T) {
	dlq := &anFakeDLQ{fail: 1}
	value := anWire(t, "d1", "TransferStateAdvanced", "other", anAt,
		map[string]any{"transfer_id": "tr-1", "from": "A", "to": "B", "age_seconds": 1})
	c := anConsumer(&anRecorder{}, dlq, io.Discard)
	if err := c.HandleMessage(context.Background(), kafkago.Message{Value: value}); err == nil {
		t.Fatal("a failed DLQ write must be an error (the message is retried)")
	}
	if err := c.HandleMessage(context.Background(), kafkago.Message{Value: value}); err != nil {
		t.Fatalf("retry after the DLQ recovered = %v", err)
	}
	if len(dlq.written()) != 1 {
		t.Fatalf("DLQ = %d messages, want 1", len(dlq.written()))
	}
}

func TestAnalyticsConsumer_RunCommitsOnlyAfterTheProjectionSucceeded(t *testing.T) {
	value := anWire(t, "loop-1", "TransferStateAdvanced", "tr-1", anAt,
		map[string]any{"transfer_id": "tr-1", "from": "A", "to": "B", "age_seconds": 1})
	reader := &fakeReader{messages: []kafkago.Message{{Value: value, Offset: 7}}}
	rec := &anRecorder{errs: []error{errors.New("db down"), errors.New("db still down")}}
	c := anConsumer(rec, &anFakeDLQ{}, io.Discard)
	c.Reader = reader

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	deadline := time.After(5 * time.Second)
	for {
		reader.mu.Lock()
		n := len(reader.commits)
		reader.mu.Unlock()
		if n == 1 {
			cancel()
			break
		}
		select {
		case <-deadline:
			cancel()
			t.Fatal("run loop never committed the handled message")
		case <-time.After(10 * time.Millisecond):
		}
	}
	<-done
	if !reflect.DeepEqual(reader.commits, []int{7}) {
		t.Fatalf("commits = %v, want exactly [7] after the third attempt succeeded", reader.commits)
	}
	if rec.calls != 3 || len(rec.events) != 1 {
		t.Fatalf("Apply calls = %d, applied = %d; want 3 calls (2 failures) and 1 applied", rec.calls, len(rec.events))
	}
	if err := c.Close(); err != nil || !reader.closed {
		t.Fatalf("Close = %v, reader closed = %v", err, reader.closed)
	}
}

func TestIsTopicNotReady(t *testing.T) {
	if !isTopicNotReady(kafkago.UnknownTopicOrPartition) || !isTopicNotReady(kafkago.LeaderNotAvailable) {
		t.Error("topic-not-ready errors must be recognised")
	}
	if isTopicNotReady(errors.New("boom")) {
		t.Error("an unrelated error is not topic-not-ready")
	}
	if !isTopicNotReady(kafkago.WriteErrors{kafkago.UnknownTopicOrPartition, nil}) {
		t.Error("WriteErrors of only not-ready errors is not-ready")
	}
	if isTopicNotReady(kafkago.WriteErrors{kafkago.UnknownTopicOrPartition, errors.New("boom")}) {
		t.Error("WriteErrors containing a real error is not not-ready")
	}
	if isTopicNotReady(kafkago.WriteErrors{nil, nil}) {
		t.Error("WriteErrors without errors is not not-ready")
	}
}

func TestWriteDLQ_RetriesWhileTheTopicHasNoLeaderThenGivesUpOnCancel(t *testing.T) {
	w := &flakyDLQ{failures: 2}
	if err := writeDLQ(context.Background(), w, kafkago.Message{Value: []byte("x")}); err != nil {
		t.Fatalf("writeDLQ = %v", err)
	}
	if w.calls != 3 {
		t.Fatalf("calls = %d, want 3", w.calls)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := writeDLQ(ctx, &flakyDLQ{failures: 100}, kafkago.Message{}); err == nil {
		t.Fatal("a cancelled context must stop the retries")
	}
}

type flakyDLQ struct {
	failures int
	calls    int
}

func (f *flakyDLQ) WriteMessages(context.Context, ...kafkago.Message) error {
	f.calls++
	if f.calls <= f.failures {
		return kafkago.UnknownTopicOrPartition
	}
	return nil
}
func (f *flakyDLQ) Close() error { return nil }
