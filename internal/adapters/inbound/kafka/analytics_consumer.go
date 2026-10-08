package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	ce "github.com/cloudevents/sdk-go/v2/event"
	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/network-inventory-planning/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/network-inventory-planning/internal/analytics/report"
)

// DLQSuffix is appended to the analytics topic to name its dead-letter
// topic (warehouse.network-inventory-planning.analytics.dlq).
const DLQSuffix = ".dlq"

// analyticsEntity is the `entity` segment of every saga-health event type.
const analyticsEntity = "saga"

// skipWarnEvery bounds the WARN lines for skipped (non-CloudEvents) messages:
// the first skip is logged, then at most one line per interval carrying the
// number suppressed since.
const skipWarnEvery = time.Minute

// The three analytics event types this projector folds into the model,
// dispatched on the FULL CloudEvents type. Every other CloudEvents type on the
// topic is acknowledged, logged and ignored (additive evolution of the stream).
var analyticsKinds = map[string]report.Kind{
	cloudevents.Type(analyticsEntity, string(report.KindStateAdvanced)):         report.KindStateAdvanced,
	cloudevents.Type(analyticsEntity, string(report.KindStuckDetected)):         report.KindStuckDetected,
	cloudevents.Type(analyticsEntity, string(report.KindRebalanceRunCompleted)): report.KindRebalanceRunCompleted,
}

// sagaData is the union of the analytics payload fields the projection reads
// (hand-mirrored from apis/asyncapi.yaml, never imported from the domain). A
// pointer field distinguishes "absent" from zero.
type sagaData struct {
	TransferID       string `json:"transfer_id"`
	From             string `json:"from"`
	To               string `json:"to"`
	State            string `json:"state"`
	AgeSeconds       *int64 `json:"age_seconds"`
	DwellSeconds     *int64 `json:"dwell_seconds"`
	ThresholdSeconds *int64 `json:"threshold_seconds"`
	RunID            string `json:"run_id"`
	ProposalCount    *int   `json:"proposal_count"`
	RejectedCount    *int   `json:"rejected_count"`
	StaleFacts       *int   `json:"stale_facts"`
}

// DeadLetterWriter is the subset of *kafkago.Writer the consumer needs.
type DeadLetterWriter interface {
	WriteMessages(ctx context.Context, msgs ...kafkago.Message) error
	Close() error
}

// AnalyticsConsumer is the projector's consumer: it reads the analytics topic
// under a FIXED, env-supplied consumer group (at-least-once, offsets
// committed only after success) and folds each saga-health event into the
// analytical model through report.Projection.
//
// Outcomes per message:
//   - not a CloudEvents 1.0 message (garbage, the retired flat envelope):
//     skipped and committed past, with a rate-limited WARN;
//   - a valid CloudEvent of any other type: logged, ignored, committed past;
//   - a known type with an unusable payload, or one the store
//     deterministically rejects: dead-lettered at once (no pointless
//     retries), committed past;
//   - a transient failure (database down, timeout): the SAME message is
//     retried with capped exponential backoff and the offset is NOT committed
//     -- it is never dead-lettered, because a database outage must not turn
//     into silently missing analytics.
type AnalyticsConsumer struct {
	Reader     Reader
	Projection report.Projection
	DLQ        DeadLetterWriter
	// DLQTopic is recorded in the poison log line.
	DLQTopic string
	Logger   *slog.Logger
	Retry    RetryPolicy

	// Now is the clock of the skip-warning sampler (time.Now when nil).
	Now func() time.Time

	skips *skipSampler
	sleep sleepFunc // test hook; nil => real, ctx-cancellable sleep
}

// NewAnalyticsConsumer constructs the consumer for topic under groupID (an
// env-configured value from the composition root, never a literal). Its
// dead-letter writer targets topic + DLQSuffix. Nothing dials Kafka here.
func NewAnalyticsConsumer(brokers []string, topic, groupID string, projection report.Projection, logger *slog.Logger) *AnalyticsConsumer {
	return &AnalyticsConsumer{
		Reader:     kafkago.NewReader(readerConfig(brokers, topic, groupID)),
		Projection: projection,
		DLQ:        newDLQWriter(brokers, topic+DLQSuffix),
		DLQTopic:   topic + DLQSuffix,
		Logger:     defaultLogger(logger),
	}
}

// Run consumes until ctx is cancelled or the reader fails.
func (c *AnalyticsConsumer) Run(ctx context.Context) error {
	loop := consumeLoop{
		reader: c.Reader,
		handle: c.handle,
		logger: defaultLogger(c.Logger),
		name:   "analytics consumer",
		retry:  c.Retry,
		sleep:  c.sleep,
	}
	return loop.run(ctx)
}

// Close releases the reader and the dead-letter writer.
func (c *AnalyticsConsumer) Close() error {
	err := c.Reader.Close()
	if c.DLQ != nil {
		err = errors.Join(err, c.DLQ.Close())
	}
	return err
}

// HandleMessage processes one message; see the type comment for outcomes. A
// non-nil error is always transient.
func (c *AnalyticsConsumer) HandleMessage(ctx context.Context, msg kafkago.Message) error {
	return c.handle(ctx, msg)
}

func (c *AnalyticsConsumer) handle(ctx context.Context, msg kafkago.Message) error {
	evt, err := cloudevents.Decode(msg.Value)
	if err != nil {
		c.skipped(ctx, msg, err)
		return nil
	}
	kind, known := analyticsKinds[evt.Type()]
	if !known {
		defaultLogger(c.Logger).InfoContext(ctx, "analytics: skipping event of an unknown type (additive evolution of the stream)",
			"type", evt.Type(), "event_id", evt.ID(), "partition", msg.Partition, "offset", msg.Offset)
		return nil
	}
	event, err := toEvent(kind, evt)
	if err != nil {
		return c.deadLetter(ctx, msg, err)
	}
	if _, err := c.Projection.Apply(ctx, event); err != nil {
		if errors.Is(err, report.ErrRejected) {
			return c.deadLetter(ctx, msg, err)
		}
		return fmt.Errorf("analytics: project %s %s: %w", kind, evt.ID(), err)
	}
	return nil
}

// toEvent validates a decoded CloudEvent of a known kind and maps it to a
// report.Event. Every error is deterministic (the same bytes can never pass).
func toEvent(kind report.Kind, evt ce.Event) (report.Event, error) {
	var d sagaData
	if err := evt.DataAs(&d); err != nil {
		return report.Event{}, fmt.Errorf("decode %s data: %w", kind, err)
	}
	if evt.Time().IsZero() {
		return report.Event{}, fmt.Errorf("%s %s: time is required", kind, evt.ID())
	}
	e := report.Event{Kind: kind, EventID: evt.ID(), At: evt.Time().UTC()}
	var err error
	switch kind {
	case report.KindStateAdvanced:
		err = fillAdvanced(&e, d, evt.Subject())
	case report.KindStuckDetected:
		err = fillStuck(&e, d, evt.Subject())
	case report.KindRebalanceRunCompleted:
		err = fillRun(&e, d, evt.Subject())
	}
	if err != nil {
		return report.Event{}, fmt.Errorf("%s %s: %w", kind, evt.ID(), err)
	}
	return e, nil
}

func fillAdvanced(e *report.Event, d sagaData, subject string) error {
	switch {
	case d.TransferID == "" || d.TransferID != subject:
		return fmt.Errorf("transfer_id %q does not match subject %q", d.TransferID, subject)
	case d.To == "":
		return errors.New("to is required")
	case d.AgeSeconds == nil || *d.AgeSeconds < 0:
		return errors.New("age_seconds is required and must not be negative")
	case d.DwellSeconds != nil && *d.DwellSeconds < 0:
		return errors.New("dwell_seconds must not be negative")
	}
	e.TransferID, e.From, e.To, e.AgeSeconds = d.TransferID, d.From, d.To, *d.AgeSeconds
	// dwell_seconds is optional (additive within v1): an event from before
	// the field existed, or a creation entry, projects as NULL.
	e.DwellSeconds = d.DwellSeconds
	return nil
}

func fillStuck(e *report.Event, d sagaData, subject string) error {
	switch {
	case d.TransferID == "" || d.TransferID != subject:
		return fmt.Errorf("transfer_id %q does not match subject %q", d.TransferID, subject)
	case d.State == "":
		return errors.New("state is required")
	case d.AgeSeconds == nil || *d.AgeSeconds < 0:
		return errors.New("age_seconds is required and must not be negative")
	case d.ThresholdSeconds != nil && *d.ThresholdSeconds < 0:
		return errors.New("threshold_seconds must not be negative")
	}
	e.TransferID, e.State, e.AgeSeconds = d.TransferID, d.State, *d.AgeSeconds
	if d.ThresholdSeconds != nil {
		e.ThresholdSeconds = *d.ThresholdSeconds
	}
	return nil
}

func fillRun(e *report.Event, d sagaData, subject string) error {
	switch {
	case d.RunID == "" || d.RunID != subject:
		return fmt.Errorf("run_id %q does not match subject %q", d.RunID, subject)
	case d.ProposalCount == nil || *d.ProposalCount < 0:
		return errors.New("proposal_count is required and must not be negative")
	case d.RejectedCount == nil || *d.RejectedCount < 0:
		return errors.New("rejected_count is required and must not be negative")
	case d.StaleFacts != nil && *d.StaleFacts < 0:
		return errors.New("stale_facts must not be negative")
	}
	e.RunID, e.ProposalCount, e.RejectedCount = d.RunID, *d.ProposalCount, *d.RejectedCount
	if d.StaleFacts != nil {
		e.StaleFacts = *d.StaleFacts
	}
	return nil
}

// skipped logs a skipped non-CloudEvents message, sampled.
func (c *AnalyticsConsumer) skipped(ctx context.Context, msg kafkago.Message, cause error) {
	if c.skips == nil {
		now := c.Now
		if now == nil {
			now = time.Now
		}
		c.skips = &skipSampler{every: skipWarnEvery, now: now}
	}
	if emit, suppressed := c.skips.allow(); emit {
		defaultLogger(c.Logger).WarnContext(ctx, "analytics: skipping message that is not a CloudEvents 1.0 event (further skips are counted, not logged, until the next interval)",
			"error", cause, "topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset, "suppressed_since_last_warning", suppressed)
	}
}

// deadLetter publishes the raw, unmodified message to the DLQ with error
// context in headers, then lets the loop commit past it. A DLQ write failure
// is returned (transient: the loop retries the message) so a poison message
// is never lost.
func (c *AnalyticsConsumer) deadLetter(ctx context.Context, msg kafkago.Message, cause error) error {
	defaultLogger(c.Logger).ErrorContext(ctx, "analytics: dead-lettering a message that can never be projected",
		"error", cause, "dlq_topic", c.DLQTopic, "partition", msg.Partition, "offset", msg.Offset)
	headers := append([]kafkago.Header{}, msg.Headers...)
	headers = append(headers,
		kafkago.Header{Key: "x-dlq-source-topic", Value: []byte(msg.Topic)},
		kafkago.Header{Key: "x-dlq-error", Value: []byte(cause.Error())},
		kafkago.Header{Key: "x-dlq-failed-at", Value: []byte(time.Now().UTC().Format(time.RFC3339))},
	)
	if err := writeDLQ(ctx, c.DLQ, kafkago.Message{Key: msg.Key, Value: msg.Value, Headers: headers}); err != nil {
		return fmt.Errorf("analytics: publish to dead-letter topic %s: %w", c.DLQTopic, err)
	}
	return nil
}

// skipSampler lets the first call through, then at most one per interval,
// reporting how many were suppressed in between.
type skipSampler struct {
	mu         sync.Mutex
	every      time.Duration
	now        func() time.Time
	last       time.Time
	suppressed int64
}

func (s *skipSampler) allow() (emit bool, suppressed int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	if s.last.IsZero() || now.Sub(s.last) >= s.every {
		suppressed, s.suppressed, s.last = s.suppressed, 0, now
		return true, suppressed
	}
	s.suppressed++
	return false, 0
}

// newDLQWriter builds the dead-letter writer. AllowAutoTopicCreation: the
// fleet leaves topic creation to the producing writer and the DLQ topic is
// only ever written on the rare poison path. A short BatchTimeout keeps a
// synchronous single-message write from waiting out kafka-go's 1s default.
func newDLQWriter(brokers []string, dlqTopic string) DeadLetterWriter {
	return &kafkago.Writer{
		Addr:                   kafkago.TCP(brokers...),
		Topic:                  dlqTopic,
		AllowAutoTopicCreation: true,
		BatchTimeout:           10 * time.Millisecond,
		// A private Transport: kafka-go's DefaultTransport caches metadata
		// process-wide, which hides a just-created DLQ topic from the writer.
		Transport: &kafkago.Transport{},
	}
}

const (
	dlqTopicReadyAttempts = 40
	dlqTopicReadyBackoff  = 250 * time.Millisecond
)

// writeDLQ publishes msg, retrying (bounded) while the auto-created DLQ topic
// has no leader yet. Any other error, or exhausting the budget, is returned.
func writeDLQ(ctx context.Context, w DeadLetterWriter, msg kafkago.Message) error {
	var err error
	for attempt := 0; attempt < dlqTopicReadyAttempts; attempt++ {
		if err = w.WriteMessages(ctx, msg); err == nil || !isTopicNotReady(err) {
			return err
		}
		select {
		case <-ctx.Done():
			return errors.Join(err, ctx.Err())
		case <-time.After(dlqTopicReadyBackoff):
		}
	}
	return err
}

func isTopicNotReady(err error) bool {
	var werrs kafkago.WriteErrors
	if errors.As(err, &werrs) {
		for _, e := range werrs {
			if e != nil && !isTopicNotReady(e) {
				return false
			}
		}
		return werrs.Count() > 0
	}
	return errors.Is(err, kafkago.UnknownTopicOrPartition) || errors.Is(err, kafkago.LeaderNotAvailable)
}
