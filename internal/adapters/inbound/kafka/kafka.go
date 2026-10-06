// Package kafka holds this service's INBOUND Kafka consumers (Phase 1):
// SiteCapabilityConsumer (facility-layout's SiteCapabilityChanged),
// SiteSkuDemandConsumer (order-management's SiteSkuDemandChanged) and
// CapacityPlanConsumer (warehouse-planning's CapacityPlanPublished). All
// three decode CloudEvents 1.0 only via internal/adapters/kafka/cloudevents,
// dispatch on the FULL `type` string, and ignore unknown types.
//
// Delivery guarantee: AT-LEAST-ONCE with an ATOMIC effect. Each message is
// handled inside ONE ports.UnitOfWork (processed-event claim + read-model
// upsert commit or roll back together) and its offset is committed only
// AFTER HandleMessage returned nil. HandleMessage returns a non-nil error
// ONLY for transient/infrastructure failures; the run loop then retries
// the SAME message with capped exponential backoff and never commits past
// it. Deterministic problems (not CloudEvents, unknown type, malformed
// payload, missing fields, domain validation failure, legacy plan without
// site_id) return nil and are logged, never retried.
package kafka

import (
	"context"
	"log/slog"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

// Reader is the subset of *kafkago.Reader a consumer needs, so unit tests
// never need a live broker. FetchMessage/CommitMessages (not ReadMessage)
// because ReadMessage with a GroupID auto-commits the offset the moment it
// returns, i.e. before the message has been handled -- a failed handling
// would then never be redelivered.
type Reader interface {
	FetchMessage(ctx context.Context) (kafkago.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafkago.Message) error
	Close() error
}

// readerConfig builds a kafka-go reader configuration shared by the three
// consumers. GroupID always comes from the caller (an env-configured value
// at the composition root -- never a literal here), satisfying
// internal/architecture's TestKafkaConsumerGroupNeverHardcodedInline.
//
// CommitInterval is deliberately left UNSET: with a GroupID kafka-go then
// commits synchronously inside CommitMessages, which the run loop calls
// only after a message was handled successfully.
func readerConfig(brokers []string, topic, groupID string) kafkago.ReaderConfig {
	return kafkago.ReaderConfig{
		Brokers: brokers,
		Topic:   topic,
		GroupID: groupID,
	}
}

// RetryPolicy is the capped exponential backoff the run loop applies while
// the SAME message keeps failing with a transient error.
type RetryPolicy struct {
	Initial time.Duration
	Max     time.Duration
}

// Default backoff: 200ms, 400ms, 800ms ... capped at 5s.
const (
	DefaultRetryInitial = 200 * time.Millisecond
	DefaultRetryMax     = 5 * time.Second
)

func (p RetryPolicy) withDefaults() RetryPolicy {
	if p.Initial <= 0 {
		p.Initial = DefaultRetryInitial
	}
	if p.Max <= 0 {
		p.Max = DefaultRetryMax
	}
	return p
}

// next doubles d, capped at p.Max.
func (p RetryPolicy) next(d time.Duration) time.Duration {
	d *= 2
	if d > p.Max {
		return p.Max
	}
	return d
}

// sleepFunc waits d or until ctx is done. A field so tests can record the
// backoff sequence without really sleeping.
type sleepFunc func(ctx context.Context, d time.Duration) error

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// consumeLoop is the at-least-once run loop shared by the three
// consumers.
type consumeLoop struct {
	reader Reader
	handle func(ctx context.Context, msg kafkago.Message) error
	logger *slog.Logger
	name   string
	retry  RetryPolicy
	sleep  sleepFunc
}

// run fetches one message at a time and does not fetch the next until the
// current one was handled successfully AND its offset committed. It
// returns only when ctx is cancelled or FetchMessage itself fails.
func (l *consumeLoop) run(ctx context.Context) error {
	policy := l.retry.withDefaults()
	sleep := l.sleep
	if sleep == nil {
		sleep = sleepCtx
	}
	for {
		msg, err := l.reader.FetchMessage(ctx)
		if err != nil {
			return err
		}
		if err := l.retryUntilOK(ctx, policy, sleep, "handling", msg, func() error { return l.handle(ctx, msg) }); err != nil {
			return err
		}
		// Commit ONLY now: the work is already durable, and if the
		// process dies first the redelivery is skipped by the
		// processed-event claim.
		if err := l.retryUntilOK(ctx, policy, sleep, "offset commit", msg, func() error {
			return l.reader.CommitMessages(ctx, msg)
		}); err != nil {
			return err
		}
	}
}

// retryUntilOK runs op until it returns nil, sleeping with capped
// exponential backoff between attempts. It NEVER skips: a message that
// cannot be handled blocks its partition (and is logged at ERROR every
// attempt) rather than being dropped.
func (l *consumeLoop) retryUntilOK(ctx context.Context, p RetryPolicy, sleep sleepFunc, what string, msg kafkago.Message, op func() error) error {
	delay := p.Initial
	for attempt := 1; ; attempt++ {
		err := op()
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		l.logger.ErrorContext(ctx, l.name+" "+what+" failed; retrying the same message",
			"error", err, "partition", msg.Partition, "offset", msg.Offset,
			"attempt", attempt, "retry_in", delay)
		if err := sleep(ctx, delay); err != nil {
			return err
		}
		delay = p.next(delay)
	}
}

func defaultLogger(logger *slog.Logger) *slog.Logger {
	if logger == nil {
		return slog.Default()
	}
	return logger
}
