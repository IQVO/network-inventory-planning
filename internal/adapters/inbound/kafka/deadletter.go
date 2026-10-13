package kafka

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

// deadLetterSink is the transfer consumers' poison-message path (ADR 0013,
// the fleet's "<topic>.dlq" convention already used by the analytics
// projector): the RAW message — key, value and headers untouched — is
// published to the DLQ topic with the failure context in x-dlq-* headers,
// then the consumer commits past it. A nil writer (unit tests, a consumer
// built without brokers) only logs.
type deadLetterSink struct {
	writer   DeadLetterWriter
	topic    string
	consumer string
	logger   *slog.Logger
}

// send publishes msg to the DLQ. A write failure is returned so the run
// loop retries the message (the processed-event claim rolls back with the
// unit of work): a poison message is never silently lost.
func (d deadLetterSink) send(ctx context.Context, msg kafkago.Message, cause error) error {
	defaultLogger(d.logger).ErrorContext(ctx, d.consumer+": dead-lettering a message that can never be applied",
		"error", cause, "dlq_topic", d.topic, "source_topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset)
	if d.writer == nil {
		return nil
	}
	headers := append([]kafkago.Header{}, msg.Headers...)
	headers = append(headers,
		kafkago.Header{Key: "x-dlq-source-topic", Value: []byte(msg.Topic)},
		kafkago.Header{Key: "x-dlq-consumer", Value: []byte(d.consumer)},
		kafkago.Header{Key: "x-dlq-error", Value: []byte(cause.Error())},
		kafkago.Header{Key: "x-dlq-failed-at", Value: []byte(time.Now().UTC().Format(time.RFC3339))},
	)
	if err := writeDLQ(ctx, d.writer, kafkago.Message{Key: msg.Key, Value: msg.Value, Headers: headers}); err != nil {
		return fmt.Errorf("%s: publish to dead-letter topic %s: %w", d.consumer, d.topic, err)
	}
	return nil
}
