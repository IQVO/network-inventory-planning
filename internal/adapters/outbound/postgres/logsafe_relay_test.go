package postgres

import (
	"errors"
	"log/slog"
	"testing"

	outboundkafka "github.com/claudioed/network-inventory-planning/internal/adapters/outbound/kafka"
)

// TestRelayFailureLogsNoEventDerivedLineBreaks: the send-failed and
// attempt-cap logs of the outbox relay must carry no line break from the
// row's event type and key or from the send error.
func TestRelayFailureLogsNoEventDerivedLineBreaks(t *testing.T) {
	rec := newCRLFRecorder()
	r := &OutboxRelay{logger: slog.New(rec), maxAttempts: 3}
	row := OutboxMessage{ID: 1, Encoded: outboundkafka.Encoded{Topic: "logsafe.topic", EventType: "logsafe.Evil" + crlfForged, Key: []byte("key" + crlfForged)}}
	sendErr := errors.New("broker refused" + crlfForged)
	r.logFailure(row, 1, sendErr)
	r.logFailure(row, 3, sendErr)
	rec.assertNoLineBreaks(t, 2)
}
