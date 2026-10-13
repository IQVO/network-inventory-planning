package kafka

import (
	"testing"

	kafkago "github.com/segmentio/kafka-go"
)

// assertAcksAll fails unless w is a synchronous kafka-go Writer that waits
// for every in-sync replica: kafka-go defaults RequiredAcks to RequireNone
// (acks=0), so a broker blip would silently drop the poison message while
// the consumer commits its offset as if it had been dead-lettered.
func assertAcksAll(t *testing.T, dw DeadLetterWriter) {
	t.Helper()
	w, ok := dw.(*kafkago.Writer)
	if !ok {
		t.Fatalf("DLQ writer is %T, want *kafkago.Writer", dw)
	}
	t.Cleanup(func() { _ = w.Close() })
	if w.RequiredAcks != kafkago.RequireAll {
		t.Fatalf("DLQ RequiredAcks = %v, want RequireAll (kafka-go's default is RequireNone, acks=0)", w.RequiredAcks)
	}
	if w.Async {
		t.Fatal("DLQ writer must be synchronous so a failed dead-letter write reaches the consumer")
	}
}

func TestNewDLQWriter_WaitsForAllInSyncReplicas(t *testing.T) {
	assertAcksAll(t, newDLQWriter([]string{"localhost:9092"}, "analytics.dlq"))
}
