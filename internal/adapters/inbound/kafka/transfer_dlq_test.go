package kafka

import (
	"context"
	"fmt"
	"testing"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/network-inventory-planning/internal/application/usecases"
	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

// refusingReplyApplier fails every reply with the given error.
type refusingReplyApplier struct {
	err   error
	calls int
}

func (r *refusingReplyApplier) ApplyAllocation(context.Context, usecases.ApplyAllocationInput) error {
	r.calls++
	return r.err
}

func (r *refusingReplyApplier) ApplyRejection(context.Context, usecases.ApplyRejectionInput) error {
	r.calls++
	return r.err
}

func (r *refusingReplyApplier) ApplyReceiptStaged(context.Context, usecases.ApplyReceiptStagedInput) error {
	r.calls++
	return r.err
}

func (r *refusingReplyApplier) ApplyStowed(context.Context, usecases.ApplyStowInput) error {
	r.calls++
	return r.err
}

func dlqReplyConsumer(applier TransferReplyApplier, claims *fakeClaims, dlq DeadLetterWriter) *TransferReplyConsumer {
	return &TransferReplyConsumer{
		Applier:         applier,
		ProcessedEvents: claims,
		UoW:             passUoW{claims: claims},
		Logger:          quietLogger(),
		DLQ:             dlq,
		DLQTopic:        InventoryTopic + DLQSuffix,
	}
}

func dlqHeader(msg kafkago.Message, key string) string {
	for _, h := range msg.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

// TestTransferReplyConsumer_RefusedReplyIsDeadLetteredNeverRetried is D4: a
// reply the aggregate refuses (line-id mismatch) is DETERMINISTIC. The
// consumer publishes the raw message to warehouse.inventory.events.dlq with
// error context, keeps the claim and returns nil so the loop commits past —
// the partition is never wedged behind it.
func TestTransferReplyConsumer_RefusedReplyIsDeadLetteredNeverRetried(t *testing.T) {
	applier := &refusingReplyApplier{err: fmt.Errorf("apply: %w", transfer.ErrReplyLineMismatch)}
	claims := newFakeClaims()
	dlq := &anFakeDLQ{}
	c := dlqReplyConsumer(applier, claims, dlq)

	msg := kafkago.Message{Topic: InventoryTopic, Key: []byte("trf-p:1"), Value: allocatedReplyValue(t, "trf-p")}
	if err := c.HandleKafkaMessage(context.Background(), msg); err != nil {
		t.Fatalf("a refused reply must be skipped (nil), got %v", err)
	}
	got := dlq.written()
	if len(got) != 1 {
		t.Fatalf("DLQ = %d messages, want 1", len(got))
	}
	if string(got[0].Value) != string(msg.Value) || string(got[0].Key) != "trf-p:1" {
		t.Fatalf("DLQ must carry the raw message: key %q", got[0].Key)
	}
	if dlqHeader(got[0], "x-dlq-source-topic") != InventoryTopic ||
		dlqHeader(got[0], "x-dlq-consumer") != transferReplyConsumerName ||
		dlqHeader(got[0], "x-dlq-error") == "" || dlqHeader(got[0], "x-dlq-failed-at") == "" {
		t.Fatalf("DLQ headers = %v", got[0].Headers)
	}
	// The claim stayed committed: a redelivery is a dedupe no-op, no second
	// apply and no second dead letter.
	if err := c.HandleKafkaMessage(context.Background(), msg); err != nil {
		t.Fatalf("redelivery: %v", err)
	}
	if applier.calls != 1 || len(dlq.written()) != 1 {
		t.Fatalf("redelivery re-applied (%d calls) or re-dead-lettered (%d)", applier.calls, len(dlq.written()))
	}
}

// TestTransferReplyConsumer_UnknownRejectionReasonIsDeadLettered proves the
// adapter-level closed-vocabulary check dead-letters too (same condition,
// same treatment as the domain's ErrUnknownRejectionReason).
func TestTransferReplyConsumer_UnknownRejectionReasonIsDeadLettered(t *testing.T) {
	applier := &fakeReplyApplier{}
	claims := newFakeClaims()
	dlq := &anFakeDLQ{}
	c := dlqReplyConsumer(applier, claims, dlq)

	msg := kafkago.Message{Topic: InventoryTopic, Value: rejectedReplyValue(t, "trf-q", "SOMETHING_ELSE")}
	if err := c.HandleKafkaMessage(context.Background(), msg); err != nil {
		t.Fatalf("unknown reason must be skipped, got %v", err)
	}
	if len(applier.rejections) != 0 {
		t.Fatalf("an unknown reason must never reach the saga")
	}
	if got := dlq.written(); len(got) != 1 || dlqHeader(got[0], "x-dlq-error") == "" {
		t.Fatalf("DLQ = %+v, want the raw rejection once", got)
	}
}

// TestTransferReplyConsumer_FailedDeadLetterWriteIsRetried: a DLQ outage
// must never lose the poison message — the error is returned (the loop
// retries), the claim rolls back, and the retry dead-letters it.
func TestTransferReplyConsumer_FailedDeadLetterWriteIsRetried(t *testing.T) {
	applier := &refusingReplyApplier{err: fmt.Errorf("apply: %w", transfer.ErrUnknownRejectionReason)}
	claims := newFakeClaims()
	dlq := &anFakeDLQ{fail: 1}
	c := dlqReplyConsumer(applier, claims, dlq)

	msg := kafkago.Message{Topic: InventoryTopic, Value: rejectedReplyValue(t, "trf-r", "INSUFFICIENT_USABLE")}
	if err := c.HandleKafkaMessage(context.Background(), msg); err == nil {
		t.Fatal("a failed DLQ write must be returned so the loop retries")
	}
	if err := c.HandleKafkaMessage(context.Background(), msg); err != nil {
		t.Fatalf("retry after the DLQ recovered: %v", err)
	}
	if applier.calls != 2 || len(dlq.written()) != 1 {
		t.Fatalf("calls = %d DLQ = %d, want 2 and 1", applier.calls, len(dlq.written()))
	}
}

// TestTransferReplyConsumer_BenignSkipIsNotDeadLettered: an unknown
// transfer or a replayed/out-of-order reply is a plain skip — it is not
// poison and must not flood the DLQ.
func TestTransferReplyConsumer_BenignSkipIsNotDeadLettered(t *testing.T) {
	for name, err := range map[string]error{
		"unknown transfer": transfer.ErrTransferNotFound,
		"replay":           &transfer.IllegalTransitionError{ID: "trf-s", From: transfer.StateAllocated, Action: "mark allocated"},
	} {
		t.Run(name, func(t *testing.T) {
			dlq := &anFakeDLQ{}
			c := dlqReplyConsumer(&refusingReplyApplier{err: err}, newFakeClaims(), dlq)
			msg := kafkago.Message{Topic: InventoryTopic, Value: allocatedReplyValue(t, "trf-s")}
			if herr := c.HandleKafkaMessage(context.Background(), msg); herr != nil {
				t.Fatalf("benign skip = %v", herr)
			}
			if len(dlq.written()) != 0 {
				t.Fatalf("a benign skip must not be dead-lettered")
			}
		})
	}
}

// TestTransferFactConsumer_RefusedFactIsDeadLettered applies the same rule
// to the fulfillment fact consumer: a fact the aggregate refuses
// (transfer.ErrFactRefused) is dead-lettered to
// warehouse.fulfillment.events.dlq and committed past.
func TestTransferFactConsumer_RefusedFactIsDeadLettered(t *testing.T) {
	claims := newFakeClaims()
	dlq := &anFakeDLQ{}
	c := &TransferFactConsumer{
		Applier:         &refusingFactApplier{err: fmt.Errorf("apply: %w", transfer.ErrFactRefused)},
		ProcessedEvents: claims,
		UoW:             passUoW{claims: claims},
		Logger:          quietLogger(),
		DLQ:             dlq,
		DLQTopic:        FulfillmentTopic + DLQSuffix,
	}
	value := encodeEvent(t, typeTransferDispatched, "task-1", replyOccurred, transferFactData{TransferRef: "trf-t", TaskID: "task-1", WorkKind: "TRANSFER_DISPATCH"})
	msg := kafkago.Message{Topic: FulfillmentTopic, Value: value}
	if err := c.HandleKafkaMessage(context.Background(), msg); err != nil {
		t.Fatalf("a refused fact must be skipped, got %v", err)
	}
	got := dlq.written()
	if len(got) != 1 || dlqHeader(got[0], "x-dlq-source-topic") != FulfillmentTopic ||
		dlqHeader(got[0], "x-dlq-consumer") != transferFactConsumerName {
		t.Fatalf("DLQ = %+v", got)
	}
}

type refusingFactApplier struct{ err error }

func (r *refusingFactApplier) ApplyPick(context.Context, usecases.ApplyPickInput) error { return r.err }
func (r *refusingFactApplier) ApplyDispatched(context.Context, usecases.FactInput) error {
	return r.err
}
func (r *refusingFactApplier) ApplyArrival(context.Context, usecases.FactInput) error { return r.err }
