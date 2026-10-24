package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/network-inventory-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/network-inventory-planning/internal/application/usecases"
	"github.com/claudioed/network-inventory-planning/internal/domain/packlevel"
)

var packOccurred = time.Date(2026, 10, 6, 21, 0, 0, 0, time.UTC)

func newPackConsumer(view *memory.PackLevelView, claims *fakeClaims, dlq DeadLetterWriter) *PackLevelConsumer {
	return &PackLevelConsumer{
		Applier:         usecases.ApplyPackLevel{PackLevels: view},
		ProcessedEvents: claims,
		UoW:             passUoW{claims: claims},
		Logger:          quietLogger(),
		DLQ:             dlq,
		DLQTopic:        ProductMasterTopic + DLQSuffix,
	}
}

func declaredValue(t *testing.T, sku string, units, version int64) []byte {
	t.Helper()
	return encodeEvent(t, typePackLevelDeclared, sku, packOccurred.Add(time.Duration(version)*time.Second), packLevelDeclaredData{
		SKU: sku, Level: "CASE", GTIN: "24006381333935", UnitsPerPack: units,
		EffectiveFrom: "2026-10-06T20:00:00Z", Version: version,
	})
}

func retiredValue(t *testing.T, sku string, version int64) []byte {
	t.Helper()
	return encodeEvent(t, typePackLevelRetired, sku, packOccurred.Add(time.Duration(version)*time.Second), packLevelRetiredData{
		SKU: sku, Level: "CASE", GTIN: "24006381333935", EffectiveTo: "2026-10-06T21:01:00Z", Version: version,
	})
}

func TestPackLevelConsumerMirrorsDeclaredAndRetired(t *testing.T) {
	view := memory.NewPackLevelView()
	c := newPackConsumer(view, newFakeClaims(), nil)
	ctx := context.Background()

	if err := c.HandleMessage(ctx, declaredValue(t, "SKU-1", 12, 3)); err != nil {
		t.Fatal(err)
	}
	got, ok, _ := view.Get(ctx, "SKU-1", packlevel.Case)
	if !ok || got.UnitsPerPack != 12 || got.Version != 3 || got.GTIN != "24006381333935" ||
		!got.EffectiveFrom.Equal(time.Date(2026, 10, 6, 20, 0, 0, 0, time.UTC)) || got.EffectiveTo != nil {
		t.Fatalf("declared not mirrored: %+v ok=%v", got, ok)
	}
	if err := c.HandleMessage(ctx, retiredValue(t, "SKU-1", 4)); err != nil {
		t.Fatal(err)
	}
	got, _, _ = view.Get(ctx, "SKU-1", packlevel.Case)
	if got.EffectiveTo == nil || !got.EffectiveTo.Equal(time.Date(2026, 10, 6, 21, 1, 0, 0, time.UTC)) || got.Version != 4 {
		t.Fatalf("retired not mirrored: %+v", got)
	}
}

func TestPackLevelConsumerIsIdempotentByEventID(t *testing.T) {
	view := memory.NewPackLevelView()
	c := newPackConsumer(view, newFakeClaims(), nil)
	ctx := context.Background()
	v1 := declaredValue(t, "SKU-1", 12, 3)
	if err := c.HandleMessage(ctx, v1); err != nil {
		t.Fatal(err)
	}
	// a newer level, then the OLD message redelivered: the claim makes it a
	// no-op (and the version gate would have refused it anyway).
	if err := c.HandleMessage(ctx, declaredValue(t, "SKU-1", 24, 5)); err != nil {
		t.Fatal(err)
	}
	if err := c.HandleMessage(ctx, v1); err != nil {
		t.Fatal(err)
	}
	got, _, _ := view.Get(ctx, "SKU-1", packlevel.Case)
	if got.UnitsPerPack != 24 || got.Version != 5 {
		t.Fatalf("got %+v", got)
	}
}

func TestPackLevelConsumerIgnoresOtherTypesAndGarbage(t *testing.T) {
	view := memory.NewPackLevelView()
	dlq := &anFakeDLQ{}
	c := newPackConsumer(view, newFakeClaims(), dlq)
	ctx := context.Background()
	other := encodeEvent(t, "com.warehouse.wms.product-master.product.ProductPackLevelMeasured", "SKU-1", packOccurred, map[string]any{"sku": "SKU-1"})
	for name, value := range map[string][]byte{"other type": other, "not CloudEvents": []byte(`{"sku":"SKU-1"}`)} {
		if err := c.HandleMessage(ctx, value); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
	if _, ok, _ := view.Get(ctx, "SKU-1", packlevel.Case); ok || len(dlq.written()) != 0 {
		t.Fatal("neither an unrelated type nor garbage may touch the view or the DLQ")
	}
}

func TestPackLevelConsumerDeadLettersPoison(t *testing.T) {
	poison := map[string][]byte{
		"declared with a blank gtin": encodeEvent(t, typePackLevelDeclared, "SKU-1", packOccurred, packLevelDeclaredData{
			SKU: "SKU-1", Level: "CASE", GTIN: "", UnitsPerPack: 12, EffectiveFrom: "2026-10-06T20:00:00Z", Version: 1}),
		"declared with zero units": encodeEvent(t, typePackLevelDeclared, "SKU-1", packOccurred, packLevelDeclaredData{
			SKU: "SKU-1", Level: "CASE", GTIN: "g", UnitsPerPack: 0, EffectiveFrom: "2026-10-06T20:00:00Z", Version: 1}),
		"declared with an unparsable date": encodeEvent(t, typePackLevelDeclared, "SKU-1", packOccurred, packLevelDeclaredData{
			SKU: "SKU-1", Level: "CASE", GTIN: "g", UnitsPerPack: 12, EffectiveFrom: "yesterday", Version: 1}),
		"declared with a payload that is not an object": encodeEvent(t, typePackLevelDeclared, "SKU-1", packOccurred, []int{1, 2}),
		"retired with an unparsable date": encodeEvent(t, typePackLevelRetired, "SKU-1", packOccurred, packLevelRetiredData{
			SKU: "SKU-1", Level: "CASE", GTIN: "g", EffectiveTo: "", Version: 2}),
		"retired with no gtin": encodeEvent(t, typePackLevelRetired, "SKU-1", packOccurred, packLevelRetiredData{
			SKU: "SKU-1", Level: "CASE", GTIN: "", EffectiveTo: "2026-10-06T21:01:00Z", Version: 2}),
		"retired with a payload that is not an object": encodeEvent(t, typePackLevelRetired, "SKU-1", packOccurred, []int{1}),
	}
	for name, value := range poison {
		t.Run(name, func(t *testing.T) {
			view := memory.NewPackLevelView()
			dlq := &anFakeDLQ{}
			c := newPackConsumer(view, newFakeClaims(), dlq)
			msg := kafkago.Message{Topic: ProductMasterTopic, Key: []byte("SKU-1"), Value: value}
			if err := c.HandleKafkaMessage(context.Background(), msg); err != nil {
				t.Fatalf("poison must be skipped (nil) after the dead letter, got %v", err)
			}
			got := dlq.written()
			if len(got) != 1 || string(got[0].Value) != string(value) || string(got[0].Key) != "SKU-1" {
				t.Fatalf("DLQ = %+v, want the raw message once", got)
			}
			if dlqHeader(got[0], "x-dlq-source-topic") != ProductMasterTopic || dlqHeader(got[0], "x-dlq-consumer") != packLevelConsumerName ||
				dlqHeader(got[0], "x-dlq-error") == "" {
				t.Fatalf("DLQ headers = %v", got[0].Headers)
			}
			if _, ok, _ := view.Get(context.Background(), "SKU-1", packlevel.Case); ok {
				t.Fatal("poison must not reach the view")
			}
		})
	}
}

func TestPackLevelConsumerRetriesAFailedDeadLetterWrite(t *testing.T) {
	dlq := &anFakeDLQ{fail: 1}
	claims := newFakeClaims()
	c := newPackConsumer(memory.NewPackLevelView(), claims, dlq)
	value := encodeEvent(t, typePackLevelDeclared, "SKU-1", packOccurred, packLevelDeclaredData{
		SKU: "SKU-1", Level: "CASE", GTIN: "", UnitsPerPack: 12, EffectiveFrom: "2026-10-06T20:00:00Z", Version: 1})
	msg := kafkago.Message{Topic: ProductMasterTopic, Value: value}
	if err := c.HandleKafkaMessage(context.Background(), msg); err == nil {
		t.Fatal("a failed DLQ write must be returned so the loop retries")
	}
	if err := c.HandleKafkaMessage(context.Background(), msg); err != nil {
		t.Fatalf("retry after the DLQ recovered: %v", err)
	}
	if len(dlq.written()) != 1 {
		t.Fatalf("DLQ = %d, want 1", len(dlq.written()))
	}
}

// failingView fails every read with a transient error.
type failingView struct{ *memory.PackLevelView }

func (failingView) Get(context.Context, string, packlevel.Level) (packlevel.PackLevel, bool, error) {
	return packlevel.PackLevel{}, false, errors.New("database is down")
}

func TestPackLevelConsumerRetriesTransientFailuresAndNeverDeadLettersThem(t *testing.T) {
	dlq := &anFakeDLQ{}
	claims := newFakeClaims()
	c := &PackLevelConsumer{
		Applier:         usecases.ApplyPackLevel{PackLevels: failingView{memory.NewPackLevelView()}},
		ProcessedEvents: claims,
		UoW:             passUoW{claims: claims},
		Logger:          quietLogger(),
		DLQ:             dlq,
	}
	value := declaredValue(t, "SKU-1", 12, 3)
	if err := c.HandleMessage(context.Background(), value); err == nil {
		t.Fatal("a transient failure must be returned so the same message is retried")
	}
	if len(dlq.written()) != 0 {
		t.Fatal("a transient failure must never be dead-lettered")
	}
	// the claim rolled back with the unit of work: the retry is processed, not skipped.
	c.Applier = usecases.ApplyPackLevel{PackLevels: memory.NewPackLevelView()}
	if err := c.HandleMessage(context.Background(), value); err != nil {
		t.Fatalf("retry: %v", err)
	}
}

func TestPackLevelConsumerRunLoopCommitsAfterHandling(t *testing.T) {
	view := memory.NewPackLevelView()
	claims := newFakeClaims()
	reader := &fakeReader{messages: []kafkago.Message{{Value: declaredValue(t, "SKU-1", 12, 3), Offset: 7}}}
	c := newPackConsumer(view, claims, nil)
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
			break
		}
		select {
		case <-deadline:
			cancel()
			t.Fatal("never committed")
		case <-time.After(10 * time.Millisecond):
		}
	}
	cancel()
	<-done
	if got, ok, _ := view.Get(context.Background(), "SKU-1", packlevel.Case); !ok || got.UnitsPerPack != 12 {
		t.Fatalf("view = %+v", got)
	}
	if err := c.Close(); err != nil || !reader.closed {
		t.Fatalf("close = %v closed=%v", err, reader.closed)
	}
}

func TestReaderConfigUsesAShortMaxWait(t *testing.T) {
	cfg := readerConfig([]string{"b:9092"}, ProductMasterTopic, "g")
	if cfg.MaxWait != 500*time.Millisecond {
		t.Fatalf("MaxWait = %s, want 500ms (graceful-stop budget)", cfg.MaxWait)
	}
	if cfg.GroupID != "g" || cfg.Topic != ProductMasterTopic {
		t.Fatalf("cfg = %+v", cfg)
	}
}
