package telemetry

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/claudioed/network-inventory-planning/internal/application/ports"
)

// newTestMetrics builds a TransferMetrics over a private ManualReader (no
// global state touched) and returns a collector for its counters.
func newTestMetrics(t *testing.T) (*TransferMetrics, func() map[string]map[string]int64) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })

	m, err := newTransferMetrics(provider.Meter(meterName))
	if err != nil {
		t.Fatalf("newTransferMetrics: %v", err)
	}
	return m, func() map[string]map[string]int64 { return collectCounters(t, reader) }
}

// collectCounters returns counter name -> "key=value[,key=value]" -> value.
func collectCounters(t *testing.T, reader sdkmetric.Reader) map[string]map[string]int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect: %v", err)
	}
	out := map[string]map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, md := range sm.Metrics {
			sum, ok := md.Data.(metricdata.Sum[int64])
			if !ok {
				continue
			}
			series := map[string]int64{}
			for _, dp := range sum.DataPoints {
				series[attrKey(dp.Attributes)] += dp.Value
			}
			out[md.Name] = series
		}
	}
	return out
}

func attrKey(set attribute.Set) string {
	key := ""
	for iter := set.Iter(); iter.Next(); {
		kv := iter.Attribute()
		if key != "" {
			key += ","
		}
		key += string(kv.Key) + "=" + kv.Value.AsString()
	}
	return key
}

func TestTransferApprovedCountsEachOutcome(t *testing.T) {
	m, collect := newTestMetrics(t)
	ctx := context.Background()

	m.TransferApproved(ctx, ports.ApprovalApproved)
	m.TransferApproved(ctx, ports.ApprovalApproved)
	m.TransferApproved(ctx, ports.ApprovalReplayed)
	m.TransferApproved(ctx, ports.ApprovalRefused)
	m.TransferApproved(ctx, "not-in-the-vocabulary") // must collapse, not mint a series

	got := collect()[transfersApprovedCounterName]
	want := map[string]int64{
		"outcome=approved": 2,
		"outcome=replayed": 1,
		"outcome=refused":  1,
		"outcome=other":    1,
	}
	assertSeries(t, transfersApprovedCounterName, got, want)
}

func TestTransferStateAdvancedCountsByTargetState(t *testing.T) {
	m, collect := newTestMetrics(t)
	ctx := context.Background()

	m.TransferStateAdvanced(ctx, "ALLOCATING")
	m.TransferStateAdvanced(ctx, "ALLOCATED")
	m.TransferStateAdvanced(ctx, "ALLOCATED")
	m.TransferStateAdvanced(ctx, "")

	assertSeries(t, stateAdvancedCounterName, collect()[stateAdvancedCounterName], map[string]int64{
		"to=ALLOCATING": 1,
		"to=ALLOCATED":  2,
		"to=other":      1,
	})
}

func TestOutboxRelayedCountsEachOutcome(t *testing.T) {
	m, collect := newTestMetrics(t)
	ctx := context.Background()

	m.OutboxRelayed(ctx, ports.RelayPublished)
	m.OutboxRelayed(ctx, ports.RelayPublished)
	m.OutboxRelayed(ctx, ports.RelayFailed)
	m.OutboxRelayed(ctx, "boom: broker unreachable at 10.0.0.1") // free text must never become a label

	assertSeries(t, outboxRelayedCounterName, collect()[outboxRelayedCounterName], map[string]int64{
		"outcome=published": 2,
		"outcome=failed":    1,
		"outcome=other":     1,
	})
}

// TestNilTransferMetricsDoesNotPanic pins the documented nil-safe path.
func TestNilTransferMetricsDoesNotPanic(t *testing.T) {
	var m *TransferMetrics
	ctx := context.Background()
	m.TransferApproved(ctx, ports.ApprovalApproved)
	m.TransferStateAdvanced(ctx, "ALLOCATED")
	m.OutboxRelayed(ctx, ports.RelayPublished)
}

// TestNewTransferMetricsOnGlobalProviderIsSafeBeforeSetup: until Setup
// installs a real provider the global one is a no-op; recording is safe.
func TestNewTransferMetricsOnGlobalProviderIsSafeBeforeSetup(t *testing.T) {
	m, err := NewTransferMetrics()
	if err != nil {
		t.Fatalf("NewTransferMetrics: %v", err)
	}
	m.TransferApproved(context.Background(), ports.ApprovalApproved)
}

func assertSeries(t *testing.T, name string, got, want map[string]int64) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s series = %v, want %v", name, got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("%s series = %v, want %v", name, got, want)
		}
	}
}
