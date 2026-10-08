package analyticsstore_test

import (
	"context"
	"testing"

	"github.com/claudioed/network-inventory-planning/internal/adapters/outbound/analyticsstore"
	"github.com/claudioed/network-inventory-planning/internal/analytics/report"
)

func TestMemoryStore_SatisfiesTheStoreContract(t *testing.T) {
	runStoreContract(t, func(t *testing.T) (report.Projection, report.Reader) {
		m := analyticsstore.NewMemory()
		return m, m
	})
}

func TestMemoryStore_IgnoresAnUnknownKindButStillRecordsTheId(t *testing.T) {
	m := analyticsstore.NewMemory()
	e := report.Event{Kind: "Other", EventID: "x-1", At: at(5, 1, 0, 0)}
	if applied, err := m.Apply(context.Background(), e); err != nil || !applied {
		t.Fatalf("Apply = %v, %v", applied, err)
	}
	if applied, _ := m.Apply(context.Background(), e); applied {
		t.Fatal("a replay of an unknown-kind id must be a no-op")
	}
}
