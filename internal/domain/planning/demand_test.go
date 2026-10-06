package planning

import (
	"testing"
	"time"
)

func demandTime() time.Time {
	return time.Date(2026, 10, 6, 9, 30, 0, 0, time.UTC).Truncate(time.Microsecond)
}

func TestNewSiteSkuDemandRejectsInvalidInput(t *testing.T) {
	cases := []struct {
		name   string
		source string
		line   int
		site   string
		sku    string
		units  int
		state  DemandState
	}{
		{"empty source order", "", 1, "WH1", "SKU-1", 5, DemandActive},
		{"zero line", "ord-1", 0, "WH1", "SKU-1", 5, DemandActive},
		{"negative line", "ord-1", -2, "WH1", "SKU-1", 5, DemandActive},
		{"empty site", "ord-1", 1, "", "SKU-1", 5, DemandActive},
		{"empty sku", "ord-1", 1, "WH1", "", 5, DemandActive},
		{"negative units", "ord-1", 1, "WH1", "SKU-1", -1, DemandActive},
		{"unknown state", "ord-1", 1, "WH1", "SKU-1", 5, DemandState("WOBBLE")},
		{"zero units active", "ord-1", 1, "WH1", "SKU-1", 0, DemandActive},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewSiteSkuDemand(tc.source, tc.line, tc.site, tc.sku, tc.units, demandTime(), tc.state, "static-site-v1"); err == nil {
				t.Fatalf("NewSiteSkuDemand(%+v) succeeded, want error", tc)
			}
		})
	}
}

func TestSiteSkuDemandRemovedAllowsZeroUnits(t *testing.T) {
	d, err := NewSiteSkuDemand("ord-1", 1, "WH1", "SKU-1", 0, demandTime(), DemandRemoved, "static-site-v1")
	if err != nil {
		t.Fatalf("REMOVED with zero units must be valid: %v", err)
	}
	if d.Active() {
		t.Fatal("REMOVED state must not be active")
	}
}

func TestSiteSkuDemandKeyAndWindow(t *testing.T) {
	d, err := NewSiteSkuDemand("ord-1", 3, "WH1", "SKU-1", 7, demandTime(), DemandActive, "static-site-v1")
	if err != nil {
		t.Fatal(err)
	}
	if d.SourceKey() != "ord-1#3" {
		t.Fatalf("SourceKey = %q", d.SourceKey())
	}
	if !d.DueIn(demandTime(), demandTime().Add(time.Hour)) {
		t.Fatal("due_at exactly at window start must be inside [start,end)")
	}
	if d.DueIn(demandTime().Add(time.Hour), demandTime().Add(2*time.Hour)) {
		t.Fatal("due_at at window end must be OUTSIDE [start,end) (exclusive)")
	}
}
