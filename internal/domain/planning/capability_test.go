package planning

import (
	"testing"
	"time"
)

func baseTime() time.Time {
	return time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC).Truncate(time.Microsecond)
}

func TestNewSiteCapabilityRejectsInvalidInput(t *testing.T) {
	cases := []struct {
		name string
		cap  SiteCapability
	}{
		{"empty site", SiteCapability{Site: "", Revision: 1}},
		{"zero revision", SiteCapability{Site: "WH1", Revision: 0}},
		{"negative revision", SiteCapability{Site: "WH1", Revision: -1}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewSiteCapability(tc.cap.Site, tc.cap.TransferOriginEnabled, tc.cap.TransferDestinationEnabled, tc.cap.Revision, baseTime()); err == nil {
				t.Fatalf("NewSiteCapability(%+v) succeeded, want error", tc.cap)
			}
		})
	}
}

func TestSiteCapabilitySupersedesOnStrictlyGreaterRevision(t *testing.T) {
	older := SiteCapability{Site: "WH1", Revision: 4, TransferOriginEnabled: true}
	newer := SiteCapability{Site: "WH1", Revision: 5}
	if !newer.Supersedes(older) {
		t.Fatal("revision 5 must supersede revision 4")
	}
	if older.Supersedes(newer) {
		t.Fatal("revision 4 must not supersede revision 5")
	}
	same := SiteCapability{Site: "WH1", Revision: 5, TransferDestinationEnabled: true}
	if same.Supersedes(newer) {
		t.Fatal("equal revision must be a no-op even with different flags")
	}
}

func TestSiteCapabilityDirectionGates(t *testing.T) {
	cap, err := NewSiteCapability("WH1", true, false, 7, baseTime())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !cap.OriginAllowed() {
		t.Fatal("transfer_origin_enabled=true must allow origin")
	}
	if cap.DestinationAllowed() {
		t.Fatal("transfer_destination_enabled=false must refuse destination")
	}
}
