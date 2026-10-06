package planning

import (
	"testing"
	"time"
)

func planTime() time.Time {
	return time.Date(2026, 10, 6, 21, 45, 0, 0, time.UTC).Truncate(time.Microsecond)
}

func validPublishedPlan() PublishedCapacityPlan {
	windowStart := time.Date(2026, 10, 7, 8, 0, 0, 0, time.UTC)
	return PublishedCapacityPlan{
		PlanID:             "0b7a4c1e-5d52-4f0e-9a39-6c1f2f3a8b10",
		SiteID:             "WH1",
		Location:           "PATH-ZONE-A",
		PathID:             "pick-rebin-pack",
		WindowStart:        windowStart,
		WindowEnd:          windowStart.Add(8 * time.Hour),
		AssignedDemand:     12000,
		CapacityOverWindow: 8000,
		Shortage:           4000,
		PublishedAt:        planTime(),
	}
}

func TestNewPublishedCapacityPlanRejectsInvalidInput(t *testing.T) {
	cases := []struct {
		name string
		mut  func(*PublishedCapacityPlan)
	}{
		{"empty plan id", func(p *PublishedCapacityPlan) { p.PlanID = "" }},
		{"empty site id", func(p *PublishedCapacityPlan) { p.SiteID = "" }},
		{"empty location", func(p *PublishedCapacityPlan) { p.Location = "" }},
		{"empty path", func(p *PublishedCapacityPlan) { p.PathID = "" }},
		{"window end not after start", func(p *PublishedCapacityPlan) { p.WindowEnd = p.WindowStart }},
		{"negative capacity", func(p *PublishedCapacityPlan) { p.CapacityOverWindow = -1 }},
		{"negative shortage", func(p *PublishedCapacityPlan) { p.Shortage = -1 }},
		{"negative demand", func(p *PublishedCapacityPlan) { p.AssignedDemand = -1 }},
		{"zero published at", func(p *PublishedCapacityPlan) { p.PublishedAt = time.Time{} }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			bad := validPublishedPlan()
			tc.mut(&bad)
			if _, err := NewPublishedCapacityPlan(bad); err == nil {
				t.Fatalf("%s: NewPublishedCapacityPlan succeeded, want error", tc.name)
			}
		})
	}
}

func TestPublishedCapacityPlanCoversDueAt(t *testing.T) {
	p := validPublishedPlan()
	// due_at exactly at window start: inside
	if !p.Covers(p.WindowStart) {
		t.Fatal("due_at == window_start must be covered ([start,end))")
	}
	// due_at exactly at window end: outside (exclusive)
	if p.Covers(p.WindowEnd) {
		t.Fatal("due_at == window_end must NOT be covered ([start,end))")
	}
}

func TestPublishedCapacityPlanSupersedesOnCETime(t *testing.T) {
	older := validPublishedPlan()
	newer := validPublishedPlan()
	newer.Shortage = 100
	newer.PublishedAt = older.PublishedAt.Add(time.Minute)
	if !newer.Supersedes(older) {
		t.Fatal("a later CE time must supersede")
	}
	if older.Supersedes(newer) {
		t.Fatal("an earlier CE time must not supersede")
	}
	equal := validPublishedPlan()
	equal.Shortage = 999
	if equal.Supersedes(older) {
		t.Fatal("equal CE time must be a no-op even with different payload")
	}
}
