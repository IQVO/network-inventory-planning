package report_test

import (
	"errors"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/claudioed/network-inventory-planning/internal/analytics/report"
)

var now = time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)

func TestParseRange_Defaults(t *testing.T) {
	cases := []struct {
		name, from, to string
		want           report.Range
	}{
		{"neither: the 30 days ending now", "", "", report.Range{From: now.Add(-30 * 24 * time.Hour), To: now}},
		{"only to: the 30 days before it", "", "2026-10-05T00:00:00Z", report.Range{From: time.Date(2026, 9, 5, 0, 0, 0, 0, time.UTC), To: time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC)}},
		{"only from: from..now", "2026-10-01T00:00:00Z", "", report.Range{From: time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC), To: now}},
		{"offsets are normalised to UTC", "2026-10-05T03:00:00+03:00", "2026-10-06T00:00:00Z", report.Range{From: time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC), To: time.Date(2026, 10, 6, 0, 0, 0, 0, time.UTC)}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := report.ParseRange(c.from, c.to, now)
			if err != nil || got != c.want || got.From.Location() != time.UTC || got.To.Location() != time.UTC {
				t.Fatalf("ParseRange = %+v, %v; want %+v", got, err, c.want)
			}
		})
	}
}

func TestParseRange_Rejections(t *testing.T) {
	cases := []struct {
		name, from, to string
		want           error
	}{
		{"bad from", "nope", "", report.ErrInvalidFrom},
		{"bad to", "", "2026-10-07", report.ErrInvalidTo},
		{"inverted", "2026-10-07T00:00:00Z", "2026-10-05T00:00:00Z", report.ErrEmptyRange},
		{"empty", "2026-10-05T00:00:00Z", "2026-10-05T00:00:00Z", report.ErrEmptyRange},
		{"one day over the cap", "2025-10-06T11:00:00Z", "2026-10-07T12:00:00Z", report.ErrRangeTooLarge},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := report.ParseRange(c.from, c.to, now); !errors.Is(err, c.want) {
				t.Fatalf("err = %v, want %v", err, c.want)
			}
		})
	}
}

func TestParseRange_TheCapIsInclusiveAtExactly366Days(t *testing.T) {
	to := time.Date(2026, 10, 7, 0, 0, 0, 0, time.UTC)
	from := to.Add(-report.MaxWindow())
	if _, err := report.ParseRange(from.Format(time.RFC3339), to.Format(time.RFC3339), now); err != nil {
		t.Fatalf("exactly 366 days must be allowed, got %v", err)
	}
	if _, err := report.ParseRange(from.Add(-time.Second).Format(time.RFC3339), to.Format(time.RFC3339), now); !errors.Is(err, report.ErrRangeTooLarge) {
		t.Fatalf("366 days + 1s must be rejected, got %v", err)
	}
	if report.DefaultWindow() != 30*24*time.Hour || report.MaxWindow() != 366*24*time.Hour {
		t.Fatal("window constants changed")
	}
}

func TestParseLimit(t *testing.T) {
	if n, err := report.ParseLimit(""); err != nil || n != 20 || n != report.DefaultLatest() {
		t.Fatalf("default = %d, %v; want 20", n, err)
	}
	for raw, want := range map[string]int{"1": 1, "20": 20, "100": 100} {
		if n, err := report.ParseLimit(raw); err != nil || n != want {
			t.Errorf("ParseLimit(%q) = %d, %v; want %d", raw, n, err, want)
		}
	}
	for _, raw := range []string{"0", "101", "-3", "x", "1.5", " 5"} {
		if _, err := report.ParseLimit(raw); !errors.Is(err, report.ErrInvalidLimit) {
			t.Errorf("ParseLimit(%q) err = %v, want ErrInvalidLimit", raw, err)
		}
	}
	if report.MaxLatest() != 100 {
		t.Fatal("MaxLatest changed")
	}
}

func TestRate(t *testing.T) {
	cases := []struct {
		num, den int
		want     float64
	}{{1, 4, 0.25}, {0, 5, 0}, {5, 5, 1}, {3, 0, 0}, {3, -2, 0}, {0, 0, 0}}
	for _, c := range cases {
		if got := report.Rate(c.num, c.den); got != c.want {
			t.Errorf("Rate(%d,%d) = %v, want %v", c.num, c.den, got, c.want)
		}
	}
}

func TestRebalanceTrend_DerivesTheRejectionRateAndKeepsOrder(t *testing.T) {
	d1 := time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC)
	days := []report.RebalanceDay{
		{Day: d1, Runs: 2, Proposals: 14, Rejected: 2, StaleFacts: 1},
		{Day: d1.Add(24 * time.Hour), Runs: 1, Proposals: 0, Rejected: 0},
	}
	got := report.RebalanceTrend(days)
	if len(got) != 2 || got[0].RejectionRate != 2.0/14.0 || got[1].RejectionRate != 0 || !got[0].Day.Equal(d1) || got[0].Runs != 2 {
		t.Fatalf("trend = %+v", got)
	}
	if empty := report.RebalanceTrend(nil); empty == nil || len(empty) != 0 {
		t.Fatalf("nil input must yield an empty non-nil slice, got %#v", empty)
	}
}

func TestPercentile_MatchesPostgresPercentileCont(t *testing.T) {
	cases := []struct {
		name   string
		values []float64
		p      float64
		want   float64
	}{
		{"empty", nil, 0.5, 0},
		{"single", []float64{7}, 0.95, 7},
		{"median of an odd count", []float64{9, 1, 5}, 0.5, 5},
		{"median interpolates between the middle pair", []float64{20, 600, 180, 1800}, 0.5, 390},
		{"p95 interpolates", []float64{20, 600, 180, 1800}, 0.95, 1620},
		{"p0 is the min and p1 the max", []float64{3, 1, 2}, 0, 1},
		{"p1", []float64{3, 1, 2}, 1, 3},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			before := append([]float64(nil), c.values...)
			if got := report.Percentile(c.values, c.p); math.Abs(got-c.want) > 1e-9 {
				t.Fatalf("Percentile = %v, want %v", got, c.want)
			}
			if !reflect.DeepEqual(before, c.values) {
				t.Fatal("Percentile must not modify its input")
			}
		})
	}
}

func TestComputeFreshness(t *testing.T) {
	if f := report.ComputeFreshness(nil, now); f.AsOf != nil || f.LagSeconds != nil {
		t.Fatalf("nothing applied: %+v, want both nil", f)
	}
	asOf := now.Add(-90 * time.Second).In(time.FixedZone("x", 3600))
	f := report.ComputeFreshness(&asOf, now)
	if f.LagSeconds == nil || *f.LagSeconds != 90 || f.AsOf == nil || f.AsOf.Location() != time.UTC || !f.AsOf.Equal(asOf) {
		t.Fatalf("freshness = %+v", f)
	}
	ahead := now.Add(time.Minute)
	if f := report.ComputeFreshness(&ahead, now); f.LagSeconds == nil || *f.LagSeconds != 0 {
		t.Fatalf("a clock behind the event reads zero lag, got %+v", f)
	}
}
