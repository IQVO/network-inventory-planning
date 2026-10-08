package analyticsstore_test

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/claudioed/network-inventory-planning/internal/analytics/report"
)

// This file is the CONTRACT every analytical store must satisfy; the same
// function runs against the in-memory twin (always) and the real Postgres
// store (postgres_integration_test.go, -tags=integration). The fixtures use
// distinct non-zero numbers, events sitting exactly ON the range bounds, a
// transfer that reaches one state twice and two detections at the same
// instant, so a naive implementation fails somewhere.

func at(day, hour, min, sec int) time.Time {
	return time.Date(2026, 10, day, hour, min, sec, 0, time.UTC)
}

// contractRange is [Oct 5 00:00:00, Oct 7 00:00:00) UTC.
var contractRange = report.Range{From: at(5, 0, 0, 0), To: at(7, 0, 0, 0)}

var seq int

func nextID(prefix string) string {
	seq++
	return fmt.Sprintf("%s-%d", prefix, seq)
}

func advance(transfer, from, to string, age int64, when time.Time) report.Event {
	return report.Event{
		Kind: report.KindStateAdvanced, EventID: nextID("adv"), At: when,
		TransferID: transfer, From: from, To: to, AgeSeconds: age,
	}
}

func stuck(transfer, state string, age, threshold int64, when time.Time) report.Event {
	return report.Event{
		Kind: report.KindStuckDetected, EventID: nextID("stk"), At: when,
		TransferID: transfer, State: state, AgeSeconds: age, ThresholdSeconds: threshold,
	}
}

func run(id string, proposals, rejected, stale int, when time.Time) report.Event {
	return report.Event{
		Kind: report.KindRebalanceRunCompleted, EventID: nextID("run"), At: when,
		RunID: id, ProposalCount: proposals, RejectedCount: rejected, StaleFacts: stale,
	}
}

func contractEvents() []report.Event {
	return []report.Event{
		// Oct 5: t1 walks the start of the saga; its creation entry has no `from`.
		advance("t1", "", "PROPOSED", 0, at(5, 8, 0, 0)),
		advance("t1", "PROPOSED", "APPROVED", 100, at(5, 8, 1, 0)),
		advance("t1", "APPROVED", "ALLOCATING", 101, at(5, 8, 1, 1)),
		advance("t2", "", "PROPOSED", 0, at(5, 9, 0, 0)),
		advance("t2", "PROPOSED", "APPROVED", 300, at(5, 9, 5, 0)),
		// t2 reaches APPROVED a second time (a redelivery under a new id):
		// the funnel counts DISTINCT transfers, the dwell counts transitions.
		advance("t2", "PROPOSED", "APPROVED", 305, at(5, 9, 6, 0)),
		advance("t3", "PROPOSED", "APPROVED", 50, at(5, 10, 0, 0)),
		advance("t4", "PROPOSED", "APPROVED", 700, at(5, 11, 0, 0)),
		// Exactly AT from: in.
		advance("t5", "PROPOSED", "APPROVED", 5, at(5, 0, 0, 0)),
		// Oct 6, on the day boundary.
		advance("t1", "ALLOCATING", "ALLOCATED", 90000, at(6, 0, 0, 0)),
		// Exactly AT to, and one second before from: out.
		advance("t6", "PROPOSED", "APPROVED", 9, at(7, 0, 0, 0)),
		advance("t7", "PROPOSED", "APPROVED", 9, at(4, 23, 59, 59)),

		stuck("t1", "ALLOCATING", 700, 600, at(5, 12, 0, 0)),
		stuck("t1", "ALLOCATING", 1300, 600, at(5, 12, 10, 0)),
		stuck("t0", "APPROVED", 4000, 3600, at(5, 12, 10, 0)), // same instant as the line above
		stuck("t2", "APPROVED", 5000, 3600, at(5, 13, 0, 0)),
		stuck("t3", "ALLOCATING", 800, 600, at(6, 8, 0, 0)),
		stuck("t9", "ALLOCATING", 900, 600, at(7, 0, 0, 0)),    // AT to: out
		stuck("t9", "ALLOCATING", 900, 600, at(4, 23, 59, 59)), // before from: out

		run("run-1", 10, 2, 1, at(5, 6, 0, 0)),
		run("run-2", 4, 0, 0, at(5, 18, 0, 0)),
		run("run-3", 0, 0, 3, at(6, 6, 0, 0)),
		run("run-4", 99, 99, 99, at(7, 0, 0, 0)), // AT to: out
	}
}

func loadContract(t *testing.T, p report.Projection) []report.Event {
	t.Helper()
	all := contractEvents()
	for _, e := range all {
		applied, err := p.Apply(context.Background(), e)
		if err != nil || !applied {
			t.Fatalf("Apply(%s) = %v, %v; want applied", e.EventID, applied, err)
		}
	}
	return all
}

func near(a, b float64) bool { return math.Abs(a-b) < 1e-6 }

type storeFactory func(t *testing.T) (report.Projection, report.Reader)

// runStoreContract runs every contract case against fresh stores from
// newStore. The cases live in a table (not nested closures) so each stays a
// flat, readable scenario.
func runStoreContract(t *testing.T, newStore storeFactory) {
	for _, c := range contractCases {
		t.Run(c.name, func(t *testing.T) { c.run(t, newStore) })
	}
}

var contractCases = []struct {
	name string
	run  func(t *testing.T, newStore storeFactory)
}{
	{"funnel: distinct transfers per state per UTC day, range [from, to)", func(t *testing.T, newStore storeFactory) {
		p, r := newStore(t)
		loadContract(t, p)
		got, err := r.TransferFunnel(context.Background(), contractRange)
		if err != nil {
			t.Fatal(err)
		}
		want := []report.FunnelDay{
			{Day: at(5, 0, 0, 0), State: "ALLOCATING", Transfers: 1},
			{Day: at(5, 0, 0, 0), State: "APPROVED", Transfers: 5},
			{Day: at(5, 0, 0, 0), State: "PROPOSED", Transfers: 2},
			{Day: at(6, 0, 0, 0), State: "ALLOCATED", Transfers: 1},
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got  %+v\nwant %+v", got, want)
		}
	}},

	{"dwell: age percentiles per `from` state, creation entries excluded", func(t *testing.T, newStore storeFactory) {
		p, r := newStore(t)
		loadContract(t, p)
		got, err := r.StateDwell(context.Background(), contractRange)
		if err != nil {
			t.Fatal(err)
		}
		want := []report.DwellDay{
			{Day: at(5, 0, 0, 0), State: "APPROVED", Transitions: 1, P50Seconds: 101, P95Seconds: 101},
			{Day: at(5, 0, 0, 0), State: "PROPOSED", Transitions: 6, P50Seconds: 200, P95Seconds: 601.25},
			{Day: at(6, 0, 0, 0), State: "ALLOCATING", Transitions: 1, P50Seconds: 90000, P95Seconds: 90000},
		}
		if len(got) != len(want) {
			t.Fatalf("got %+v, want %+v", got, want)
		}
		for i := range want {
			g, w := got[i], want[i]
			if !g.Day.Equal(w.Day) || g.State != w.State || g.Transitions != w.Transitions ||
				!near(g.P50Seconds, w.P50Seconds) || !near(g.P95Seconds, w.P95Seconds) {
				t.Errorf("row %d = %+v, want %+v", i, g, w)
			}
		}
	}},

	{"stuck days: detections and distinct transfers per state per day", func(t *testing.T, newStore storeFactory) {
		p, r := newStore(t)
		loadContract(t, p)
		got, err := r.StuckDays(context.Background(), contractRange)
		if err != nil {
			t.Fatal(err)
		}
		want := []report.StuckDay{
			{Day: at(5, 0, 0, 0), State: "ALLOCATING", Detections: 2, Transfers: 1},
			{Day: at(5, 0, 0, 0), State: "APPROVED", Detections: 2, Transfers: 2},
			{Day: at(6, 0, 0, 0), State: "ALLOCATING", Detections: 1, Transfers: 1},
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got  %+v\nwant %+v", got, want)
		}
	}},

	{"stuck latest: newest first, ties by transfer id, limited", func(t *testing.T, newStore storeFactory) {
		p, r := newStore(t)
		loadContract(t, p)
		all, err := r.StuckLatest(context.Background(), contractRange, 100)
		if err != nil {
			t.Fatal(err)
		}
		want := []report.StuckOccurrence{
			{TransferID: "t3", State: "ALLOCATING", AgeSeconds: 800, ThresholdSeconds: 600, DetectedAt: at(6, 8, 0, 0)},
			{TransferID: "t2", State: "APPROVED", AgeSeconds: 5000, ThresholdSeconds: 3600, DetectedAt: at(5, 13, 0, 0)},
			{TransferID: "t0", State: "APPROVED", AgeSeconds: 4000, ThresholdSeconds: 3600, DetectedAt: at(5, 12, 10, 0)},
			{TransferID: "t1", State: "ALLOCATING", AgeSeconds: 1300, ThresholdSeconds: 600, DetectedAt: at(5, 12, 10, 0)},
			{TransferID: "t1", State: "ALLOCATING", AgeSeconds: 700, ThresholdSeconds: 600, DetectedAt: at(5, 12, 0, 0)},
		}
		if !reflect.DeepEqual(all, want) {
			t.Fatalf("got  %+v\nwant %+v", all, want)
		}
		top2, err := r.StuckLatest(context.Background(), contractRange, 2)
		if err != nil || !reflect.DeepEqual(top2, want[:2]) {
			t.Fatalf("limit 2 = %+v, %v; want %+v", top2, err, want[:2])
		}
	}},

	{"rebalance days: runs, proposals, rejected, stale per UTC day", func(t *testing.T, newStore storeFactory) {
		p, r := newStore(t)
		loadContract(t, p)
		got, err := r.RebalanceDays(context.Background(), contractRange)
		if err != nil {
			t.Fatal(err)
		}
		want := []report.RebalanceDay{
			{Day: at(5, 0, 0, 0), Runs: 2, Proposals: 14, Rejected: 2, StaleFacts: 1},
			{Day: at(6, 0, 0, 0), Runs: 1, Proposals: 0, Rejected: 0, StaleFacts: 3},
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got  %+v\nwant %+v", got, want)
		}
	}},

	{"range bounds: from inclusive, to exclusive, to the instant", func(t *testing.T, newStore storeFactory) {
		p, r := newStore(t)
		loadContract(t, p)
		ctx := context.Background()
		runs := func(rg report.Range) int {
			days, err := r.RebalanceDays(ctx, rg)
			if err != nil {
				t.Fatal(err)
			}
			n := 0
			for _, d := range days {
				n += d.Runs
			}
			return n
		}
		if got := runs(report.Range{From: at(5, 6, 0, 0), To: at(5, 6, 0, 1)}); got != 1 {
			t.Errorf("[06:00:00, 06:00:01) runs = %d, want 1 (from inclusive)", got)
		}
		if got := runs(report.Range{From: at(5, 6, 0, 1), To: at(5, 18, 0, 0)}); got != 0 {
			t.Errorf("[06:00:01, 18:00:00) runs = %d, want 0 (to exclusive)", got)
		}
		if got := runs(report.Range{From: at(5, 6, 0, 1), To: at(5, 18, 0, 1)}); got != 1 {
			t.Errorf("[06:00:01, 18:00:01) runs = %d, want 1", got)
		}
		if got := runs(report.Range{From: at(5, 0, 0, 0), To: at(7, 0, 1, 0)}); got != 4 {
			t.Errorf("a range reaching past Oct 7 00:00:00 must include run-4, got %d runs", got)
		}
	}},

	{"replaying an applied id is a no-op", func(t *testing.T, newStore storeFactory) {
		p, r := newStore(t)
		events := loadContract(t, p)
		before, err := r.RebalanceDays(context.Background(), contractRange)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range events {
			applied, err := p.Apply(context.Background(), e)
			if err != nil || applied {
				t.Fatalf("replay %s = %v, %v; want not applied", e.EventID, applied, err)
			}
		}
		// A replay carrying different numbers under the same id must not change anything.
		var tampered report.Event
		for _, e := range events {
			if e.Kind == report.KindRebalanceRunCompleted {
				tampered = e
				break
			}
		}
		tampered.ProposalCount = 99999
		if applied, err := p.Apply(context.Background(), tampered); err != nil || applied {
			t.Fatalf("tampered replay = %v, %v", applied, err)
		}
		after, _ := r.RebalanceDays(context.Background(), contractRange)
		if !reflect.DeepEqual(before, after) {
			t.Fatalf("replay changed the model:\nbefore %+v\nafter  %+v", before, after)
		}
	}},

	{"last event time is the newest applied CloudEvents time", func(t *testing.T, newStore storeFactory) {
		p, r := newStore(t)
		loadContract(t, p)
		got, err := r.LastEventAt(context.Background())
		if err != nil || got == nil || !got.Equal(at(7, 0, 0, 0)) || got.Location() != time.UTC {
			t.Fatalf("LastEventAt = %v, %v; want Oct 7 00:00:00 UTC", got, err)
		}
		// An older event applied later does not move it back.
		if _, err := p.Apply(context.Background(), advance("t1", "PROPOSED", "APPROVED", 1, at(5, 1, 0, 0))); err != nil {
			t.Fatal(err)
		}
		if again, _ := r.LastEventAt(context.Background()); !again.Equal(at(7, 0, 0, 0)) {
			t.Fatalf("LastEventAt moved back to %v", again)
		}
	}},

	{"an empty store answers with empty non-nil slices", func(t *testing.T, newStore storeFactory) {
		_, r := newStore(t)
		ctx := context.Background()
		if last, err := r.LastEventAt(ctx); err != nil || last != nil {
			t.Fatalf("LastEventAt on an empty store = %v, %v; want nil", last, err)
		}
		f, e1 := r.TransferFunnel(ctx, contractRange)
		d, e2 := r.StateDwell(ctx, contractRange)
		s, e3 := r.StuckDays(ctx, contractRange)
		l, e4 := r.StuckLatest(ctx, contractRange, 5)
		b, e5 := r.RebalanceDays(ctx, contractRange)
		if e1 != nil || e2 != nil || e3 != nil || e4 != nil || e5 != nil {
			t.Fatalf("errors: %v %v %v %v %v", e1, e2, e3, e4, e5)
		}
		if f == nil || d == nil || s == nil || l == nil || b == nil || len(f)+len(d)+len(s)+len(l)+len(b) != 0 {
			t.Fatalf("want empty non-nil slices, got %#v %#v %#v %#v %#v", f, d, s, l, b)
		}
	}},
}
