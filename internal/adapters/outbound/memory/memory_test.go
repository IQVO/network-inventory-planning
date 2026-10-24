package memory

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/network-inventory-planning/internal/domain/planning"
	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

var unitNow = time.Date(2026, 10, 6, 15, 0, 0, 0, time.UTC)

func TestFixedClockStartsFrozenAndUTC(t *testing.T) {
	local := time.Date(2026, 10, 6, 12, 0, 0, 0, time.FixedZone("BRT", -3*3600))
	c := NewFixedClock(local)
	if got := c.Now(); !got.Equal(unitNow) {
		t.Fatalf("Now() = %v, want %v (UTC-normalised)", got, unitNow)
	}
	if got := c.Now().Location(); got != time.UTC {
		t.Fatalf("Now() location = %v, want UTC", got)
	}
}

func TestFixedClockAdvanceMovesForwardOnly(t *testing.T) {
	c := NewFixedClock(unitNow)
	c.Advance(90 * time.Minute)
	if got := c.Now(); !got.Equal(unitNow.Add(90 * time.Minute)) {
		t.Fatalf("Now() after 90m = %v, want %v", got, unitNow.Add(90*time.Minute))
	}
	c.Advance(-time.Hour) // scenarios never do this; Add still applies
	if got := c.Now(); !got.Equal(unitNow.Add(30 * time.Minute)) {
		t.Fatalf("Now() after -1h = %v, want %v", got, unitNow.Add(30*time.Minute))
	}
}

func TestFixedClockConcurrentAdvanceAndNow(t *testing.T) {
	c := NewFixedClock(unitNow)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 500; i++ {
			c.Advance(time.Millisecond)
			_ = c.Now()
		}
	}()
	<-done
	if got := c.Now(); !got.Equal(unitNow.Add(500 * time.Millisecond)) {
		t.Fatalf("Now() = %v, want %v after 500 advances", got, unitNow.Add(500*time.Millisecond))
	}
}

func TestUnitOfWorkRunsFnAndPropagatesErrorAndContext(t *testing.T) {
	ctx := context.WithValue(context.Background(), ctxKey{}, "v")
	var u UnitOfWork
	ran := 0
	err := u.Do(ctx, func(inner context.Context) error {
		ran++
		if inner.Value(ctxKey{}) != "v" {
			t.Fatal("the unit of work did not pass the caller's context through")
		}
		return nil
	})
	if err != nil || ran != 1 {
		t.Fatalf("Do(nil-fn) = %v, ran %d; want nil error, one run", err, ran)
	}
	sentinel := errors.New("boom")
	if err := u.Do(ctx, func(context.Context) error { return sentinel }); !errors.Is(err, sentinel) {
		t.Fatalf("Do error = %v, want %v passed through unchanged", err, sentinel)
	}
}

type ctxKey struct{}

func TestPlanningFactsSetThenLoadRoundTrip(t *testing.T) {
	p := NewPlanningFacts()
	facts := planning.Facts{
		Capabilities: []planning.SiteCapability{{Site: "WH1"}},
		Demands:      []planning.SiteSkuDemand{{Site: "WH1"}},
		Plans:        []planning.PublishedCapacityPlan{{PlanID: "p-1"}},
	}
	p.Set(facts)
	got, err := p.Load(context.Background())
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(got.Capabilities) != 1 || len(got.Demands) != 1 || len(got.Plans) != 1 {
		t.Fatalf("Load = %+v, want one of each", got)
	}
}

func TestPlanningFactsSetReplacesAndCopies(t *testing.T) {
	p := NewPlanningFacts()
	first := planning.Facts{Capabilities: []planning.SiteCapability{{Site: "WH1"}}}
	p.Set(first)
	p.Set(planning.Facts{}) // a second Set replaces, not appends
	got, _ := p.Load(context.Background())
	if len(got.Capabilities) != 0 {
		t.Fatalf("after Set(empty) Load = %+v, want no capabilities", got)
	}

	// Mutation of the caller's slice after Set must not leak into the store.
	mutable := planning.Facts{Demands: []planning.SiteSkuDemand{{Site: "WH1"}}}
	p.Set(mutable)
	mutable.Demands[0].Site = "WH9"
	stored, _ := p.Load(context.Background())
	if stored.Demands[0].Site != "WH1" {
		t.Fatalf("Set stored the caller's slice by reference: got %+v", stored.Demands)
	}
	// Mutation of the Load result must not corrupt the store either.
	loaded, _ := p.Load(context.Background())
	loaded.Demands[0].Site = "WH8"
	again, _ := p.Load(context.Background())
	if again.Demands[0].Site != "WH1" {
		t.Fatalf("Load handed out the stored slice by reference: got %+v", again.Demands)
	}
}

func TestRebalanceRunsRecordAssignsSequentialIDs(t *testing.T) {
	r := NewRebalanceRuns()
	first, err := r.Record(context.Background(), transfer.RebalanceRun{StartedAt: unitNow})
	if err != nil || first != 1 {
		t.Fatalf("first Record = (%d, %v), want (1, nil)", first, err)
	}
	second, _ := r.Record(context.Background(), transfer.RebalanceRun{StartedAt: unitNow})
	if second != 2 {
		t.Fatalf("second Record id = %d, want 2", second)
	}
}

func TestRebalanceRunsRecordClonesFailClosedReason(t *testing.T) {
	r := NewRebalanceRuns()
	reason := "stale facts"
	if _, err := r.Record(context.Background(), transfer.RebalanceRun{StartedAt: unitNow, FailClosedReason: &reason}); err != nil {
		t.Fatalf("Record: %v", err)
	}
	reason = "mutated" // must not rewrite the stored row
	runs, _ := r.List(context.Background(), 10)
	if len(runs) != 1 || *runs[0].FailClosedReason != "stale facts" {
		t.Fatalf("stored FailClosedReason = %+v, want the value at Record time", runs)
	}
}

func TestRebalanceRunsListNewestFirstThenID(t *testing.T) {
	r := NewRebalanceRuns()
	older := unitNow
	same := unitNow.Add(time.Hour)
	r.Record(context.Background(), transfer.RebalanceRun{StartedAt: older, Outcome: "FAILED"})   // id 1
	r.Record(context.Background(), transfer.RebalanceRun{StartedAt: same, Outcome: "COMPLETED"}) // id 2
	r.Record(context.Background(), transfer.RebalanceRun{StartedAt: same, Outcome: "COMPLETED"}) // id 3, ties with id 2

	runs, err := r.List(context.Background(), 0)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(runs) != 3 {
		t.Fatalf("List returned %d runs, want 3", len(runs))
	}
	// Newest started_at first; the equal-16:00 pair breaks on the LATER id
	// (3 before 2), like the Postgres ORDER BY.
	wantOrder := []int64{3, 2, 1}
	for i, want := range wantOrder {
		if runs[i].ID != want {
			t.Fatalf("runs[%d].ID = %d, want %d (order %v)", i, runs[i].ID, want, runs)
		}
	}
}

func TestRebalanceRunsListHonoursLimit(t *testing.T) {
	r := NewRebalanceRuns()
	for i := 0; i < 3; i++ {
		if _, err := r.Record(context.Background(), transfer.RebalanceRun{StartedAt: unitNow.Add(time.Duration(i) * time.Minute)}); err != nil {
			t.Fatalf("Record: %v", err)
		}
	}
	runs, _ := r.List(context.Background(), 2)
	if len(runs) != 2 {
		t.Fatalf("List(2) returned %d runs, want 2", len(runs))
	}
	// The page keeps the NEWEST two: ids 3 and 2.
	if runs[0].ID != 3 || runs[1].ID != 2 {
		t.Fatalf("List(2) = %v IDs, want [3 2]", []int64{runs[0].ID, runs[1].ID})
	}
}
