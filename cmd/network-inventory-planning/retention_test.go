package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/network-inventory-planning/internal/application/contract"
)

// scriptedPurge answers PurgeExpired from script in order (the last repeats)
// and counts calls; Do is never the purge worker's business.
type scriptedPurge struct {
	batches []int
	err     error
	calls   int
}

func (s *scriptedPurge) Do(context.Context, string, string, func(context.Context) (contract.StoredResponse, error)) (contract.StoredResponse, bool, error) {
	return contract.StoredResponse{}, false, errors.New("not used by the purge worker")
}

func (s *scriptedPurge) PurgeExpired(context.Context, int) (int, error) {
	i := s.calls
	s.calls++
	if i >= len(s.batches) {
		i = len(s.batches) - 1
	}
	return s.batches[i], s.err
}

// The pass loops one bounded batch at a time until a batch comes back short:
// however far behind the purge is, no single statement holds a long lock.
func TestPurgePassLoopsBatchesUntilAShortOne(t *testing.T) {
	store := &scriptedPurge{batches: []int{idempotencyPurgeBatch, idempotencyPurgeBatch, 3}}
	n, err := purgeIdempotencyPass(context.Background(), store)
	if err != nil || n != 2*idempotencyPurgeBatch+3 || store.calls != 3 {
		t.Fatalf("pass = %d, %v (calls %d), want 3 batches totalling %d", n, err, store.calls, 2*idempotencyPurgeBatch+3)
	}
}

// A short first batch means nothing expired is left: exactly one call.
func TestPurgePassStopsOnTheFirstShortBatch(t *testing.T) {
	store := &scriptedPurge{batches: []int{10}}
	n, err := purgeIdempotencyPass(context.Background(), store)
	if err != nil || n != 10 || store.calls != 1 {
		t.Fatalf("pass = %d, %v (calls %d), want one short batch of 10", n, err, store.calls)
	}
}

// A failed batch is the pass's error (the runner logs it and retries on the
// next tick); the rows already deleted are kept in the total.
func TestPurgePassReturnsTheError(t *testing.T) {
	store := &scriptedPurge{batches: []int{idempotencyPurgeBatch, idempotencyPurgeBatch}, err: errors.New("db gone")}
	n, err := purgeIdempotencyPass(context.Background(), store)
	if !errors.Is(err, store.err) || n != idempotencyPurgeBatch {
		t.Fatalf("pass = %d, %v, want the first batch kept and the error surfaced", n, err)
	}
}

// A cancelled context stops the pass between batches; a cancelled context
// before the first batch deletes nothing.
func TestPurgePassStopsWithContext(t *testing.T) {
	store := &scriptedPurge{batches: []int{idempotencyPurgeBatch}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	n, err := purgeIdempotencyPass(ctx, store)
	if !errors.Is(err, context.Canceled) || n != 0 || store.calls != 0 {
		t.Fatalf("cancelled pass = %d, %v (calls %d), want 0 and context.Canceled", n, err, store.calls)
	}
}

// wireIdempotencyPurge appends a runner that ends cleanly (nil) once the
// root context is cancelled, and only after a final tick was attempted.
func TestThePurgeRunnerStopsCleanly(t *testing.T) {
	store := &scriptedPurge{batches: []int{0}}
	var runners []func() error
	ctx, cancel := context.WithCancel(context.Background())
	wireIdempotencyPurge(ctx, quietLogger(), store, &runners)
	if len(runners) != 1 {
		t.Fatalf("runners = %d, want 1", len(runners))
	}
	done := make(chan error, 1)
	go func() { done <- runners[0]() }()
	// The runner ticks immediately; give it a moment, then cancel.
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("runner = %v, want a clean nil on cancellation", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the purge runner did not stop on cancellation")
	}
	if store.calls < 1 {
		t.Fatal("the runner never ticked: the purge must run at start, then hourly")
	}
}
