package bootretry

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRetryWithDelay_ReturnsNilOnFirstSuccess(t *testing.T) {
	calls := 0
	if err := RetryWithDelay(context.Background(), nil, "op", time.Millisecond, func() error { calls++; return nil }); err != nil || calls != 1 {
		t.Fatalf("err = %v, calls = %d; want nil and 1", err, calls)
	}
}

func TestRetryWithDelay_RecoversAfterTransientFailures(t *testing.T) {
	calls := 0
	err := RetryWithDelay(context.Background(), nil, "op", time.Millisecond, func() error {
		calls++
		if calls < 3 {
			return errors.New("connection reset by peer")
		}
		return nil
	})
	if err != nil || calls != 3 {
		t.Fatalf("err = %v, calls = %d; want nil and 3", err, calls)
	}
}

func TestRetryWithDelay_GivesUpWithTheLastError(t *testing.T) {
	calls := 0
	boom := errors.New("permanent")
	err := RetryWithDelay(context.Background(), nil, "dial", time.Millisecond, func() error { calls++; return boom })
	if !errors.Is(err, boom) || calls != Retries {
		t.Fatalf("err = %v, calls = %d; want the last error after %d attempts", err, calls, Retries)
	}
}

func TestRetryWithDelay_StopsWhenTheContextIsCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	calls := 0
	err := RetryWithDelay(ctx, nil, "dial", time.Hour, func() error { calls++; return errors.New("x") })
	if !errors.Is(err, context.Canceled) || calls != 1 {
		t.Fatalf("err = %v, calls = %d; want context.Canceled after one attempt", err, calls)
	}
}

func TestRetry_UsesTheFleetBudget(t *testing.T) {
	if Retries != 5 || Delay != time.Second {
		t.Fatalf("budget = %d x %v, want 5 x 1s (~31s total)", Retries, Delay)
	}
	if err := Retry(context.Background(), nil, "op", func() error { return nil }); err != nil {
		t.Fatal(err)
	}
}
