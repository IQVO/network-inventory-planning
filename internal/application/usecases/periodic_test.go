package usecases

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"
)

// TestPeriodicRunnerTicksAndStops drives the runner with an INJECTABLE
// wait (no sleeps, no real clocks): it proves the first tick fires
// immediately, later ticks fire after each wait, and cancellation stops
// the loop cleanly even mid-wait.
func TestPeriodicRunnerTicksAndStops(t *testing.T) {
	var mu sync.Mutex
	ticks := 0
	release := make(chan struct{}) // test releases each wait

	waitCalls := 0
	r := PeriodicRunner{
		Name:     "probe",
		Interval: time.Minute,
		Tick: func(ctx context.Context) error {
			mu.Lock()
			ticks++
			mu.Unlock()
			return nil
		},
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		wait: func(ctx context.Context, d time.Duration) error {
			mu.Lock()
			waitCalls++
			mu.Unlock()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-release:
				return nil
			}
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()

	// First tick is immediate: no wait before it.
	deadline := time.After(2 * time.Second)
	for {
		mu.Lock()
		n := ticks
		mu.Unlock()
		if n == 1 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("first tick did not fire immediately")
		case <-time.After(time.Millisecond):
		}
	}

	// Release the wait twice: two more ticks.
	release <- struct{}{}
	release <- struct{}{}
	deadline = time.After(2 * time.Second)
	for {
		mu.Lock()
		n := ticks
		mu.Unlock()
		if n == 3 {
			break
		}
		select {
		case <-deadline:
			mu.Lock()
			stalled := ticks
			mu.Unlock()
			t.Fatalf("ticks stalled at %d, want 3", stalled)
		case <-time.After(time.Millisecond):
		}
	}

	// Cancel mid-wait: Run returns nil.
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned %v on cancellation, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancellation")
	}
}

func TestPeriodicRunnerSurvivesTickError(t *testing.T) {
	// A failing tick is logged and retried on the NEXT interval — the
	// loop must never stop on a tick error.
	var mu sync.Mutex
	calls := 0
	next := make(chan struct{})
	r := PeriodicRunner{
		Name:     "failing probe",
		Interval: time.Minute,
		Tick: func(ctx context.Context) error {
			mu.Lock()
			calls++
			mu.Unlock()
			return errors.New("injected tick failure")
		},
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		wait: func(ctx context.Context, d time.Duration) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-next:
				return nil
			}
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()

	<-time.After(50 * time.Millisecond)
	next <- struct{}{}
	<-time.After(50 * time.Millisecond)
	mu.Lock()
	n := calls
	mu.Unlock()
	cancel()
	<-done
	if n < 2 {
		t.Fatalf("tick calls = %d, want >= 2 (a failed tick must be retried, not fatal)", n)
	}
}

func TestPeriodicRunnerZeroIntervalIsNoOp(t *testing.T) {
	r := PeriodicRunner{Name: "off", Interval: 0, Tick: func(ctx context.Context) error {
		t.Fatal("tick fired on a disabled runner")
		return nil
	}}
	if err := r.Run(context.Background()); err != nil {
		t.Fatalf("Run = %v, want nil", err)
	}
}
