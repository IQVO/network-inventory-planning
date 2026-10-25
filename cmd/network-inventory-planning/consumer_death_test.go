package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"
)

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func recvDeath(t *testing.T, ch <-chan error) (error, bool) {
	t.Helper()
	select {
	case err := <-ch:
		return err, true
	case <-time.After(300 * time.Millisecond):
		return nil, false
	}
}

// A consumer that stops on its own (a broker reset, "Group Load In Progress")
// must take the process down; one that stops because the service is being shut
// down must not.
func TestStartRunners(t *testing.T) {
	boom := errors.New("read tcp: connection reset by peer")
	cases := []struct {
		name      string
		runner    func(ctx context.Context) func() error
		cancelCtx bool // cancel before the runner returns
		wantDeath bool
	}{
		{"an error while running is a death", func(context.Context) func() error { return func() error { return boom } }, false, true},
		{"returning nil while running is a death", func(context.Context) func() error { return func() error { return nil } }, false, true},
		{"context.Canceled is a clean stop", func(context.Context) func() error { return func() error { return context.Canceled } }, false, false},
		{"an error after the context ended is a clean stop", func(ctx context.Context) func() error {
			return func() error { <-ctx.Done(); return boom }
		}, true, false},
		{"nil after the context ended is a clean stop", func(ctx context.Context) func() error {
			return func() error { <-ctx.Done(); return nil }
		}, true, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			deaths := startRunners(ctx, []func() error{tc.runner(ctx)}, quietLogger())
			if tc.cancelCtx {
				cancel()
			}
			err, got := recvDeath(t, deaths)
			if got != tc.wantDeath {
				t.Fatalf("death reported = %v (%v), want %v", got, err, tc.wantDeath)
			}
			if tc.wantDeath && err == nil {
				t.Fatal("a death must carry an error")
			}
		})
	}
}

// Several consumers dying together must not block each other: nobody reads the
// channel until all of them have finished, and exactly one death is held.
func TestStartRunners_SeveralDeathsNeverBlockAndOneIsHeld(t *testing.T) {
	boom := errors.New("boom")
	const n = 3
	finished := make(chan struct{}, n)
	runner := func() error { defer func() { finished <- struct{}{} }(); return boom }
	deaths := startRunners(context.Background(), []func() error{runner, runner, runner}, quietLogger())
	for i := 0; i < n; i++ {
		select {
		case <-finished:
		case <-time.After(time.Second):
			t.Fatalf("only %d of %d runners finished: a dying consumer blocked", i, n)
		}
	}
	time.Sleep(50 * time.Millisecond) // let the sends after the returns land
	err, got := recvDeath(t, deaths)
	if !got || !errors.Is(err, boom) {
		t.Fatalf("held death = %v (reported %v), want it to wrap boom", err, got)
	}
	if _, more := recvDeath(t, deaths); more {
		t.Fatal("more than one death was held")
	}
}
