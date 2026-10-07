package usecases

import (
	"context"
	"log/slog"
	"time"
)

// PeriodicRunner runs one operation on a fixed interval until its context
// is cancelled (ADR 0007: the bounded in-process tickers). It exists so
// the health check and the scheduled rebalance share one loop shape and
// tests can drive ticks deterministically through the exported Tick — no
// sleeps, no real clocks.
type PeriodicRunner struct {
	// Name labels log lines and errors.
	Name string
	// Interval is the tick period; must be positive.
	Interval time.Duration
	// Tick runs one pass. Its error is logged and retried on the NEXT
	// tick (a failed pass never stops the loop).
	Tick func(ctx context.Context) error
	// Logger receives tick failures (nil means slog.Default).
	Logger *slog.Logger
	// now/wait are injectable for tests; production uses time.Now and a
	// real timer. wait must return nil after d or ctx.Err() when
	// cancelled.
	wait func(ctx context.Context, d time.Duration) error
}

// Run ticks immediately, then every Interval, until ctx is cancelled.
// It returns nil on cancellation, never a tick error.
func (r PeriodicRunner) Run(ctx context.Context) error {
	if r.Interval <= 0 {
		return nil
	}
	logger := r.Logger
	if logger == nil {
		logger = slog.Default()
	}
	wait := r.wait
	if wait == nil {
		wait = func(ctx context.Context, d time.Duration) error {
			t := time.NewTimer(d)
			defer t.Stop()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-t.C:
				return nil
			}
		}
	}
	for {
		if err := r.Tick(ctx); err != nil && ctx.Err() == nil {
			logger.Error(r.Name+" tick failed; retrying next interval", "error", err)
		}
		if err := wait(ctx, r.Interval); err != nil {
			return nil // cancelled
		}
	}
}
