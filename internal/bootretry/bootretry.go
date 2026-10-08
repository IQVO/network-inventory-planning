// Package bootretry provides the exponential-backoff retry helper the
// analytics binaries (cmd/nip-projector, cmd/nip-reports) use to survive their
// very first outbound dial.
//
// It exists because in this fleet EVERY injected pod's FIRST outbound TCP
// dial (Postgres, Kafka) is reset ~10s after the app starts (Istio native
// sidecars; holdApplicationUntilProxyStarts is a no-op for them). A single
// attempt turns that known, transient condition into CrashLoopBackOff.
//
// The retry is NOT a weakening of any fail-closed rule. After the budget is
// exhausted the caller still refuses to boot; this only stops treating a
// sidecar warm-up as a permanent failure. Same constants, backoff and "return
// the last error" contract as the fleet's other services.
package bootretry

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// Retries and Delay bound the startup retry budget: ~31s total (1+2+4+8+16),
// comfortably past the ~10s first-dial reset and still far inside the liveness
// probe's own tolerance, so a genuinely unreachable dependency still fails the
// pod rather than hanging it.
const (
	Retries = 5
	Delay   = time.Second
)

// Retry runs op with exponential backoff (base Delay, Retries attempts),
// returning the LAST error so a permanent failure still reports its real cause
// rather than a generic timeout.
func Retry(ctx context.Context, logger *slog.Logger, what string, op func() error) error {
	return RetryWithDelay(ctx, logger, what, Delay, op)
}

// RetryWithDelay is Retry with the base delay injected, so tests can exercise
// the give-up path without sleeping out the real ~31s budget. A cancelled ctx
// abandons the retries (the error wraps ctx.Err()).
func RetryWithDelay(ctx context.Context, logger *slog.Logger, what string, base time.Duration, op func() error) error {
	if logger == nil {
		logger = slog.Default()
	}
	delay := base
	var err error
	for attempt := 1; attempt <= Retries; attempt++ {
		if err = op(); err == nil {
			if attempt > 1 {
				logger.Info("succeeded after retry", "op", what, "attempt", attempt)
			}
			return nil
		}
		if attempt == Retries {
			break
		}
		logger.Warn("retrying", "op", what, "attempt", attempt, "in", delay, "err", err)
		select {
		case <-ctx.Done():
			return fmt.Errorf("%s: %w", what, ctx.Err())
		case <-time.After(delay):
		}
		delay *= 2
	}
	return fmt.Errorf("%s (after %d attempts): %w", what, Retries, err)
}
