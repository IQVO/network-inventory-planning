package main

import (
	"context"
	"log/slog"
	"time"

	"github.com/claudioed/network-inventory-planning/internal/application/contract"
	"github.com/claudioed/network-inventory-planning/internal/application/ports"
	"github.com/claudioed/network-inventory-planning/internal/application/usecases"
)

// The Idempotency-Key retention (ADR 0024, addendum 2026-11-22): until it,
// nothing ever deleted an idempotency_keys row and a stored answer replayed
// forever. Every idempotencyPurgeInterval the purge deletes the rows older
// than contract.IdempotencyTTL — exactly the rows the claim's refresh
// predicate and the replay's age guard already ignore — at most
// idempotencyPurgeBatch per statement, so one pass never holds a long lock
// however far behind it is. The interval and the batch are fixed on purpose
// (like the sibling services): the age is the TTL, the rest is housekeeping,
// not configuration. The loop is the repo's own PeriodicRunner (ADR 0007):
// it ticks at start, then hourly, and a failed pass is logged and retried on
// the next tick — the purge is idempotent and an expired row is never
// replayed, so a failure costs only disk.
const (
	idempotencyPurgeInterval = time.Hour
	idempotencyPurgeBatch    = 500
)

// wireIdempotencyPurge starts the always-on retention loop over store and
// appends its runner, on a context derived from run()'s root signal context
// (ADR 0019 clean-stop; see wireOutbox for the race a private signal context
// creates).
func wireIdempotencyPurge(ctx context.Context, logger *slog.Logger, store ports.IdempotencyStore, runners *[]func() error) {
	runner := usecases.PeriodicRunner{
		Name:     "idempotency purge",
		Interval: idempotencyPurgeInterval,
		Tick: func(ctx context.Context) error {
			n, err := purgeIdempotencyPass(ctx, store)
			if n > 0 {
				logger.InfoContext(ctx, "idempotency purge", "deleted", n)
			}
			return err
		},
		Logger: logger,
	}
	purgeCtx, stop := context.WithCancel(ctx)
	*runners = append(*runners, func() error { defer stop(); return runner.Run(purgeCtx) })
	logger.Info("idempotency purge running",
		"interval", idempotencyPurgeInterval.String(),
		"batch", idempotencyPurgeBatch,
		"ttl", contract.IdempotencyTTL.String())
}

// purgeIdempotencyPass deletes expired rows batch by batch until a batch
// comes back short (nothing expired is left) or ctx ends, and returns how
// many rows went. A failed batch is the pass's error: the runner logs it and
// retries on the next tick; rows already deleted stay deleted, so the pass
// can simply run again.
func purgeIdempotencyPass(ctx context.Context, store ports.IdempotencyStore) (int, error) {
	total := 0
	for ctx.Err() == nil {
		n, err := store.PurgeExpired(ctx, idempotencyPurgeBatch)
		total += n
		if err != nil {
			return total, err
		}
		if n < idempotencyPurgeBatch {
			return total, nil
		}
	}
	return total, ctx.Err()
}
