package usecases

import (
	"context"
	"fmt"
	"time"

	"github.com/claudioed/network-inventory-planning/internal/application/ports"
	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

// CheckStuckTransfers is the saga-health check pass (ADR 0007): it reads
// the non-terminal transfers, evaluates each against the per-state stuck
// thresholds, and publishes one TransferStuckDetected analytics
// occurrence per stuck transfer. OBSERVE-ONLY: it never mutates saga
// state, cancels a transfer, or releases a reservation — what an operator
// does with the signal is a human decision.
type CheckStuckTransfers struct {
	// Reader is the narrow non-terminal projection (never aggregates).
	Reader ports.StuckTransferReader
	// Events publishes the TransferStuckDetected occurrences (the
	// transactional outbox; without a broker the occurrences wait as
	// rows, exactly like saga events).
	Events ports.TransferEventPublisher
	// Check holds the per-state thresholds.
	Check transfer.StuckCheck
	// Limit bounds one pass (the "bounded" in bounded ticker: a pass
	// never scans an unbounded fleet). <= 0 means 500.
	Limit int
	// Now supplies the check clock (never time.Now directly).
	Now func() time.Time
}

// Execute runs one pass and returns the number of stuck transfers
// detected (and published). A publish failure fails the pass so the next
// tick retries the same view — occurrences are at-least-once, and a
// duplicate is harmless (an occurrence, not a state change).
func (u CheckStuckTransfers) Execute(ctx context.Context) (int, error) {
	if u.Now == nil {
		return 0, fmt.Errorf("check stuck transfers: clock is required")
	}
	limit := u.Limit
	if limit <= 0 {
		limit = 500
	}
	views, err := u.Reader.ListNonTerminal(ctx, limit)
	if err != nil {
		return 0, fmt.Errorf("check stuck transfers: list non-terminal: %w", err)
	}
	now := u.Now().UTC()
	stuck := u.Check.Evaluate(views, now)
	for _, s := range stuck {
		evt := transfer.StuckDetected{
			TransferID:       s.TransferID,
			State:            s.State,
			AgeSeconds:       s.AgeSeconds,
			ThresholdSeconds: s.ThresholdSeconds,
			OccurredAt:       now,
		}
		if err := u.Events.Publish(ctx, evt); err != nil {
			return 0, fmt.Errorf("check stuck transfers: publish TransferStuckDetected for %s: %w", s.TransferID, err)
		}
	}
	return len(stuck), nil
}
