package usecases

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/claudioed/network-inventory-planning/internal/application/ports"
	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

// ErrPackLevelsUnavailable marks a failure to read the local pack-level view
// (an outage, not a problem with the request): the HTTP layer answers 503.
var ErrPackLevelsUnavailable = errors.New("pack levels are unavailable")

// GenerateTransferProposals accepts one coherent, externally projected network
// snapshot. The application layer deliberately receives inputs; it never makes
// synchronous calls into Inventory Storage, Order Management, or facility services.
type GenerateTransferProposals struct {
	Planner transfer.Planner
	// Packs is the local pack-level view (ADR 0017). With it, a SKU that has
	// an active CASE level at the snapshot's as-of is proposed in whole
	// cases and its remainder is reported; without it (nil) every SKU is
	// planned in eaches, exactly as before.
	Packs ports.PackLevelView
}

// GenerateInput is the explicit boundary of a reproducible planning run.
type GenerateInput struct {
	AsOf      time.Time
	Positions []transfer.Position
	Policies  []transfer.Policy
	Lanes     []transfer.Lane
}

// Execute returns only advisory proposals. Downstream lifecycle orchestration
// must obtain an explicit Inventory Storage reservation before any WES work.
func (u GenerateTransferProposals) Execute(ctx context.Context, input GenerateInput) ([]transfer.Proposal, error) {
	if input.AsOf.IsZero() {
		return nil, fmt.Errorf("generate transfer proposals: asOf is required")
	}
	for _, position := range input.Positions {
		if !position.AsOf.Equal(input.AsOf) {
			return nil, fmt.Errorf("generate transfer proposals: mixed position snapshot")
		}
	}
	multiples, err := ActiveCaseMultiples(ctx, u.Packs, policySKUs(input.Policies), input.AsOf)
	if err != nil {
		return nil, fmt.Errorf("generate transfer proposals: %w: %w", ErrPackLevelsUnavailable, err)
	}
	return u.Planner.WithCaseMultiples(multiples).Generate(input.Positions, input.Policies, input.Lanes), nil
}

// policySKUs lists the SKUs the policies manage (duplicates allowed;
// ActiveCaseMultiples reads each once).
func policySKUs(policies []transfer.Policy) []string {
	skus := make([]string, 0, len(policies))
	for _, p := range policies {
		skus = append(skus, string(p.SKU))
	}
	return skus
}
