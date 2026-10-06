package usecases

import (
	"fmt"
	"time"

	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

// GenerateTransferProposals accepts one coherent, externally projected network
// snapshot. The application layer deliberately receives inputs; it never makes
// synchronous calls into Inventory Storage, Order Management, or facility services.
type GenerateTransferProposals struct {
	Planner transfer.Planner
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
func (u GenerateTransferProposals) Execute(input GenerateInput) ([]transfer.Proposal, error) {
	if input.AsOf.IsZero() {
		return nil, fmt.Errorf("generate transfer proposals: asOf is required")
	}
	for _, position := range input.Positions {
		if !position.AsOf.Equal(input.AsOf) {
			return nil, fmt.Errorf("generate transfer proposals: mixed position snapshot")
		}
	}
	return u.Planner.Generate(input.Positions, input.Policies, input.Lanes), nil
}
