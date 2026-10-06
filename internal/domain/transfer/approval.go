package transfer

import (
	"fmt"
	"strings"
	"time"

	"github.com/claudioed/network-inventory-planning/internal/domain/planning"
)

// ErrFactsIncomplete marks a fail-closed validation: a fact the approval
// needs is missing, stale or disabled. The API layer maps this to 422
// (proposition invalid against current facts) or 503 (read models not
// usable at all) — never to a silent partial approval.
var ErrFactsIncomplete = fmt.Errorf("transfer: planning facts incomplete for approval")

// ApprovalFacts is the CURRENT fail-closed snapshot an approval is validated
// against — the same facts the advisory simulation reads, plus the
// proposals's own snapshot for comparison.
type ApprovalFacts struct {
	Snapshot planning.PlanningSnapshot
	// MaxStaleness is the freshness budget the snapshot was built with.
	MaxStaleness time.Duration
}

// ValidateApproval proves an about-to-be-approved proposal is still valid
// against the CURRENT read models. Fail-closed, mirroring
// planning.BuildSnapshot: a missing or stale fact refuses the approval
// (wrapped ErrFactsIncomplete), never a zero-filled pass.
//
// v1 rules (single line, site-level):
//   - both sites must PARTICIPATE in the snapshot (each carries capability,
//     demand and capacity facts);
//   - the origin must be origin-enabled, the destination
//     destination-enabled (a disabled participating site is a hard error in
//     BuildSnapshot already, so reaching here with it disabled is a
//     contract break);
//   - the destination must still show a deficit for the SKU (there is
//     in-window ACTIVE demand for it) and the origin must show headroom
//     (capacity over window above its own in-window demand).
func ValidateApproval(in ProposalInput, facts ApprovalFacts) error {
	snap := facts.Snapshot
	if len(snap.Capabilities) == 0 {
		return fmt.Errorf("%w: planning snapshot carries no participating sites", ErrFactsIncomplete)
	}

	originCap, ok := snap.Capabilities[in.OriginSiteID]
	if !ok {
		return fmt.Errorf("%w: origin site %s has no complete facts in the planning snapshot", ErrFactsIncomplete, in.OriginSiteID)
	}
	if !originCap.OriginAllowed() {
		return fmt.Errorf("%w: origin site %s is not transfer-origin enabled", ErrFactsIncomplete, in.OriginSiteID)
	}
	destCap, ok := snap.Capabilities[in.DestinationSiteID]
	if !ok {
		return fmt.Errorf("%w: destination site %s has no complete facts in the planning snapshot", ErrFactsIncomplete, in.DestinationSiteID)
	}
	if !destCap.DestinationAllowed() {
		return fmt.Errorf("%w: destination site %s is not transfer-destination enabled", ErrFactsIncomplete, in.DestinationSiteID)
	}

	// The SKU must still be in deficit at the destination: v1 approximates
	// "destination still needs it" by in-window ACTIVE demand for the SKU.
	if snap.DemandBySiteSKU[in.DestinationSiteID+"\x00"+in.SKU] <= 0 {
		return fmt.Errorf("%w: destination site %s shows no in-window demand for SKU %s", ErrFactsIncomplete, in.DestinationSiteID, in.SKU)
	}

	// The origin must have capacity headroom: its published capacity over
	// the window must exceed its own in-window demand plus the transfer.
	originPlan, ok := snap.CapacityBySite[in.OriginSiteID]
	if !ok {
		return fmt.Errorf("%w: origin site %s has no published capacity plan", ErrFactsIncomplete, in.OriginSiteID)
	}
	originDemand := 0
	for key, units := range snap.DemandBySiteSKU {
		if strings.HasPrefix(key, in.OriginSiteID+"\x00") {
			originDemand += units
		}
	}
	if int(originPlan.CapacityOverWindow) < originDemand+in.Quantity {
		return fmt.Errorf("%w: origin site %s capacity %d cannot cover its in-window demand %d plus the transfer of %d",
			ErrFactsIncomplete, in.OriginSiteID, int(originPlan.CapacityOverWindow), originDemand, in.Quantity)
	}
	return nil
}
