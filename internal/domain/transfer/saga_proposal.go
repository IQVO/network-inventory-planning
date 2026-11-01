package transfer

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// ProposalInput is everything ProposeTransfer needs, including the clock
// (never time.Now inside the domain).
type ProposalInput struct {
	ID                TransferID
	IdempotencyKey    string
	OriginSiteID      string
	DestinationSiteID string
	SKU               string
	Quantity          int
	// CaseRounding is set only when the SKU has an active CASE level and
	// Quantity is the rounded whole-case quantity (ADR 0017); nil leaves
	// the transfer exactly as before.
	CaseRounding   *CaseRounding
	PolicyVersion  string
	OperatorReason string
	ProposalAsOf   time.Time
	ExpiresAt      time.Time
	Now            time.Time
}

// ProposeTransfer validates the proposal snapshot and returns the aggregate
// in state PROPOSED, with the creation and proposal transitions already in
// its audit trail. The caller (the approval use case) then applies Approve
// and RequestAllocation in the same transaction.
func ProposeTransfer(in ProposalInput) (*InterWarehouseTransfer, error) {
	if err := in.validate(); err != nil {
		return nil, err
	}
	now := in.Now.UTC()
	t := &InterWarehouseTransfer{
		id:                in.ID,
		idempotencyKey:    in.IdempotencyKey,
		originSiteID:      in.OriginSiteID,
		destinationSiteID: in.DestinationSiteID,
		sku:               in.SKU,
		quantity:          in.Quantity,
		caseRounding:      copyRounding(in.CaseRounding),
		policyVersion:     in.PolicyVersion,
		operatorReason:    in.OperatorReason,
		proposalAsOf:      in.ProposalAsOf.UTC(),
		expiresAt:         in.ExpiresAt.UTC(),
		// No state yet: the first transition is the creation entry, whose
		// From is "" (see AuditEntry).
		createdAt: now,
		updatedAt: now,
	}
	t.transition(StateDraft, "TransferDrafted", "drafted from advisory proposal snapshot", now)
	t.transition(StateProposed, "TransferProposed", in.OperatorReason, now)
	return t, nil
}

// ErrInvalidProposal marks a proposal snapshot the domain refuses on its face
// (a missing field, a non-positive quantity, origin == destination). It is the
// caller's input, never an outage: the approve use case maps it to
// ErrInvalidApproval so the API answers 422, not 503.
var ErrInvalidProposal = errors.New("transfer: invalid proposal")

// invalidProposalError keeps the message exactly as written while letting
// errors.Is(err, ErrInvalidProposal) match.
type invalidProposalError struct{ msg string }

func (e *invalidProposalError) Error() string { return e.msg }

func (e *invalidProposalError) Is(target error) bool { return target == ErrInvalidProposal }

func invalidProposal(format string, args ...any) error {
	return &invalidProposalError{msg: fmt.Sprintf(format, args...)}
}

func (in ProposalInput) validate() error {
	if strings.TrimSpace(string(in.ID)) == "" {
		return invalidProposal("transfer proposal: transfer id is required")
	}
	if strings.TrimSpace(in.IdempotencyKey) == "" {
		return invalidProposal("transfer proposal %s: idempotency key is required", in.ID)
	}
	if strings.TrimSpace(in.OriginSiteID) == "" {
		return invalidProposal("transfer proposal %s: origin site is required", in.ID)
	}
	if strings.TrimSpace(in.DestinationSiteID) == "" {
		return invalidProposal("transfer proposal %s: destination site is required", in.ID)
	}
	if in.OriginSiteID == in.DestinationSiteID {
		return invalidProposal("transfer proposal %s: origin and destination must differ", in.ID)
	}
	if strings.TrimSpace(in.SKU) == "" {
		return invalidProposal("transfer proposal %s: sku is required", in.ID)
	}
	if in.Quantity <= 0 {
		return invalidProposal("transfer proposal %s: quantity must be positive, got %d", in.ID, in.Quantity)
	}
	if in.CaseRounding != nil {
		if err := in.CaseRounding.check(in.Quantity); err != nil {
			return invalidProposal("transfer proposal %s: case rounding: %v", in.ID, err)
		}
	}
	if strings.TrimSpace(in.PolicyVersion) == "" {
		return invalidProposal("transfer proposal %s: policy version is required", in.ID)
	}
	if in.ProposalAsOf.IsZero() {
		return invalidProposal("transfer proposal %s: proposal as-of is required", in.ID)
	}
	if in.Now.IsZero() {
		return invalidProposal("transfer proposal %s: clock is required", in.ID)
	}
	if in.ExpiresAt.IsZero() {
		return invalidProposal("transfer proposal %s: expiry is required", in.ID)
	}
	if !in.ExpiresAt.After(in.Now) {
		return fmt.Errorf("%w: transfer %s expires at %s, now %s", ErrProposalExpired, in.ID, in.ExpiresAt.UTC().Format(time.RFC3339), in.Now.UTC().Format(time.RFC3339))
	}
	return nil
}
