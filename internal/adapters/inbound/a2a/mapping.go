package a2a

import (
	"fmt"
	"time"

	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

// problemTypeBase is the REST adapter's problem namespace
// (internal/adapters/inbound/http writeProblem), so a delegating agent sees
// the same "type" URIs for the same outcomes a REST caller would.
const problemTypeBase = "https://warehouse.example/problems/"

// problem is the RFC 7807-shaped rejection an agent client receives instead of
// a result: domain rejections and request-shape failures are business answers,
// not transport errors, so they travel in the message envelope.
type problem struct {
	Type   string `json:"type"`
	Title  string `json:"title"`
	Detail string `json:"detail"`
	Status int    `json:"status"`
}

// summary renders the one-line form used for the reply's text part.
func (p *problem) summary() string {
	return fmt.Sprintf("%s (%d): %s", p.Title, p.Status, p.Detail)
}

func problemMalformedRequest(detail string) *problem {
	return &problem{Type: problemTypeBase + "malformed-skill-request", Title: "Malformed skill request", Detail: detail, Status: 400}
}

func problemUnknownSkill(skill string) *problem {
	return &problem{Type: problemTypeBase + "unknown-skill", Title: "Unknown skill", Detail: fmt.Sprintf("skill %q is not offered by this agent; the Agent Card lists the supported skills", skill), Status: 400}
}

func problemInvalidArgument(detail string) *problem {
	return &problem{Type: problemTypeBase + "invalid-argument", Title: "Invalid skill argument", Detail: detail, Status: 400}
}

func problemReadSideNotConfigured() *problem {
	return &problem{Type: problemTypeBase + "read-side-unavailable", Title: "Transfer read side is not configured", Detail: "this instance runs without DATABASE_URL, so there is no transfer store to read", Status: 503}
}

// envelope is the structured payload of every skill answer: exactly one of
// Data (a skill DTO) or Problem (why there is no result), plus the skill id
// the answer belongs to.
type envelope struct {
	Skill   string   `json:"skill"`
	Data    skillDTO `json:"data,omitempty"`
	Problem *problem `json:"problem,omitempty"`
}

// skillDTO is the union of the skill result DTOs. A2A parts are
// machine-readable JSON; each skill fills its own field and Summary, keeping
// the envelope flat and versioned as one contract.
type skillDTO struct {
	TransferStatus *transferStatus `json:"transferStatus,omitempty"`
	StuckTransfers *stuckTransfers `json:"stuckTransfers,omitempty"`
	summary        string
}

// Summary is the human-readable one-liner for the reply's text part.
func (s skillDTO) Summary() string { return s.summary }

// transferView is the wire shape of one transfer, camelCase like the REST
// DTO. Optional facts are omitted until the saga has produced them. Domain
// types never cross the boundary.
type transferView struct {
	ID    string `json:"id"`
	State string `json:"state"`
	SKU   string `json:"sku"`
	// Quantity is in eaches.
	Quantity int `json:"quantity"`
	// Case rounding (ADR 0017), present only for a SKU that had an active
	// CASE level at approval.
	CaseMultiple       *int   `json:"caseMultiple,omitempty"`
	Cases              *int   `json:"cases,omitempty"`
	RequestedQtyEaches *int   `json:"requestedQtyEaches,omitempty"`
	RoundedQtyEaches   *int   `json:"roundedQtyEaches,omitempty"`
	RemainderEaches    *int   `json:"remainderEaches,omitempty"`
	PickedQuantity     int    `json:"pickedQuantity,omitempty"`
	OriginSiteID       string `json:"originSiteId"`
	DestinationSiteID  string `json:"destinationSiteId"`
	PolicyVersion      string `json:"policyVersion"`
	ReservationID      string `json:"reservationId,omitempty"`
	RejectionReason    string `json:"rejectionReason,omitempty"`
	ExpiresAt          string `json:"expiresAt"`
	CreatedAt          string `json:"createdAt"`
	UpdatedAt          string `json:"updatedAt"`
	Version            int64  `json:"version"`
}

// auditView is one state transition: when, what moved and the cause.
type auditView struct {
	Seq        int64  `json:"seq"`
	From       string `json:"from,omitempty"`
	To         string `json:"to"`
	Event      string `json:"event"`
	Cause      string `json:"cause"`
	OccurredAt string `json:"occurredAt"`
}

// transferStatus is the DTO of the transfer_status skill: the transfer and
// its audit trail.
type transferStatus struct {
	transferView
	Audit   []auditView `json:"audit"`
	summary string
}

// stuckTransfers is the DTO of the find_stuck_transfers skill: one page of
// stuck transfers, stalest first. Total counts every match before paging, so
// Total > len(Transfers) means there are more.
type stuckTransfers struct {
	OlderThanMinutes int            `json:"olderThanMinutes"`
	State            string         `json:"state,omitempty"`
	Limit            int            `json:"limit"`
	Total            int            `json:"total"`
	Transfers        []transferView `json:"transfers"`
	summary          string
}

// rfc3339 renders a timestamp the way every view does (UTC, RFC 3339).
func rfc3339(t time.Time) string { return t.UTC().Format(time.RFC3339) }

func toTransferView(t *transfer.InterWarehouseTransfer) transferView {
	view := transferView{
		ID:                string(t.ID()),
		State:             string(t.State()),
		SKU:               t.SKU(),
		Quantity:          t.Quantity(),
		PickedQuantity:    t.PickedQuantity(),
		OriginSiteID:      t.OriginSiteID(),
		DestinationSiteID: t.DestinationSiteID(),
		PolicyVersion:     t.PolicyVersion(),
		ReservationID:     t.ReservationID(),
		RejectionReason:   string(t.RejectionReason()),
		ExpiresAt:         rfc3339(t.ExpiresAt()),
		CreatedAt:         rfc3339(t.CreatedAt()),
		UpdatedAt:         rfc3339(t.UpdatedAt()),
		Version:           t.Version(),
	}
	if r := t.CaseRounding(); r != nil {
		view.CaseMultiple, view.Cases = &r.CaseMultiple, &r.Cases
		view.RequestedQtyEaches, view.RoundedQtyEaches, view.RemainderEaches = &r.RequestedEaches, &r.RoundedEaches, &r.RemainderEaches
	}
	return view
}

func toTransferViews(items []*transfer.InterWarehouseTransfer) []transferView {
	views := make([]transferView, 0, len(items))
	for _, t := range items {
		views = append(views, toTransferView(t))
	}
	return views
}

// toTransferStatus maps the aggregate (with its audit trail) into the skill
// DTO and its one-line summary.
func toTransferStatus(t *transfer.InterWarehouseTransfer) transferStatus {
	audit := t.Audit()
	entries := make([]auditView, 0, len(audit))
	for _, e := range audit {
		entries = append(entries, auditView{
			Seq: e.Seq, From: string(e.From), To: string(e.To), Event: e.Event, Cause: e.Reason, OccurredAt: rfc3339(e.OccurredAt),
		})
	}
	view := toTransferView(t)
	return transferStatus{
		transferView: view,
		Audit:        entries,
		summary: fmt.Sprintf("transfer %s: %s, %d x %s from %s to %s, last change %s, %d audit entries",
			view.ID, view.State, view.Quantity, view.SKU, view.OriginSiteID, view.DestinationSiteID, view.UpdatedAt, len(entries)),
	}
}

// toStuckTransfers maps one page into the skill DTO and its one-line summary.
func toStuckTransfers(page transfer.TransferPage, olderThanMinutes int, state string, limit int) stuckTransfers {
	views := toTransferViews(page.Items)
	summary := fmt.Sprintf("%d of %d transfers have not advanced for more than %d minutes", len(views), page.Total, olderThanMinutes)
	if len(views) > 0 {
		summary += fmt.Sprintf("; stalest: %s (%s, last change %s)", views[0].ID, views[0].State, views[0].UpdatedAt)
	}
	return stuckTransfers{OlderThanMinutes: olderThanMinutes, State: state, Limit: limit, Total: page.Total, Transfers: views, summary: summary}
}
