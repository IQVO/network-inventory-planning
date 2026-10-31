package a2a

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"

	"github.com/claudioed/network-inventory-planning/internal/application/usecases"
	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

// tracerName is the OTel instrumentation scope for A2A skill spans, mirroring
// the MCP adapter's mcp.tool spans.
const tracerName = "github.com/claudioed/network-inventory-planning/internal/adapters/inbound/a2a"

// Skill ids advertised on the Agent Card (see Card).
const (
	skillTransferStatus     = "transfer_status"
	skillFindStuckTransfers = "find_stuck_transfers"
)

// maxOlderThanMinutes bounds olderThanMinutes (ten years): beyond it the
// minutes-to-Duration conversion would overflow into a negative threshold.
const maxOlderThanMinutes = 10 * 365 * 24 * 60

// skillRequest is the wire contract of every skill call: the first meaningful
// message part carries (as a Data part, or a Text part holding the same JSON)
// the skill id and its arguments. Strict on purpose -- an agent client is
// machine-to-machine, so unknown fields are rejected, never ignored: a
// misspelled name like "olderThanMinute" must not silently drop the threshold.
// (Go's encoding/json matches field names case-insensitively, so
// "transferid" still reaches transferId; only genuinely unknown names are
// refused.)
type skillRequest struct {
	Skill     string          `json:"skill"`
	Arguments json.RawMessage `json:"arguments"`
}

// transferStatusArgs and findStuckArgs are the per-skill argument shapes.
type transferStatusArgs struct {
	TransferID string `json:"transferId"`
}

type findStuckArgs struct {
	OlderThanMinutes int    `json:"olderThanMinutes"`
	State            string `json:"state"`
	Limit            *int   `json:"limit"`
}

// answer maps one incoming message to a skill and returns the agent's reply
// Message. It is the whole executor body: this is a message-only agent, so a
// skill run is exactly one request mapped to one answer (ADR-0020).
func answer(ctx context.Context, deps Deps, ec *a2asrv.ExecutorContext) a2a.Event {
	req, problem := parseSkillRequest(ec)
	if problem != nil {
		return reply(ec, envelope{Skill: "", Problem: problem}, problem.summary())
	}

	ctx, span := otel.Tracer(tracerName).Start(ctx, "a2a.skill "+req.Skill)
	defer span.End()
	span.SetAttributes(attribute.String("a2a.skill", req.Skill))

	dto, problem := run(ctx, deps, req)
	if problem != nil {
		span.SetStatus(codes.Error, problem.Detail)
		return reply(ec, envelope{Skill: req.Skill, Problem: problem}, problem.summary())
	}
	return reply(ec, envelope{Skill: req.Skill, Data: dto}, dto.Summary())
}

// run dispatches one validated request to its skill.
func run(ctx context.Context, deps Deps, req skillRequest) (skillDTO, *problem) {
	switch req.Skill {
	case skillTransferStatus:
		data, problem := deps.transferStatus(ctx, req.Arguments)
		if problem != nil {
			return skillDTO{}, problem
		}
		return skillDTO{TransferStatus: &data, summary: data.summary}, nil
	case skillFindStuckTransfers:
		data, problem := deps.findStuckTransfers(ctx, req.Arguments)
		if problem != nil {
			return skillDTO{}, problem
		}
		return skillDTO{StuckTransfers: &data, summary: data.summary}, nil
	default:
		return skillDTO{}, problemUnknownSkill(req.Skill)
	}
}

// reply builds the agent Message for one skill run: a Data part carrying the
// structured envelope and a Text part carrying the one-line summary, bound to
// the caller's context (and task, when the client referenced one).
func reply(ec *a2asrv.ExecutorContext, env envelope, summary string) a2a.Event {
	return a2a.NewMessageForTask(a2a.MessageRoleAgent, ec, a2a.NewDataPart(env), a2a.NewTextPart(summary))
}

// parseSkillRequest extracts the skillRequest from the message's first
// meaningful part. A Data part (or a Text part holding the same JSON object)
// is accepted; anything else is a request-shape problem, answered -- not
// raised -- because a malformed delegation is a business answer for the
// calling agent, not a transport failure.
func parseSkillRequest(ec *a2asrv.ExecutorContext) (skillRequest, *problem) {
	msg := ec.Message
	if msg == nil || len(msg.Parts) == 0 {
		return skillRequest{}, problemMalformedRequest("the message carries no parts")
	}
	raw, err := partJSON(msg.Parts[0])
	if err != nil {
		return skillRequest{}, problemMalformedRequest(fmt.Sprintf("the first message part is not a skill request: %v", err))
	}
	var req skillRequest
	if err := decodeStrict(raw, &req); err != nil {
		return skillRequest{}, problemMalformedRequest(fmt.Sprintf("the skill request is not valid: %v", err))
	}
	if req.Skill == "" {
		return skillRequest{}, problemMalformedRequest(`the skill request carries no "skill" field`)
	}
	return req, nil
}

// partJSON renders one part's content as the JSON bytes of a skill request.
func partJSON(part *a2a.Part) ([]byte, error) {
	switch content := part.Content.(type) {
	case a2a.Data:
		return json.Marshal(content.Value)
	case a2a.Text:
		return []byte(string(content)), nil
	default:
		return nil, errors.New("expected a data part (or a text part holding the skill JSON)")
	}
}

// decodeStrict decodes one JSON document into dst, rejecting unknown fields
// and trailing data. Absent arguments decode as the empty object so each
// skill's own required-field checks produce the specific problem.
func decodeStrict(raw []byte, dst any) error {
	if len(bytes.TrimSpace(raw)) == 0 {
		raw = []byte("{}")
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return err
	}
	if dec.More() {
		return errors.New("unexpected data after the JSON document")
	}
	return nil
}

// transferStatus is the single-transfer read skill: the transfer and its audit
// trail from this context's own state.
func (d Deps) transferStatus(ctx context.Context, rawArgs json.RawMessage) (transferStatus, *problem) {
	var args transferStatusArgs
	if err := decodeStrict(rawArgs, &args); err != nil {
		return transferStatus{}, problemInvalidArgument(fmt.Sprintf("arguments: %v", err))
	}
	if strings.TrimSpace(args.TransferID) == "" {
		return transferStatus{}, problemInvalidArgument("transferId is required")
	}
	if d.GetTransfer == nil {
		return transferStatus{}, problemReadSideNotConfigured()
	}
	t, err := d.GetTransfer.Execute(ctx, args.TransferID)
	if err != nil {
		return transferStatus{}, problemFromUseCase(err)
	}
	return toTransferStatus(t), nil
}

// validate checks the find_stuck_transfers arguments in the adapter, with
// adapter-owned texts that name only the keys this surface accepts (the use
// case's own wording names the MCP argument older_than).
func (a findStuckArgs) validate() *problem {
	if a.OlderThanMinutes <= 0 {
		return problemInvalidArgument("olderThanMinutes is required and must be a positive integer")
	}
	if a.OlderThanMinutes > maxOlderThanMinutes {
		return problemInvalidArgument(fmt.Sprintf("olderThanMinutes must not exceed %d", maxOlderThanMinutes))
	}
	if a.Limit != nil && (*a.Limit < 1 || *a.Limit > transfer.MaxListLimit) {
		return problemInvalidArgument(fmt.Sprintf("limit must be between 1 and %d, got %d", transfer.MaxListLimit, *a.Limit))
	}
	return nil
}

// findStuckTransfers is the stuck-transfer read skill. "Stuck" is defined by
// the use case (non-terminal state unchanged for strictly longer than the
// threshold), not here.
func (d Deps) findStuckTransfers(ctx context.Context, rawArgs json.RawMessage) (stuckTransfers, *problem) {
	var args findStuckArgs
	if err := decodeStrict(rawArgs, &args); err != nil {
		return stuckTransfers{}, problemInvalidArgument(fmt.Sprintf("arguments: %v", err))
	}
	if p := args.validate(); p != nil {
		return stuckTransfers{}, p
	}
	if d.FindStuckTransfers == nil {
		return stuckTransfers{}, problemReadSideNotConfigured()
	}
	limit := transfer.DefaultListLimit
	if args.Limit != nil {
		limit = *args.Limit
	}
	page, err := d.FindStuckTransfers.Execute(ctx, usecases.FindStuckInput{
		OlderThan: time.Duration(args.OlderThanMinutes) * time.Minute,
		State:     args.State,
		Limit:     limit,
	})
	if err != nil {
		return stuckTransfers{}, problemFromUseCase(err)
	}
	return toStuckTransfers(page, args.OlderThanMinutes, strings.ToUpper(strings.TrimSpace(args.State)), limit), nil
}

// problemFromUseCase maps a use case error to a problem answer, using the
// SAME classification as the REST read handlers (internal/adapters/inbound/http
// writeReadProblem): 400 for an invalid query, 404 for an unknown transfer,
// 503 for anything else (the store could not be read; the detail of an
// infrastructure failure is logged, never echoed). Domain rejections are
// EXPECTED business outcomes and are answered as RFC 7807-shaped problems, so
// the calling agent always receives a structured, machine-readable result.
func problemFromUseCase(err error) *problem {
	switch {
	case errors.Is(err, usecases.ErrInvalidTransferQuery):
		return &problem{Type: problemTypeBase + "invalid-query", Title: "Invalid query", Detail: err.Error(), Status: 400}
	case errors.Is(err, transfer.ErrTransferNotFound):
		return &problem{Type: problemTypeBase + "transfer-not-found", Title: "Transfer not found", Detail: err.Error(), Status: 404}
	default:
		slog.Error("a2a skill failed", "error", err)
		return &problem{Type: problemTypeBase + "read-side-unavailable", Title: "Transfer read side is unavailable", Detail: "the transfer store could not be read", Status: 503}
	}
}
