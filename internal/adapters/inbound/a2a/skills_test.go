package a2a

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2aclient"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/claudioed/network-inventory-planning/internal/application/usecases"
	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

var now = time.Date(2026, 10, 6, 15, 0, 0, 0, time.UTC)

// fakeQuery is an in-memory ports.TransferQuery honouring the filter fields
// the skills exercise (states, updated-before, order, paging), so the tests
// drive the real use cases end to end.
type fakeQuery struct {
	transfers []*transfer.InterWarehouseTransfer
	err       error
}

func (f *fakeQuery) Get(_ context.Context, id transfer.TransferID) (*transfer.InterWarehouseTransfer, error) {
	if f.err != nil {
		return nil, f.err
	}
	for _, t := range f.transfers {
		if t.ID() == id {
			return t, nil
		}
	}
	return nil, transfer.ErrTransferNotFound
}

func (f *fakeQuery) List(_ context.Context, filter transfer.ListFilter) (transfer.TransferPage, error) {
	if f.err != nil {
		return transfer.TransferPage{}, f.err
	}
	var matched []*transfer.InterWarehouseTransfer
	for _, t := range f.transfers {
		if len(filter.States) > 0 && !containsState(filter.States, t.State()) {
			continue
		}
		if !filter.UpdatedBefore.IsZero() && !t.UpdatedAt().Before(filter.UpdatedBefore) {
			continue
		}
		matched = append(matched, t)
	}
	if filter.Order == transfer.OrderStalestFirst {
		sort.SliceStable(matched, func(i, j int) bool { return matched[i].UpdatedAt().Before(matched[j].UpdatedAt()) })
	}
	total := len(matched)
	if filter.Limit > 0 && len(matched) > filter.Limit {
		matched = matched[:filter.Limit]
	}
	return transfer.TransferPage{Items: matched, Total: total}, nil
}

func containsState(states []transfer.TransferState, s transfer.TransferState) bool {
	for _, candidate := range states {
		if candidate == s {
			return true
		}
	}
	return false
}

func snapshotTransfer(id string, state transfer.TransferState, origin, destination string, updated time.Time) *transfer.InterWarehouseTransfer {
	return transfer.Rehydrate(transfer.Snapshot{
		ID: transfer.TransferID(id), IdempotencyKey: "key-" + id, OriginSiteID: origin, DestinationSiteID: destination,
		SKU: "SKU-1", Quantity: 10, PolicyVersion: "p1", OperatorReason: "rebalance",
		ProposalAsOf: now.Add(-48 * time.Hour), ExpiresAt: now.Add(24 * time.Hour),
		State: state, CreatedAt: updated.Add(-time.Hour), UpdatedAt: updated, Version: 3,
		Audit: []transfer.AuditEntry{
			{Seq: 1, To: transfer.StateDraft, Event: "TransferDrafted", Reason: "drafted from advisory proposal snapshot", OccurredAt: updated.Add(-time.Hour)},
			{Seq: 2, From: transfer.StateDraft, To: state, Event: "TransferMoved", Reason: "cause-" + id, OccurredAt: updated},
		},
	})
}

func fixtureTransfers() []*transfer.InterWarehouseTransfer {
	return []*transfer.InterWarehouseTransfer{
		snapshotTransfer("tr-alloc-stale", transfer.StateAllocating, "WH1", "WH2", now.Add(-3*time.Hour)),
		snapshotTransfer("tr-alloc-fresh", transfer.StateAllocating, "WH1", "WH3", now.Add(-5*time.Minute)),
		snapshotTransfer("tr-transit-stale", transfer.StateInTransit, "WH2", "WH1", now.Add(-26*time.Hour)),
		snapshotTransfer("tr-received-old", transfer.StateReceived, "WH1", "WH2", now.Add(-72*time.Hour)),
		snapshotTransfer("tr-cancelled-old", transfer.StateCancelled, "WH3", "WH2", now.Add(-90*time.Hour)),
	}
}

func newDeps(q *fakeQuery) Deps {
	return Deps{
		GetTransfer:        &usecases.GetTransfer{Query: q},
		FindStuckTransfers: &usecases.FindStuckTransfers{Query: q, Now: func() time.Time { return now }},
	}
}

func fixtureDeps() Deps { return newDeps(&fakeQuery{transfers: fixtureTransfers()}) }

// ---- the wire: JSON-RPC over HTTP, driven by the SDK's own client ----------

// wireEnvelope is a TEST-LOCAL copy of the answer contract. If the adapter's
// envelope drifts, this suite fails instead of silently following it.
type wireTransfer struct {
	ID                string `json:"id"`
	State             string `json:"state"`
	SKU               string `json:"sku"`
	Quantity          int    `json:"quantity"`
	OriginSiteID      string `json:"originSiteId"`
	DestinationSiteID string `json:"destinationSiteId"`
	PolicyVersion     string `json:"policyVersion"`
	UpdatedAt         string `json:"updatedAt"`
	Version           int64  `json:"version"`
}

type wireEnvelope struct {
	Skill string `json:"skill"`
	Data  struct {
		TransferStatus *struct {
			wireTransfer
			Audit []struct {
				Seq        int64  `json:"seq"`
				From       string `json:"from"`
				To         string `json:"to"`
				Event      string `json:"event"`
				Cause      string `json:"cause"`
				OccurredAt string `json:"occurredAt"`
			} `json:"audit"`
		} `json:"transferStatus"`
		StuckTransfers *struct {
			OlderThanMinutes int            `json:"olderThanMinutes"`
			State            string         `json:"state"`
			Limit            int            `json:"limit"`
			Total            int            `json:"total"`
			Transfers        []wireTransfer `json:"transfers"`
		} `json:"stuckTransfers"`
	} `json:"data"`
	Problem *struct {
		Type   string `json:"type"`
		Title  string `json:"title"`
		Detail string `json:"detail"`
		Status int    `json:"status"`
	} `json:"problem"`
}

type wire struct {
	client *a2aclient.Client
}

func newWire(t *testing.T, deps Deps) *wire {
	t.Helper()
	srv := httptest.NewServer(Handler(NewExecutor(deps)))
	t.Cleanup(srv.Close)
	card := Card(Config{AgentURL: srv.URL + "/", Version: "test"})
	client, err := a2aclient.NewFromCard(context.Background(), card)
	if err != nil {
		t.Fatalf("NewFromCard: %v", err)
	}
	return &wire{client: client}
}

// call sends one message part and returns the decoded envelope and the
// one-line text summary of the agent's reply.
func (w *wire) call(t *testing.T, part *a2a.Part) (wireEnvelope, string) {
	t.Helper()
	resp, err := w.client.SendMessage(context.Background(), &a2a.SendMessageRequest{Message: a2a.NewMessage(a2a.MessageRoleUser, part)})
	if err != nil {
		t.Fatalf("SendMessage: %v (a domain rejection must be answered in-band, never raised)", err)
	}
	msg, ok := resp.(*a2a.Message)
	if !ok || len(msg.Parts) < 2 {
		t.Fatalf("reply = %T, want a message with a data and a text part", resp)
	}
	data, ok := msg.Parts[0].Content.(a2a.Data)
	if !ok {
		t.Fatalf("first reply part is %T, want data", msg.Parts[0].Content)
	}
	raw, _ := json.Marshal(data.Value)
	var env wireEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatalf("reply is not the skill envelope: %v (%s)", err, raw)
	}
	text, _ := msg.Parts[1].Content.(a2a.Text)
	return env, string(text)
}

func (w *wire) skill(t *testing.T, skill string, args map[string]any) (wireEnvelope, string) {
	t.Helper()
	return w.call(t, a2a.NewDataPart(map[string]any{"skill": skill, "arguments": args}))
}

func wantProblem(t *testing.T, env wireEnvelope, status int, slug string) {
	t.Helper()
	if env.Problem == nil {
		t.Fatalf("expected a %d problem, got data: %+v", status, env.Data)
	}
	if env.Problem.Status != status || env.Problem.Type != problemTypeBase+slug {
		t.Fatalf("problem = %+v, want %d %s", env.Problem, status, slug)
	}
}

func ids(ts []wireTransfer) []string {
	out := make([]string, 0, len(ts))
	for _, tr := range ts {
		out = append(out, tr.ID)
	}
	return out
}

func TestWireTransferStatusReadsTheTransfer(t *testing.T) {
	w := newWire(t, fixtureDeps())

	env, text := w.skill(t, skillTransferStatus, map[string]any{"transferId": "tr-alloc-stale"})
	s := env.Data.TransferStatus
	if env.Problem != nil || s == nil || env.Skill != skillTransferStatus {
		t.Fatalf("envelope = %+v", env)
	}
	want := wireTransfer{
		ID: "tr-alloc-stale", State: "ALLOCATING", SKU: "SKU-1", Quantity: 10, OriginSiteID: "WH1", DestinationSiteID: "WH2",
		PolicyVersion: "p1", UpdatedAt: now.Add(-3 * time.Hour).Format(time.RFC3339), Version: 3,
	}
	if s.wireTransfer != want {
		t.Fatalf("transfer = %+v, want %+v", s.wireTransfer, want)
	}
	wantText := "transfer tr-alloc-stale: ALLOCATING, 10 x SKU-1 from WH1 to WH2, last change 2026-10-06T12:00:00Z, 2 audit entries"
	if text != wantText {
		t.Fatalf("summary = %q, want %q", text, wantText)
	}

	// The id is trimmed like the use case does for REST and MCP.
	env, _ = w.skill(t, skillTransferStatus, map[string]any{"transferId": "  tr-transit-stale "})
	if env.Data.TransferStatus == nil || env.Data.TransferStatus.State != "IN_TRANSIT" {
		t.Fatalf("a padded id must resolve, got %+v", env)
	}
}

func TestWireTransferStatusCarriesTheAuditTrail(t *testing.T) {
	w := newWire(t, fixtureDeps())
	env, _ := w.skill(t, skillTransferStatus, map[string]any{"transferId": "tr-alloc-stale"})
	if env.Data.TransferStatus == nil {
		t.Fatalf("envelope = %+v", env)
	}
	audit := env.Data.TransferStatus.Audit
	if len(audit) != 2 {
		t.Fatalf("audit = %+v, want 2 entries", audit)
	}
	first, second := audit[0], audit[1]
	if first.Seq != 1 || first.From != "" || first.To != "DRAFT" {
		t.Errorf("first audit entry = %+v", first)
	}
	if second.From != "DRAFT" || second.To != "ALLOCATING" || second.Cause != "cause-tr-alloc-stale" || second.Event != "TransferMoved" {
		t.Errorf("second audit entry = %+v", second)
	}
}

func TestWireTransferStatusUnknownIdIsAnsweredAs404(t *testing.T) {
	w := newWire(t, fixtureDeps())
	env, text := w.skill(t, skillTransferStatus, map[string]any{"transferId": "tr-nope"})
	wantProblem(t, env, 404, "transfer-not-found")
	if !strings.Contains(text, "Transfer not found (404)") {
		t.Fatalf("summary = %q", text)
	}
}

func TestWireFindStuckTransfersReturnsStalestFirstAndSkipsFinishedOnes(t *testing.T) {
	w := newWire(t, fixtureDeps())

	env, text := w.skill(t, skillFindStuckTransfers, map[string]any{"olderThanMinutes": 60})
	s := env.Data.StuckTransfers
	if env.Problem != nil || s == nil || env.Skill != skillFindStuckTransfers {
		t.Fatalf("envelope = %+v", env)
	}
	// 26h IN_TRANSIT before 3h ALLOCATING; the 5-minute-old one is fresh and
	// RECEIVED / CANCELLED are finished however old.
	if got := ids(s.Transfers); len(got) != 2 || got[0] != "tr-transit-stale" || got[1] != "tr-alloc-stale" {
		t.Fatalf("stuck = %v, want [tr-transit-stale tr-alloc-stale]", got)
	}
	if s.Total != 2 || s.Limit != 50 || s.OlderThanMinutes != 60 || s.State != "" {
		t.Fatalf("page = %+v", s)
	}
	want := "2 of 2 transfers have not advanced for more than 60 minutes; stalest: tr-transit-stale (IN_TRANSIT, last change 2026-10-05T13:00:00Z)"
	if text != want {
		t.Fatalf("summary = %q, want %q", text, want)
	}
}

// Strictly longer than the threshold: 180 minutes is exactly the age of
// tr-alloc-stale, which is therefore not yet stuck.
func TestWireFindStuckTransfersThresholdIsStrict(t *testing.T) {
	w := newWire(t, fixtureDeps())
	env, _ := w.skill(t, skillFindStuckTransfers, map[string]any{"olderThanMinutes": 180})
	if env.Data.StuckTransfers == nil {
		t.Fatalf("envelope = %+v", env)
	}
	if got := ids(env.Data.StuckTransfers.Transfers); len(got) != 1 || got[0] != "tr-transit-stale" {
		t.Fatalf("stuck at the exact threshold = %v, want only tr-transit-stale", got)
	}
}

func TestWireFindStuckTransfersNothingStuckIsDataNotAProblem(t *testing.T) {
	w := newWire(t, fixtureDeps())
	env, text := w.skill(t, skillFindStuckTransfers, map[string]any{"olderThanMinutes": 100000})
	s := env.Data.StuckTransfers
	if s == nil || s.Total != 0 || len(s.Transfers) != 0 {
		t.Fatalf("nothing stuck = %+v", env)
	}
	if text != "0 of 0 transfers have not advanced for more than 100000 minutes" {
		t.Fatalf("summary = %q", text)
	}
}

func TestWireFindStuckTransfersNarrowsByStateAndPages(t *testing.T) {
	w := newWire(t, fixtureDeps())

	env, _ := w.skill(t, skillFindStuckTransfers, map[string]any{"olderThanMinutes": 60, "state": "allocating"})
	s := env.Data.StuckTransfers
	if s == nil || len(s.Transfers) != 1 || s.Transfers[0].ID != "tr-alloc-stale" || s.State != "ALLOCATING" {
		t.Fatalf("state narrowing = %+v", s)
	}

	env, _ = w.skill(t, skillFindStuckTransfers, map[string]any{"olderThanMinutes": 60, "limit": 1})
	s = env.Data.StuckTransfers
	if s == nil || len(s.Transfers) != 1 || s.Total != 2 || s.Limit != 1 || s.Transfers[0].ID != "tr-transit-stale" {
		t.Fatalf("paging = %+v, want one item of total 2", s)
	}
}

func TestWireFindStuckTransfersRejectionsFromTheUseCaseAreAnsweredAs400(t *testing.T) {
	w := newWire(t, fixtureDeps())
	for name, state := range map[string]string{"terminal state": "RECEIVED", "unknown state": "LOST"} {
		t.Run(name, func(t *testing.T) {
			env, _ := w.skill(t, skillFindStuckTransfers, map[string]any{"olderThanMinutes": 60, "state": state})
			wantProblem(t, env, 400, "invalid-query")
		})
	}
}

func TestWireArgumentValidation(t *testing.T) {
	w := newWire(t, fixtureDeps())
	cases := map[string]struct {
		skill string
		args  map[string]any
		slug  string
		want  string
	}{
		"status: missing id":             {skillTransferStatus, map[string]any{}, "invalid-argument", "transferId is required"},
		"status: blank id":               {skillTransferStatus, map[string]any{"transferId": "   "}, "invalid-argument", "transferId is required"},
		"status: unknown argument":       {skillTransferStatus, map[string]any{"transferId": "tr-1", "audit": false}, "invalid-argument", `unknown field "audit"`},
		"status: MCP snake_case name":    {skillTransferStatus, map[string]any{"transfer_id": "tr-1"}, "invalid-argument", `unknown field "transfer_id"`},
		"status: id of the wrong type":   {skillTransferStatus, map[string]any{"transferId": 7}, "invalid-argument", "arguments:"},
		"stuck: missing threshold":       {skillFindStuckTransfers, map[string]any{}, "invalid-argument", "olderThanMinutes is required"},
		"stuck: zero threshold":          {skillFindStuckTransfers, map[string]any{"olderThanMinutes": 0}, "invalid-argument", "olderThanMinutes is required"},
		"stuck: negative threshold":      {skillFindStuckTransfers, map[string]any{"olderThanMinutes": -5}, "invalid-argument", "olderThanMinutes is required"},
		"stuck: overflowing threshold":   {skillFindStuckTransfers, map[string]any{"olderThanMinutes": 9223372036854775}, "invalid-argument", "must not exceed"},
		"stuck: fractional threshold":    {skillFindStuckTransfers, map[string]any{"olderThanMinutes": 1.5}, "invalid-argument", "arguments:"},
		"stuck: MCP snake_case name":     {skillFindStuckTransfers, map[string]any{"older_than_minutes": 30}, "invalid-argument", `unknown field "older_than_minutes"`},
		"stuck: typo'd optional field":   {skillFindStuckTransfers, map[string]any{"olderThanMinutes": 30, "stat": "ALLOCATING"}, "invalid-argument", `unknown field "stat"`},
		"stuck: limit zero":              {skillFindStuckTransfers, map[string]any{"olderThanMinutes": 30, "limit": 0}, "invalid-argument", "limit must be between 1 and 200"},
		"stuck: limit above the maximum": {skillFindStuckTransfers, map[string]any{"olderThanMinutes": 30, "limit": 201}, "invalid-argument", "limit must be between 1 and 200"},
		"stuck: negative limit":          {skillFindStuckTransfers, map[string]any{"olderThanMinutes": 30, "limit": -1}, "invalid-argument", "limit must be between 1 and 200"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			env, _ := w.skill(t, tc.skill, tc.args)
			wantProblem(t, env, 400, tc.slug)
			if !strings.Contains(env.Problem.Detail, tc.want) {
				t.Fatalf("detail = %q, want it to contain %q", env.Problem.Detail, tc.want)
			}
		})
	}
}

// TestWireNeverTeachesTheMCPArgumentNames: the shared use case words its
// refusal with the MCP argument names (older_than, transfer id). Those names
// are refused here as unknown arguments, so no answer an agent can receive may
// teach them; the adapter validates first and owns the detail text.
func TestWireNeverTeachesTheMCPArgumentNames(t *testing.T) {
	w := newWire(t, fixtureDeps())
	for _, args := range []map[string]any{
		{"olderThanMinutes": 0},
		{"olderThanMinutes": -3},
		{},
	} {
		env, text := w.skill(t, skillFindStuckTransfers, args)
		wantProblem(t, env, 400, "invalid-argument")
		for _, s := range []string{env.Problem.Detail, env.Problem.Title, env.Problem.Type, text} {
			if strings.Contains(s, "older_than") {
				t.Fatalf("an answer teaches the MCP argument name older_than: %q", s)
			}
		}
	}
	env, text := w.skill(t, skillTransferStatus, map[string]any{"transferId": ""})
	wantProblem(t, env, 400, "invalid-argument")
	for _, s := range []string{env.Problem.Detail, text} {
		if strings.Contains(s, "transfer_id") || strings.Contains(s, "transfer id is required") {
			t.Fatalf("an answer teaches the use case wording: %q", s)
		}
	}
}

func TestWireRequestShapeProblems(t *testing.T) {
	w := newWire(t, fixtureDeps())

	env, _ := w.call(t, a2a.NewTextPart("not json at all"))
	wantProblem(t, env, 400, "malformed-skill-request")

	env, _ = w.call(t, a2a.NewDataPart(map[string]any{"arguments": map[string]any{}}))
	wantProblem(t, env, 400, "malformed-skill-request")

	env, _ = w.call(t, a2a.NewDataPart(map[string]any{"skill": skillTransferStatus, "arguments": map[string]any{}, "extra": 1}))
	wantProblem(t, env, 400, "malformed-skill-request")

	// Trailing data after the JSON document is refused, not ignored.
	env, _ = w.call(t, a2a.NewTextPart(`{"skill":"transfer_status","arguments":{"transferId":"tr-alloc-stale"}} {"skill":"x"}`))
	wantProblem(t, env, 400, "malformed-skill-request")

	env, _ = w.skill(t, "approve_transfer", map[string]any{"transferId": "tr-alloc-stale"})
	wantProblem(t, env, 400, "unknown-skill")

	env, _ = w.skill(t, "list_transfers", map[string]any{})
	wantProblem(t, env, 400, "unknown-skill")

	// A text part holding the same JSON is accepted (documented contract).
	env, _ = w.call(t, a2a.NewTextPart(`{"skill":"transfer_status","arguments":{"transferId":"tr-alloc-stale"}}`))
	if env.Problem != nil || env.Data.TransferStatus == nil {
		t.Fatalf("a text part holding the skill JSON must be accepted, got %+v", env)
	}
}

// TestWireWithoutAStoreAnswers503 mirrors REST: a nil use case (no store) is a
// 503 problem naming the unavailable read side, never a fabricated answer.
func TestWireWithoutAStoreAnswers503(t *testing.T) {
	w := newWire(t, Deps{})
	env, _ := w.skill(t, skillTransferStatus, map[string]any{"transferId": "tr-1"})
	wantProblem(t, env, 503, "read-side-unavailable")
	env, _ = w.skill(t, skillFindStuckTransfers, map[string]any{"olderThanMinutes": 30})
	wantProblem(t, env, 503, "read-side-unavailable")
}

// TestWireInfrastructureFailuresAre503AndNeverEchoed mirrors REST's
// writeReadProblem: an unrecognised error is a 503 whose detail is generic.
func TestWireInfrastructureFailuresAre503AndNeverEchoed(t *testing.T) {
	secret := "dial tcp 10.0.0.9:5432: password authentication failed for user nip"
	w := newWire(t, newDeps(&fakeQuery{transfers: fixtureTransfers(), err: errors.New(secret)}))
	for _, call := range []struct {
		skill string
		args  map[string]any
	}{
		{skillTransferStatus, map[string]any{"transferId": "tr-alloc-stale"}},
		{skillFindStuckTransfers, map[string]any{"olderThanMinutes": 30}},
	} {
		env, text := w.skill(t, call.skill, call.args)
		wantProblem(t, env, 503, "read-side-unavailable")
		for _, s := range []string{env.Problem.Detail, env.Problem.Title, text} {
			if strings.Contains(s, "password") || strings.Contains(s, "10.0.0.9") {
				t.Fatalf("an infrastructure error leaked into an answer: %q", s)
			}
		}
	}
}

// TestSkillRunsAreTraced proves each skill run opens an a2a.skill span carrying
// the a2a.skill attribute (mirroring the MCP adapter's mcp.tool span), and that
// a rejected run marks its span as an error.
func TestSkillRunsAreTraced(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	previous := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)
	t.Cleanup(func() { otel.SetTracerProvider(previous) })

	w := newWire(t, fixtureDeps())
	w.skill(t, skillTransferStatus, map[string]any{"transferId": "tr-alloc-stale"})
	w.skill(t, skillTransferStatus, map[string]any{"transferId": "tr-nope"})

	var okSpans, errSpans int
	for _, span := range recorder.Ended() {
		if span.Name() != "a2a.skill "+skillTransferStatus {
			continue
		}
		var attr string
		for _, kv := range span.Attributes() {
			if string(kv.Key) == "a2a.skill" {
				attr = kv.Value.AsString()
			}
		}
		if attr != skillTransferStatus {
			t.Errorf("span %q has a2a.skill = %q", span.Name(), attr)
		}
		if span.Status().Code == 1 { // codes.Error
			errSpans++
		} else {
			okSpans++
		}
	}
	if okSpans != 1 || errSpans != 1 {
		t.Fatalf("spans: %d ok, %d error, want one of each", okSpans, errSpans)
	}
}

// TestCardIsServedAndDialable proves discovery end to end: the card handler
// serves the card at the well-known path and a client built from the FETCHED
// card reaches the agent.
func TestCardIsServedAndDialable(t *testing.T) {
	mux := http.NewServeMux()
	mux.Handle("/", Handler(NewExecutor(fixtureDeps())))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	mux.Handle(a2asrv.WellKnownAgentCardPath, a2asrv.NewStaticAgentCardHandler(Card(Config{AgentURL: srv.URL + "/", Version: "test"})))

	res, err := http.Get(srv.URL + a2asrv.WellKnownAgentCardPath)
	if err != nil || res.StatusCode != http.StatusOK {
		t.Fatalf("card fetch = %v / %v", res, err)
	}
	defer func() { _ = res.Body.Close() }()
	var card a2a.AgentCard
	if err := json.NewDecoder(res.Body).Decode(&card); err != nil {
		t.Fatal(err)
	}
	client, err := a2aclient.NewFromCard(context.Background(), &card)
	if err != nil {
		t.Fatal(err)
	}
	w := &wire{client: client}
	env, _ := w.skill(t, skillTransferStatus, map[string]any{"transferId": "tr-alloc-stale"})
	if env.Data.TransferStatus == nil {
		t.Fatalf("a client built from the served card could not call the agent: %+v", env)
	}
}
