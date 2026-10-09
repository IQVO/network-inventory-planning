// Package main_test hosts the godog (Cucumber for Go) acceptance suite. It
// drives the REAL HTTP routers of this service over a real TCP listener —
// the OLTP API (internal/adapters/inbound/http.Handler) and the read-only
// analytics reports (ReportsServer) — wired exactly as the cmd/ binaries
// wire them, but over in-memory outbound adapters, so every scenario in
// features/*.feature is a black-box test of the REST API.
//
// Inbound Kafka facts (inventory-storage replies, fulfillment-execution
// facts) have no REST surface; Given/When steps hand them to the same use
// cases the Kafka consumers call. Outbound events are encoded with the
// production CloudEvents encoders and the analytics ones are consumed by the
// production AnalyticsConsumer, so the report scenarios exercise the real
// publish → project → report path. Nothing here calls a sibling context:
// planning facts are declared locally, as the read models hold them.
package main_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cucumber/godog"

	inboundhttp "github.com/claudioed/network-inventory-planning/internal/adapters/inbound/http"
	"github.com/claudioed/network-inventory-planning/internal/adapters/outbound/analyticsstore"
	"github.com/claudioed/network-inventory-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/network-inventory-planning/internal/application/usecases"
)

// TestFeatures runs every Gherkin feature under features/ against freshly
// wired HTTP servers.
func TestFeatures(t *testing.T) {
	suite := godog.TestSuite{
		ScenarioInitializer: InitializeScenario,
		Options: &godog.Options{
			Format:   "pretty",
			Paths:    []string{"features"},
			Strict:   true,
			TestingT: t,
		},
	}
	if suite.Run() != 0 {
		t.Fatal("non-zero status returned, failed to run feature tests")
	}
}

// scenarioStart is the frozen "now" every scenario begins at.
var scenarioStart = time.Date(2026, 10, 6, 15, 0, 0, 0, time.UTC)

// maxStaleness is the fail-closed freshness budget the service runs with
// when PLANNING_MAX_STALENESS is unset (cmd defaultMaxStaleness).
const maxStaleness = 10 * time.Minute

// world is the per-scenario state: fresh in-memory adapters behind two real
// HTTP servers, plus whatever the last HTTP call returned.
type world struct {
	clock     *memory.FixedClock
	facts     *memory.PlanningFacts
	transfers *memory.TransferStore
	runs      *memory.RebalanceRuns
	bus       *eventBus
	analytics *analyticsstore.Memory
	release   usecases.WorkReleaseConfig
	approve   *usecases.ApproveTransfer

	oltp          *httptest.Server
	reports       *httptest.Server
	routes        http.Handler
	declared      declaredFacts
	snapshot      proposalSnapshot
	projected     int
	stuckDetected int

	status  int
	body    []byte
	headers http.Header

	// ids maps an idempotency key (the scenario's name for a transfer) to
	// the transfer id the service minted for it.
	ids map[string]string
	// lastErr is the outcome of the last inbound fact/reply applied.
	lastErr error
}

// start builds the composition root the way cmd/network-inventory-planning
// does (same use cases, same Handler), but over the memory adapters and a
// fixed clock, and exposes it over real TCP listeners.
func (w *world) start() {
	w.clock = memory.NewFixedClock(scenarioStart)
	w.facts = memory.NewPlanningFacts()
	w.transfers = memory.NewTransferStore()
	w.runs = memory.NewRebalanceRuns()
	w.bus = newEventBus()
	w.analytics = analyticsstore.NewMemory()
	w.release = usecases.WorkReleaseConfig{
		PickPathID:        "transfer-pick-path",
		PickCPTOffset:     2 * time.Hour,
		DispatchPathID:    "transfer-dispatch-path",
		DispatchCPTOffset: 3 * time.Hour,
	}
	w.approve = &usecases.ApproveTransfer{
		Transfers:    w.transfers,
		Events:       w.bus,
		Snapshot:     w.facts,
		UoW:          memory.UnitOfWork{},
		MaxStaleness: maxStaleness,
		Release:      w.release,
		Now:          w.clock.Now,
	}
	w.routes = w.fullHandler().Routes()
	w.declared = declaredFacts{}
	w.snapshot = proposalSnapshot{}
	w.ids = map[string]string{}
	w.projected, w.stuckDetected = 0, 0
	w.status, w.body, w.headers, w.lastErr = 0, nil, nil, nil

	w.oltp = httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		w.routes.ServeHTTP(rw, r)
	}))
	reportsServer := &inboundhttp.ReportsServer{Reader: w.analytics, Now: w.clock.Now}
	w.reports = httptest.NewServer(reportsServer.Routes())
}

// fullHandler is the OLTP surface as cmd wires it when DATABASE_URL, the
// outbox and the work-release path are all configured.
func (w *world) fullHandler() inboundhttp.Handler {
	return inboundhttp.Handler{
		Generate: usecases.GenerateTransferProposals{},
		Simulate: &usecases.SimulateTransferOptions{
			Snapshot:     w.facts,
			MaxStaleness: maxStaleness,
			Now:          w.clock.Now,
		},
		Approve:           w.approve,
		ListRebalanceRuns: &usecases.ListRebalanceRuns{Runs: w.runs},
		GetTransfer:       &usecases.GetTransfer{Query: w.transfers},
		ListTransfers:     &usecases.ListTransfers{Query: w.transfers},
	}
}

func (w *world) stop() {
	for _, srv := range []*httptest.Server{w.oltp, w.reports} {
		if srv != nil {
			srv.Close()
		}
	}
	w.oltp, w.reports = nil, nil
}

// request describes one HTTP call.
type request struct {
	server  *httptest.Server
	method  string
	path    string
	headers map[string]string
	body    []byte
}

// call issues a real net/http request and returns status, body and headers
// without touching the recorded "last response".
func (w *world) call(ctx context.Context, r request) (int, []byte, http.Header, error) {
	var reader io.Reader = http.NoBody
	if r.body != nil {
		reader = bytes.NewReader(r.body)
	}
	req, err := http.NewRequestWithContext(ctx, r.method, r.server.URL+r.path, reader)
	if err != nil {
		return 0, nil, nil, err
	}
	if r.body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range r.headers {
		req.Header.Set(k, v)
	}
	resp, err := r.server.Client().Do(req)
	if err != nil {
		return 0, nil, nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, nil, nil, err
	}
	return resp.StatusCode, body, resp.Header, nil
}

// record issues a request and remembers it as the response the Then steps
// assert against.
func (w *world) record(ctx context.Context, r request) error {
	status, body, headers, err := w.call(ctx, r)
	if err != nil {
		return err
	}
	w.status, w.body, w.headers = status, body, headers
	w.rememberApproval(r)
	return nil
}

// rememberApproval binds the idempotency key of a successful approval to the
// transfer id it produced, so later steps can say {T1}.
func (w *world) rememberApproval(r request) {
	key := r.headers["Idempotency-Key"]
	if key == "" || w.status != http.StatusOK || r.path != "/v1/transfers:approve" {
		return
	}
	var out struct {
		TransferID string `json:"transferId"`
	}
	if json.Unmarshal(w.body, &out) == nil && out.TransferID != "" {
		w.ids[key] = out.TransferID
	}
}

// expand replaces {alias} placeholders with the transfer ids minted so far.
func (w *world) expand(s string) string {
	for alias, id := range w.ids {
		s = strings.ReplaceAll(s, "{"+alias+"}", id)
	}
	return s
}

// resolve maps an alias to a transfer id; an unknown alias is used as the id
// itself (to exercise "unknown transfer").
func (w *world) resolve(alias string) string {
	if id, ok := w.ids[alias]; ok {
		return id
	}
	return alias
}

func (w *world) doc() (any, error) {
	var doc any
	if err := json.Unmarshal(w.body, &doc); err != nil {
		return nil, fmt.Errorf("response body is not valid JSON (%w): %s", err, string(w.body))
	}
	return doc, nil
}

// lookup walks a dotted path through decoded JSON; numeric segments index
// arrays.
func lookup(doc any, path string) (any, bool) {
	if path == "" {
		return doc, true
	}
	cur := doc
	for _, seg := range strings.Split(path, ".") {
		switch v := cur.(type) {
		case map[string]any:
			next, ok := v[seg]
			if !ok {
				return nil, false
			}
			cur = next
		case []any:
			i, err := strconv.Atoi(seg)
			if err != nil || i < 0 || i >= len(v) {
				return nil, false
			}
			cur = v[i]
		default:
			return nil, false
		}
	}
	return cur, true
}

func (w *world) field(path string) (any, bool, error) {
	doc, err := w.doc()
	if err != nil {
		return nil, false, err
	}
	value, ok := lookup(doc, path)
	return value, ok, nil
}

// ------------------------------------------------------------- requests ----

func (w *world) iRequestGET(ctx context.Context, path string) error {
	return w.record(ctx, request{server: w.oltp, method: http.MethodGet, path: w.expand(path)})
}

func (w *world) iRequestTheReport(ctx context.Context, path string) error {
	return w.record(ctx, request{server: w.reports, method: http.MethodGet, path: path})
}

func (w *world) iPostBody(ctx context.Context, path string, body *godog.DocString) error {
	return w.record(ctx, request{server: w.oltp, method: http.MethodPost, path: path, body: []byte(w.expand(body.Content))})
}

func (w *world) iPostBodyWithKey(ctx context.Context, path, key string, body *godog.DocString) error {
	return w.record(ctx, request{
		server: w.oltp, method: http.MethodPost, path: path,
		headers: map[string]string{"Idempotency-Key": key},
		body:    []byte(w.expand(body.Content)),
	})
}

// ----------------------------------------------------------------- Then ----

func (w *world) theResponseStatusIs(expected int) error {
	if w.status != expected {
		return fmt.Errorf("expected status %d, got %d: %s", expected, w.status, string(w.body))
	}
	return nil
}

func (w *world) theResponseContentTypeIs(expected string) error {
	if ct := w.headers.Get("Content-Type"); ct != expected {
		return fmt.Errorf("expected Content-Type %q, got %q", expected, ct)
	}
	return nil
}

// theProblemDetailTypeIs asserts the RFC 7807 body: correct content type, a
// "type" URI whose last segment identifies the error category, and a body
// status that matches the HTTP status.
func (w *world) theProblemDetailTypeIs(slug string) error {
	if err := w.theResponseContentTypeIs("application/problem+json"); err != nil {
		return err
	}
	var problem struct {
		Type   string `json:"type"`
		Title  string `json:"title"`
		Status int    `json:"status"`
	}
	if err := json.Unmarshal(w.body, &problem); err != nil {
		return fmt.Errorf("problem body is not JSON: %w: %s", err, string(w.body))
	}
	if got := problem.Type[strings.LastIndex(problem.Type, "/")+1:]; got != slug {
		return fmt.Errorf("expected problem type %q, got %q (from %q)", slug, got, problem.Type)
	}
	if problem.Title == "" {
		return fmt.Errorf("problem has no title: %s", string(w.body))
	}
	if problem.Status != w.status {
		return fmt.Errorf("problem body status %d does not match HTTP status %d", problem.Status, w.status)
	}
	return nil
}

func (w *world) theResponseIsAProblem() error {
	return w.theResponseContentTypeIs("application/problem+json")
}

func (w *world) theJSONFieldIs(path, literal string) error {
	value, ok, err := w.field(path)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("JSON field %q is absent: %s", path, string(w.body))
	}
	var expected any
	if err := json.Unmarshal([]byte(w.expand(literal)), &expected); err != nil {
		return fmt.Errorf("expected value %s is not a JSON literal: %w", literal, err)
	}
	if !reflect.DeepEqual(value, expected) {
		return fmt.Errorf("JSON field %q: expected %v, got %v", path, expected, value)
	}
	return nil
}

func (w *world) theJSONFieldIsPresent(path string) error {
	_, ok, err := w.field(path)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("expected JSON field %q to be present: %s", path, string(w.body))
	}
	return nil
}

func (w *world) theJSONFieldIsAbsent(path string) error {
	value, ok, err := w.field(path)
	if err != nil {
		return err
	}
	if ok {
		return fmt.Errorf("expected JSON field %q to be absent, got %v", path, value)
	}
	return nil
}

func (w *world) theJSONFieldIsNull(path string) error {
	value, ok, err := w.field(path)
	if err != nil {
		return err
	}
	if !ok || value != nil {
		return fmt.Errorf("expected JSON field %q to be null, got %v (present: %t)", path, value, ok)
	}
	return nil
}

func (w *world) theJSONFieldStartsWith(path, prefix string) error {
	return w.stringField(path, func(s string) bool { return strings.HasPrefix(s, prefix) }, "start with "+prefix)
}

func (w *world) theJSONFieldContains(path, fragment string) error {
	return w.stringField(path, func(s string) bool { return strings.Contains(s, fragment) }, "contain "+fragment)
}

func (w *world) stringField(path string, ok func(string) bool, want string) error {
	value, present, err := w.field(path)
	if err != nil {
		return err
	}
	text, isString := value.(string)
	if !present || !isString || !ok(text) {
		return fmt.Errorf("expected JSON field %q to %s, got %v", path, want, value)
	}
	return nil
}

func (w *world) theJSONArrayHasElements(path string, expected int) error {
	value, ok, err := w.field(path)
	if err != nil {
		return err
	}
	list, isList := value.([]any)
	if !ok || !isList {
		return fmt.Errorf("expected JSON field %q to be an array, got %v (present: %t)", path, value, ok)
	}
	if len(list) != expected {
		return fmt.Errorf("expected %d elements in %q, got %d: %s", expected, path, len(list), string(w.body))
	}
	return nil
}

// ------------------------------------------------------------- wiring ------

// InitializeScenario registers the step definitions and gives every scenario
// its own servers and its own in-memory state.
func InitializeScenario(sc *godog.ScenarioContext) {
	w := &world{}

	sc.Before(func(ctx context.Context, _ *godog.Scenario) (context.Context, error) {
		w.start()
		return ctx, nil
	})
	sc.After(func(ctx context.Context, _ *godog.Scenario, _ error) (context.Context, error) {
		w.stop()
		return ctx, nil
	})

	w.registerHTTPSteps(sc)
	w.registerFactSteps(sc)
	w.registerSagaSteps(sc)
	w.registerReportSteps(sc)
}

func (w *world) registerHTTPSteps(sc *godog.ScenarioContext) {
	sc.Step(`^I request GET "([^"]*)"$`, w.iRequestGET)
	sc.Step(`^I request the report "([^"]*)"$`, w.iRequestTheReport)
	sc.Step(`^I send POST "([^"]*)" with this body:$`, w.iPostBody)
	sc.Step(`^I send POST "([^"]*)" with the idempotency key "([^"]*)" and this body:$`, w.iPostBodyWithKey)

	sc.Step(`^the response status is (\d+)$`, w.theResponseStatusIs)
	sc.Step(`^the response content type is "([^"]*)"$`, w.theResponseContentTypeIs)
	sc.Step(`^the problem detail type is "([^"]*)"$`, w.theProblemDetailTypeIs)
	sc.Step(`^the response is a problem detail$`, w.theResponseIsAProblem)
	sc.Step(`^the JSON field "([^"]*)" is ("[^"]*"|-?\d[\d.eE+-]*|true|false|\[.*\]|\{.*\})$`, w.theJSONFieldIs)
	sc.Step(`^the JSON field "([^"]*)" is null$`, w.theJSONFieldIsNull)
	sc.Step(`^the JSON field "([^"]*)" is present$`, w.theJSONFieldIsPresent)
	sc.Step(`^the JSON field "([^"]*)" is absent$`, w.theJSONFieldIsAbsent)
	sc.Step(`^the JSON field "([^"]*)" starts with "([^"]*)"$`, w.theJSONFieldStartsWith)
	sc.Step(`^the JSON field "([^"]*)" contains "([^"]*)"$`, w.theJSONFieldContains)
	sc.Step(`^the JSON array "([^"]*)" has (\d+) elements?$`, w.theJSONArrayHasElements)

	sc.Step(`^(\d+) (second|minute|hour|day)s? (?:pass|passes)$`, w.timePasses)
	sc.Step(`^this instance runs without a database$`, w.noDatabase)
}

func (w *world) timePasses(amount int, unit string) error {
	units := map[string]time.Duration{
		"second": time.Second, "minute": time.Minute, "hour": time.Hour, "day": 24 * time.Hour,
	}
	w.clock.Advance(time.Duration(amount) * units[unit])
	return nil
}

// noDatabase swaps the routes for the zero-config diagnostic service: what
// cmd wires when DATABASE_URL is unset (only the explicit-snapshot generator).
func (w *world) noDatabase() error {
	w.routes = inboundhttp.Handler{Generate: usecases.GenerateTransferProposals{}}.Routes()
	return nil
}
