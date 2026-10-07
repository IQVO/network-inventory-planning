package mcp_test

import (
	"errors"
	"regexp"
	"strings"
	"testing"

	sdk "github.com/modelcontextprotocol/go-sdk/mcp"

	inboundmcp "github.com/claudioed/network-inventory-planning/internal/adapters/inbound/mcp"
)

// wantTools is the curated tool surface: name -> read-only. EVERY tool is
// read-only in this phase; the approve action stays REST/operator-only.
var wantTools = map[string]bool{
	"get_transfer":              true,
	"list_transfers":            true,
	"find_stuck_transfers":      true,
	"simulate_transfer_options": true,
}

// maxTools is the tool budget. Raising it is a deliberate, reviewed act:
// TestToolSurface still pins the exact curated set.
const maxTools = 6

// toolsWithoutArguments lists the tools whose input is legitimately empty.
var toolsWithoutArguments = map[string]bool{"simulate_transfer_options": true}

// The MCP governance charter's mechanical gate: the advertised tool set is
// exactly the curated one, within the tool budget, snake_case verb_noun,
// annotated read-only, described, and every argument is snake_case and
// documented. There is no write tool: any tool not annotated read-only
// fails the build.
func TestToolSurface(t *testing.T) {
	h := newHarness(t)
	res, err := h.session.ListTools(t.Context(), nil)
	if err != nil {
		t.Fatalf("list tools: %v", err)
	}
	if len(res.Tools) != len(wantTools) || len(res.Tools) > maxTools {
		t.Fatalf("advertised %d tools, want exactly %d (budget %d)", len(res.Tools), len(wantTools), maxTools)
	}
	for _, tool := range res.Tools {
		if _, known := wantTools[tool.Name]; !known {
			t.Errorf("unexpected tool %q", tool.Name)
			continue
		}
		checkToolMetadata(t, tool)
		checkToolArguments(t, tool)
	}
}

var (
	toolNaming = regexp.MustCompile(`^[a-z]+(_[a-z]+)+$`)
	argNaming  = regexp.MustCompile(`^[a-z][a-z0-9]*(_[a-z0-9]+)*$`)
	// writeVerbs are the leading verbs of a mutating tool. None may appear.
	writeVerbs = regexp.MustCompile(`^(approve|create|register|publish|cancel|update|delete|set|release|reserve|allocate|retry|reject|submit|declare)_`)
)

func checkToolMetadata(t *testing.T, tool *sdk.Tool) {
	t.Helper()
	if !toolNaming.MatchString(tool.Name) {
		t.Errorf("tool %q is not snake_case verb_noun", tool.Name)
	}
	if writeVerbs.MatchString(tool.Name) {
		t.Errorf("tool %q looks like a write tool; this server is read-only in this phase", tool.Name)
	}
	if tool.Description == "" {
		t.Errorf("tool %q has no description", tool.Name)
	}
	if !wantTools[tool.Name] {
		t.Errorf("tool %q is not on the read-only allowlist", tool.Name)
	}
	if tool.Annotations == nil || !tool.Annotations.ReadOnlyHint {
		t.Errorf("tool %q is not annotated ReadOnlyHint=true: %+v", tool.Name, tool.Annotations)
		return
	}
	if tool.Annotations.DestructiveHint != nil && *tool.Annotations.DestructiveHint {
		t.Errorf("read-only tool %q is annotated destructive", tool.Name)
	}
}

func checkToolArguments(t *testing.T, tool *sdk.Tool) {
	t.Helper()
	schema, isMap := tool.InputSchema.(map[string]any)
	if !isMap {
		t.Errorf("tool %q input schema = %T", tool.Name, tool.InputSchema)
		return
	}
	props, _ := schema["properties"].(map[string]any)
	if len(props) == 0 && !toolsWithoutArguments[tool.Name] {
		t.Errorf("tool %q has no arguments in its schema", tool.Name)
	}
	for arg, p := range props {
		if !argNaming.MatchString(arg) {
			t.Errorf("tool %q argument %q is not snake_case", tool.Name, arg)
		}
		if d, _ := p.(map[string]any)["description"].(string); d == "" {
			t.Errorf("tool %q argument %q has no description", tool.Name, arg)
		}
	}
}

// Tool errors must never leak infrastructure detail (DSNs, SQL) to the model:
// an untyped failure is reported as a generic internal-error.
func TestInfrastructureErrorsDoNotLeak(t *testing.T) {
	q := &fakeQuery{err: errors.New("postgres dsn secret unreachable")}
	session := connectSession(t, newDeps(q, stubSnapshot{facts: simulationFacts()}))
	h := &harness{session: session}
	for name, args := range map[string]map[string]any{
		"get_transfer":         {"transfer_id": "tr-1"},
		"list_transfers":       {},
		"find_stuck_transfers": {"older_than_minutes": 10},
	} {
		res := h.call(t, name, args)
		if !res.IsError {
			t.Fatalf("%s: expected an isError result", name)
		}
		got := text(res)
		if strings.Contains(got, "secret") || !strings.Contains(got, "internal-error") {
			t.Errorf("%s: error text %q must be a generic internal-error", name, got)
		}
	}
}

// The unconfigured server (no DATABASE_URL) answers every tool with an
// isError result naming the unavailable read side, never a fabricated answer
// and never a protocol failure.
func TestUnconfiguredServerAnswersReadSideUnavailable(t *testing.T) {
	session := connectSession(t, inboundmcp.Deps{})
	h := &harness{session: session}
	for name, args := range map[string]map[string]any{
		"get_transfer":         {"transfer_id": "tr-1"},
		"list_transfers":       {},
		"find_stuck_transfers": {"older_than_minutes": 10},
	} {
		h.fail(t, name, args, "read-side-unavailable")
	}
	h.fail(t, "simulate_transfer_options", map[string]any{}, "read-models-unavailable")
}
