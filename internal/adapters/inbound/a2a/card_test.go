package a2a

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/a2aproject/a2a-go/v2/a2a"
)

const testAgentURL = "http://network-inventory-planning-a2a.warehouse-systems.svc.cluster.local:8093/"

// TestCardShape pins the identity, interface and capability surface of the
// Agent Card: the discovery contract other contexts program against.
func TestCardShape(t *testing.T) {
	card := Card(Config{AgentURL: testAgentURL, Version: "1.0.0"})

	if card.Name != "network-inventory-planning" {
		t.Errorf("card name = %q, want network-inventory-planning", card.Name)
	}
	if len(card.SupportedInterfaces) != 1 {
		t.Fatalf("card has %d interfaces, want 1", len(card.SupportedInterfaces))
	}
	iface := card.SupportedInterfaces[0]
	if iface.URL != testAgentURL || iface.ProtocolBinding != a2a.TransportProtocolJSONRPC {
		t.Errorf("interface = %+v, want JSONRPC at the configured agent URL", iface)
	}
	if card.Capabilities.Streaming {
		t.Error("streaming must not be advertised: this is a message-only agent")
	}
	if card.SecuritySchemes != nil {
		t.Error("card must declare no security schemes: the surface is unauthenticated fleet-wide")
	}
	if card.Provider == nil || card.Provider.Org != "IQVO" {
		t.Errorf("provider = %+v, want IQVO", card.Provider)
	}
}

// TestCardSkillsAreCuratedReadOnlyAndTruthful pins the two-skill, read-only
// surface (the MCP surface is read-only, so the write gate makes this agent
// read-only) and proves the card never lies about its own contract: every
// advertised example must parse under the SAME strict decoder the executor
// uses, name its own skill, and carry arguments that skill accepts.
func TestCardSkillsAreCuratedReadOnlyAndTruthful(t *testing.T) {
	card := Card(Config{AgentURL: testAgentURL, Version: "1.0.0"})

	want := map[string]bool{skillTransferStatus: true, skillFindStuckTransfers: true}
	if len(card.Skills) != len(want) {
		t.Fatalf("card has %d skills, want exactly %d (curated, goal-shaped -- not one per endpoint)", len(card.Skills), len(want))
	}
	for _, s := range card.Skills {
		if !want[s.ID] {
			t.Errorf("unexpected skill %q on the card", s.ID)
			continue
		}
		if s.Name == "" || s.Description == "" || len(s.Examples) == 0 {
			t.Errorf("skill %q lacks a name, description or example", s.ID)
		}
		if !containsTag(s.Tags, "read") || containsTag(s.Tags, "write") {
			t.Errorf("skill %q must be tagged read and never write (read-only agent), tags = %v", s.ID, s.Tags)
		}
		for _, ex := range s.Examples {
			var req skillRequest
			if err := decodeStrict([]byte(ex), &req); err != nil {
				t.Errorf("skill %q example is not valid under the strict decoder: %v", s.ID, err)
				continue
			}
			if req.Skill != s.ID {
				t.Errorf("skill %q example names skill %q", s.ID, req.Skill)
			}
			assertArgsAccepted(t, s.ID, req.Arguments)
			assertExampleValid(t, s.ID, req.Arguments)
		}
	}
}

func assertArgsAccepted(t *testing.T, skill string, raw json.RawMessage) {
	t.Helper()
	var err error
	switch skill {
	case skillTransferStatus:
		err = decodeStrict(raw, &transferStatusArgs{})
	case skillFindStuckTransfers:
		err = decodeStrict(raw, &findStuckArgs{})
	}
	if err != nil {
		t.Errorf("skill %q example arguments are rejected by its own strict decoder: %v", skill, err)
	}
}

// assertExampleValid proves each advertised example also passes the skill's
// own argument validation, not just the decoder.
func assertExampleValid(t *testing.T, skill string, raw json.RawMessage) {
	t.Helper()
	if skill != skillFindStuckTransfers {
		return
	}
	var args findStuckArgs
	if err := decodeStrict(raw, &args); err != nil {
		t.Fatal(err)
	}
	if p := args.validate(); p != nil {
		t.Errorf("skill %q example arguments fail validation: %s", skill, p.Detail)
	}
}

func containsTag(tags []string, want string) bool {
	for _, tag := range tags {
		if tag == want {
			return true
		}
	}
	return false
}

// TestCardNamesNoWriteVerbSkill backs the write gate: no skill id starts with
// a verb that changes state.
func TestCardNamesNoWriteVerbSkill(t *testing.T) {
	for _, s := range Card(Config{AgentURL: testAgentURL, Version: "1.0.0"}).Skills {
		for _, verb := range []string{"approve", "cancel", "retry", "create", "update", "delete", "assign", "release"} {
			if strings.HasPrefix(s.ID, verb) {
				t.Errorf("skill %q starts with the write verb %q: this agent is read-only", s.ID, verb)
			}
		}
	}
}

// TestCardDoesNotAdvertiseTheMCPArgumentNames keeps the card on the camelCase
// names this surface accepts: the MCP snake_case names are a different
// surface's vocabulary and would be refused here as unknown arguments.
func TestCardDoesNotAdvertiseTheMCPArgumentNames(t *testing.T) {
	raw, err := json.Marshal(Card(Config{AgentURL: testAgentURL, Version: "1.0.0"}))
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"older_than", "transfer_id"} {
		if strings.Contains(string(raw), name) {
			t.Errorf("the Agent Card mentions %q, an MCP argument name this surface does not accept", name)
		}
	}
}

// TestCardSerializable proves the card marshals to JSON a client can parse
// back, with the discovery fields intact.
func TestCardSerializable(t *testing.T) {
	raw, err := json.Marshal(Card(Config{AgentURL: testAgentURL, Version: "1.0.0"}))
	if err != nil {
		t.Fatalf("marshal card: %v", err)
	}
	var back a2a.AgentCard
	if err := json.Unmarshal(raw, &back); err != nil {
		t.Fatalf("card JSON does not round-trip: %v", err)
	}
	if back.Name != "network-inventory-planning" || len(back.Skills) != 2 {
		t.Errorf("round-tripped card = %q with %d skills", back.Name, len(back.Skills))
	}
}
