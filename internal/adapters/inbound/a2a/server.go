// Package a2a is the inbound Agent2Agent adapter: it exposes this bounded
// context's transfer READ side to the fleet's agent ecosystem as a third
// driving adapter over the same application-layer use cases the REST and MCP
// adapters use. It is built on the official A2A Go SDK (a2aproject/a2a-go) and
// speaks JSON-RPC 2.0 over HTTP, with the Agent Card served at the well-known
// path.
//
// Per ADR-0020, A2A is the DELEGATION channel between bounded contexts: an
// agent client sends a goal-shaped skill request as a structured message part
// and this agent answers from its own state through its own use cases. It is
// not a data-sync channel -- facts between contexts still travel exclusively
// as CloudEvents on Kafka.
//
// The agent is READ-ONLY, exactly like the MCP surface (ADR-0008): approving a
// transfer stays an operator action on the REST API, so no skill changes
// state and nothing here touches the transactional outbox.
//
// Like the MCP adapter, this package depends inward on the application layer
// (use cases and ports) and the domain only -- never on an outbound adapter,
// never on the REST adapter -- and nothing else may depend on it
// (internal/architecture's TestA2AAdapterDependencyRule). The composition root
// (cmd/a2a) wires concrete repositories into the use cases. Domain structs
// never leak across the message boundary. There is no auth of any kind
// (fleet-wide revert 2026-09-11; TestNoAuthMiddlewareReintroduced).
package a2a

import (
	"context"
	"iter"
	"net/http"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"

	"github.com/claudioed/network-inventory-planning/internal/application/usecases"
)

// Config carries the deployment-specific identity of this agent: the absolute
// URL clients use to reach its JSON-RPC endpoint (advertised on the Agent
// Card) and the card version. It is supplied by the composition root from
// A2A_AGENT_URL; the card MUST advertise a URL the fleet's agent clients can
// actually dial (in-cluster:
// http://<release>-a2a.<ns>.svc.cluster.local:8093/).
type Config struct {
	AgentURL string
	Version  string
}

// Deps is everything the A2A skills need, injected by the composition root.
// It carries the SAME read use cases the REST and MCP adapters use; the
// adapter never constructs an outbound adapter itself. The agent is read-only,
// so there is deliberately NO write use case here (the approve use case is
// REST/operator-only; TestA2ADepsCarryNoWriteUseCase pins it). Any field may
// be nil (a unit-test seam; cmd/a2a refuses to boot without DATABASE_URL): the
// skill then answers a 503 problem naming the unavailable read side, never a
// fabricated answer.
type Deps struct {
	// GetTransfer reads one transfer with its audit trail.
	GetTransfer *usecases.GetTransfer
	// FindStuckTransfers returns the non-terminal transfers whose state has
	// not changed for longer than a threshold, stalest first.
	FindStuckTransfers *usecases.FindStuckTransfers
}

// Card builds this agent's Agent Card (ADR-0020). The surface is two curated,
// goal-shaped READ skills -- not one skill per endpoint, the same curation
// discipline ADR-0008 applied to MCP tools. transfer_status answers "where is
// this transfer and how did it get there"; find_stuck_transfers answers "which
// transfers have stopped advancing". The card declares no security schemes on
// purpose: this surface is unauthenticated by the same fleet-wide decision as
// REST and MCP; schemes are added here, not in a redesign, if that decision is
// ever reversed.
func Card(cfg Config) *a2a.AgentCard {
	return &a2a.AgentCard{
		Name:        "network-inventory-planning",
		Description: "Network Inventory Planning agent for the Warehouse Execution System, read-only: reports where one inter-warehouse transfer stands (state, quantities, sites, reservation, rejection reason and the full audit trail of state transitions) and finds the transfers that have stopped advancing (non-terminal state unchanged for longer than a threshold, stalest first). It makes the transfer saga legible; it never approves, cancels or retries a transfer, which stay operator actions on the REST API.",
		Version:     cfg.Version,
		Provider: &a2a.AgentProvider{
			Org: "IQVO",
			URL: "https://github.com/IQVO/network-inventory-planning",
		},
		SupportedInterfaces: []*a2a.AgentInterface{
			a2a.NewAgentInterface(cfg.AgentURL, a2a.TransportProtocolJSONRPC),
		},
		DefaultInputModes:  []string{"application/json", "text/plain"},
		DefaultOutputModes: []string{"application/json", "text/plain"},
		Capabilities:       a2a.AgentCapabilities{Streaming: false},
		Skills: []a2a.AgentSkill{
			{
				ID:          skillTransferStatus,
				Name:        "Status of one transfer",
				Description: "Reads one inter-warehouse transfer by id: its current state (DRAFT, PROPOSED, APPROVED, ALLOCATING, ALLOCATED, PICKED, IN_TRANSIT, ARRIVED, RECEIVED, UNFULFILLABLE or CANCELLED), sku, quantity, picked quantity, origin and destination sites, reservation id once allocated, rejection reason once unfulfillable, and the audit trail of state transitions (timestamp and cause of each). Answered as a 404 problem for an unknown id.",
				Tags:        []string{"transfers", "status", "read"},
				InputModes:  []string{"application/json"},
				Examples: []string{
					`{"skill":"transfer_status","arguments":{"transferId":"tr-1"}}`,
				},
			},
			{
				ID:          skillFindStuckTransfers,
				Name:        "Find transfers that are not advancing",
				Description: "Finds transfers in a NON-terminal state (RECEIVED, UNFULFILLABLE and CANCELLED are finished and never reported) whose state has been unchanged for MORE than olderThanMinutes, stalest first, with the total number of matches before paging. Optionally narrow to one state, e.g. ALLOCATING for an allocation reply that never arrived, and cap the page with limit (1 to 200, default 50).",
				Tags:        []string{"transfers", "stuck", "read"},
				InputModes:  []string{"application/json"},
				Examples: []string{
					`{"skill":"find_stuck_transfers","arguments":{"olderThanMinutes":30}}`,
					`{"skill":"find_stuck_transfers","arguments":{"olderThanMinutes":120,"state":"ALLOCATING","limit":20}}`,
				},
			},
		},
	}
}

// NewExecutor returns the AgentExecutor that maps an incoming message to a
// skill and answers with a single agent Message (a message-only agent in A2A
// terms): both curated skills are transactional request/response reads, so
// there is no long-running Task lifecycle to track. The response carries the
// caller's contextId (and taskId when the client referenced one) so a
// delegating saga can group its interactions.
func NewExecutor(deps Deps) a2asrv.AgentExecutor {
	return a2asrv.AgentExecutorFunc(func(ctx context.Context, ec *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
		return func(yield func(a2a.Event, error) bool) {
			yield(answer(ctx, deps, ec), nil)
		}
	})
}

// Handler wires the executor into the transport-agnostic A2A request handler
// and wraps it in the JSON-RPC 2.0 HTTP handler -- the only transport this
// agent ships (ADR-0020), mirroring ADR-0008's "Streamable HTTP only"
// minimalism for MCP.
func Handler(executor a2asrv.AgentExecutor) http.Handler {
	return a2asrv.NewJSONRPCHandler(a2asrv.NewHandler(executor))
}
