// Package mcp is the inbound Model Context Protocol adapter: it exposes this
// bounded context's transfer READ side to the AI ecosystem as a second driving
// adapter over the same application-layer use cases the REST adapter uses. It
// is built on the official MCP Go SDK and served over Streamable HTTP only.
//
// Every tool is read-only: the approve action stays REST/operator-only for
// now (see docs/docs/adr/0007-transfer-read-side-and-read-only-mcp.md).
//
// Per ADR-0008 (warehouse-systems) this package depends inward on the
// application layer (use cases and ports) and the domain only -- never on an
// outbound adapter or the REST adapter -- and nothing else may depend on it
// (internal/architecture's TestMCPAdapterDependencyRule). The composition
// root (cmd/mcp) wires concrete repositories into the use cases. There is no
// auth of any kind (fleet-wide revert 2026-09-11; TestNoAuthMiddlewareReintroduced).
package mcp

import (
	"net/http"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// serverName and serverVersion identify this server in the MCP initialize
// handshake.
const (
	serverName    = "network-inventory-planning-mcp"
	serverVersion = "1.0.0"
)

// NewServer builds the MCP server for this bounded context with every tool
// registered.
func NewServer(deps Deps) *mcp.Server {
	server := mcp.NewServer(
		&mcp.Implementation{Name: serverName, Version: serverVersion},
		&mcp.ServerOptions{
			Instructions: "Network inventory planning (inter-warehouse transfers), read-only: " +
				"read one transfer's status and audit trail with get_transfer, page through transfers with list_transfers, " +
				"find transfers that have not advanced with find_stuck_transfers, and read the advisory network simulation " +
				"with simulate_transfer_options. Nothing here changes a transfer: approving a transfer is an operator action on the REST API.",
		},
	)

	deps.registerTools(server)

	return server
}

// Handler returns the Streamable HTTP handler for the MCP server.
func Handler(server *mcp.Server) http.Handler {
	return mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return server }, nil)
}
