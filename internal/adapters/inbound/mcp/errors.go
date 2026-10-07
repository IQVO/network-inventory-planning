package mcp

import (
	"errors"
	"fmt"
	"log/slog"

	"github.com/claudioed/network-inventory-planning/internal/application/usecases"
	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

// slugFor names each typed error with the same stable slug the REST adapter
// uses as the last segment of its RFC 7807 "type" URI
// (internal/adapters/inbound/http/transfers_read.go), so a client sees the
// same error vocabulary over both surfaces. ok is false for an untyped error.
func slugFor(err error) (slug string, ok bool) {
	catalog := []struct {
		target error
		slug   string
	}{
		{usecases.ErrInvalidTransferQuery, "invalid-query"},
		{transfer.ErrTransferNotFound, "transfer-not-found"},
	}
	for _, entry := range catalog {
		if errors.Is(err, entry.target) {
			return entry.slug, true
		}
	}
	return "", false
}

// toolError builds a tool-level error "<slug>: <detail>". Returned from a
// handler it becomes an isError tool result, never a transport failure.
func toolError(slug, detail string) error {
	return fmt.Errorf("%s: %s", slug, detail)
}

// mapError turns a use-case/repository error into the tool error the model
// sees. Typed errors are prefixed with their REST slug and keep their
// message; anything else is logged and reported generically so
// infrastructure details (DSNs, SQL) never reach the model.
func mapError(err error) error {
	if slug, ok := slugFor(err); ok {
		return toolError(slug, err.Error())
	}
	slog.Error("mcp tool failed with an unexpected error", "error", err)
	return toolError("internal-error", "an unexpected internal error occurred")
}
