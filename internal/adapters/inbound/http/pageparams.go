package http

import (
	"errors"
	"net/http"

	"github.com/claudioed/network-inventory-planning/internal/adapters/inbound/http/page"
	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

// pageParams validates the paging query of a collection GET and returns the
// page size and the keyset to continue after. On any violation it writes the
// problem (400 unknown-query-parameter / invalid-limit / invalid-cursor)
// and reports false. filters lists the collection's own query parameters.
func pageParams(w http.ResponseWriter, r *http.Request, filters ...string) (int, *transfer.PageAfter, bool) {
	if name := page.Unknown(r, filters...); name != "" {
		writeProblem(w, http.StatusBadRequest, "unknown-query-parameter",
			"Unknown query parameter", "this collection does not declare the query parameter "+name)
		return 0, nil, false
	}
	limit, err := page.Limit(r)
	if err != nil {
		writeProblem(w, http.StatusBadRequest, "invalid-limit", "limit must be an integer between 1 and 500", err.Error())
		return 0, nil, false
	}
	pos, err := page.After(r)
	if err != nil {
		writeInvalidCursor(w, err)
		return 0, nil, false
	}
	if pos == nil {
		return limit, nil, true
	}
	return limit, &transfer.PageAfter{Sort: pos.Sort, ID: pos.ID}, true
}

func writeInvalidCursor(w http.ResponseWriter, err error) {
	writeProblem(w, http.StatusBadRequest, "invalid-cursor", "cursor is not a valid page token", err.Error())
}

// isInvalidCursor reports whether a store refused a well-formed cursor that
// does not belong to its collection.
func isInvalidCursor(err error) bool {
	return errors.Is(err, transfer.ErrInvalidPageAfter) || errors.Is(err, page.ErrInvalidCursor)
}
