package http

import (
	"net/http"

	"github.com/claudioed/network-inventory-planning/internal/application/ports"
)

// ReplayEntry is one problem type of the catalogue as the replay-class tests
// see it: its slug, status and declared replay class (empty when it declares
// none), and how to answer it through the production error writer.
type ReplayEntry struct {
	Slug   string
	Status int
	Class  string
	pt     problemType
}

// Refuse answers the entry exactly as production does — through writeProblem,
// so the class the catalogue declares marks the middleware's capture.
func (e ReplayEntry) Refuse(w http.ResponseWriter, r *http.Request) {
	writeProblem(w, r, e.pt, "Refused for the replay-class test", "the problem type "+e.Slug+" answered by the production writer")
}

// ReplayCatalogue exposes every problem type of the catalogue with the
// replay class its entry declares (empty when it declares none).
func ReplayCatalogue() []ReplayEntry {
	rows := make([]ReplayEntry, 0, len(problemCatalogue))
	for _, p := range problemCatalogue {
		rows = append(rows, ReplayEntry{Slug: p.slug, Status: p.status, Class: string(p.class), pt: p})
	}
	return rows
}

// IdempotentForTest is the production Idempotency-Key middleware (the same
// closure Routes wraps every keyed route in) over store, wrapping next.
func IdempotentForTest(store ports.IdempotencyStore, next http.Handler) http.Handler {
	return Handler{Idempotency: store}.idempotent()(next)
}

// SyntheticOutcomeEntry is a problem type of the OUTCOME class that is NOT
// in the catalogue: no real problem type of this service commits state and
// then refuses (ADR 0024), so the mechanism — a stored refusal whose
// handler's writes commit with the key row — is proven with this stand-in
// only. It exists nowhere in production code.
func SyntheticOutcomeEntry() ReplayEntry {
	pt := problemType{slug: "synthetic-outcome-probe", status: http.StatusConflict, class: replayOutcome}
	return ReplayEntry{Slug: pt.slug, Status: pt.status, Class: string(pt.class), pt: pt}
}
