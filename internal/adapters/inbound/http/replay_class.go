package http

import (
	"context"
	"net/http"
)

// replayClass says what the Idempotency-Key middleware does with a refusal
// when the same request is retried under the same key (ADR 0024, fleet rule
// .claude/rules/fleet/idempotency-and-outbox.md). It is declared on the
// problem type's catalogue entry (problems.go); there is no default.
type replayClass string

// The three replay classes.
const (
	// replayFinal: the answer is a function of the request alone (or of a
	// fact that never changes back). Stored and replayed.
	replayFinal replayClass = "FINAL"
	// replayOutcome: the command committed state and then answered an error
	// that must be remembered. Stored, and the handler's writes commit with
	// the stored answer (this store keeps a stored answer's writes). No
	// problem type of this service has this class today: see ADR 0024.
	replayOutcome replayClass = "OUTCOME"
	// replayRetryable: nothing committed and the answer depends on state
	// that can change (event-fed facts, another request's aggregate, an
	// optimistic-lock race). NOT stored: the transaction rolls back exactly
	// like the 5xx branch, the key is released and a retry executes.
	replayRetryable replayClass = "RETRYABLE"
)

// stored reports whether an answer of this class is remembered under its key.
func (c replayClass) stored() bool { return c == replayFinal || c == replayOutcome }

// replayMark carries the class of the answer a handler writes to the
// Idempotency-Key middleware that is running it. writeProblem sets it; the
// middleware reads it and holds no list of problem slugs.
type replayMark struct{ class replayClass }

type replayMarkKey struct{}

// withReplayMark returns ctx carrying a fresh mark and the mark itself.
func withReplayMark(ctx context.Context) (context.Context, *replayMark) {
	m := &replayMark{}
	return context.WithValue(ctx, replayMarkKey{}, m), m
}

// markReplayClass records the class of the answer being written. Outside an
// idempotent route there is no mark and nothing to record.
func markReplayClass(ctx context.Context, class replayClass) {
	if m, ok := ctx.Value(replayMarkKey{}).(*replayMark); ok {
		m.class = class
	}
}

// transient reports whether the answer with this status must NOT be stored
// (the store rolls the request back and releases the key). A 2xx is stored;
// a 5xx never is; every other answer is stored only when its problem type
// declared FINAL or OUTCOME. A refusal that declared nothing (a problem
// written without the catalogue) is released like RETRYABLE: the safe
// fallback.
func (m *replayMark) transient(status int) bool {
	switch {
	case status >= http.StatusInternalServerError:
		return true
	case status >= http.StatusOK && status < http.StatusMultipleChoices:
		return false
	default:
		return !m.class.stored()
	}
}
