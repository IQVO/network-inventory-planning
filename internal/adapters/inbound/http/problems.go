package http

import (
	"encoding/json"
	"net/http"
)

// problem is the RFC 9457 wire shape of every refusal this adapter answers.
type problem struct {
	Type   string `json:"type"`
	Title  string `json:"title"`
	Status int    `json:"status"`
	Detail string `json:"detail"`
}

// problemType is one problem type of the catalogue: the slug, the status it
// answers with, and its replay class — what the Idempotency-Key middleware
// does when the same request is retried under the same key (ADR 0024). The
// title and detail are situational and stay at the call site.
type problemType struct {
	slug   string
	status int
	class  replayClass
}

// The catalogue: every problem type this adapter can answer, one entry per
// (slug, status). A problem type without a class fails
// TestEveryProblemTypeDeclaresAReplayClass; a literal outside this file
// fails TestProblemTypesAreDeclaredOnlyInTheCatalogue. The class table with
// its reasons is ADR 0024.
var problemCatalogue = []problemType{
	// FINAL: a function of the request alone (or of a fact that never
	// changes back). Stored and replayed.
	{slug: "idempotency-key-required", status: http.StatusBadRequest, class: replayFinal},
	{slug: "idempotency-key-invalid", status: http.StatusBadRequest, class: replayFinal},
	{slug: "idempotency-key-reused", status: http.StatusConflict, class: replayFinal},
	{slug: "invalid-request", status: http.StatusBadRequest, class: replayFinal},
	{slug: "invalid-if-match", status: http.StatusBadRequest, class: replayFinal},
	{slug: "invalid-catalogue-entry", status: http.StatusUnprocessableEntity, class: replayFinal},
	{slug: "invalid-planning-snapshot", status: http.StatusUnprocessableEntity, class: replayFinal},
	{slug: "invalid-approval", status: http.StatusUnprocessableEntity, class: replayFinal},
	{slug: "invalid-cancel", status: http.StatusUnprocessableEntity, class: replayFinal},
	{slug: "proposal-expired", status: http.StatusUnprocessableEntity, class: replayFinal},
	{slug: "cancel-refused", status: http.StatusConflict, class: replayFinal},
	{slug: "unknown-query-parameter", status: http.StatusBadRequest, class: replayFinal},
	{slug: "invalid-limit", status: http.StatusBadRequest, class: replayFinal},
	{slug: "invalid-cursor", status: http.StatusBadRequest, class: replayFinal},
	{slug: "invalid-query", status: http.StatusBadRequest, class: replayFinal},
	{slug: "invalid-report-range", status: http.StatusBadRequest, class: replayFinal},
	{slug: "invalid-report-limit", status: http.StatusBadRequest, class: replayFinal},

	// RETRYABLE: nothing committed, the answer depends on state that can
	// change. NOT stored; the key is released and a retry executes.
	{slug: "below-one-case", status: http.StatusUnprocessableEntity, class: replayRetryable},
	{slug: "facts-incomplete", status: http.StatusUnprocessableEntity, class: replayRetryable},
	{slug: "transfer-not-found", status: http.StatusNotFound, class: replayRetryable},
	{slug: "version-mismatch", status: http.StatusPreconditionFailed, class: replayRetryable},
	{slug: "concurrent-modification", status: http.StatusConflict, class: replayRetryable},

	// 5xx problem types: never stored by the store's own rule, so their
	// class is RETRYABLE (a retry executes once the dependency is back).
	{slug: "approval-unavailable", status: http.StatusServiceUnavailable, class: replayRetryable},
	{slug: "cancel-unavailable", status: http.StatusServiceUnavailable, class: replayRetryable},
	{slug: "catalogue-unavailable", status: http.StatusServiceUnavailable, class: replayRetryable},
	{slug: "config-incomplete", status: http.StatusServiceUnavailable, class: replayRetryable},
	{slug: "idempotency-unavailable", status: http.StatusServiceUnavailable, class: replayRetryable},
	{slug: "pack-levels-unavailable", status: http.StatusServiceUnavailable, class: replayRetryable},
	{slug: "read-models-incomplete", status: http.StatusServiceUnavailable, class: replayRetryable},
	{slug: "read-models-unavailable", status: http.StatusServiceUnavailable, class: replayRetryable},
	{slug: "read-side-unavailable", status: http.StatusServiceUnavailable, class: replayRetryable},
	{slug: "rebalance-runs-unavailable", status: http.StatusServiceUnavailable, class: replayRetryable},
	{slug: "report-store-error", status: http.StatusInternalServerError, class: replayRetryable},
	{slug: "saga-unavailable", status: http.StatusServiceUnavailable, class: replayRetryable},
}

// The catalogue entries, by name for the call sites: the name carries the
// slug and the entry carries the status and the replay class.
var (
	ptIdempotencyKeyRequired   = entry("idempotency-key-required")
	ptIdempotencyKeyInvalid    = entry("idempotency-key-invalid")
	ptIdempotencyKeyReused     = entry("idempotency-key-reused")
	ptInvalidRequest           = entry("invalid-request")
	ptInvalidIfMatch           = entry("invalid-if-match")
	ptInvalidCatalogueEntry    = entry("invalid-catalogue-entry")
	ptInvalidPlanningSnapshot  = entry("invalid-planning-snapshot")
	ptInvalidApproval          = entry("invalid-approval")
	ptInvalidCancel            = entry("invalid-cancel")
	ptProposalExpired          = entry("proposal-expired")
	ptCancelRefused            = entry("cancel-refused")
	ptUnknownQueryParameter    = entry("unknown-query-parameter")
	ptInvalidLimit             = entry("invalid-limit")
	ptInvalidCursor            = entry("invalid-cursor")
	ptInvalidQuery             = entry("invalid-query")
	ptInvalidReportRange       = entry("invalid-report-range")
	ptInvalidReportLimit       = entry("invalid-report-limit")
	ptBelowOneCase             = entry("below-one-case")
	ptFactsIncomplete          = entry("facts-incomplete")
	ptTransferNotFound         = entry("transfer-not-found")
	ptVersionMismatch          = entry("version-mismatch")
	ptConcurrentModification   = entry("concurrent-modification")
	ptApprovalUnavailable      = entry("approval-unavailable")
	ptCancelUnavailable        = entry("cancel-unavailable")
	ptCatalogueUnavailable     = entry("catalogue-unavailable")
	ptConfigIncomplete         = entry("config-incomplete")
	ptIdempotencyUnavailable   = entry("idempotency-unavailable")
	ptPackLevelsUnavailable    = entry("pack-levels-unavailable")
	ptReadModelsIncomplete     = entry("read-models-incomplete")
	ptReadModelsUnavailable    = entry("read-models-unavailable")
	ptReadSideUnavailable      = entry("read-side-unavailable")
	ptRebalanceRunsUnavailable = entry("rebalance-runs-unavailable")
	ptReportStoreError         = entry("report-store-error")
	ptSagaUnavailable          = entry("saga-unavailable")
)

// entry looks a problem type up by slug; a miss is a programming error
// caught at boot by the tests that hold the catalogue complete.
func entry(slug string) problemType {
	for _, p := range problemCatalogue {
		if p.slug == slug {
			return p
		}
	}
	panic("problems.go: unknown problem type " + slug)
}

// writeProblem answers with the catalogue entry's problem (RFC 9457
// application/problem+json) and marks the replay class of the entry on the
// Idempotency-Key middleware's capture, so the middleware knows — from the
// class alone, holding no slug list — whether the refusal is stored.
func writeProblem(w http.ResponseWriter, r *http.Request, p problemType, title, detail string) {
	markReplayClass(r.Context(), p.class)
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(p.status)
	_ = json.NewEncoder(w).Encode(problem{
		Type:   "https://warehouse.example/problems/" + p.slug,
		Title:  title,
		Status: p.status,
		Detail: detail,
	})
}
