package http

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/claudioed/network-inventory-planning/internal/application/usecases"
	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

var httpCancelNow = time.Date(2026, 10, 8, 9, 0, 0, 0, time.UTC)

func cancelHandler(t *testing.T, seed map[string]transfer.TransferState) (Handler, *httpFakeTransfers, *httpFakeEvents) {
	t.Helper()
	transfers := newHttpFakeTransfers()
	for id, state := range seed {
		transfers.rows[id] = transfer.Rehydrate(transfer.Snapshot{
			ID: transfer.TransferID(id), IdempotencyKey: "idem-" + id,
			OriginSiteID: "WH1", DestinationSiteID: "WH2", SKU: "SKU-1", Quantity: 5,
			PolicyVersion: "policy-v3", State: state,
			ExpiresAt: httpCancelNow.Add(time.Hour), CreatedAt: httpCancelNow.Add(-time.Hour), UpdatedAt: httpCancelNow.Add(-time.Hour),
		})
	}
	events := &httpFakeEvents{}
	return Handler{Cancel: &usecases.CancelTransfer{
		Transfers: transfers, Events: events, UoW: httpFakeUoW{},
		Now: func() time.Time { return httpCancelNow },
	}}, transfers, events
}

func postCancel(h Handler, path, body string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodPost, path, bytes.NewBufferString(body))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	h.Routes().ServeHTTP(response, request)
	return response
}

func problemType(t *testing.T, response *httptest.ResponseRecorder) string {
	t.Helper()
	if ct := response.Header().Get("Content-Type"); ct != "application/problem+json" {
		t.Fatalf("content-type = %q, want application/problem+json", ct)
	}
	var body problem
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatalf("decode problem: %v", err)
	}
	if body.Status != response.Code {
		t.Fatalf("problem status %d != response status %d", body.Status, response.Code)
	}
	return body.Type[strings.LastIndex(body.Type, "/")+1:]
}

func TestCancelEndpointCancelsAPreReleaseTransfer(t *testing.T) {
	h, transfers, events := cancelHandler(t, map[string]transfer.TransferState{"trf-1": transfer.StateAllocating})
	response := postCancel(h, "/v1/transfers/trf-1:cancel", `{"reason":"operator withdrew the demand"}`)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", response.Code, response.Body.String())
	}
	var view transferDTO
	if err := json.NewDecoder(response.Body).Decode(&view); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if view.ID != "trf-1" || view.State != "CANCELLED" {
		t.Fatalf("view = %+v, want trf-1 CANCELLED", view)
	}
	if got := transfers.rows["trf-1"].State(); got != transfer.StateCancelled {
		t.Fatalf("persisted state = %s, want CANCELLED", got)
	}
	if len(events.events) != 1 {
		t.Fatalf("events = %d, want the single analytics occurrence", len(events.events))
	}
	if _, ok := events.events[0].(transfer.StateAdvanced); !ok {
		t.Fatalf("event = %+v, want TransferStateAdvanced", events.events[0])
	}
}

func TestCancelEndpointIsIdempotent(t *testing.T) {
	h, _, events := cancelHandler(t, map[string]transfer.TransferState{"trf-1": transfer.StateApproved})
	for attempt := 1; attempt <= 2; attempt++ {
		response := postCancel(h, "/v1/transfers/trf-1:cancel", `{"reason":"again"}`)
		if response.Code != http.StatusOK {
			t.Fatalf("attempt %d: status = %d: %s", attempt, response.Code, response.Body.String())
		}
	}
	if len(events.events) != 1 {
		t.Fatalf("events = %d, want 1 (the second cancel is a no-op)", len(events.events))
	}
}

func TestCancelEndpointErrorMapping(t *testing.T) {
	seed := map[string]transfer.TransferState{
		"trf-live":      transfer.StateAllocating,
		"trf-allocated": transfer.StateAllocated,
		"trf-received":  transfer.StateReceived,
	}
	cases := []struct {
		name     string
		path     string
		body     string
		wantCode int
		wantType string
	}{
		{"unknown transfer", "/v1/transfers/trf-nope:cancel", `{"reason":"x"}`, http.StatusNotFound, "transfer-not-found"},
		{"allocated is past release", "/v1/transfers/trf-allocated:cancel", `{"reason":"x"}`, http.StatusConflict, "transfer-not-cancellable"},
		{"received is terminal", "/v1/transfers/trf-received:cancel", `{"reason":"x"}`, http.StatusConflict, "transfer-not-cancellable"},
		{"blank reason", "/v1/transfers/trf-live:cancel", `{"reason":"   "}`, http.StatusUnprocessableEntity, "invalid-cancellation"},
		{"missing reason", "/v1/transfers/trf-live:cancel", `{}`, http.StatusUnprocessableEntity, "invalid-cancellation"},
		{"malformed json", "/v1/transfers/trf-live:cancel", `{"reason":`, http.StatusBadRequest, "invalid-request"},
		{"unknown field", "/v1/transfers/trf-live:cancel", `{"reason":"x","force":true}`, http.StatusBadRequest, "invalid-request"},
		{"no :cancel action", "/v1/transfers/trf-live", `{"reason":"x"}`, http.StatusNotFound, "not-found"},
		{"empty id", "/v1/transfers/:cancel", `{"reason":"x"}`, http.StatusNotFound, "not-found"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h, transfers, events := cancelHandler(t, seed)
			before := map[string]transfer.TransferState{}
			for id, tr := range transfers.rows {
				before[id] = tr.State()
			}
			response := postCancel(h, tc.path, tc.body)
			if response.Code != tc.wantCode {
				t.Fatalf("status = %d, want %d: %s", response.Code, tc.wantCode, response.Body.String())
			}
			if got := problemType(t, response); got != tc.wantType {
				t.Fatalf("problem type = %q, want %q", got, tc.wantType)
			}
			for id, tr := range transfers.rows {
				if tr.State() != before[id] {
					t.Fatalf("%s changed %s -> %s on a refused cancel", id, before[id], tr.State())
				}
			}
			if len(events.events) != 0 {
				t.Fatalf("a refused cancel published %d events", len(events.events))
			}
		})
	}
}

func TestCancelEndpointUnconfiguredIs503(t *testing.T) {
	response := postCancel(Handler{}, "/v1/transfers/trf-1:cancel", `{"reason":"x"}`)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 (never a fabricated cancellation)", response.Code)
	}
	if got := problemType(t, response); got != "cancel-unavailable" {
		t.Fatalf("problem type = %q, want cancel-unavailable", got)
	}
}

// TestCancelResponseMatchesTheOpenAPISchema keeps the spec honest: the
// 200 body must be a TransferView (every key a declared property, every
// required property present) and the path must document exactly the
// statuses the handler can answer.
func TestCancelResponseMatchesTheOpenAPISchema(t *testing.T) {
	h, _, _ := cancelHandler(t, map[string]transfer.TransferState{"trf-1": transfer.StateProposed})
	response := postCancel(h, "/v1/transfers/trf-1:cancel", `{"reason":"contract check"}`)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", response.Code, response.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	schemas := openAPISchemas(t)
	props, required, _ := schemaKeys(t, schemas, "TransferView")
	declared := map[string]bool{}
	for _, p := range props {
		declared[p] = true
	}
	for k := range body {
		if !declared[k] {
			t.Errorf("response key %q is not a TransferView property %v", k, props)
		}
	}
	for _, r := range required {
		if _, ok := body[r]; !ok {
			t.Errorf("required TransferView property %q missing from the response", r)
		}
	}

	_, _, reqSchema := schemaKeys(t, schemas, "CancelTransferRequest")
	if reqRequired, _ := reqSchema["required"].([]any); len(reqRequired) != 1 || reqRequired[0] != "reason" {
		t.Errorf("CancelTransferRequest.required = %v, want [reason]", reqRequired)
	}

	raw, err := os.ReadFile("../../../../apis/openapi.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Paths map[string]map[string]any `yaml:"paths"`
	}
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	op, ok := doc.Paths["/v1/transfers/{id}:cancel"]["post"].(map[string]any)
	if !ok {
		t.Fatal("openapi.yaml has no POST /v1/transfers/{id}:cancel")
	}
	responses, _ := op["responses"].(map[string]any)
	for _, status := range []string{"200", "400", "404", "409", "422", "503"} {
		if _, ok := responses[status]; !ok {
			t.Errorf("POST /v1/transfers/{id}:cancel does not document %s", status)
		}
	}
}
