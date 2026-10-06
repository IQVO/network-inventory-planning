package http

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/claudioed/network-inventory-planning/internal/application/usecases"
	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

func TestGenerateReturnsAdvisoryProposal(t *testing.T) {
	asOf := time.Date(2026, 10, 5, 15, 0, 0, 0, time.UTC)
	payload, err := json.Marshal(generateRequest{
		AsOf: asOf,
		Positions: []transfer.Position{
			{Site: "A", SKU: "SKU", Available: 100, AsOf: asOf},
			{Site: "B", SKU: "SKU", Available: 0, AsOf: asOf},
		},
		Policies: []transfer.Policy{
			{Version: "v1", Site: "A", SKU: "SKU", SafetyStock: 10},
			{Version: "v1", Site: "B", SKU: "SKU", TargetStock: 50, UnitPriority: 2},
		},
		Lanes: []transfer.Lane{{Origin: "A", Destination: "B", Enabled: true}},
	})
	if err != nil {
		t.Fatal(err)
	}

	h := Handler{Generate: usecases.GenerateTransferProposals{}}
	request := httptest.NewRequest(http.MethodPost, "/v1/transfer-proposals:generate", bytes.NewReader(payload))
	response := httptest.NewRecorder()
	h.Routes().ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", response.Code, response.Body.String())
	}
	var result struct {
		Proposals []transfer.Proposal `json:"proposals"`
	}
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	if len(result.Proposals) != 1 || result.Proposals[0].Quantity != 50 {
		t.Fatalf("proposals = %+v", result.Proposals)
	}
}

func TestGenerateReturnsProblemForInvalidSnapshot(t *testing.T) {
	h := Handler{Generate: usecases.GenerateTransferProposals{}}
	request := httptest.NewRequest(http.MethodPost, "/v1/transfer-proposals:generate", bytes.NewBufferString(`{"positions":[]}`))
	response := httptest.NewRecorder()
	h.Routes().ServeHTTP(response, request)

	if response.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d: %s", response.Code, response.Body.String())
	}
	if response.Header().Get("Content-Type") != "application/problem+json" {
		t.Fatalf("content type = %q", response.Header().Get("Content-Type"))
	}
}
