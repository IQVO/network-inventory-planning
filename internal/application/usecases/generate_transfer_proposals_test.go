package usecases

import (
	"strings"
	"testing"
	"time"

	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

func TestGenerateTransferProposalsRejectsMixedSnapshotAtApplicationBoundary(t *testing.T) {
	asOf := time.Date(2026, 10, 5, 15, 0, 0, 0, time.UTC)
	_, err := (GenerateTransferProposals{}).Execute(GenerateInput{
		AsOf: asOf,
		Positions: []transfer.Position{
			{Site: "A", SKU: "SKU", AsOf: asOf},
			{Site: "B", SKU: "SKU", AsOf: asOf.Add(-time.Minute)},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "mixed position snapshot") {
		t.Fatalf("error = %v, want mixed position snapshot", err)
	}
}

func TestGenerateTransferProposalsRequiresAsOf(t *testing.T) {
	_, err := (GenerateTransferProposals{}).Execute(GenerateInput{})
	if err == nil || !strings.Contains(err.Error(), "asOf is required") {
		t.Fatalf("error = %v, want asOf is required", err)
	}
}
