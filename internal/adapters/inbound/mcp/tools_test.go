package mcp_test

import (
	"testing"

	"github.com/claudioed/network-inventory-planning/internal/domain/planning"
)

func transferIDs(t *testing.T, out map[string]any) []string {
	t.Helper()
	raw, ok := out["transfers"].([]any)
	if !ok {
		t.Fatalf("transfers = %#v, want an array", out["transfers"])
	}
	ids := make([]string, 0, len(raw))
	for _, item := range raw {
		ids = append(ids, item.(map[string]any)["id"].(string))
	}
	return ids
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestGetTransferReturnsStatusAndAuditTrail(t *testing.T) {
	out := newHarness(t).ok(t, "get_transfer", map[string]any{"transfer_id": "tr-alloc-stale"})

	want := map[string]any{
		"id": "tr-alloc-stale", "state": "ALLOCATING", "sku": "SKU-1", "quantity": float64(10), "version": float64(3),
		"origin_site_id": "WH1", "destination_site_id": "WH2", "updated_at": "2026-10-06T12:00:00Z",
	}
	for key, value := range want {
		if out[key] != value {
			t.Errorf("%s = %v, want %v", key, out[key], value)
		}
	}
	for _, absent := range []string{"reservation_id", "picked_quantity", "rejection_reason"} {
		if _, present := out[absent]; present {
			t.Errorf("%s must be omitted before the saga produces it", absent)
		}
	}
	audit, ok := out["audit"].([]any)
	if !ok || len(audit) != 2 {
		t.Fatalf("audit = %#v, want 2 entries", out["audit"])
	}
	wantLast := map[string]any{
		"from": "DRAFT", "to": "ALLOCATING", "event": "TransferMoved",
		"cause": "cause-tr-alloc-stale", "occurred_at": "2026-10-06T12:00:00Z",
	}
	last := audit[1].(map[string]any)
	for key, value := range wantLast {
		if last[key] != value {
			t.Errorf("audit[1].%s = %v, want %v", key, last[key], value)
		}
	}
}

func TestGetTransferErrorsAreSlugPrefixed(t *testing.T) {
	h := newHarness(t)
	h.fail(t, "get_transfer", map[string]any{"transfer_id": "nope"}, "transfer-not-found:")
	h.fail(t, "get_transfer", map[string]any{"transfer_id": "  "}, "invalid-query:")
}

func TestListTransfersFiltersByStateAndSite(t *testing.T) {
	h := newHarness(t)

	all := h.ok(t, "list_transfers", map[string]any{})
	if all["total"] != float64(5) || len(transferIDs(t, all)) != 5 {
		t.Fatalf("unfiltered list = %#v", all)
	}

	byState := h.ok(t, "list_transfers", map[string]any{"state": "allocating"})
	if got := transferIDs(t, byState); !equalStrings(got, []string{"tr-alloc-stale", "tr-alloc-fresh"}) {
		t.Fatalf("state=allocating -> %v", got)
	}

	// Site matches origin OR destination: WH3 is the destination of
	// tr-alloc-fresh and the origin of tr-cancelled-old.
	bySite := h.ok(t, "list_transfers", map[string]any{"site": "WH3"})
	if got := transferIDs(t, bySite); !equalStrings(got, []string{"tr-alloc-fresh", "tr-cancelled-old"}) {
		t.Fatalf("site=WH3 -> %v", got)
	}

	both := h.ok(t, "list_transfers", map[string]any{"state": "ALLOCATING", "site": "WH3"})
	if got := transferIDs(t, both); !equalStrings(got, []string{"tr-alloc-fresh"}) {
		t.Fatalf("state+site -> %v", got)
	}
}

func TestListTransfersPagesAndReportsTotal(t *testing.T) {
	out := newHarness(t).ok(t, "list_transfers", map[string]any{"limit": 2})
	if len(transferIDs(t, out)) != 2 || out["total"] != float64(5) {
		t.Fatalf("page = %#v, want 2 items of total 5", out)
	}
}

func TestListTransfersEmptyResultIsAnEmptyArray(t *testing.T) {
	out := newHarness(t).ok(t, "list_transfers", map[string]any{"state": "PICKED"})
	if got := transferIDs(t, out); len(got) != 0 || out["total"] != float64(0) {
		t.Fatalf("out = %#v", out)
	}
}

func TestListTransfersRejectsBadArguments(t *testing.T) {
	h := newHarness(t)
	h.fail(t, "list_transfers", map[string]any{"state": "FLYING"}, "invalid-query:")
	h.fail(t, "list_transfers", map[string]any{"limit": 201}, "invalid-query:")
	h.fail(t, "list_transfers", map[string]any{"limit": -1}, "invalid-query:")
}

func TestFindStuckTransfersExcludesFreshAndTerminalTransfers(t *testing.T) {
	h := newHarness(t)

	// Older than 60 minutes: the 3h-old ALLOCATING and the 26h-old
	// IN_TRANSIT qualify; the 5-minute-old one does not; RECEIVED and
	// CANCELLED are terminal and never stuck however old.
	out := h.ok(t, "find_stuck_transfers", map[string]any{"older_than_minutes": 60})
	if got := transferIDs(t, out); !equalStrings(got, []string{"tr-alloc-stale", "tr-transit-stale"}) {
		t.Fatalf("stuck(60m) -> %v", got)
	}

	// A shorter threshold pulls the fresh one in too.
	out = h.ok(t, "find_stuck_transfers", map[string]any{"older_than_minutes": 1})
	if got := transferIDs(t, out); len(got) != 3 {
		t.Fatalf("stuck(1m) -> %v, want 3 non-terminal transfers", got)
	}
	for _, id := range transferIDs(t, out) {
		if id == "tr-received-old" || id == "tr-cancelled-old" {
			t.Fatalf("terminal transfer %s reported as stuck", id)
		}
	}

	// Strictly older: a threshold longer than every non-terminal age is empty.
	out = h.ok(t, "find_stuck_transfers", map[string]any{"older_than_minutes": 60 * 48})
	if got := transferIDs(t, out); len(got) != 0 {
		t.Fatalf("stuck(48h) -> %v, want none", got)
	}
}

func TestFindStuckTransfersNarrowsByState(t *testing.T) {
	out := newHarness(t).ok(t, "find_stuck_transfers", map[string]any{"older_than_minutes": 60, "state": "in_transit"})
	if got := transferIDs(t, out); !equalStrings(got, []string{"tr-transit-stale"}) {
		t.Fatalf("stuck in IN_TRANSIT -> %v", got)
	}
}

func TestFindStuckTransfersRejectsBadArguments(t *testing.T) {
	h := newHarness(t)
	h.fail(t, "find_stuck_transfers", map[string]any{"older_than_minutes": 0}, "invalid-query:")
	h.fail(t, "find_stuck_transfers", map[string]any{"older_than_minutes": -5}, "invalid-query:")
	h.fail(t, "find_stuck_transfers", map[string]any{"older_than_minutes": 10, "state": "RECEIVED"}, "terminal")
	h.fail(t, "find_stuck_transfers", map[string]any{"older_than_minutes": 10, "state": "FLYING"}, "invalid-query:")
}

func TestSimulateTransferOptionsReturnsTheAdvisorySimulation(t *testing.T) {
	out := newHarness(t).ok(t, "simulate_transfer_options", map[string]any{})
	if out["advisory"] != true {
		t.Fatalf("simulation must carry advisory: true, got %#v", out)
	}
	sites, ok := out["sites"].([]any)
	if !ok || len(sites) != 2 {
		t.Fatalf("sites = %#v", out["sites"])
	}
	wh1 := sites[0].(map[string]any)
	if wh1["site"] != "WH1" || wh1["total_demand"] != float64(40) || wh1["capacity_headroom"] != float64(460) ||
		wh1["origin_enabled"] != true || wh1["destination_enabled"] != true {
		t.Fatalf("WH1 = %#v", wh1)
	}
}

func TestSimulateTransferOptionsFailsClosedOnIncompleteReadModels(t *testing.T) {
	session := connectSession(t, newDeps(&fakeQuery{}, stubSnapshot{facts: planning.Facts{}}))
	h := &harness{session: session}
	h.fail(t, "simulate_transfer_options", map[string]any{}, "read-models-incomplete:")
}
