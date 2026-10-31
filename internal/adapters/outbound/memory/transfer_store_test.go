package memory

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

// proposeForStore drives a valid aggregate through the domain constructor,
// so store tests persist a real InterWarehouseTransfer (never a hand-built
// Snapshot), exactly as the approve use case does.
func proposeForStore(t *testing.T, id, origin string, createdAt time.Time) *transfer.InterWarehouseTransfer {
	t.Helper()
	trf, err := transfer.ProposeTransfer(transfer.ProposalInput{
		ID:                transfer.TransferID(id),
		IdempotencyKey:    "key-" + id,
		OriginSiteID:      origin,
		DestinationSiteID: "WH8",
		SKU:               "SKU-1",
		Quantity:          10,
		PolicyVersion:     "pv-1",
		ProposalAsOf:      createdAt,
		ExpiresAt:         createdAt.Add(24 * time.Hour),
		Now:               createdAt,
	})
	if err != nil {
		t.Fatalf("ProposeTransfer(%s): %v", id, err)
	}
	return trf
}

func TestTransferStoreCreatePersistsAndNumbersAudit(t *testing.T) {
	s := NewTransferStore()
	trf := proposeForStore(t, "trf-1", "WH1", unitNow)
	existing, err := s.Create(context.Background(), trf, "key-1")
	if err != nil || existing != nil {
		t.Fatalf("Create = (%p, %v), want (nil, nil) on a fresh key", existing, err)
	}
	if trf.Version() != 2 { // drafted + proposed audit entries
		t.Fatalf("aggregate version after Create = %d, want 2", trf.Version())
	}
	loaded, err := s.Load(context.Background(), "trf-1")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if loaded.State() != transfer.StateProposed {
		t.Fatalf("loaded state = %s, want PROPOSED", loaded.State())
	}
	audit := loaded.Audit()
	if len(audit) != 2 || audit[0].Seq != 1 || audit[1].Seq != 2 {
		t.Fatalf("audit = %+v, want sequences 1,2 numbered from 1", audit)
	}
	if loaded.IdempotencyKey() != "key-1" {
		t.Fatalf("stored idempotency key = %q, want key-1", loaded.IdempotencyKey())
	}
}

func TestTransferStoreCreateIsIdempotentPerKey(t *testing.T) {
	s := NewTransferStore()
	first := proposeForStore(t, "trf-1", "WH1", unitNow)
	if _, err := s.Create(context.Background(), first, "key-1"); err != nil {
		t.Fatalf("first Create: %v", err)
	}
	// A DIFFERENT aggregate arrives under the SAME key (the replay path):
	// the store must return the FIRST transfer untouched.
	second := proposeForStore(t, "trf-2", "WH1", unitNow.Add(time.Minute))
	existing, err := s.Create(context.Background(), second, "key-1")
	if err != nil {
		t.Fatalf("replayed Create: %v", err)
	}
	if existing == nil || existing.ID() != "trf-1" {
		t.Fatalf("replayed Create returned %v, want the first transfer trf-1", existing)
	}
	if _, err := s.Load(context.Background(), "trf-2"); err == nil {
		t.Fatal("the replayed aggregate must not be persisted")
	}
}

func TestTransferStoreLoadUnknownIsNotFound(t *testing.T) {
	s := NewTransferStore()
	if _, err := s.Load(context.Background(), "nope"); err != transfer.ErrTransferNotFound {
		t.Fatalf("Load(unknown) = %v, want ErrTransferNotFound", err)
	}
	if _, err := s.Get(context.Background(), "nope"); err != transfer.ErrTransferNotFound {
		t.Fatalf("Get(unknown) = %v, want ErrTransferNotFound (Get delegates to Load)", err)
	}
}

func TestTransferStoreUpdateStateGuardsVersionAndAudit(t *testing.T) {
	s := NewTransferStore()
	trf := proposeForStore(t, "trf-1", "WH1", unitNow)
	if _, err := s.Create(context.Background(), trf, "key-1"); err != nil {
		t.Fatalf("Create: %v", err)
	}

	// A stale copy (the aggregate as loaded BEFORE someone else advanced it)
	// must be refused with the optimistic-concurrency error.
	stale := proposeForStore(t, "trf-1", "WH1", unitNow)
	stale.SetVersion(1)
	err := s.UpdateState(context.Background(), stale)
	if err == nil || !strings.Contains(err.Error(), "optimistic concurrency conflict") {
		t.Fatalf("UpdateState(stale) = %v, want an optimistic concurrency conflict", err)
	}

	// The current copy advances one step: audit stays append-only and the
	// new entries are numbered after the stored trail.
	if _, err := trf.Approve(unitNow.Add(time.Minute)); err != nil {
		t.Fatalf("Approve: %v", err)
	}
	if err := s.UpdateState(context.Background(), trf); err != nil {
		t.Fatalf("UpdateState: %v", err)
	}
	if trf.Version() != 3 {
		t.Fatalf("version after update = %d, want 3", trf.Version())
	}
	loaded, _ := s.Load(context.Background(), "trf-1")
	audit := loaded.Audit()
	if len(audit) != 3 || audit[2].Seq != 3 || audit[2].Event != "TransferPlanApproved" {
		t.Fatalf("audit after update = %+v, want 3 entries ending in the approval", audit)
	}
	// The stored idempotency key survives an update.
	if loaded.IdempotencyKey() != "key-1" {
		t.Fatalf("idempotency key after update = %q, want key-1", loaded.IdempotencyKey())
	}
}

func TestTransferStoreUpdateStateUnknownTransfer(t *testing.T) {
	s := NewTransferStore()
	ghost := proposeForStore(t, "trf-ghost", "WH1", unitNow)
	if err := s.UpdateState(context.Background(), ghost); err != transfer.ErrTransferNotFound {
		t.Fatalf("UpdateState(unknown) = %v, want ErrTransferNotFound", err)
	}
}

func seedListStore(t *testing.T) *TransferStore {
	t.Helper()
	s := NewTransferStore()
	// Three transfers with distinct creation times; trf-2 shuttles WH9→WH1
	// so the origin/destination/site filters have something to bite on.
	for _, tc := range []struct {
		id      string
		origin  string
		created time.Time
	}{
		{"trf-1", "WH1", unitNow},
		{"trf-2", "WH9", unitNow.Add(time.Minute)},
		{"trf-3", "WH2", unitNow.Add(2 * time.Minute)},
	} {
		trf := proposeForStore(t, tc.id, tc.origin, tc.created)
		if _, err := s.Create(context.Background(), trf, "key-"+tc.id); err != nil {
			t.Fatalf("Create(%s): %v", tc.id, err)
		}
	}
	return s
}

func ids(page transfer.TransferPage) []string {
	out := make([]string, 0, len(page.Items))
	for _, t := range page.Items {
		out = append(out, string(t.ID()))
	}
	return out
}

func TestTransferStoreListNewestFirstWithIDTieBreak(t *testing.T) {
	s := seedListStore(t)
	// Two transfers share trf-3's creation time; the id breaks the tie.
	tie := proposeForStore(t, "trf-0", "WH3", unitNow.Add(2*time.Minute))
	if _, err := s.Create(context.Background(), tie, "key-tie"); err != nil {
		t.Fatalf("Create(tie): %v", err)
	}
	page, err := s.List(context.Background(), transfer.ListFilter{})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	want := []string{"trf-0", "trf-3", "trf-2", "trf-1"} // newest first, id up on ties
	got := ids(page)
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("List order = %v, want %v", got, want)
	}
	if page.Total != 4 {
		t.Fatalf("Total = %d, want 4", page.Total)
	}
}

func TestTransferStoreListFilters(t *testing.T) {
	s := seedListStore(t)
	cases := []struct {
		name   string
		filter transfer.ListFilter
		want   []string
	}{
		{"state", transfer.ListFilter{States: []transfer.TransferState{transfer.StateProposed}}, []string{"trf-3", "trf-2", "trf-1"}},
		{"other state matches nothing", transfer.ListFilter{States: []transfer.TransferState{transfer.StateReceived}}, nil},
		{"origin", transfer.ListFilter{OriginSiteID: "WH1"}, []string{"trf-1"}},
		{"destination", transfer.ListFilter{DestinationSiteID: "WH8"}, []string{"trf-3", "trf-2", "trf-1"}},
		{"site matches origin or destination", transfer.ListFilter{SiteID: "WH9"}, []string{"trf-2"}},
		{"updated before cuts strictly", transfer.ListFilter{UpdatedBefore: unitNow.Add(time.Minute)}, []string{"trf-1"}},
		{"filters combine (AND)", transfer.ListFilter{States: []transfer.TransferState{transfer.StateProposed}, OriginSiteID: "WH2"}, []string{"trf-3"}},
	}
	for _, tc := range cases {
		page, err := s.List(context.Background(), tc.filter)
		if err != nil {
			t.Fatalf("%s: List: %v", tc.name, err)
		}
		got := ids(page)
		if strings.Join(got, ",") != strings.Join(tc.want, ",") {
			t.Errorf("%s: List = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestTransferStoreListPaging(t *testing.T) {
	s := seedListStore(t)
	page, err := s.List(context.Background(), transfer.ListFilter{Limit: 2, Offset: 1})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if got := ids(page); strings.Join(got, ",") != "trf-2,trf-1" {
		t.Fatalf("page 2 (limit 2, offset 1) = %v, want [trf-2 trf-1]", got)
	}
	if page.Total != 3 {
		t.Fatalf("Total = %d, want the unpaged 3", page.Total)
	}
	// An offset past the end is an empty page, never an error.
	page, err = s.List(context.Background(), transfer.ListFilter{Limit: 2, Offset: 9})
	if err != nil || len(page.Items) != 0 || page.Total != 3 {
		t.Fatalf("List(offset past end) = (%v, %v), want empty page, total 3", page, err)
	}
	// limit <= 0 falls back to the default page size.
	page, err = s.List(context.Background(), transfer.ListFilter{})
	if err != nil || len(page.Items) != 3 {
		t.Fatalf("List(no limit) = (%d items, %v), want all 3 via the default limit", len(page.Items), err)
	}
}

func TestTransferStoreListStalestFirst(t *testing.T) {
	s := seedListStore(t)
	page, err := s.List(context.Background(), transfer.ListFilter{Order: transfer.OrderStalestFirst})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	// All three were last touched at their creation: oldest-updated first.
	want := []string{"trf-1", "trf-2", "trf-3"}
	if got := ids(page); strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("stalest-first order = %v, want %v", got, want)
	}
}

func TestTransferStoreListNonTerminalOldestUpdatedFirst(t *testing.T) {
	s := seedListStore(t)
	// trf-1 leaves the non-terminal set entirely: walk it along the real
	// saga path (approve → request allocation → rejected) to UNFULFILLABLE.
	trf1, _ := s.Load(context.Background(), "trf-1")
	if _, err := trf1.Approve(unitNow.Add(3 * time.Minute)); err != nil {
		t.Fatalf("Approve: %v", err)
	}
	if _, err := trf1.RequestAllocation(unitNow.Add(3 * time.Minute)); err != nil {
		t.Fatalf("RequestAllocation: %v", err)
	}
	if err := trf1.MarkUnfulfillable(trf1.ID().LineID(), transfer.RejectionInsufficientUsable, unitNow.Add(4*time.Minute)); err != nil {
		t.Fatalf("MarkUnfulfillable: %v", err)
	}
	if err := s.UpdateState(context.Background(), trf1); err != nil {
		t.Fatalf("UpdateState: %v", err)
	}
	// trf-2 is the stalest remaining (last touched at +1m), trf-3 next (+2m).
	views, err := s.ListNonTerminal(context.Background(), 0)
	if err != nil {
		t.Fatalf("ListNonTerminal: %v", err)
	}
	if len(views) != 2 {
		t.Fatalf("views = %+v, want the two non-terminal transfers", views)
	}
	if views[0].ID != "trf-2" || views[1].ID != "trf-3" {
		t.Fatalf("order = %v, want [trf-2 trf-3] (oldest-updated first)", []string{string(views[0].ID), string(views[1].ID)})
	}
	if views[0].State != transfer.StateProposed {
		t.Fatalf("view state = %s, want PROPOSED", views[0].State)
	}
	// The bound caps the projection.
	views, err = s.ListNonTerminal(context.Background(), 1)
	if err != nil || len(views) != 1 {
		t.Fatalf("ListNonTerminal(1) = (%d views, %v), want 1", len(views), err)
	}
}
