package usecases

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

// fakeTransferQuery implements ports.TransferQuery in memory and records
// the filter it was asked for.
type fakeTransferQuery struct {
	items     map[string]*transfer.InterWarehouseTransfer
	page      transfer.TransferPage
	listErr   error
	getErr    error
	gotFilter transfer.ListFilter
	listCalls int
}

func (f *fakeTransferQuery) Get(_ context.Context, id transfer.TransferID) (*transfer.InterWarehouseTransfer, error) {
	if f.getErr != nil {
		return nil, f.getErr
	}
	t, ok := f.items[string(id)]
	if !ok {
		return nil, transfer.ErrTransferNotFound
	}
	return t, nil
}

func (f *fakeTransferQuery) List(_ context.Context, filter transfer.ListFilter) (transfer.TransferPage, error) {
	f.listCalls++
	f.gotFilter = filter
	return f.page, f.listErr
}

var readNow = time.Date(2026, 10, 6, 15, 0, 0, 0, time.UTC)

func proposedTransfer(t *testing.T, id string) *transfer.InterWarehouseTransfer {
	t.Helper()
	tr, err := transfer.ProposeTransfer(transfer.ProposalInput{
		ID: transfer.TransferID(id), IdempotencyKey: "key-" + id, OriginSiteID: "WH1", DestinationSiteID: "WH2",
		SKU: "SKU-1", Quantity: 5, PolicyVersion: "p1", OperatorReason: "rebalance",
		ProposalAsOf: readNow, ExpiresAt: readNow.Add(24 * time.Hour), Now: readNow,
	})
	if err != nil {
		t.Fatalf("propose: %v", err)
	}
	return tr
}

func TestGetTransferReturnsTheTransfer(t *testing.T) {
	want := proposedTransfer(t, "tr-1")
	uc := GetTransfer{Query: &fakeTransferQuery{items: map[string]*transfer.InterWarehouseTransfer{"tr-1": want}}}
	got, err := uc.Execute(context.Background(), " tr-1 ")
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if got != want {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestGetTransferRefusesBlankID(t *testing.T) {
	_, err := GetTransfer{Query: &fakeTransferQuery{}}.Execute(context.Background(), "  ")
	if !errors.Is(err, ErrInvalidTransferQuery) {
		t.Fatalf("err = %v, want ErrInvalidTransferQuery", err)
	}
}

func TestGetTransferPassesNotFoundThrough(t *testing.T) {
	_, err := GetTransfer{Query: &fakeTransferQuery{}}.Execute(context.Background(), "nope")
	if !errors.Is(err, transfer.ErrTransferNotFound) {
		t.Fatalf("err = %v, want ErrTransferNotFound", err)
	}
}

func TestListTransfersAppliesDefaultsAndTrimsFilters(t *testing.T) {
	q := &fakeTransferQuery{page: transfer.TransferPage{Total: 7}}
	page, err := ListTransfers{Query: q}.Execute(context.Background(), ListTransfersInput{
		State: " allocating ", OriginSiteID: " WH1 ", DestinationSiteID: "WH2", Site: " WH3 ",
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if page.Total != 7 {
		t.Fatalf("Total = %d, want 7", page.Total)
	}
	f := q.gotFilter
	if len(f.States) != 1 || f.States[0] != transfer.StateAllocating {
		t.Fatalf("States = %v, want [ALLOCATING] (case-insensitive input)", f.States)
	}
	if f.OriginSiteID != "WH1" || f.DestinationSiteID != "WH2" || f.SiteID != "WH3" {
		t.Fatalf("sites = %q/%q/%q, want trimmed WH1/WH2/WH3", f.OriginSiteID, f.DestinationSiteID, f.SiteID)
	}
	if f.Limit != transfer.DefaultListLimit || f.Offset != 0 {
		t.Fatalf("paging = %d/%d, want default %d/0", f.Limit, f.Offset, transfer.DefaultListLimit)
	}
	if f.Order != transfer.OrderNewestFirst {
		t.Fatalf("Order = %q, want newest-first", f.Order)
	}
	if !f.UpdatedBefore.IsZero() {
		t.Fatalf("a plain listing must not filter by staleness, got %s", f.UpdatedBefore)
	}
}

func TestListTransfersWithoutStateDoesNotFilterState(t *testing.T) {
	q := &fakeTransferQuery{}
	if _, err := (ListTransfers{Query: q}).Execute(context.Background(), ListTransfersInput{Limit: 200, Offset: 10}); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(q.gotFilter.States) != 0 {
		t.Fatalf("States = %v, want none", q.gotFilter.States)
	}
	if q.gotFilter.Limit != 200 || q.gotFilter.Offset != 10 {
		t.Fatalf("paging = %d/%d, want 200/10", q.gotFilter.Limit, q.gotFilter.Offset)
	}
}

func TestListTransfersRefusesBadInput(t *testing.T) {
	cases := map[string]ListTransfersInput{
		"unknown state":   {State: "FLYING"},
		"limit too large": {Limit: transfer.MaxListLimit + 1},
		"negative limit":  {Limit: -1},
		"negative offset": {Offset: -1},
	}
	for name, in := range cases {
		t.Run(name, func(t *testing.T) {
			q := &fakeTransferQuery{}
			_, err := ListTransfers{Query: q}.Execute(context.Background(), in)
			if !errors.Is(err, ErrInvalidTransferQuery) {
				t.Fatalf("err = %v, want ErrInvalidTransferQuery", err)
			}
			if q.listCalls != 0 {
				t.Fatal("an invalid request must not reach the query port")
			}
		})
	}
}

func TestListTransfersPropagatesPortErrors(t *testing.T) {
	boom := errors.New("db down")
	_, err := ListTransfers{Query: &fakeTransferQuery{listErr: boom}}.Execute(context.Background(), ListTransfersInput{})
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want the port error", err)
	}
}

func stuckUseCase(q *fakeTransferQuery) FindStuckTransfers {
	return FindStuckTransfers{Query: q, Now: func() time.Time { return readNow }}
}

func TestFindStuckExcludesTerminalStatesAndAppliesTheThreshold(t *testing.T) {
	q := &fakeTransferQuery{page: transfer.TransferPage{Total: 3}}
	page, err := stuckUseCase(q).Execute(context.Background(), FindStuckInput{OlderThan: 90 * time.Minute})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if page.Total != 3 {
		t.Fatalf("Total = %d, want 3", page.Total)
	}
	f := q.gotFilter
	if want := readNow.Add(-90 * time.Minute); !f.UpdatedBefore.Equal(want) {
		t.Fatalf("UpdatedBefore = %s, want %s (now - threshold)", f.UpdatedBefore, want)
	}
	for _, s := range f.States {
		if s.IsTerminal() {
			t.Fatalf("terminal state %s must be excluded from a stuck search", s)
		}
	}
	if len(f.States) != len(transfer.NonTerminalStates()) {
		t.Fatalf("States = %v, want every non-terminal state", f.States)
	}
	if f.Order != transfer.OrderStalestFirst {
		t.Fatalf("Order = %q, want stalest-first", f.Order)
	}
	if f.Limit != transfer.DefaultListLimit {
		t.Fatalf("Limit = %d, want default", f.Limit)
	}
}

func TestFindStuckNarrowsToOneNonTerminalState(t *testing.T) {
	q := &fakeTransferQuery{}
	_, err := stuckUseCase(q).Execute(context.Background(), FindStuckInput{OlderThan: time.Minute, State: "allocating", Limit: 5, Offset: 2})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(q.gotFilter.States) != 1 || q.gotFilter.States[0] != transfer.StateAllocating {
		t.Fatalf("States = %v, want [ALLOCATING]", q.gotFilter.States)
	}
	if q.gotFilter.Limit != 5 || q.gotFilter.Offset != 2 {
		t.Fatalf("paging = %d/%d, want 5/2", q.gotFilter.Limit, q.gotFilter.Offset)
	}
}

func TestFindStuckRefusesBadInput(t *testing.T) {
	cases := map[string]FindStuckInput{
		"zero threshold":     {OlderThan: 0},
		"negative threshold": {OlderThan: -time.Minute},
		"unknown state":      {OlderThan: time.Minute, State: "FLYING"},
		"terminal state":     {OlderThan: time.Minute, State: "RECEIVED"},
		"bad limit":          {OlderThan: time.Minute, Limit: 201},
		"bad offset":         {OlderThan: time.Minute, Offset: -3},
	}
	for name, in := range cases {
		t.Run(name, func(t *testing.T) {
			q := &fakeTransferQuery{}
			_, err := stuckUseCase(q).Execute(context.Background(), in)
			if !errors.Is(err, ErrInvalidTransferQuery) {
				t.Fatalf("err = %v, want ErrInvalidTransferQuery", err)
			}
			if q.listCalls != 0 {
				t.Fatal("an invalid request must not reach the query port")
			}
		})
	}
}

func TestFindStuckRequiresAClock(t *testing.T) {
	_, err := FindStuckTransfers{Query: &fakeTransferQuery{}}.Execute(context.Background(), FindStuckInput{OlderThan: time.Minute})
	if err == nil || errors.Is(err, ErrInvalidTransferQuery) {
		t.Fatalf("err = %v, want a wiring error that is not a client error", err)
	}
}
