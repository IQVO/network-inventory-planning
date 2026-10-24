package usecases

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/network-inventory-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/network-inventory-planning/internal/domain/packlevel"
	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

var plT0 = time.Date(2026, 10, 1, 8, 0, 0, 0, time.UTC)

func declared(version, units int64, gtin string) PackLevelDeclaredChange {
	return PackLevelDeclaredChange{SKU: "SKU-1", Level: "CASE", GTIN: gtin, UnitsPerPack: units, EffectiveFrom: plT0, Version: version}
}

func TestApplyPackLevelDeclaredIsVersionGated(t *testing.T) {
	view := memory.NewPackLevelView()
	a := ApplyPackLevel{PackLevels: view}
	ctx := context.Background()

	if out, err := a.Declared(ctx, declared(3, 12, "g1")); err != nil || out != PackLevelApplied {
		t.Fatalf("first = %v %v", out, err)
	}
	for _, v := range []int64{3, 2} {
		if out, err := a.Declared(ctx, declared(v, 24, "g2")); err != nil || out != PackLevelStale {
			t.Fatalf("version %d = %v %v, want STALE", v, out, err)
		}
	}
	got, _, _ := view.Get(ctx, "SKU-1", packlevel.Case)
	if got.UnitsPerPack != 12 || got.Version != 3 {
		t.Fatalf("a stale event must change nothing, got %+v", got)
	}
	if out, err := a.Declared(ctx, declared(4, 24, "g2")); err != nil || out != PackLevelApplied {
		t.Fatalf("v4 = %v %v", out, err)
	}
	got, _, _ = view.Get(ctx, "SKU-1", packlevel.Case)
	if got.UnitsPerPack != 24 || got.GTIN != "g2" {
		t.Fatalf("got %+v", got)
	}
}

func TestApplyPackLevelDeclaredRefusesInvalidPayloads(t *testing.T) {
	a := ApplyPackLevel{PackLevels: memory.NewPackLevelView()}
	for name, c := range map[string]PackLevelDeclaredChange{
		"blank sku":  {SKU: " ", Level: "CASE", GTIN: "g", UnitsPerPack: 12, Version: 1},
		"bad level":  {SKU: "S", Level: "BOX", GTIN: "g", UnitsPerPack: 12, Version: 1},
		"blank gtin": {SKU: "S", Level: "CASE", GTIN: "", UnitsPerPack: 12, Version: 1},
		"zero units": {SKU: "S", Level: "CASE", GTIN: "g", UnitsPerPack: 0, Version: 1},
		"zero ver":   {SKU: "S", Level: "CASE", GTIN: "g", UnitsPerPack: 12, Version: 0},
	} {
		if _, err := a.Declared(context.Background(), c); !errors.Is(err, ErrInvalidPackLevelEvent) {
			t.Errorf("%s: err = %v, want ErrInvalidPackLevelEvent", name, err)
		}
	}
}

func TestApplyPackLevelRetired(t *testing.T) {
	view := memory.NewPackLevelView()
	a := ApplyPackLevel{PackLevels: view}
	ctx := context.Background()
	end := plT0.Add(time.Hour)
	retired := func(gtin string, v int64) PackLevelRetiredChange {
		return PackLevelRetiredChange{SKU: "SKU-1", Level: "CASE", GTIN: gtin, EffectiveTo: end, Version: v}
	}

	if out, err := a.Retired(ctx, retired("g1", 2)); err != nil || out != PackLevelIgnored {
		t.Fatalf("retire of an unseen level = %v %v, want IGNORED", out, err)
	}
	if _, err := a.Declared(ctx, declared(3, 12, "g1")); err != nil {
		t.Fatal(err)
	}
	if out, err := a.Retired(ctx, retired("other", 4)); err != nil || out != PackLevelIgnored {
		t.Fatalf("retire of another gtin = %v %v, want IGNORED", out, err)
	}
	for _, v := range []int64{3, 2} {
		if out, err := a.Retired(ctx, retired("g1", v)); err != nil || out != PackLevelStale {
			t.Fatalf("retire v%d = %v %v, want STALE", v, out, err)
		}
	}
	if out, err := a.Retired(ctx, retired("g1", 4)); err != nil || out != PackLevelApplied {
		t.Fatalf("retire = %v %v", out, err)
	}
	got, _, _ := view.Get(ctx, "SKU-1", packlevel.Case)
	if got.EffectiveTo == nil || !got.EffectiveTo.Equal(end) || got.Version != 4 {
		t.Fatalf("got %+v", got)
	}
	for name, c := range map[string]PackLevelRetiredChange{
		"blank gtin": {SKU: "S", Level: "CASE", GTIN: "", EffectiveTo: end, Version: 1},
		"zero ver":   {SKU: "S", Level: "CASE", GTIN: "g", EffectiveTo: end, Version: 0},
		"bad level":  {SKU: "S", Level: "BOX", GTIN: "g", EffectiveTo: end, Version: 1},
		"blank sku":  {SKU: "", Level: "CASE", GTIN: "g", EffectiveTo: end, Version: 1},
	} {
		if _, err := a.Retired(ctx, c); !errors.Is(err, ErrInvalidPackLevelEvent) {
			t.Errorf("%s: err = %v", name, err)
		}
	}
}

func TestActiveCaseMultiples(t *testing.T) {
	view := memory.NewPackLevelView()
	a := ApplyPackLevel{PackLevels: view}
	ctx := context.Background()
	must := func(_ PackLevelOutcome, err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(a.Declared(ctx, PackLevelDeclaredChange{SKU: "ACTIVE", Level: "CASE", GTIN: "g", UnitsPerPack: 12, EffectiveFrom: plT0, Version: 1}))
	must(a.Declared(ctx, PackLevelDeclaredChange{SKU: "FUTURE", Level: "CASE", GTIN: "g", UnitsPerPack: 6, EffectiveFrom: plT0.Add(time.Hour), Version: 1}))
	must(a.Declared(ctx, PackLevelDeclaredChange{SKU: "RETIRED", Level: "CASE", GTIN: "g", UnitsPerPack: 6, EffectiveFrom: plT0, Version: 1}))
	must(a.Retired(ctx, PackLevelRetiredChange{SKU: "RETIRED", Level: "CASE", GTIN: "g", EffectiveTo: plT0.Add(30 * time.Minute), Version: 2}))
	must(a.Declared(ctx, PackLevelDeclaredChange{SKU: "PALLETONLY", Level: "PALLET", GTIN: "g", UnitsPerPack: 480, EffectiveFrom: plT0, Version: 1}))
	must(a.Declared(ctx, PackLevelDeclaredChange{SKU: "EACHONLY", Level: "EACH", GTIN: "g", UnitsPerPack: 1, EffectiveFrom: plT0, Version: 1}))

	at := plT0.Add(45 * time.Minute)
	got, err := ActiveCaseMultiples(ctx, view, []string{"ACTIVE", "ACTIVE", "FUTURE", "RETIRED", "PALLETONLY", "EACHONLY", "UNKNOWN"}, at)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got["ACTIVE"] != 12 {
		t.Fatalf("got %v, want only ACTIVE=12", got)
	}
	later, _ := ActiveCaseMultiples(ctx, view, []string{"FUTURE"}, plT0.Add(time.Hour))
	if later["FUTURE"] != 6 {
		t.Fatalf("a level is active from its effective_from (inclusive), got %v", later)
	}
	if m, err := ActiveCaseMultiples(ctx, nil, []string{"ACTIVE"}, at); err != nil || len(m) != 0 {
		t.Fatalf("a nil view must yield an empty table, got %v %v", m, err)
	}
	var _ transfer.CaseMultiples = got
}
