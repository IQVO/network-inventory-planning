// Package viewcontract holds behaviour contracts shared by every adapter of
// a port, so the in-memory double used in unit tests and the Postgres
// adapter used at runtime provably behave the same. It is imported only by
// tests.
package viewcontract

import (
	"context"
	"testing"
	"time"

	"github.com/claudioed/network-inventory-planning/internal/application/ports"
	"github.com/claudioed/network-inventory-planning/internal/domain/packlevel"
)

var (
	bg = context.Background()
	// T0 is the effective_from every contract level carries.
	T0 = time.Date(2026, 10, 1, 8, 0, 0, 0, time.UTC)
)

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func level(t *testing.T, sku, lvl, gtin string, units, version int64) packlevel.PackLevel {
	t.Helper()
	l, err := packlevel.New(sku, lvl, gtin, units, T0, version)
	must(t, err)
	return l
}

// PackLevelView runs the ports.PackLevelView contract. newView must return an
// EMPTY view on every call.
func PackLevelView(t *testing.T, newView func(t *testing.T) ports.PackLevelView) {
	t.Helper()
	t.Run("an unseen level is not found", func(t *testing.T) { packLevelUnseen(t, newView) })
	t.Run("save then get round trips every field", func(t *testing.T) { packLevelRoundTrip(t, newView) })
	t.Run("a level is keyed by sku and level", func(t *testing.T) { packLevelKeyed(t, newView) })
	t.Run("saving again replaces the level, an end date included", func(t *testing.T) { packLevelReplaced(t, newView) })
}

// packLevelUnseen: an unseen level is not found.
func packLevelUnseen(t *testing.T, newView func(t *testing.T) ports.PackLevelView) {
	_, ok, err := newView(t).Get(bg, "SKU-1", packlevel.Case)
	must(t, err)
	if ok {
		t.Fatal("found a level that was never saved")
	}
}

// packLevelRoundTrip: save then get round trips every field.
func packLevelRoundTrip(t *testing.T, newView func(t *testing.T) ports.PackLevelView) {
	v := newView(t)
	in := level(t, "SKU-1", "CASE", "24006381333935", 12, 3)
	must(t, v.Save(bg, in))
	got, ok, err := v.Get(bg, "SKU-1", packlevel.Case)
	must(t, err)
	if !ok || got.SKU != in.SKU || got.Level != in.Level || got.GTIN != in.GTIN || got.UnitsPerPack != 12 ||
		got.Version != 3 || !got.EffectiveFrom.Equal(T0) || got.EffectiveTo != nil {
		t.Fatalf("got %+v ok=%v, want %+v", got, ok, in)
	}
}

// packLevelKeyed: a level is keyed by sku and level.
func packLevelKeyed(t *testing.T, newView func(t *testing.T) ports.PackLevelView) {
	v := newView(t)
	must(t, v.Save(bg, level(t, "SKU-1", "CASE", "24006381333935", 12, 3)))
	must(t, v.Save(bg, level(t, "SKU-1", "PALLET", "34006381333936", 480, 4)))
	must(t, v.Save(bg, level(t, "SKU-2", "CASE", "24006381333942", 6, 1)))
	for _, c := range []struct {
		sku string
		lvl packlevel.Level
		ups int64
	}{{"SKU-1", packlevel.Case, 12}, {"SKU-1", packlevel.Pallet, 480}, {"SKU-2", packlevel.Case, 6}} {
		got, ok, err := v.Get(bg, c.sku, c.lvl)
		must(t, err)
		if !ok || got.UnitsPerPack != c.ups {
			t.Fatalf("%s/%s: got %+v ok=%v", c.sku, c.lvl, got, ok)
		}
	}
	if _, ok, _ := v.Get(bg, "SKU-2", packlevel.Pallet); ok {
		t.Fatal("SKU-2 has no pallet level")
	}
}

// packLevelReplaced: saving again replaces the level, an end date included.
func packLevelReplaced(t *testing.T, newView func(t *testing.T) ports.PackLevelView) {
	v := newView(t)
	first := level(t, "SKU-1", "CASE", "24006381333935", 12, 3)
	must(t, v.Save(bg, first))
	end := T0.Add(time.Hour)
	retired, ok := first.Retire("24006381333935", end, 4)
	if !ok {
		t.Fatal("retire refused")
	}
	must(t, v.Save(bg, retired))
	got, _, err := v.Get(bg, "SKU-1", packlevel.Case)
	must(t, err)
	if got.EffectiveTo == nil || !got.EffectiveTo.Equal(end) || got.Version != 4 {
		t.Fatalf("got %+v", got)
	}
	must(t, v.Save(bg, level(t, "SKU-1", "CASE", "24006381333942", 24, 5)))
	got, _, err = v.Get(bg, "SKU-1", packlevel.Case)
	must(t, err)
	if got.EffectiveTo != nil || got.GTIN != "24006381333942" || got.UnitsPerPack != 24 || got.Version != 5 {
		t.Fatalf("the new level must replace the end-dated one, got %+v", got)
	}
}
