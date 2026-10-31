package packlevel

import (
	"errors"
	"testing"
	"time"
)

var t0 = time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)

func TestNewValidates(t *testing.T) {
	cases := []struct {
		name             string
		sku, level, gtin string
		units, version   int64
		want             error
	}{
		{"valid case", "SKU-1", "CASE", "24006381333935", 12, 3, nil},
		{"valid each of one", "SKU-1", "EACH", "04006381333931", 1, 1, nil},
		{"each level name", "SKU-1", "INNER", "04006381333931", 6, 1, nil},
		{"pallet", "SKU-1", "PALLET", "34006381333936", 480, 1, nil},
		{"blank sku", "  ", "CASE", "g", 12, 1, ErrBlankSKU},
		{"empty sku", "", "CASE", "g", 12, 1, ErrBlankSKU},
		{"bad level", "SKU-1", "BOX", "g", 12, 1, ErrInvalidLevel},
		{"lower case level", "SKU-1", "case", "g", 12, 1, ErrInvalidLevel},
		{"blank gtin", "SKU-1", "CASE", " ", 12, 1, ErrBlankGTIN},
		{"zero units", "SKU-1", "CASE", "g", 0, 1, ErrInvalidUnits},
		{"zero version", "SKU-1", "CASE", "g", 12, 0, ErrInvalidVersion},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := New(tc.sku, tc.level, tc.gtin, tc.units, t0, tc.version)
			if !errors.Is(err, tc.want) || (tc.want == nil && err != nil) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
			if tc.want == nil && (got.UnitsPerPack != tc.units || got.Version != tc.version || got.Level != Level(tc.level) || got.EffectiveTo != nil) {
				t.Fatalf("got %+v", got)
			}
		})
	}
}

func TestActiveAtBoundaries(t *testing.T) {
	l, _ := New("S", "CASE", "g", 12, t0, 1)
	end := t0.Add(time.Hour)
	l.EffectiveTo = &end
	for name, tc := range map[string]struct {
		at   time.Time
		want bool
	}{
		"before start":    {t0.Add(-time.Nanosecond), false},
		"at start":        {t0, true},
		"inside":          {t0.Add(time.Minute), true},
		"just before end": {end.Add(-time.Nanosecond), true},
		"at end (excl.)":  {end, false},
		"after end":       {end.Add(time.Second), false},
	} {
		if got := l.ActiveAt(tc.at); got != tc.want {
			t.Errorf("%s: ActiveAt = %v, want %v", name, got, tc.want)
		}
	}
	open, _ := New("S", "CASE", "g", 12, t0, 1)
	if !open.ActiveAt(t0.AddDate(50, 0, 0)) {
		t.Error("an open-ended level never ends")
	}
}

func TestSupersedesIsStrict(t *testing.T) {
	cur, _ := New("S", "CASE", "g", 12, t0, 3)
	for v, want := range map[int64]bool{2: false, 3: false, 4: true} {
		in, _ := New("S", "CASE", "g", 12, t0, v)
		if got := in.Supersedes(cur); got != want {
			t.Errorf("version %d supersedes version 3 = %v, want %v", v, got, want)
		}
	}
	in, _ := New("S", "CASE", "g", 12, t0, 1)
	if !in.Supersedes(PackLevel{}) {
		t.Error("anything supersedes nothing stored")
	}
}

func TestRetire(t *testing.T) {
	cur, _ := New("S", "CASE", "g1", 12, t0, 3)
	end := t0.Add(time.Hour)
	got, ok := cur.Retire("g1", end, 4)
	if !ok || got.EffectiveTo == nil || !got.EffectiveTo.Equal(end) || got.Version != 4 {
		t.Fatalf("retire = %+v ok=%v", got, ok)
	}
	if cur.EffectiveTo != nil {
		t.Fatal("Retire must not mutate the receiver")
	}
	if _, ok := cur.Retire("other", end, 4); ok {
		t.Error("a retirement of another GTIN must be ignored")
	}
	if _, ok := cur.Retire("g1", end, 3); ok {
		t.Error("an equal version is stale")
	}
	if _, ok := cur.Retire("g1", end, 2); ok {
		t.Error("an older version is stale")
	}
}
