// Package packlevel holds the PackLevel value of the local pack-level view
// (ADR 0017): one packaging level (EACH, INNER, CASE or PALLET) of a SKU as
// product-master declared it. It is pure: no I/O, no clock reads, standard
// library only.
package packlevel

import (
	"errors"
	"strings"
	"time"
)

// Level is a packaging level of a SKU, spelled exactly as on product-master's
// wire.
type Level string

// The levels. Only Case drives a transfer multiple in v1.
const (
	Each   Level = "EACH"
	Inner  Level = "INNER"
	Case   Level = "CASE"
	Pallet Level = "PALLET"
)

// Domain errors, as sentinel values (compare with errors.Is).
var (
	// ErrBlankSKU is returned when the SKU is empty or only white space.
	ErrBlankSKU = errors.New("packlevel: sku must not be blank")
	// ErrInvalidLevel is returned for a level that is not EACH, INNER, CASE or PALLET.
	ErrInvalidLevel = errors.New("packlevel: level must be EACH, INNER, CASE or PALLET")
	// ErrBlankGTIN is returned when the GTIN is empty or only white space.
	ErrBlankGTIN = errors.New("packlevel: gtin must not be blank")
	// ErrInvalidUnits is returned when units per pack is below 1.
	ErrInvalidUnits = errors.New("packlevel: units per pack must be at least 1")
	// ErrInvalidVersion is returned when the product version is below 1.
	ErrInvalidVersion = errors.New("packlevel: version must be at least 1")
)

// PackLevel is one packaging level of a SKU. UnitsPerPack is the direct factor
// to eaches. Version is the product-master aggregate version that last changed
// the level: a message is applied only when its version is greater. EffectiveTo
// is nil while the level is open-ended.
type PackLevel struct {
	SKU           string
	Level         Level
	GTIN          string
	UnitsPerPack  int64
	EffectiveFrom time.Time
	EffectiveTo   *time.Time
	Version       int64
}

// New builds a level from a ProductPackLevelDeclared payload.
func New(sku, level, gtin string, unitsPerPack int64, effectiveFrom time.Time, version int64) (PackLevel, error) {
	if isBlank(sku) {
		return PackLevel{}, ErrBlankSKU
	}
	if !Level(level).valid() {
		return PackLevel{}, ErrInvalidLevel
	}
	if isBlank(gtin) {
		return PackLevel{}, ErrBlankGTIN
	}
	if unitsPerPack < 1 {
		return PackLevel{}, ErrInvalidUnits
	}
	if version < 1 {
		return PackLevel{}, ErrInvalidVersion
	}
	return PackLevel{
		SKU: sku, Level: Level(level), GTIN: gtin, UnitsPerPack: unitsPerPack,
		EffectiveFrom: effectiveFrom.UTC(), Version: version,
	}, nil
}

func isBlank(s string) bool { return strings.TrimSpace(s) == "" }

func (l Level) valid() bool {
	switch l {
	case Each, Inner, Case, Pallet:
		return true
	}
	return false
}

// ActiveAt reports whether the level is in force at t: effective from its
// start and until, not including, its end date.
func (p PackLevel) ActiveAt(t time.Time) bool {
	if t.Before(p.EffectiveFrom) {
		return false
	}
	return p.EffectiveTo == nil || t.Before(*p.EffectiveTo)
}

// Supersedes reports whether p should replace cur in the view: only a strictly
// greater version does, so a replay or a late message changes nothing. The zero
// PackLevel (nothing stored) is superseded by anything.
func (p PackLevel) Supersedes(cur PackLevel) bool {
	return p.Version > cur.Version
}

// Retire end-dates the level at effectiveTo and records version. It applies
// only to the level with the given GTIN (a late retirement of a superseded
// GTIN must not end the GTIN that replaced it) and only with a strictly greater
// version; otherwise it reports false and returns the level unchanged.
func (p PackLevel) Retire(gtin string, effectiveTo time.Time, version int64) (PackLevel, bool) {
	if gtin != p.GTIN || version <= p.Version {
		return p, false
	}
	end := effectiveTo.UTC()
	p.EffectiveTo = &end
	p.Version = version
	return p, true
}
