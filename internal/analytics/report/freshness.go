package report

import (
	"math"
	"time"
)

// Freshness is the projection's freshness (fleet analytics charter §4):
// AsOf is the CloudEvents time of the newest event applied and LagSeconds is
// now - AsOf. Both are nil while the projection has applied nothing.
type Freshness struct {
	AsOf       *time.Time `json:"as_of"`
	LagSeconds *float64   `json:"lag_seconds"`
}

// ComputeFreshness derives the lag from the newest applied event time. A
// clock that is behind the event (skew) reads as zero lag, never negative.
func ComputeFreshness(asOf *time.Time, now time.Time) Freshness {
	if asOf == nil {
		return Freshness{}
	}
	// math.Max, not `if lag < 0`: both yield 0 at the boundary, which would
	// leave an equivalent `<` -> `<=` mutant.
	lag := math.Max(0, now.Sub(*asOf).Seconds())
	utc := asOf.UTC()
	return Freshness{AsOf: &utc, LagSeconds: &lag}
}
