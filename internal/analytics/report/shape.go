package report

import (
	"math"
	"sort"
)

// RebalanceTrendDay is a RebalanceDay plus RejectionRate: the fraction of that
// day's proposals the runs' own safety checks refused.
type RebalanceTrendDay struct {
	RebalanceDay
	RejectionRate float64
}

// RebalanceTrend derives each day's RejectionRate. Order is preserved; a nil
// input yields an empty, non-nil slice.
func RebalanceTrend(days []RebalanceDay) []RebalanceTrendDay {
	out := make([]RebalanceTrendDay, 0, len(days))
	for _, d := range days {
		out = append(out, RebalanceTrendDay{RebalanceDay: d, RejectionRate: Rate(d.Rejected, d.Proposals)})
	}
	return out
}

// Rate is num/den, or 0 when den is not positive (no proposals, no rate).
func Rate(num, den int) float64 {
	if den <= 0 {
		return 0
	}
	return float64(num) / float64(den)
}

// Percentile is the continuous percentile (linear interpolation between
// closest ranks, p in [0,1]) of values, the same definition as Postgres'
// percentile_cont. values need not be sorted; it is not modified. An empty
// input yields 0.
func Percentile(values []float64, p float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	rank := p * float64(len(sorted)-1)
	lo := math.Floor(rank)
	hi := math.Ceil(rank)
	frac := rank - lo
	return sorted[int(lo)] + (sorted[int(hi)]-sorted[int(lo)])*frac
}
