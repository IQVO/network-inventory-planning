package transfer

// copyRounding returns an independent copy (nil stays nil), so an aggregate,
// its snapshot and the events it raises never share one mutable report.
func copyRounding(r *CaseRounding) *CaseRounding {
	if r == nil {
		return nil
	}
	c := *r
	return &c
}

// RestoreCaseRounding rebuilds the report of a persisted transfer from the
// two stored values and the transfer's quantity: the number of cases and the
// remainder are derived, never stored twice. It trusts the database (CHECK
// constraints guard the columns) and performs no validation.
func RestoreCaseRounding(caseMultiple, requestedEaches, quantity int) *CaseRounding {
	return &CaseRounding{
		CaseMultiple:    caseMultiple,
		Cases:           quantity / caseMultiple,
		RequestedEaches: requestedEaches,
		RoundedEaches:   quantity,
		RemainderEaches: requestedEaches - quantity,
	}
}
