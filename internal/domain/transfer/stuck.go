package transfer

import (
	"fmt"
	"strings"
	"time"
)

// DefaultStuckThresholds are the per-state ages beyond which a
// non-terminal transfer is considered stuck (ADR 0007). The clocks match
// the physical stage: ALLOCATING waits only for inventory-storage's
// reply; PICKED waits for the pick work; IN_TRANSIT waits for the freight.
// States without an entry (DRAFT/PROPOSED/APPROVED/ALLOCATED/ARRIVED) use
// DefaultStuckThreshold.
const DefaultStuckThreshold = 24 * time.Hour

// DefaultStuckThresholds maps a state to its threshold. Every non-terminal
// state MAY have an entry; the ones that do not fall back to
// DefaultStuckThreshold.
func DefaultStuckThresholds() map[TransferState]time.Duration {
	return map[TransferState]time.Duration{
		StateAllocating: time.Hour,
		StatePicked:     24 * time.Hour,
		StateInTransit:  72 * time.Hour,
	}
}

// StuckThresholds resolves the threshold of one state: the per-state
// override when present, else DefaultStuckThreshold. An unknown state
// (misspelled in configuration) resolves to the default too — the
// ticker's job is to notice aging transfers, never to crash on config.
type StuckThresholds map[TransferState]time.Duration

// ThresholdFor resolves the threshold of state.
func (t StuckThresholds) ThresholdFor(state TransferState) time.Duration {
	if d, ok := t[state]; ok && d > 0 {
		return d
	}
	return DefaultStuckThreshold
}

// ParseStuckThresholds parses NIP_STUCK_THRESHOLDS (ADR 0007): a
// comma-separated list of STATE=DURATION pairs, e.g.
// "ALLOCATING=1h,PICKED=24h,IN_TRANSIT=72h". Entries are merged over
// DefaultStuckThresholds, so a partial override keeps the defaults for
// every unlisted state. Empty raw yields exactly the defaults. An
// unparsable duration or an unknown state name is an ERROR (fail-closed
// config: silently ignoring a threshold typo would ship a check that
// never fires for that state).
func ParseStuckThresholds(raw string) (StuckThresholds, error) {
	out := DefaultStuckThresholds()
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return out, nil
	}
	for _, entry := range strings.Split(raw, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		stateRaw, durRaw, ok := strings.Cut(entry, "=")
		if !ok {
			return nil, fmt.Errorf("stuck thresholds: entry %q is not STATE=DURATION", entry)
		}
		state := TransferState(strings.TrimSpace(stateRaw))
		if !validState(state) {
			return nil, fmt.Errorf("stuck thresholds: unknown state %q", stateRaw)
		}
		d, err := time.ParseDuration(strings.TrimSpace(durRaw))
		if err != nil {
			return nil, fmt.Errorf("stuck thresholds: state %s: %w", state, err)
		}
		if d <= 0 {
			return nil, fmt.Errorf("stuck thresholds: state %s: threshold must be positive, got %s", state, d)
		}
		out[state] = d
	}
	return out, nil
}

// validState reports whether s is one of the eleven saga states.
func validState(s TransferState) bool {
	switch s {
	case StateDraft, StateProposed, StateApproved, StateAllocating, StateAllocated,
		StatePicked, StateInTransit, StateArrived, StateReceived,
		StateUnfulfillable, StateCancelled:
		return true
	default:
		return false
	}
}

// StuckCheck evaluates one non-terminal transfer view against the
// thresholds (ADR 0007). Pure: no clock reads, no mutation — the ticker
// only reads and publishes.
type StuckCheck struct {
	Thresholds StuckThresholds
}

// Stuck is the verdict for one view at now.
type Stuck struct {
	TransferID       TransferID
	State            TransferState
	AgeSeconds       int64
	ThresholdSeconds int64
}

// Evaluate returns the stuck verdicts for views: one per view whose age
// in state exceeds its threshold, in the input order (stable for
// deterministic tests). Views in terminal states (defensive — the reader
// already filters) never fire.
func (c StuckCheck) Evaluate(views []StuckView, now time.Time) []Stuck {
	out := make([]Stuck, 0, len(views))
	for _, v := range views {
		if !v.State.NonTerminal() {
			continue
		}
		age := now.Sub(v.UpdatedAt)
		if age <= 0 {
			continue
		}
		threshold := c.Thresholds.ThresholdFor(v.State)
		if age > threshold {
			out = append(out, Stuck{
				TransferID:       v.ID,
				State:            v.State,
				AgeSeconds:       int64(age.Seconds()),
				ThresholdSeconds: int64(threshold.Seconds()),
			})
		}
	}
	return out
}

// NonTerminalTail reports whether the state sits in the PICKED..RECEIVED
// tail where a picked quantity must exist (persistence CHECK constraint
// helper; not part of the stuck check itself).
func (s TransferState) NonTerminalTail() bool {
	switch s {
	case StatePicked, StateInTransit, StateArrived, StateReceived:
		return true
	default:
		return false
	}
}
