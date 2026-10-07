package transfer

import "testing"

func TestAllStatesIsTheClosedLifecycleVocabulary(t *testing.T) {
	want := []TransferState{
		StateDraft, StateProposed, StateApproved, StateAllocating, StateAllocated,
		StatePicked, StateInTransit, StateArrived, StateReceived, StateUnfulfillable, StateCancelled,
	}
	got := AllStates()
	if len(got) != len(want) {
		t.Fatalf("AllStates() has %d states, want %d: %v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("AllStates()[%d] = %s, want %s", i, got[i], want[i])
		}
	}
	// A caller mutating the returned slice must not corrupt the vocabulary.
	got[0] = "BOGUS"
	if AllStates()[0] != StateDraft {
		t.Fatal("AllStates must return a copy")
	}
}

func TestTransferStateValid(t *testing.T) {
	for _, s := range AllStates() {
		if !s.Valid() {
			t.Errorf("%s must be valid", s)
		}
	}
	for _, s := range []TransferState{"", "allocating", "BOGUS", "RECEIVED "} {
		if s.Valid() {
			t.Errorf("%q must not be valid", s)
		}
	}
}

func TestTransferStateIsTerminal(t *testing.T) {
	terminal := map[TransferState]bool{StateReceived: true, StateUnfulfillable: true, StateCancelled: true}
	for _, s := range AllStates() {
		if got := s.IsTerminal(); got != terminal[s] {
			t.Errorf("%s.IsTerminal() = %v, want %v", s, got, terminal[s])
		}
	}
}

func TestNonTerminalStatesExcludeEveryTerminalState(t *testing.T) {
	got := NonTerminalStates()
	want := []TransferState{
		StateDraft, StateProposed, StateApproved, StateAllocating, StateAllocated,
		StatePicked, StateInTransit, StateArrived,
	}
	if len(got) != len(want) {
		t.Fatalf("NonTerminalStates() = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("NonTerminalStates()[%d] = %s, want %s", i, got[i], want[i])
		}
	}
}
