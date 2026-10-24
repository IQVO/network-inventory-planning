package transfer

import "testing"

// TestDomainEventNames locks the wire contract of every DomainEvent: the
// EventName string is the CloudEvents `type` segment the outbound encoders
// and the analytics projector dispatch on, so a silent rename would break
// consumers in other bounded contexts. These marker methods had no unit
// coverage before (only the acceptance suite reached them indirectly).
func TestDomainEventNames(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		want string
	}{
		{"TransferPlanApproved", PlanApproved{}.EventName()},
		{"TransferAllocationRequested", AllocationRequested{}.EventName()},
		{"WorkDemandReleased", DemandReleased{}.EventName()},
		{"TransferStateAdvanced", StateAdvanced{}.EventName()},
		{"TransferStuckDetected", StuckDetected{}.EventName()},
		{"RebalanceRunCompleted", RebalanceRunCompleted{}.EventName()},
	}
	for _, tc := range cases {
		if got := tc.want; got != tc.name {
			t.Errorf("EventName() = %q, want %q", got, tc.name)
		}
	}
}

// TestDomainEventInterfaceAssertion proves every event type satisfies the
// DomainEvent marker interface (the outbox publisher port's compile-time
// contract) at unit-test time, not only where production code happens to
// assign one.
func TestDomainEventInterfaceAssertion(t *testing.T) {
	t.Parallel()
	var _ DomainEvent = PlanApproved{}
	var _ DomainEvent = AllocationRequested{}
	var _ DomainEvent = DemandReleased{}
	var _ DomainEvent = StateAdvanced{}
	var _ DomainEvent = StuckDetected{}
	var _ DomainEvent = RebalanceRunCompleted{}
}
