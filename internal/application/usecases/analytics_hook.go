package usecases

import (
	"context"

	"github.com/claudioed/network-inventory-planning/internal/application/ports"
	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

// publishStateAdvanced emits one TransferStateAdvanced analytics
// occurrence per transition appended since loadedVersion (ADR 0007). It
// is called INSIDE the same UnitOfWork as the transition, so the
// occurrence outbox row commits with the state change — an occurrence
// exists for every step of every transfer on the analytics topic.
//
// events nil (no broker/outbox configured) is a documented SKIP, not an
// error: the saga's fail-closed contract is its integration events; the
// analytics stream is health signal and must never refuse a transition.
func publishStateAdvanced(ctx context.Context, events ports.TransferEventPublisher, t *transfer.InterWarehouseTransfer, loadedVersion int64) error {
	if events == nil {
		return nil
	}
	advanced := t.StateAdvancedSince(loadedVersion)
	if len(advanced) == 0 {
		return nil
	}
	domain := make([]transfer.DomainEvent, 0, len(advanced))
	for _, a := range advanced {
		domain = append(domain, a)
	}
	return events.Publish(ctx, domain...)
}

// approvalEvents bundles an approval's two integration events with the
// TransferStateAdvanced occurrences of its creation transitions, so
// ApproveTransfer publishes everything in ONE port call (ADR 0007).
func approvalEvents(t *transfer.InterWarehouseTransfer, approved transfer.PlanApproved, command transfer.AllocationRequested) []transfer.DomainEvent {
	advanced := t.StateAdvancedSince(0)
	events := make([]transfer.DomainEvent, 0, 2+len(advanced))
	events = append(events, approved, command)
	for _, a := range advanced {
		events = append(events, a)
	}
	return events
}
