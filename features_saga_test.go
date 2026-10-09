package main_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/cucumber/godog"

	"github.com/claudioed/network-inventory-planning/internal/adapters/kafka/cloudevents"
	outboundkafka "github.com/claudioed/network-inventory-planning/internal/adapters/outbound/kafka"
	"github.com/claudioed/network-inventory-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/network-inventory-planning/internal/application/usecases"
	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

// ------------------------------------------------------------ event bus ----

// eventBus stands in for the transactional outbox plus the broker. It
// implements the outbox port (ports.TransferEventPublisher) and encodes
// every domain event with the PRODUCTION encoders, so the messages scenarios
// inspect are the exact CloudEvents 1.0 bytes the relay would put on Kafka.
type eventBus struct {
	mu       sync.Mutex
	events   []transfer.DomainEvent
	messages []outboundkafka.Encoded
	encoders []outboundkafka.Encoder
}

func newEventBus() *eventBus {
	return &eventBus{encoders: []outboundkafka.Encoder{
		outboundkafka.NewTransferEncoder(),
		outboundkafka.NewAnalyticsEncoder(),
	}}
}

// Publish implements ports.TransferEventPublisher.
func (b *eventBus) Publish(ctx context.Context, events ...transfer.DomainEvent) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, event := range events {
		b.events = append(b.events, event)
		for _, encoder := range b.encoders {
			encoded, err := encoder.Encode(ctx, event)
			if err != nil {
				return err
			}
			b.messages = append(b.messages, encoded...)
		}
	}
	return nil
}

func (b *eventBus) snapshot() ([]transfer.DomainEvent, []outboundkafka.Encoded) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]transfer.DomainEvent(nil), b.events...), append([]outboundkafka.Encoded(nil), b.messages...)
}

// ------------------------------------------------------------ approvals ----

const (
	defaultOrigin      = "WH1"
	defaultDestination = "WH2"
	defaultSKU         = "SKU-1"
	defaultPolicy      = "policy-v3"
	defaultQuantity    = 5
	// proposalAsOf is a constant so replaying a key with "the same payload"
	// really is the same payload whatever the clock does in between.
	proposalAsOf = "2026-10-06T14:55:00Z"
)

type approval struct {
	origin, destination, sku, policy string
	quantity                         int
}

func defaultApproval() approval {
	return approval{defaultOrigin, defaultDestination, defaultSKU, defaultPolicy, defaultQuantity}
}

func (a approval) body() []byte {
	body, _ := json.Marshal(map[string]any{
		"originSiteId": a.origin, "destinationSiteId": a.destination, "sku": a.sku,
		"quantity": a.quantity, "policyVersion": a.policy,
		"operatorReason": "operator approved rebalance", "proposalAsOf": proposalAsOf,
	})
	return body
}

func (w *world) approveRequest(a approval, key string) request {
	return request{
		server: w.oltp, method: http.MethodPost, path: "/v1/transfers:approve",
		headers: map[string]string{"Idempotency-Key": key}, body: a.body(),
	}
}

func (w *world) iApprove(ctx context.Context, qty int, sku, origin, destination, policy, key string) error {
	return w.record(ctx, w.approveRequest(approval{origin, destination, sku, policy, qty}, key))
}

// approveAs approves through the REST API and insists on a fresh 200.
func (w *world) approveAs(ctx context.Context, a approval, key string) error {
	if err := w.record(ctx, w.approveRequest(a, key)); err != nil {
		return err
	}
	if w.status != http.StatusOK {
		return fmt.Errorf("approving %q: expected 200, got %d: %s", key, w.status, string(w.body))
	}
	if w.ids[key] == "" {
		return fmt.Errorf("approving %q returned no transferId: %s", key, string(w.body))
	}
	return nil
}

func (w *world) anApprovedTransfer(ctx context.Context, key string) error {
	return w.approveAs(ctx, defaultApproval(), key)
}

func (w *world) anApprovedTransferOf(ctx context.Context, key string, qty int, sku, origin, destination string) error {
	return w.approveAs(ctx, approval{origin, destination, sku, defaultPolicy, qty}, key)
}

func (w *world) aTransferInState(ctx context.Context, key, state string) error {
	return w.aTransferOfInState(ctx, key, defaultQuantity, defaultSKU, defaultOrigin, defaultDestination, state)
}

func (w *world) aTransferOfInState(ctx context.Context, key string, qty int, sku, origin, destination, state string) error {
	if err := w.approveAs(ctx, approval{origin, destination, sku, defaultPolicy, qty}, key); err != nil {
		return err
	}
	return w.advanceTo(ctx, key, state)
}

// pathTo lists the inbound facts that take a freshly approved (ALLOCATING)
// transfer to each state.
var pathTo = map[string][]string{
	"ALLOCATING":    nil,
	"ALLOCATED":     {"TransferStockAllocated"},
	"UNFULFILLABLE": {"TransferStockAllocationRejected"},
	"PICKED":        {"TransferStockAllocated", "TransferPicked"},
	"IN_TRANSIT":    {"TransferStockAllocated", "TransferPicked", "TransferDispatched"},
	"ARRIVED":       {"TransferStockAllocated", "TransferPicked", "TransferDispatched", "TransferReceiptStaged"},
	"RECEIVED":      {"TransferStockAllocated", "TransferPicked", "TransferDispatched", "TransferReceiptStaged", "TransferStockStowed"},
}

func (w *world) advanceTo(ctx context.Context, key, state string) error {
	steps, ok := pathTo[state]
	if !ok {
		return fmt.Errorf("no scripted path to state %q", state)
	}
	for _, fact := range steps {
		w.clock.Advance(time.Minute)
		if err := w.applyFact(ctx, fact, w.resolve(key), factArgs{}); err != nil {
			return fmt.Errorf("driving %q to %s with %s: %w", key, state, fact, err)
		}
	}
	return nil
}

// ---------------------------------------------- inbound facts and replies ----

// factArgs carries the optional parameters of a fact; zero values mean "what
// a well-behaved sibling would send for this transfer".
type factArgs struct {
	quantity    int
	quantitySet bool
	site        string
	reason      transfer.RejectionReason
}

// peek reads the transfer through the query port; ok is false for an
// unknown id (so "unknown transfer" facts still reach the real use case).
func (w *world) peek(ctx context.Context, id string) (*transfer.InterWarehouseTransfer, bool) {
	t, err := w.transfers.Get(ctx, transfer.TransferID(id))
	return t, err == nil
}

func (w *world) applyFact(ctx context.Context, fact, id string, args factArgs) error {
	t, known := w.peek(ctx, id)
	tid := transfer.TransferID(id)
	now := w.clock.Now()
	uow := memory.UnitOfWork{}

	switch fact {
	case "TransferStockAllocated":
		return w.applyAllocated(ctx, tid, t, known)
	case "TransferStockAllocationRejected":
		reason := args.reason
		if reason == "" {
			reason = transfer.RejectionInsufficientUsable
		}
		return usecases.ApplyTransferRejection{Transfers: w.transfers, UoW: uow}.Execute(ctx,
			usecases.ApplyRejectionInput{TransferID: tid, LineID: tid.LineID(), Reason: reason, OccurredAt: now})
	case "TransferPicked":
		qty := args.quantity
		if !args.quantitySet && known {
			qty = t.Quantity()
		}
		return usecases.ApplyTransferPick{Transfers: w.transfers, Events: w.bus, UoW: uow, Release: w.release}.Execute(ctx,
			usecases.ApplyPickInput{TransferID: tid, Picked: transfer.Picked{PickedQuantity: qty}, OccurredAt: now})
	case "TransferDispatched":
		return usecases.ApplyTransferDispatched{Transfers: w.transfers, UoW: uow}.Execute(ctx, usecases.FactInput{TransferID: tid, OccurredAt: now})
	case "TransferArrived":
		return usecases.ApplyTransferArrival{Transfers: w.transfers, UoW: uow}.Execute(ctx, usecases.FactInput{TransferID: tid, OccurredAt: now})
	case "TransferReceiptStaged":
		return usecases.ApplyTransferReceiptStaged{Transfers: w.transfers, UoW: uow}.Execute(ctx,
			usecases.ApplyReceiptStagedInput{TransferID: tid, LineID: tid.LineID(), OccurredAt: now})
	case "TransferStockStowed":
		return w.applyStowed(ctx, tid, t, known, args)
	default:
		return fmt.Errorf("unknown fact %q", fact)
	}
}

func (w *world) applyAllocated(ctx context.Context, tid transfer.TransferID, t *transfer.InterWarehouseTransfer, known bool) error {
	allocation := transfer.StockAllocation{
		TransferLineID: tid.LineID(), ReservationID: "res-1", ExpiresAt: w.clock.Now().Add(2 * time.Hour),
	}
	if known {
		allocation.OriginSiteID, allocation.SKU, allocation.Quantity = t.OriginSiteID(), t.SKU(), t.Quantity()
		allocation.Allocations = []transfer.Allocation{{StockUnitID: "su-1", BinID: "bin-A1", Quantity: t.Quantity()}}
	}
	uc := usecases.ApplyTransferAllocation{Transfers: w.transfers, Events: w.bus, UoW: memory.UnitOfWork{}, Release: w.release, Now: w.clock.Now}
	return uc.Execute(ctx, usecases.ApplyAllocationInput{TransferID: tid, Allocation: allocation, OccurredAt: w.clock.Now()})
}

func (w *world) applyStowed(ctx context.Context, tid transfer.TransferID, t *transfer.InterWarehouseTransfer, known bool, args factArgs) error {
	stowed := transfer.Stowed{TransferLineID: tid.LineID(), DestinationSite: args.site}
	qty := args.quantity
	if known {
		stowed.SKU = t.SKU()
		if stowed.DestinationSite == "" {
			stowed.DestinationSite = t.DestinationSiteID()
		}
		if !args.quantitySet {
			qty = t.Quantity()
		}
	}
	stowed.ReceivedQuantity, stowed.StowedQuantity = qty, qty
	stowed.Allocations = []transfer.StowAllocation{{StockUnitID: "su-9", BinID: "bin-B2", Quantity: qty}}
	return usecases.ApplyTransferStow{Transfers: w.transfers, UoW: memory.UnitOfWork{}}.Execute(ctx,
		usecases.ApplyStowInput{TransferID: tid, Stowed: stowed, OccurredAt: w.clock.Now()})
}

func (w *world) aFactArrives(ctx context.Context, fact, alias string) error {
	w.lastErr = w.applyFact(ctx, fact, w.resolve(alias), factArgs{})
	return nil
}

func (w *world) aPickedFactArrives(ctx context.Context, qty int, alias string) error {
	w.lastErr = w.applyFact(ctx, "TransferPicked", w.resolve(alias), factArgs{quantity: qty, quantitySet: true})
	return nil
}

func (w *world) aRejectionFactArrives(ctx context.Context, reason, alias string) error {
	w.lastErr = w.applyFact(ctx, "TransferStockAllocationRejected", w.resolve(alias), factArgs{reason: transfer.RejectionReason(reason)})
	return nil
}

func (w *world) aStowedFactArrives(ctx context.Context, qty int, site, alias string) error {
	w.lastErr = w.applyFact(ctx, "TransferStockStowed", w.resolve(alias), factArgs{quantity: qty, quantitySet: true, site: site})
	return nil
}

func (w *world) theFactIsApplied() error {
	if w.lastErr != nil {
		return fmt.Errorf("expected the fact to be applied, got: %w", w.lastErr)
	}
	return nil
}

// theFactIsRefused asserts a deterministic refusal: the consumers log it and
// commit past the message instead of retrying it forever.
func (w *world) refusal(match func(error) bool, what string) error {
	if w.lastErr == nil {
		return fmt.Errorf("expected the fact to be refused as %s, but it was applied", what)
	}
	if !match(w.lastErr) {
		return fmt.Errorf("expected the fact to be refused as %s, got: %w", what, w.lastErr)
	}
	if !usecases.IsDeterministicFact(w.lastErr) {
		return fmt.Errorf("a refusal as %s must be deterministic (skipped, never retried), got: %w", what, w.lastErr)
	}
	return nil
}

func (w *world) theFactIsRefusedAsIllegal() error {
	var illegal *transfer.IllegalTransitionError
	return w.refusal(func(err error) bool { return errors.As(err, &illegal) }, "an illegal transition")
}

func (w *world) theFactIsRefusedAsInconsistent() error {
	return w.refusal(func(err error) bool { return errors.Is(err, transfer.ErrFactRefused) }, "inconsistent with the transfer")
}

func (w *world) theFactIsSkippedAsUnknown() error {
	return w.refusal(func(err error) bool { return errors.Is(err, transfer.ErrTransferNotFound) }, "an unknown transfer")
}

func (w *world) theFactIsAppliedButDispatchCannotBeReleased() error {
	return w.refusal(func(err error) bool { return errors.Is(err, usecases.ErrWorkReleaseNotConfigured) }, "a dispatch leg that is not configured")
}

func (w *world) dispatchReleaseNotConfigured() error {
	w.release.DispatchPathID = ""
	return nil
}

func (w *world) pickReleaseNotConfigured() error {
	w.approve.Release = usecases.WorkReleaseConfig{}
	return nil
}

// ---------------------------------------------------------------- Then ----

// theTransferIsInState reads the transfer back through the public API,
// out of band so the recorded response is left alone.
func (w *world) theTransferIsInState(ctx context.Context, alias, state string) error {
	status, body, _, err := w.call(ctx, request{server: w.oltp, method: http.MethodGet, path: "/v1/transfers/" + w.resolve(alias)})
	if err != nil {
		return err
	}
	var view struct {
		State string `json:"state"`
	}
	if status != http.StatusOK || json.Unmarshal(body, &view) != nil {
		return fmt.Errorf("reading transfer %q: status %d: %s", alias, status, string(body))
	}
	if view.State != state {
		return fmt.Errorf("expected transfer %q in state %s, got %s", alias, state, view.State)
	}
	return nil
}

func (w *world) auditColumn(column string) ([]string, error) {
	value, ok, err := w.field("audit")
	if err != nil {
		return nil, err
	}
	entries, isList := value.([]any)
	if !ok || !isList {
		return nil, fmt.Errorf("the response has no audit trail: %s", string(w.body))
	}
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		entry, _ := e.(map[string]any)
		text, _ := entry[column].(string)
		out = append(out, text)
	}
	return out, nil
}

func (w *world) theAuditTrailReads(kind, expected string) error {
	column := map[string]string{"events": "event", "states": "to"}[kind]
	got, err := w.auditColumn(column)
	if err != nil {
		return err
	}
	if joined := strings.Join(got, ", "); joined != expected {
		return fmt.Errorf("audit %s: expected [%s], got [%s]", column, expected, joined)
	}
	return nil
}

func (w *world) theAuditSequenceIsConsecutive() error {
	value, _, err := w.field("audit")
	if err != nil {
		return err
	}
	entries, _ := value.([]any)
	for i, e := range entries {
		entry, _ := e.(map[string]any)
		if seq, _ := entry["seq"].(float64); int(seq) != i+1 {
			return fmt.Errorf("audit entry %d has seq %v, want %d", i, entry["seq"], i+1)
		}
	}
	return nil
}

// ------------------------------------------------------- published events ---

func (w *world) countEvents(name string) int {
	events, _ := w.bus.snapshot()
	n := 0
	for _, e := range events {
		if e.EventName() == name {
			n++
		}
	}
	return n
}

func (w *world) theDomainEventWasPublishedTimes(name string, expected int) error {
	if got := w.countEvents(name); got != expected {
		return fmt.Errorf("expected domain event %q to be published %d time(s), got %d", name, expected, got)
	}
	return nil
}

func (w *world) theDomainEventWasPublished(name string) error {
	if w.countEvents(name) == 0 {
		return fmt.Errorf("expected domain event %q to be published, but it was not", name)
	}
	return nil
}

func (w *world) theDomainEventWasNotPublished(name string) error {
	return w.theDomainEventWasPublishedTimes(name, 0)
}

func (w *world) noDomainEventsWerePublished() error {
	events, _ := w.bus.snapshot()
	if len(events) != 0 {
		return fmt.Errorf("expected nothing to be published, got %d events", len(events))
	}
	return nil
}

func (w *world) aWorkDemandWasReleased(demand, kind string, qty int) error {
	events, _ := w.bus.snapshot()
	demand = w.expand(demand)
	var seen []string
	for _, e := range events {
		released, ok := e.(transfer.DemandReleased)
		if !ok {
			continue
		}
		seen = append(seen, fmt.Sprintf("%s/%s/%d", released.DemandID, released.WorkKind, released.Quantity))
		if released.DemandID == demand && string(released.WorkKind) == kind && released.Quantity == qty {
			return nil
		}
	}
	return fmt.Errorf("expected a %s work demand %q for %d units, released: %v", kind, demand, qty, seen)
}

func (w *world) noWorkDemandWasReleased(demand string) error {
	events, _ := w.bus.snapshot()
	demand = w.expand(demand)
	for _, e := range events {
		if released, ok := e.(transfer.DemandReleased); ok && released.DemandID == demand {
			return fmt.Errorf("expected work demand %q NOT to be released, but it was", demand)
		}
	}
	return nil
}

// ------------------------------------------------------ CloudEvents wire ----

func (w *world) everyMessageIsACloudEvent() error {
	_, messages := w.bus.snapshot()
	if len(messages) == 0 {
		return fmt.Errorf("nothing was published, so there is no CloudEvents envelope to check")
	}
	for _, m := range messages {
		evt, err := cloudevents.Decode(m.Value)
		if err != nil {
			return fmt.Errorf("message on %s is not a CloudEvents 1.0 event: %w", m.Topic, err)
		}
		if evt.ID() == "" || evt.Subject() == "" || evt.Time().IsZero() || evt.DataSchema() == "" {
			return fmt.Errorf("CloudEvent %s is missing id/subject/time/dataschema: %s", evt.Type(), string(m.Value))
		}
		if evt.DataContentType() != "application/json" {
			return fmt.Errorf("CloudEvent %s has datacontenttype %q", evt.Type(), evt.DataContentType())
		}
		if !strings.HasPrefix(evt.Type(), "com.warehouse.") {
			return fmt.Errorf("CloudEvent type %q does not follow com.warehouse.<subdomain>.<context>.<entity>.<Event>", evt.Type())
		}
	}
	return nil
}

func (w *world) typesOnTopic(topic string) []string {
	_, messages := w.bus.snapshot()
	var names []string
	for _, m := range messages {
		if m.Topic == topic {
			names = append(names, m.EventType[strings.LastIndex(m.EventType, ".")+1:])
		}
	}
	sort.Strings(names)
	return names
}

func (w *world) theTopicCarries(topic string, table *godog.Table) error {
	var expected []string
	for _, row := range table.Rows[1:] {
		expected = append(expected, row.Cells[0].Value)
	}
	sort.Strings(expected)
	if got := w.typesOnTopic(topic); strings.Join(got, ",") != strings.Join(expected, ",") {
		return fmt.Errorf("topic %s: expected CloudEvents %v, got %v", topic, expected, got)
	}
	return nil
}

func (w *world) theCloudEventHasSubject(name, subject string) error {
	_, messages := w.bus.snapshot()
	subject = w.expand(subject)
	for _, m := range messages {
		evt, err := cloudevents.Decode(m.Value)
		if err == nil && strings.HasSuffix(evt.Type(), "."+name) {
			if evt.Subject() != subject {
				return fmt.Errorf("CloudEvent %s has subject %q, want %q", name, evt.Subject(), subject)
			}
			return nil
		}
	}
	return fmt.Errorf("no %s CloudEvent was published", name)
}

func (w *world) registerSagaSteps(sc *godog.ScenarioContext) {
	sc.Step(`^I approve a transfer of (\d+) units? of SKU "([^"]*)" from "([^"]*)" to "([^"]*)" under policy "([^"]*)" with the idempotency key "([^"]*)"$`, w.iApprove)
	sc.Step(`^an approved transfer "([^"]*)"$`, w.anApprovedTransfer)
	sc.Step(`^an approved transfer "([^"]*)" of (\d+) units? of SKU "([^"]*)" from "([^"]*)" to "([^"]*)"$`, w.anApprovedTransferOf)
	sc.Step(`^a transfer "([^"]*)" in state "([A-Z_]+)"$`, w.aTransferInState)
	sc.Step(`^a transfer "([^"]*)" of (\d+) units? of SKU "([^"]*)" from "([^"]*)" to "([^"]*)" in state "([A-Z_]+)"$`, w.aTransferOfInState)
	sc.Step(`^the pick work release is not configured$`, w.pickReleaseNotConfigured)
	sc.Step(`^the dispatch work release is not configured$`, w.dispatchReleaseNotConfigured)

	sc.Step(`^a "(Transfer[A-Za-z]+)" fact arrives for transfer "([^"]*)"$`, w.aFactArrives)
	sc.Step(`^a "TransferPicked" fact for (\d+) units? arrives for transfer "([^"]*)"$`, w.aPickedFactArrives)
	sc.Step(`^a "TransferStockAllocationRejected" fact with reason "([^"]*)" arrives for transfer "([^"]*)"$`, w.aRejectionFactArrives)
	sc.Step(`^a "TransferStockStowed" fact for (\d+) units? at site "([^"]*)" arrives for transfer "([^"]*)"$`, w.aStowedFactArrives)
	sc.Step(`^the fact is applied$`, w.theFactIsApplied)
	sc.Step(`^the fact is refused as an illegal transition$`, w.theFactIsRefusedAsIllegal)
	sc.Step(`^the fact is refused as inconsistent with the transfer$`, w.theFactIsRefusedAsInconsistent)
	sc.Step(`^the fact is skipped because the transfer is unknown$`, w.theFactIsSkippedAsUnknown)
	sc.Step(`^the fact is applied but the dispatch work cannot be released$`, w.theFactIsAppliedButDispatchCannotBeReleased)
	sc.Step(`^transfer "([^"]*)" is in state "([A-Z_]+)"$`, w.theTransferIsInState)

	sc.Step(`^the audit trail (events|states) read "([^"]*)"$`, w.theAuditTrailReads)
	sc.Step(`^the audit trail is numbered consecutively from 1$`, w.theAuditSequenceIsConsecutive)

	sc.Step(`^the domain event "([^"]*)" was published$`, w.theDomainEventWasPublished)
	sc.Step(`^the domain event "([^"]*)" was published (\d+) times?$`, w.theDomainEventWasPublishedTimes)
	sc.Step(`^the domain event "([^"]*)" was not published$`, w.theDomainEventWasNotPublished)
	sc.Step(`^no domain events were published$`, w.noDomainEventsWerePublished)
	sc.Step(`^the (TRANSFER_[A-Z]+) work demand "([^"]*)" for (\d+) units? was released$`, func(kind, demand string, qty int) error {
		return w.aWorkDemandWasReleased(demand, kind, qty)
	})
	sc.Step(`^the work demand "([^"]*)" was not released$`, w.noWorkDemandWasReleased)

	sc.Step(`^every published message is a valid CloudEvents 1.0 event$`, w.everyMessageIsACloudEvent)
	sc.Step(`^the topic "([^"]*)" carries these CloudEvents:$`, w.theTopicCarries)
	sc.Step(`^the "([^"]*)" CloudEvent has subject "([^"]*)"$`, w.theCloudEventHasSubject)
}
