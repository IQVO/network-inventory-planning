package main_test

// Step vocabulary of the documentation-derived scenarios (features/*_rules.feature).
//
// The coverage-driven suite hands facts to the use cases directly. These
// steps add what the documentation also pins and that suite could not reach:
//
//   - the REAL Kafka consumers (claim, dead-letter, skip policies, ADR 0013),
//     fed hand-built CloudEvents 1.0 messages;
//   - the pack-level view (ADR 0017) and its outage;
//   - read-model facts at the documented boundaries (window, staleness,
//     revisions);
//   - the keyed-write edge cases of the Idempotency-Key guideline.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	ce "github.com/cloudevents/sdk-go/v2/event"
	"github.com/cucumber/godog"
	kafkago "github.com/segmentio/kafka-go"

	inboundkafka "github.com/claudioed/network-inventory-planning/internal/adapters/inbound/kafka"
	"github.com/claudioed/network-inventory-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/network-inventory-planning/internal/application/ports"
	"github.com/claudioed/network-inventory-planning/internal/application/usecases"
	"github.com/claudioed/network-inventory-planning/internal/domain/packlevel"
	"github.com/claudioed/network-inventory-planning/internal/domain/planning"
	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

// ----------------------------------------------------------------- state ----

// rulesState is the per-scenario state of these steps.
type rulesState struct {
	claims       *claimSet
	dlq          *captureDLQ
	delivered    int // dead letters present before the last delivery
	msgErr       error
	msg          []byte
	policyBroken bool
	remembered   []byte
}

// claimSet is the processed-event claim store of the consumers: the first
// claim of (consumer, CloudEvents id) wins, every later one is a duplicate.
type claimSet struct{ seen map[string]bool }

func (c *claimSet) Claim(_ context.Context, consumer, eventID string) (bool, error) {
	key := consumer + "/" + eventID
	if c.seen[key] {
		return false, nil
	}
	c.seen[key] = true
	return true, nil
}

// captureDLQ records what the consumers dead-letter.
type captureDLQ struct{ msgs []kafkago.Message }

func (c *captureDLQ) WriteMessages(_ context.Context, msgs ...kafkago.Message) error {
	c.msgs = append(c.msgs, msgs...)
	return nil
}

func (c *captureDLQ) Close() error { return nil }

func (w *world) wireClaims() *claimSet {
	if w.rules.claims == nil {
		w.rules.claims = &claimSet{seen: map[string]bool{}}
	}
	return w.rules.claims
}

func (w *world) wireDLQ() *captureDLQ {
	if w.rules.dlq == nil {
		w.rules.dlq = &captureDLQ{}
	}
	return w.rules.dlq
}

// replyConsumer is the production inventory-storage reply consumer over the
// scenario's own use cases, claim store and dead-letter capture.
func (w *world) replyConsumer() *inboundkafka.TransferReplyConsumer {
	uow := memory.UnitOfWork{}
	return &inboundkafka.TransferReplyConsumer{
		Applier: inboundkafka.ReplyUseCases{
			Allocate: usecases.ApplyTransferAllocation{Transfers: w.transfers, Events: w.bus, UoW: uow, Release: w.release, Now: w.clock.Now},
			Reject:   usecases.ApplyTransferRejection{Transfers: w.transfers, Events: w.bus, UoW: uow},
			Staged:   usecases.ApplyTransferReceiptStaged{Transfers: w.transfers, Events: w.bus, UoW: uow},
			Stow:     usecases.ApplyTransferStow{Transfers: w.transfers, Events: w.bus, UoW: uow},
			Released: usecases.ApplyTransferAllocationReleased{Transfers: w.transfers, Events: w.bus, UoW: uow},
		},
		ProcessedEvents: w.wireClaims(),
		UoW:             uow,
		Logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
		DLQ:             w.wireDLQ(),
		DLQTopic:        inboundkafka.InventoryTopic + inboundkafka.DLQSuffix,
	}
}

// factConsumer is the production fulfillment-execution fact consumer.
func (w *world) factConsumer() *inboundkafka.TransferFactConsumer {
	uow := memory.UnitOfWork{}
	return &inboundkafka.TransferFactConsumer{
		Applier: inboundkafka.FactUseCases{
			Pick:       usecases.ApplyTransferPick{Transfers: w.transfers, Events: w.bus, UoW: uow, Release: w.release},
			Dispatched: usecases.ApplyTransferDispatched{Transfers: w.transfers, Events: w.bus, UoW: uow},
			Arrival:    usecases.ApplyTransferArrival{Transfers: w.transfers, Events: w.bus, UoW: uow},
		},
		ProcessedEvents: w.wireClaims(),
		UoW:             uow,
		Logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
		DLQ:             w.wireDLQ(),
		DLQTopic:        inboundkafka.FulfillmentTopic + inboundkafka.DLQSuffix,
	}
}

// ------------------------------------------------------- wire messages ----

type wireKind struct {
	ceType    string
	inventory bool // else fulfillment-execution
}

var wireKinds = map[string]wireKind{
	"TransferStockAllocated":          {"com.warehouse.wms.inventory-storage.reservation.TransferStockAllocated", true},
	"TransferStockAllocationRejected": {"com.warehouse.wms.inventory-storage.reservation.TransferStockAllocationRejected", true},
	"TransferStockAllocationReleased": {"com.warehouse.wms.inventory-storage.reservation.TransferStockAllocationReleased", true},
	"TransferReceiptStaged":           {"com.warehouse.wms.inventory-storage.stock.TransferReceiptStaged", true},
	"TransferStockStowed":             {"com.warehouse.wms.inventory-storage.stock.TransferStockStowed", true},
	"TransferPicked":                  {"com.warehouse.wes.fulfillment-execution.transfer.TransferPicked", false},
	"TransferDispatched":              {"com.warehouse.wes.fulfillment-execution.transfer.TransferDispatched", false},
	"TransferArrived":                 {"com.warehouse.wes.fulfillment-execution.transfer.TransferArrived", false},
}

// wireData builds the payload a well-behaved sibling would publish for the
// transfer; an alias that names no transfer gets plausible defaults.
func (w *world) wireData(ctx context.Context, kind, alias string) map[string]any {
	id := w.resolve(alias)
	origin, destination, sku, quantity, reservation := defaultOrigin, defaultDestination, defaultSKU, defaultQuantity, "res-1"
	if t, known := w.peek(ctx, id); known {
		origin, destination, sku, quantity = t.OriginSiteID(), t.DestinationSiteID(), t.SKU(), t.Quantity()
		if t.ReservationID() != "" {
			reservation = t.ReservationID()
		}
	}
	line := id + ":1"
	switch kind {
	case "TransferStockAllocated":
		return map[string]any{
			"transfer_id": id, "transfer_line_id": line, "origin_site_id": origin, "reservation_id": reservation,
			"sku": sku, "quantity": quantity,
			"allocations": []map[string]any{{"stock_unit_id": "su-1", "bin_id": "bin-A1", "quantity": quantity}},
			"expires_at":  w.clock.Now().Add(2 * time.Hour).UTC().Format(time.RFC3339),
		}
	case "TransferStockAllocationRejected":
		return map[string]any{
			"transfer_id": id, "transfer_line_id": line, "origin_site_id": origin, "sku": sku,
			"requested_quantity": quantity, "reason": "INSUFFICIENT_USABLE",
		}
	case "TransferStockAllocationReleased":
		return map[string]any{
			"transfer_id": id, "transfer_line_id": line, "reservation_id": reservation, "origin_site_id": origin,
			"sku": sku, "quantity": quantity, "outcome": "RELEASED", "reason": "revoked for cancelled transfer",
		}
	case "TransferReceiptStaged":
		return map[string]any{
			"transfer_id": id, "transfer_line_id": line, "destination_site_id": destination, "sku": sku,
			"expected_quantity": quantity, "received_quantity": quantity, "variance": 0,
		}
	case "TransferStockStowed":
		return map[string]any{
			"transfer_id": id, "transfer_line_id": line, "destination_site_id": destination, "sku": sku,
			"received_quantity": quantity, "stowed_quantity": quantity,
			"allocations": []map[string]any{{"stock_unit_id": "su-9", "bin_id": "bin-B2", "quantity": quantity}},
		}
	case "TransferPicked":
		return map[string]any{
			"transfer_ref": id, "work_unit_id": "wu-1", "task_id": "task-1", "work_kind": "TRANSFER_PICK",
			"site_id": origin, "sku": sku, "quantity": quantity,
		}
	default: // TransferDispatched, TransferArrived
		return map[string]any{"transfer_ref": id, "work_unit_id": "wu-1", "task_id": "task-1", "work_kind": "TRANSFER_DISPATCH", "site_id": origin, "sku": sku}
	}
}

// cloudEvent encodes one structured-mode CloudEvents 1.0 message.
func (w *world) cloudEvent(id, ceType, subject string, data any) ([]byte, error) {
	e := ce.New(ce.CloudEventsVersionV1)
	e.SetID(id)
	e.SetSource("/warehouse/sibling")
	e.SetType(ceType)
	e.SetSubject(subject)
	e.SetTime(w.clock.Now().UTC())
	if err := e.SetData("application/json", data); err != nil {
		return nil, err
	}
	if err := e.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(e)
}

// deliver hands one raw message to the consumer that owns the topic.
func (w *world) deliver(ctx context.Context, inventory bool, raw []byte) {
	w.rules.msg = raw
	w.rules.delivered = len(w.wireDLQ().msgs)
	if inventory {
		w.rules.msgErr = w.replyConsumer().HandleKafkaMessage(ctx, kafkago.Message{Topic: inboundkafka.InventoryTopic, Value: raw})
		return
	}
	w.rules.msgErr = w.factConsumer().HandleKafkaMessage(ctx, kafkago.Message{Topic: inboundkafka.FulfillmentTopic, Value: raw})
}

func (w *world) publishFact(ctx context.Context, source, eventID, kind, alias, field, literal string) error {
	k, ok := wireKinds[kind]
	if !ok {
		return fmt.Errorf("unknown transfer message kind %q", kind)
	}
	if k.inventory != (source == "inventory-storage") {
		return fmt.Errorf("%s is not published by %s", kind, source)
	}
	data := w.wireData(ctx, kind, alias)
	if field != "" {
		var value any
		if err := json.Unmarshal([]byte(literal), &value); err != nil {
			return fmt.Errorf("override %s=%s is not a JSON literal: %w", field, literal, err)
		}
		data[field] = value
	}
	raw, err := w.cloudEvent(eventID, k.ceType, w.resolve(alias)+":1", data)
	if err != nil {
		return err
	}
	w.deliver(ctx, k.inventory, raw)
	return nil
}

func (w *world) publishPlain(ctx context.Context, source, eventID, kind, alias string) error {
	return w.publishFact(ctx, source, eventID, kind, alias, "", "")
}

func (w *world) publishOverride(ctx context.Context, source, eventID, kind, alias, field, literal string) error {
	return w.publishFact(ctx, source, eventID, kind, alias, field, strings.TrimSpace(literal))
}

func (w *world) topicCarriesType(ctx context.Context, topic, ceType string) error {
	raw, err := w.cloudEvent("evt-foreign", ceType, "subject-1", map[string]string{"note": "not ours"})
	if err != nil {
		return err
	}
	w.deliver(ctx, topic == "inventory", raw)
	return nil
}

func (w *world) topicCarriesRaw(ctx context.Context, topic string, body *godog.DocString) error {
	w.deliver(ctx, topic == "inventory", []byte(body.Content))
	return nil
}

func (w *world) newDeadLetters() []kafkago.Message {
	return w.wireDLQ().msgs[w.rules.delivered:]
}

func (w *world) theMessageIsAcknowledged() error {
	if w.rules.msgErr != nil {
		return fmt.Errorf("expected the message to be acknowledged (committed past), the consumer returned: %w", w.rules.msgErr)
	}
	if n := len(w.newDeadLetters()); n != 0 {
		return fmt.Errorf("expected no dead letter, got %d", n)
	}
	return nil
}

func header(msg kafkago.Message, key string) string {
	for _, h := range msg.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func (w *world) theMessageIsDeadLettered(consumer string) error {
	if w.rules.msgErr != nil {
		return fmt.Errorf("a dead-lettered message must be committed past, the consumer returned: %w", w.rules.msgErr)
	}
	letters := w.newDeadLetters()
	if len(letters) != 1 {
		return fmt.Errorf("expected exactly one dead letter, got %d", len(letters))
	}
	topic := inboundkafka.InventoryTopic
	name := "transfer-reply-consumer"
	if consumer == "transfer fact" {
		topic, name = inboundkafka.FulfillmentTopic, "transfer-fact-consumer"
	}
	letter := letters[0]
	if got := header(letter, "x-dlq-consumer"); got != name {
		return fmt.Errorf("x-dlq-consumer = %q, want %q", got, name)
	}
	if got := header(letter, "x-dlq-source-topic"); got != topic {
		return fmt.Errorf("x-dlq-source-topic = %q, want %q", got, topic)
	}
	if !bytes.Equal(letter.Value, w.rules.msg) {
		return fmt.Errorf("the dead letter is not the original message byte for byte")
	}
	return nil
}

func (w *world) theDeadLetterSays(fragment string) error {
	letters := w.newDeadLetters()
	if len(letters) != 1 {
		return fmt.Errorf("expected one dead letter, got %d", len(letters))
	}
	if got := header(letters[0], "x-dlq-error"); !strings.Contains(got, fragment) {
		return fmt.Errorf("x-dlq-error %q does not contain %q", got, fragment)
	}
	return nil
}

// ------------------------------------------------------- event payloads ----

func (w *world) theStuckDetectionReports(alias, state string, age, threshold int) error {
	events, _ := w.bus.snapshot()
	id := transfer.TransferID(w.resolve(alias))
	var seen []string
	for _, e := range events {
		stuck, ok := e.(transfer.StuckDetected)
		if !ok || stuck.TransferID != id {
			continue
		}
		seen = append(seen, fmt.Sprintf("%s/%ds/%ds", stuck.State, stuck.AgeSeconds, stuck.ThresholdSeconds))
		if string(stuck.State) == state && stuck.AgeSeconds == int64(age) && stuck.ThresholdSeconds == int64(threshold) {
			return nil
		}
	}
	return fmt.Errorf("no TransferStuckDetected for %s with state %s, age %ds, threshold %ds; saw %v", alias, state, age, threshold, seen)
}

func (w *world) theReleaseCommandNames(alias, reservation string, quantity int, sku, site, reason string) error {
	events, _ := w.bus.snapshot()
	id := transfer.TransferID(w.resolve(alias))
	for _, e := range events {
		cmd, ok := e.(transfer.ReleaseRequested)
		if !ok || cmd.TransferID != id {
			continue
		}
		if cmd.ReservationID != reservation || cmd.Quantity != quantity || cmd.SKU != sku || cmd.OriginSiteID != site || cmd.Reason != reason || cmd.TransferLineID != string(id)+":1" {
			return fmt.Errorf("release command = %+v", cmd)
		}
		return nil
	}
	return fmt.Errorf("no TransferAllocationReleaseRequested was published for %s", alias)
}

func (w *world) theAllocationRequestAsks(quantity int, sku, site string) error {
	events, _ := w.bus.snapshot()
	for _, e := range events {
		cmd, ok := e.(transfer.AllocationRequested)
		if !ok {
			continue
		}
		if cmd.Quantity != quantity || cmd.SKU != sku || cmd.OriginSiteID != site {
			return fmt.Errorf("allocation request = %+v, want %d units of %s at %s", cmd, quantity, sku, site)
		}
		return nil
	}
	return fmt.Errorf("no TransferAllocationRequested was published")
}

func (w *world) thePlanApprovedCarriesCases(quantity, cases, multiple, requested, remainder int) error {
	events, _ := w.bus.snapshot()
	for _, e := range events {
		approved, ok := e.(transfer.PlanApproved)
		if !ok {
			continue
		}
		r := approved.CaseRounding
		if approved.Quantity != quantity || r == nil || r.Cases != cases || r.CaseMultiple != multiple || r.RequestedEaches != requested || r.RoundedEaches != quantity || r.RemainderEaches != remainder {
			return fmt.Errorf("TransferPlanApproved = %+v (rounding %+v)", approved, r)
		}
		return nil
	}
	return fmt.Errorf("no TransferPlanApproved was published")
}

func (w *world) thePlanApprovedHasNoCases(quantity int) error {
	events, _ := w.bus.snapshot()
	for _, e := range events {
		if approved, ok := e.(transfer.PlanApproved); ok {
			if approved.Quantity != quantity || approved.CaseRounding != nil {
				return fmt.Errorf("TransferPlanApproved = %+v, want %d units and no case rounding", approved, quantity)
			}
			return nil
		}
	}
	return fmt.Errorf("no TransferPlanApproved was published")
}

func (w *world) theRebalanceRunIdIs(runID string) error {
	events, _ := w.bus.snapshot()
	for _, e := range events {
		if done, ok := e.(transfer.RebalanceRunCompleted); ok {
			if done.RunID != runID {
				return fmt.Errorf("RebalanceRunCompleted run id = %q, want %q", done.RunID, runID)
			}
			return nil
		}
	}
	return fmt.Errorf("no RebalanceRunCompleted was published")
}

func (w *world) theWorkDemandWasReleasedAt(kind, demand, site, path, cpt string) error {
	want, err := time.Parse(time.RFC3339, cpt)
	if err != nil {
		return err
	}
	events, _ := w.bus.snapshot()
	demand = w.expand(demand)
	for _, e := range events {
		released, ok := e.(transfer.DemandReleased)
		if !ok || released.DemandID != demand {
			continue
		}
		if string(released.WorkKind) != kind || released.SiteID != site || released.PathID != path || !released.CPT.Equal(want) {
			return fmt.Errorf("work demand %s = kind %s, site %s, path %s, CPT %s", demand, released.WorkKind, released.SiteID, released.PathID, released.CPT.Format(time.RFC3339))
		}
		return nil
	}
	return fmt.Errorf("work demand %q was not released", demand)
}

// theTransferEnteredOnce reads the audit trail and proves the state was
// entered exactly once, by the named event.
func (w *world) theTransferEnteredOnce(ctx context.Context, alias, state, event string) error {
	status, body, _, err := w.call(ctx, request{server: w.oltp, method: http.MethodGet, path: "/v1/transfers/" + w.resolve(alias)})
	if err != nil {
		return fmt.Errorf("reading transfer %q: %w", alias, err)
	}
	if status != http.StatusOK {
		return fmt.Errorf("reading transfer %q: status %d: %s", alias, status, string(body))
	}
	var view struct {
		Audit []struct {
			To    string `json:"to"`
			Event string `json:"event"`
		} `json:"audit"`
	}
	if err := json.Unmarshal(body, &view); err != nil {
		return err
	}
	var events []string
	for _, a := range view.Audit {
		if a.To == state {
			events = append(events, a.Event)
		}
	}
	if len(events) != 1 || events[0] != event {
		return fmt.Errorf("entries into %s = %v, want exactly [%s]", state, events, event)
	}
	return nil
}

// ------------------------------------------------------------ pack levels ----

type brokenPacks struct{}

func (brokenPacks) Get(context.Context, string, packlevel.Level) (packlevel.PackLevel, bool, error) {
	return packlevel.PackLevel{}, false, errors.New("pack-level store is down")
}

func (brokenPacks) Save(context.Context, packlevel.PackLevel) error {
	return errors.New("pack-level store is down")
}

var _ ports.PackLevelView = brokenPacks{}

func (w *world) skuPackedInCases(sku string, units int, from, until string) error {
	effectiveFrom := w.clock.Now().Add(-24 * time.Hour)
	if from != "" {
		t, err := time.Parse(time.RFC3339, from)
		if err != nil {
			return err
		}
		effectiveFrom = t
	}
	level, err := packlevel.New(sku, string(packlevel.Case), "GTIN-"+sku, int64(units), effectiveFrom, 1)
	if err != nil {
		return err
	}
	if until != "" {
		t, err := time.Parse(time.RFC3339, until)
		if err != nil {
			return err
		}
		level.EffectiveTo = &t
	}
	return w.packs.Save(context.Background(), level)
}

func (w *world) skuCaseLevelRetired(ctx context.Context, sku string) error {
	level, found, err := w.packs.Get(ctx, sku, packlevel.Case)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("SKU %s has no case level to retire", sku)
	}
	now := w.clock.Now()
	level.EffectiveTo = &now
	level.Version++
	return w.packs.Save(ctx, level)
}

func (w *world) packViewUnavailable() error {
	w.packView = brokenPacks{}
	w.approve.Packs = w.packView
	w.routes = w.fullHandler().Routes()
	return nil
}

// ------------------------------------------------- planning facts at edges ----

// planOf returns the declared capacity plan of the site (the window every
// demand of the site is judged against).
func (w *world) planOf(site string) (planning.PublishedCapacityPlan, error) {
	for _, p := range w.declared.Plans {
		if p.SiteID == site {
			return p, nil
		}
	}
	return planning.PublishedCapacityPlan{}, fmt.Errorf("site %s has no declared capacity plan", site)
}

func (w *world) siteHasDemandDueAt(site string, units int, sku, when string) error {
	plan, err := w.planOf(site)
	if err != nil {
		return err
	}
	due := map[string]time.Time{
		"at the start":                plan.WindowStart,
		"at the end":                  plan.WindowEnd,
		"one second before the start": plan.WindowStart.Add(-time.Second),
		"one second before the end":   plan.WindowEnd.Add(-time.Second),
	}[when]
	w.declared.Demands = append(w.declared.Demands, w.demand(site, sku, "edge", units, planning.DemandActive, due))
	w.facts.Set(w.declared)
	return nil
}

func (w *world) siteHasStaleDemandRow(site string, units int, state, sku string, minutes int) error {
	demandState := planning.DemandActive
	if state == "removed" {
		demandState = planning.DemandRemoved
	}
	d := w.demand(site, sku, "stale", units, demandState, w.clock.Now().Add(2*time.Hour))
	d.AsOf = w.clock.Now().Add(-time.Duration(minutes) * time.Minute)
	w.declared.Demands = append(w.declared.Demands, d)
	w.facts.Set(w.declared)
	return nil
}

// siteFactsAgedSeconds stamps every fact of one site as last updated
// `seconds` ago.
func (w *world) siteFactsAgedSeconds(site string, seconds int) error {
	asOf := w.clock.Now().Add(-time.Duration(seconds) * time.Second)
	for i := range w.declared.Capabilities {
		if w.declared.Capabilities[i].Site == site {
			w.declared.Capabilities[i].AsOf = asOf
		}
	}
	for i := range w.declared.Plans {
		if w.declared.Plans[i].SiteID == site {
			w.declared.Plans[i].PublishedAt = asOf
		}
	}
	for i := range w.declared.Demands {
		if w.declared.Demands[i].Site == site {
			w.declared.Demands[i].AsOf = asOf
		}
	}
	w.facts.Set(w.declared)
	return nil
}

// sitePublishedAnotherPlan appends a second plan of the same site and window
// published `seconds` ago.
func (w *world) sitePublishedAnotherPlan(site string, capacity, seconds int) error {
	base, err := w.planOf(site)
	if err != nil {
		return err
	}
	plan, err := planning.NewPublishedCapacityPlan(planning.PublishedCapacityPlan{
		PlanID: fmt.Sprintf("plan-%s-extra-%d", site, len(w.declared.Plans)+1), SiteID: site, Location: base.Location, PathID: base.PathID,
		WindowStart: base.WindowStart, WindowEnd: base.WindowEnd,
		AssignedDemand: base.AssignedDemand, CapacityOverWindow: float64(capacity),
		PublishedAt: w.clock.Now().Add(-time.Duration(seconds) * time.Second),
	})
	if err != nil {
		return err
	}
	w.declared.Plans = append(w.declared.Plans, plan)
	w.facts.Set(w.declared)
	return nil
}

func (w *world) sitePublishedCapabilityRevision(site string, revision int, can string) error {
	capability, err := planning.NewSiteCapability(site, true, can == "can", int64(revision), w.clock.Now().Add(-time.Minute))
	if err != nil {
		return err
	}
	w.declared.Capabilities = append(w.declared.Capabilities, capability)
	w.facts.Set(w.declared)
	return nil
}

// ------------------------------------------------------- keyed-write edges ----

func (w *world) iApproveWithKeyOfLength(ctx context.Context, qty int, sku, origin, destination, policy string, length int) error {
	return w.iApprove(ctx, qty, sku, origin, destination, policy, strings.Repeat("k", length))
}

func (w *world) iApproveWithTwoKeys(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.oltp.URL+"/v1/transfer-approvals", bytes.NewReader(defaultApproval().body()))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Add("Idempotency-Key", "first-key")
	req.Header.Add("Idempotency-Key", "second-key")
	resp, err := w.oltp.Client().Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	w.status, w.body, w.headers = resp.StatusCode, body, resp.Header
	return nil
}

func (w *world) pickReleaseConfiguredAgain() error {
	w.approve.Release = w.release
	return nil
}

func (w *world) iRememberTheResponse() error {
	w.rules.remembered = append([]byte(nil), w.body...)
	return nil
}

func (w *world) theResponseIsTheRememberedOne() error {
	if !bytes.Equal(w.rules.remembered, w.body) {
		return fmt.Errorf("the response differs from the remembered one:\n%s\n%s", string(w.rules.remembered), string(w.body))
	}
	return nil
}

// ------------------------------------------------------------- catalogue ----

type brokenPolicies struct {
	ports.RebalancePolicyRepository
}

func (brokenPolicies) List(context.Context) ([]transfer.RebalancePolicy, error) {
	return nil, errors.New("catalogue store is down")
}

// policyRepo is what the scheduled pass reads: the catalogue, or its outage.
func (r *rulesState) policyRepo(w *world) ports.RebalancePolicyRepository {
	if r.policyBroken {
		return brokenPolicies{w.policies}
	}
	return w.policies
}

func (w *world) policyCatalogueUnreadable() error {
	w.rules.policyBroken = true
	return nil
}

// -------------------------------------------------------------- register ----

func (w *world) registerRulesSteps(sc *godog.ScenarioContext) {
	// Real consumers, hand-built CloudEvents.
	sc.Step(`^(inventory-storage|fulfillment-execution) publishes event "([^"]*)" (Transfer[A-Za-z]+) for transfer "([^"]*)"$`, w.publishPlain)
	sc.Step(`^(inventory-storage|fulfillment-execution) publishes event "([^"]*)" (Transfer[A-Za-z]+) for transfer "([^"]*)" with "([^"]*)" set to (.+)$`, w.publishOverride)
	sc.Step(`^the (inventory|fulfillment) topic carries the CloudEvent type "([^"]*)"$`, w.topicCarriesType)
	sc.Step(`^the (inventory|fulfillment) topic carries this raw message:$`, w.topicCarriesRaw)
	sc.Step(`^the message is acknowledged without a dead letter$`, w.theMessageIsAcknowledged)
	sc.Step(`^the message is dead-lettered by the (transfer reply|transfer fact) consumer$`, w.theMessageIsDeadLettered)
	sc.Step(`^the dead letter says "([^"]*)"$`, w.theDeadLetterSays)

	// Published payloads.
	sc.Step(`^the stuck detection for transfer "([^"]*)" reports state "([A-Z_]+)" aged (\d+) seconds against a threshold of (\d+) seconds$`, w.theStuckDetectionReports)
	sc.Step(`^the release command for transfer "([^"]*)" names reservation "([^"]*)" for (\d+) units of SKU "([^"]*)" at site "([^"]*)" because "([^"]*)"$`, w.theReleaseCommandNames)
	sc.Step(`^the allocation request asks inventory-storage for (\d+) units of SKU "([^"]*)" at site "([^"]*)"$`, w.theAllocationRequestAsks)
	sc.Step(`^the TransferPlanApproved event carries (\d+) units as (\d+) cases? of (\d+) with (\d+) requested and a remainder of (-?\d+)$`, w.thePlanApprovedCarriesCases)
	sc.Step(`^the TransferPlanApproved event carries (\d+) units and no case rounding$`, w.thePlanApprovedHasNoCases)
	sc.Step(`^the RebalanceRunCompleted run id is "([^"]*)"$`, w.theRebalanceRunIdIs)
	sc.Step(`^the (TRANSFER_[A-Z]+) work demand "([^"]*)" was released at site "([^"]*)" on path "([^"]*)" with a critical pull time of "([^"]*)"$`, w.theWorkDemandWasReleasedAt)
	sc.Step(`^transfer "([^"]*)" entered "([A-Z_]+)" exactly once, caused by the event "([^"]*)"$`, w.theTransferEnteredOnce)

	// Pack levels.
	sc.Step(`^SKU "([^"]*)" is packed in cases of (\d+)(?: from "([^"]*)")?(?: until "([^"]*)")?$`, w.skuPackedInCases)
	sc.Step(`^SKU "([^"]*)" has its case level retired$`, w.skuCaseLevelRetired)
	sc.Step(`^the pack-level view is unavailable$`, w.packViewUnavailable)

	// Facts at the documented edges.
	sc.Step(`^site "([^"]*)" also has (\d+) units of demand for SKU "([^"]*)" due (at the start|at the end|one second before the start|one second before the end) of its capacity window$`, w.siteHasDemandDueAt)
	sc.Step(`^site "([^"]*)" also has a (\d+)-unit (active|removed) demand for SKU "([^"]*)" last updated (\d+) minutes ago$`, w.siteHasStaleDemandRow)
	sc.Step(`^the facts of site "([^"]*)" were last updated (\d+) seconds ago$`, w.siteFactsAgedSeconds)
	sc.Step(`^site "([^"]*)" also published a capacity plan of (\d+) units (\d+) seconds ago$`, w.sitePublishedAnotherPlan)
	sc.Step(`^site "([^"]*)" also published capability revision (\d+) that (can|cannot) receive transfers$`, w.sitePublishedCapabilityRevision)

	// Keyed writes.
	sc.Step(`^I approve a transfer of (\d+) units? of SKU "([^"]*)" from "([^"]*)" to "([^"]*)" under policy "([^"]*)" with an idempotency key of (\d+) characters$`, w.iApproveWithKeyOfLength)
	sc.Step(`^I approve a transfer with two Idempotency-Key headers$`, w.iApproveWithTwoKeys)
	sc.Step(`^the pick work release is configured again$`, w.pickReleaseConfiguredAgain)
	sc.Step(`^I remember the response$`, w.iRememberTheResponse)
	sc.Step(`^the response is the one I remembered$`, w.theResponseIsTheRememberedOne)

	// Catalogue outage.
	sc.Step(`^the policy catalogue is unreadable$`, w.policyCatalogueUnreadable)
}
