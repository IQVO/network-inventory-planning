package kafka

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/network-inventory-planning/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/network-inventory-planning/internal/application/ports"
	"github.com/claudioed/network-inventory-planning/internal/domain/planning"
)

// FacilityTopic is facility-layout's integration topic.
const FacilityTopic = "warehouse.facility.events"

// typeSiteCapabilityChanged is facility-layout's confirmed `type` string
// (read from its apis/asyncapi.yaml on origin/develop -- never guessed).
const typeSiteCapabilityChanged = "com.warehouse.wms.facility-layout.site.SiteCapabilityChanged"

// siteCapabilityConsumerName namespaces this consumer's rows in
// ports.ProcessedEventRepository.
const siteCapabilityConsumerName = "site-capability-consumer"

// siteCapabilityChangedData mirrors facility-layout's payload
// hand-mirrored locally, never imported from that service's Go types.
type siteCapabilityChangedData struct {
	SiteCode                   string `json:"site_code"`
	TransferOriginEnabled      bool   `json:"transfer_origin_enabled"`
	TransferDestinationEnabled bool   `json:"transfer_destination_enabled"`
	CapabilityRevision         int64  `json:"capability_revision"`
}

// SiteCapabilityConsumer mirrors facility-layout's SiteCapabilityChanged
// into the local site_capability read model (keyed site_id,
// last-write-wins on capability_revision).
type SiteCapabilityConsumer struct {
	Reader          Reader
	Capabilities    ports.SiteCapabilityRepository
	ProcessedEvents ports.ProcessedEventRepository
	UoW             ports.UnitOfWork
	Logger          *slog.Logger
	Retry           RetryPolicy

	sleep sleepFunc // test hook
}

// NewSiteCapabilityConsumer constructs a SiteCapabilityConsumer reading
// FacilityTopic from brokers under groupID (env-configured by the
// composition root).
func NewSiteCapabilityConsumer(
	brokers []string,
	groupID string,
	capabilities ports.SiteCapabilityRepository,
	processedEvents ports.ProcessedEventRepository,
	uow ports.UnitOfWork,
	logger *slog.Logger,
) *SiteCapabilityConsumer {
	return &SiteCapabilityConsumer{
		Reader:          kafkago.NewReader(readerConfig(brokers, FacilityTopic, groupID)),
		Capabilities:    capabilities,
		ProcessedEvents: processedEvents,
		UoW:             uow,
		Logger:          defaultLogger(logger),
	}
}

// Run consumes FacilityTopic until ctx is cancelled or the reader fails.
func (c *SiteCapabilityConsumer) Run(ctx context.Context) error {
	loop := consumeLoop{
		reader: c.Reader,
		handle: func(ctx context.Context, msg kafkago.Message) error { return c.HandleMessage(ctx, msg.Value) },
		logger: c.Logger,
		name:   "site capability consumer",
		retry:  c.Retry,
		sleep:  c.sleep,
	}
	return loop.run(ctx)
}

// HandleMessage decodes one CloudEvents 1.0 message and, when it is a
// SiteCapabilityChanged, upserts the local capability snapshot. It returns
// nil for anything deterministic (failed validation, unknown type,
// malformed payload, missing fields, domain validation rejection, a
// duplicate event id) and non-nil ONLY for transient/infrastructure
// failures, after the unit of work rolled back.
func (c *SiteCapabilityConsumer) HandleMessage(ctx context.Context, value []byte) error {
	e, err := cloudevents.Decode(value)
	if err != nil {
		c.Logger.WarnContext(ctx, "skipping non-CloudEvents facility message", "error", err)
		return nil
	}
	if e.Type() != typeSiteCapabilityChanged {
		return nil // unknown type: ignore (full-type dispatch)
	}

	var data siteCapabilityChangedData
	if err := e.DataAs(&data); err != nil {
		c.Logger.WarnContext(ctx, "skipping malformed SiteCapabilityChanged payload", "error", err, "event_id", e.ID())
		return nil
	}

	occurredAt := e.Time().UTC()
	capability, err := planning.NewSiteCapability(data.SiteCode, data.TransferOriginEnabled, data.TransferDestinationEnabled, data.CapabilityRevision, occurredAt)
	if err != nil {
		c.Logger.WarnContext(ctx, "skipping SiteCapabilityChanged that failed domain validation", "error", err, "event_id", e.ID())
		return nil
	}

	// Claim + upsert are ONE transaction: if the upsert fails the claim
	// rolls back with it, so the retry is processed instead of skipped.
	return c.UoW.Do(ctx, func(ctx context.Context) error {
		claimed, err := c.ProcessedEvents.Claim(ctx, siteCapabilityConsumerName, e.ID())
		if err != nil {
			return fmt.Errorf("claim processed event: %w", err)
		}
		if !claimed {
			c.Logger.InfoContext(ctx, "skipping already-processed SiteCapabilityChanged event", "event_id", e.ID())
			return nil
		}
		if _, err := c.Capabilities.Upsert(ctx, capability); err != nil {
			return fmt.Errorf("upsert site capability: %w", err)
		}
		return nil
	})
}

// Close releases the underlying Kafka reader.
func (c *SiteCapabilityConsumer) Close() error {
	return c.Reader.Close()
}

// OrderTopic is order-management's integration topic.
const OrderTopic = "warehouse.order-management.events"

// typeSiteSkuDemandChanged is order-management's confirmed `type` string.
const typeSiteSkuDemandChanged = "com.warehouse.wes.order-management.siteskudemand.SiteSkuDemandChanged"

// siteSkuDemandConsumerName namespaces this consumer's processed-event rows.
const siteSkuDemandConsumerName = "site-sku-demand-consumer"

// siteSkuDemandChangedData mirrors order-management's payload
// (internal/adapters/outbound/kafka/publisher.go's siteSkuDemandData).
type siteSkuDemandChangedData struct {
	SourceOrderID     string `json:"source_order_id"`
	LineNo            int    `json:"line_no"`
	SiteID            string `json:"site_id"`
	SKU               string `json:"sku"`
	DemandedUnits     int    `json:"demanded_units"`
	DueAt             string `json:"due_at"`
	State             string `json:"state"`
	AssignmentVersion string `json:"assignment_version"`
}

// SiteSkuDemandConsumer mirrors order-management's SiteSkuDemandChanged
// into the local site_sku_demand read model (keyed source order + line).
type SiteSkuDemandConsumer struct {
	Reader          Reader
	Demands         ports.SiteSkuDemandRepository
	ProcessedEvents ports.ProcessedEventRepository
	UoW             ports.UnitOfWork
	Logger          *slog.Logger
	Retry           RetryPolicy

	sleep sleepFunc // test hook
}

// NewSiteSkuDemandConsumer constructs a SiteSkuDemandConsumer reading
// OrderTopic from brokers under groupID (env-configured).
func NewSiteSkuDemandConsumer(
	brokers []string,
	groupID string,
	demands ports.SiteSkuDemandRepository,
	processedEvents ports.ProcessedEventRepository,
	uow ports.UnitOfWork,
	logger *slog.Logger,
) *SiteSkuDemandConsumer {
	return &SiteSkuDemandConsumer{
		Reader:          kafkago.NewReader(readerConfig(brokers, OrderTopic, groupID)),
		Demands:         demands,
		ProcessedEvents: processedEvents,
		UoW:             uow,
		Logger:          defaultLogger(logger),
	}
}

// Run consumes OrderTopic until ctx is cancelled or the reader fails.
func (c *SiteSkuDemandConsumer) Run(ctx context.Context) error {
	loop := consumeLoop{
		reader: c.Reader,
		handle: func(ctx context.Context, msg kafkago.Message) error { return c.HandleMessage(ctx, msg.Value) },
		logger: c.Logger,
		name:   "site sku demand consumer",
		retry:  c.Retry,
		sleep:  c.sleep,
	}
	return loop.run(ctx)
}

// HandleMessage decodes one CloudEvents 1.0 message and, when it is a
// SiteSkuDemandChanged, upserts the local demand row.
func (c *SiteSkuDemandConsumer) HandleMessage(ctx context.Context, value []byte) error {
	e, err := cloudevents.Decode(value)
	if err != nil {
		c.Logger.WarnContext(ctx, "skipping non-CloudEvents order message", "error", err)
		return nil
	}
	if e.Type() != typeSiteSkuDemandChanged {
		return nil
	}

	var data siteSkuDemandChangedData
	if err := e.DataAs(&data); err != nil {
		c.Logger.WarnContext(ctx, "skipping malformed SiteSkuDemandChanged payload", "error", err, "event_id", e.ID())
		return nil
	}
	dueAt, err := time.Parse(time.RFC3339, data.DueAt)
	if err != nil {
		c.Logger.WarnContext(ctx, "skipping SiteSkuDemandChanged with unparsable due_at", "error", err, "event_id", e.ID())
		return nil
	}

	occurredAt := e.Time().UTC()
	demand, err := planning.NewSiteSkuDemand(data.SourceOrderID, data.LineNo, data.SiteID, data.SKU, data.DemandedUnits, dueAt, planning.DemandState(data.State), data.AssignmentVersion)
	if err != nil {
		c.Logger.WarnContext(ctx, "skipping SiteSkuDemandChanged that failed domain validation", "error", err, "event_id", e.ID())
		return nil
	}
	demand.AsOf = occurredAt

	return c.UoW.Do(ctx, func(ctx context.Context) error {
		claimed, err := c.ProcessedEvents.Claim(ctx, siteSkuDemandConsumerName, e.ID())
		if err != nil {
			return fmt.Errorf("claim processed event: %w", err)
		}
		if !claimed {
			c.Logger.InfoContext(ctx, "skipping already-processed SiteSkuDemandChanged event", "event_id", e.ID())
			return nil
		}
		if _, err := c.Demands.Upsert(ctx, demand); err != nil {
			return fmt.Errorf("upsert site sku demand: %w", err)
		}
		return nil
	})
}

// Close releases the underlying Kafka reader.
func (c *SiteSkuDemandConsumer) Close() error {
	return c.Reader.Close()
}

// PlanningTopic is warehouse-planning's integration topic.
const PlanningTopic = "warehouse.warehouse-planning.events"

// typeCapacityPlanPublished is warehouse-planning's confirmed `type`.
const typeCapacityPlanPublished = "com.warehouse.wes.warehouse-planning.capacityplan.CapacityPlanPublished"

// capacityPlanConsumerName namespaces this consumer's processed-event rows.
const capacityPlanConsumerName = "capacity-plan-consumer"

// capacityPlanPublishedData mirrors warehouse-planning's v1 payload plus
// the additive site_id. A message WITHOUT site_id (legacy, published before
// the field existed) is EXCLUDED from the read model: the site can never
// be inferred from warehouse_id.
type capacityPlanPublishedData struct {
	PlanID             string  `json:"plan_id"`
	SiteID             string  `json:"site_id"`
	WarehouseID        string  `json:"warehouse_id"`
	Location           string  `json:"location"`
	PathID             string  `json:"path_id"`
	WindowStart        string  `json:"window_start"`
	WindowEnd          string  `json:"window_end"`
	AssignedDemand     float64 `json:"assigned_demand"`
	PathCapacity       float64 `json:"path_capacity"`
	CapacityOverWindow float64 `json:"capacity_over_window"`
	Shortage           float64 `json:"shortage"`
	BottleneckStep     string  `json:"bottleneck_step"`
	PublishedAt        string  `json:"published_at"`
}

// CapacityPlanConsumer mirrors warehouse-planning's CapacityPlanPublished
// into the local published_capacity_plan read model (keyed plan_id,
// last-write-wins on the CloudEvents time).
type CapacityPlanConsumer struct {
	Reader          Reader
	Plans           ports.PublishedCapacityPlanRepository
	ProcessedEvents ports.ProcessedEventRepository
	UoW             ports.UnitOfWork
	Logger          *slog.Logger
	Retry           RetryPolicy

	sleep sleepFunc // test hook
}

// NewCapacityPlanConsumer constructs a CapacityPlanConsumer reading
// PlanningTopic from brokers under groupID (env-configured).
func NewCapacityPlanConsumer(
	brokers []string,
	groupID string,
	plans ports.PublishedCapacityPlanRepository,
	processedEvents ports.ProcessedEventRepository,
	uow ports.UnitOfWork,
	logger *slog.Logger,
) *CapacityPlanConsumer {
	return &CapacityPlanConsumer{
		Reader:          kafkago.NewReader(readerConfig(brokers, PlanningTopic, groupID)),
		Plans:           plans,
		ProcessedEvents: processedEvents,
		UoW:             uow,
		Logger:          defaultLogger(logger),
	}
}

// Run consumes PlanningTopic until ctx is cancelled or the reader fails.
func (c *CapacityPlanConsumer) Run(ctx context.Context) error {
	loop := consumeLoop{
		reader: c.Reader,
		handle: func(ctx context.Context, msg kafkago.Message) error { return c.HandleMessage(ctx, msg.Value) },
		logger: c.Logger,
		name:   "capacity plan consumer",
		retry:  c.Retry,
		sleep:  c.sleep,
	}
	return loop.run(ctx)
}

// HandleMessage decodes one CloudEvents 1.0 message and, when it is a
// CapacityPlanPublished, upserts the local plan row. A legacy payload
// without site_id is skipped-and-logged (deterministic), never stored.
func (c *CapacityPlanConsumer) HandleMessage(ctx context.Context, value []byte) error {
	e, err := cloudevents.Decode(value)
	if err != nil {
		c.Logger.WarnContext(ctx, "skipping non-CloudEvents planning message", "error", err)
		return nil
	}
	if e.Type() != typeCapacityPlanPublished {
		return nil
	}

	var data capacityPlanPublishedData
	if err := e.DataAs(&data); err != nil {
		c.Logger.WarnContext(ctx, "skipping malformed CapacityPlanPublished payload", "error", err, "event_id", e.ID())
		return nil
	}
	if data.SiteID == "" {
		// Legacy v1 payload published before site_id existed. The site
		// is NEVER inferred from warehouse_id: the fact is excluded.
		c.Logger.InfoContext(ctx, "excluding legacy CapacityPlanPublished without site_id", "event_id", e.ID(), "plan_id", data.PlanID)
		return nil
	}
	windowStart, err := time.Parse(time.RFC3339, data.WindowStart)
	if err != nil {
		c.Logger.WarnContext(ctx, "skipping CapacityPlanPublished with unparsable window_start", "error", err, "event_id", e.ID())
		return nil
	}
	windowEnd, err := time.Parse(time.RFC3339, data.WindowEnd)
	if err != nil {
		c.Logger.WarnContext(ctx, "skipping CapacityPlanPublished with unparsable window_end", "error", err, "event_id", e.ID())
		return nil
	}

	occurredAt := e.Time().UTC()
	plan, err := planning.NewPublishedCapacityPlan(planning.PublishedCapacityPlan{
		PlanID:             data.PlanID,
		SiteID:             data.SiteID,
		Location:           data.Location,
		PathID:             data.PathID,
		WindowStart:        windowStart,
		WindowEnd:          windowEnd,
		AssignedDemand:     data.AssignedDemand,
		CapacityOverWindow: data.CapacityOverWindow,
		Shortage:           data.Shortage,
		PublishedAt:        occurredAt,
	})
	if err != nil {
		c.Logger.WarnContext(ctx, "skipping CapacityPlanPublished that failed domain validation", "error", err, "event_id", e.ID())
		return nil
	}

	return c.UoW.Do(ctx, func(ctx context.Context) error {
		claimed, err := c.ProcessedEvents.Claim(ctx, capacityPlanConsumerName, e.ID())
		if err != nil {
			return fmt.Errorf("claim processed event: %w", err)
		}
		if !claimed {
			c.Logger.InfoContext(ctx, "skipping already-processed CapacityPlanPublished event", "event_id", e.ID())
			return nil
		}
		if _, err := c.Plans.Upsert(ctx, plan); err != nil {
			return fmt.Errorf("upsert published capacity plan: %w", err)
		}
		return nil
	})
}

// Close releases the underlying Kafka reader.
func (c *CapacityPlanConsumer) Close() error {
	return c.Reader.Close()
}
