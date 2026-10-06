package ports

import (
	"context"
	"time"

	"github.com/claudioed/network-inventory-planning/internal/domain/planning"
)

// PlanningSnapshotRepository is the read side of the Phase-1 local read
// models. Load retrieves every current fact; the FAIL-CLOSED assembly into
// a planning.PlanningSnapshot happens in the domain (BuildSnapshot), never
// here — the repository must not silently degrade a missing fact into a
// zero value.
type PlanningSnapshotRepository interface {
	// Load returns the current site capabilities, site/SKU demands and
	// published capacity plans held in the local read models, together
	// with each fact's watermark. A completely empty read model is
	// returned as empty slices (BuildSnapshot then refuses), never as an
	// error: empty is a legitimate state the fail-closed rules must see.
	Load(ctx context.Context) (planning.Facts, error)
}

// SiteCapabilityRepository is the write side of the site_capability read
// model (keyed site_id, last-write-wins on capability_revision). Inside a
// UnitOfWork the upsert joins the transaction carried in ctx, so the
// processed-event claim and the fact commit or roll back together.
type SiteCapabilityRepository interface {
	// Upsert applies c when its revision supersedes the stored row (or
	// the site is new). It reports whether the row changed.
	Upsert(ctx context.Context, c planning.SiteCapability) (applied bool, err error)
}

// SiteSkuDemandRepository is the write side of the site_sku_demand read
// model (keyed source_order_id+line_no; state ACTIVE/REMOVED).
type SiteSkuDemandRepository interface {
	// Upsert replaces the row keyed by d's source order + line whatever
	// the state (a REMOVED fact tombstones the line's demand).
	Upsert(ctx context.Context, d planning.SiteSkuDemand) (applied bool, err error)
}

// PublishedCapacityPlanRepository is the write side of the
// published_capacity_plan read model (keyed plan_id, last-write-wins on
// the CloudEvents time).
type PublishedCapacityPlanRepository interface {
	// Upsert applies p when its CE time supersedes the stored row (or
	// the plan is new). It reports whether the row changed.
	Upsert(ctx context.Context, p planning.PublishedCapacityPlan) (applied bool, err error)
}

// ProcessedEventRepository is the idempotency guard shared by every
// inbound Kafka consumer in this service. A consumer claims an event by
// (consumer name, CloudEvents id) INSIDE the same UnitOfWork.Do as the
// event's side effects, so the claim commits or rolls back WITH them.
type ProcessedEventRepository interface {
	Claim(ctx context.Context, consumer, eventID string) (claimed bool, err error)
}

// UnitOfWork runs fn as ONE atomic unit (claim + read-model upserts
// commit or roll back together). Re-entrant: a ctx already carrying a
// unit of work is joined, not nested.
type UnitOfWork interface {
	Do(ctx context.Context, fn func(ctx context.Context) error) error
}

// Clock abstracts time.Now for deterministic tests.
type Clock interface {
	Now() time.Time
}
