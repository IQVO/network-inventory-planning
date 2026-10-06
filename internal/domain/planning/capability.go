// Package planning holds the Phase-1 local read-model domain types: the
// site transfer capability snapshot consumed from facility-layout, the
// site/SKU demand projection consumed from order-management, and the
// published capacity plan consumed from warehouse-planning. These are
// read-side facts this context owns LOCALLY (never shared aggregates) and
// are the fail-closed inputs of the planning snapshot.
package planning

import (
	"fmt"
	"strings"
	"time"
)

// SiteCapability is the last-write-wins snapshot of one site's transfer
// capability, mirrored from facility-layout's SiteCapabilityChanged. A site
// the planner knows nothing about is treated as capable of NOTHING (see the
// PlanningSnapshot rules): unknown is fail-closed, never optimistic.
type SiteCapability struct {
	Site                       string
	TransferOriginEnabled      bool
	TransferDestinationEnabled bool
	// Revision is facility-layout's monotonically increasing
	// capability_revision; a strictly greater revision supersedes.
	Revision int64
	// AsOf is the CloudEvents `time` of the snapshot applied last.
	AsOf time.Time
}

// NewSiteCapability validates and constructs a SiteCapability.
func NewSiteCapability(site string, originEnabled, destinationEnabled bool, revision int64, asOf time.Time) (SiteCapability, error) {
	if strings.TrimSpace(site) == "" {
		return SiteCapability{}, fmt.Errorf("site capability: site is required")
	}
	if revision <= 0 {
		return SiteCapability{}, fmt.Errorf("site capability %s: revision must be positive, got %d", site, revision)
	}
	return SiteCapability{
		Site:                       site,
		TransferOriginEnabled:      originEnabled,
		TransferDestinationEnabled: destinationEnabled,
		Revision:                   revision,
		AsOf:                       asOf.UTC(),
	}, nil
}

// Supersedes reports whether c is the newer snapshot for the same site:
// a strictly greater revision wins; an equal or lower revision is a no-op
// (last-write-wins on capability_revision, per the producer's contract).
func (c SiteCapability) Supersedes(other SiteCapability) bool {
	return c.Revision > other.Revision
}

// OriginAllowed reports whether the site may donate stock.
func (c SiteCapability) OriginAllowed() bool {
	return c.TransferOriginEnabled
}

// DestinationAllowed reports whether the site may receive stock.
func (c SiteCapability) DestinationAllowed() bool {
	return c.TransferDestinationEnabled
}
