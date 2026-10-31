package usecases

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/claudioed/network-inventory-planning/internal/domain/planning"
	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
)

// threeSiteFacts is approveFacts plus a WH3 carrying its own full fact set
// (a copy of WH2's with another site name).
func threeSiteFacts() planning.Facts {
	f := approveFacts()
	capability := f.Capabilities[1]
	capability.Site = "WH3"
	demand := f.Demands[1]
	demand.Site = "WH3"
	demand.SourceOrderID += "-wh3"
	plan := f.Plans[1]
	plan.SiteID = "WH3"
	plan.PlanID = "plan-3"
	f.Capabilities = append(f.Capabilities, capability)
	f.Demands = append(f.Demands, demand)
	f.Plans = append(f.Plans, plan)
	return f
}

// makeSiteStale ages one kind of fact of one site past the 10m budget.
func makeSiteStale(f planning.Facts, site, kind string) planning.Facts {
	old := approveNow.Add(-time.Hour)
	for i := range f.Capabilities {
		if kind == "capability" && f.Capabilities[i].Site == site {
			f.Capabilities[i].AsOf = old
		}
	}
	for i := range f.Demands {
		if kind == "demand" && f.Demands[i].Site == site {
			f.Demands[i].AsOf = old
		}
	}
	for i := range f.Plans {
		if kind == "plan" && f.Plans[i].SiteID == site {
			f.Plans[i].PublishedAt = old
		}
	}
	return f
}

func TestApproveTransferIgnoresAStaleThirdSite(t *testing.T) {
	for _, kind := range []string{"capability", "demand", "plan"} {
		t.Run(kind, func(t *testing.T) {
			repo := newFakeTransferRepo()
			uc := approveUseCase(repo, &fakeEventPublisher{}, makeSiteStale(threeSiteFacts(), "WH3", kind), nil)
			if _, err := uc.Execute(context.Background(), baseApprovalInput()); err != nil {
				t.Fatalf("a stale WH3 must not block WH1 -> WH2: %v", err)
			}
			if len(repo.rows) != 1 {
				t.Fatalf("persisted %d transfers, want 1", len(repo.rows))
			}
		})
	}
}

func TestApproveTransferRefusesAStaleOriginOrDestinationNamingIt(t *testing.T) {
	cases := []struct{ name, site, kind, want string }{
		{"origin capability", "WH1", "capability", "origin site WH1 is excluded from the planning snapshot: stale site capability"},
		{"origin demand", "WH1", "demand", "origin site WH1 is excluded from the planning snapshot: stale site sku demand"},
		{"destination plan", "WH2", "plan", "destination site WH2 is excluded from the planning snapshot: stale published capacity plan plan-2"},
		{"destination capability", "WH2", "capability", "destination site WH2 is excluded from the planning snapshot: stale site capability"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			repo := newFakeTransferRepo()
			pub := &fakeEventPublisher{}
			uc := approveUseCase(repo, pub, makeSiteStale(threeSiteFacts(), tc.site, tc.kind), nil)
			_, err := uc.Execute(context.Background(), baseApprovalInput())
			if !errors.Is(err, transfer.ErrFactsIncomplete) {
				t.Fatalf("err = %v, want ErrFactsIncomplete", err)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %q, want it to contain %q", err, tc.want)
			}
			if len(repo.rows) != 0 || len(pub.events) != 0 {
				t.Fatal("a refused approval persists and publishes nothing")
			}
		})
	}
}
