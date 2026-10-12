package transfer

import (
	"errors"
	"testing"
	"time"
)

var catalogueNow = time.Date(2026, 10, 12, 9, 0, 0, 0, time.UTC)

func validPolicy() Policy {
	return Policy{Version: "policy-v1", Site: "WH2", SKU: "SKU-1", SafetyStock: 5, TargetStock: 50, UnitPriority: 10}
}

func validLane() Lane {
	return Lane{Origin: "WH1", Destination: "WH2", LeadTime: 48 * time.Hour, UnitHandlingCost: 1, Enabled: true}
}

func TestPolicyValidate(t *testing.T) {
	if err := validPolicy().Validate(); err != nil {
		t.Fatalf("valid policy refused: %v", err)
	}
	cases := map[string]func(*Policy){
		"no version":           func(p *Policy) { p.Version = " " },
		"no site":              func(p *Policy) { p.Site = "" },
		"no sku":               func(p *Policy) { p.SKU = "" },
		"negative safety":      func(p *Policy) { p.SafetyStock = -1 },
		"negative target":      func(p *Policy) { p.TargetStock = -1 },
		"target below safety":  func(p *Policy) { p.TargetStock = 4 },
		"negative priority":    func(p *Policy) { p.UnitPriority = -1 },
		"blank site with tabs": func(p *Policy) { p.Site = "\t" },
	}
	for name, mut := range cases {
		t.Run(name, func(t *testing.T) {
			p := validPolicy()
			mut(&p)
			if err := p.Validate(); !errors.Is(err, ErrInvalidCatalogueEntry) {
				t.Fatalf("err = %v, want ErrInvalidCatalogueEntry", err)
			}
		})
	}
	edge := validPolicy()
	edge.SafetyStock, edge.TargetStock, edge.UnitPriority = 0, 0, 0
	if err := edge.Validate(); err != nil {
		t.Fatalf("zero safety/target/priority is a legal (do-not-stock) policy: %v", err)
	}
}

func TestLaneValidate(t *testing.T) {
	if err := validLane().Validate(); err != nil {
		t.Fatalf("valid lane refused: %v", err)
	}
	cases := map[string]func(*Lane){
		"no origin":           func(l *Lane) { l.Origin = "" },
		"no destination":      func(l *Lane) { l.Destination = " " },
		"same sites":          func(l *Lane) { l.Destination = l.Origin },
		"negative lead time":  func(l *Lane) { l.LeadTime = -time.Second },
		"negative handling":   func(l *Lane) { l.UnitHandlingCost = -1 },
		"blank origin spaces": func(l *Lane) { l.Origin = "  " },
	}
	for name, mut := range cases {
		t.Run(name, func(t *testing.T) {
			l := validLane()
			mut(&l)
			if err := l.Validate(); !errors.Is(err, ErrInvalidCatalogueEntry) {
				t.Fatalf("err = %v, want ErrInvalidCatalogueEntry", err)
			}
		})
	}
	zero := validLane()
	zero.LeadTime, zero.UnitHandlingCost, zero.Enabled = 0, 0, false
	if err := zero.Validate(); err != nil {
		t.Fatalf("a disabled zero-cost lane is legal: %v", err)
	}
}

func TestRebalancePolicyLifecycle(t *testing.T) {
	created, err := NewRebalancePolicy(validPolicy(), catalogueNow)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	if created.Revision != 1 || !created.UpdatedAt.Equal(catalogueNow) || created.Policy != validPolicy() {
		t.Fatalf("created = %+v", created)
	}

	same, changed, err := created.Revise(validPolicy(), catalogueNow.Add(time.Hour))
	if err != nil || changed {
		t.Fatalf("an identical PUT must be a no-op: changed=%v err=%v", changed, err)
	}
	if same != created {
		t.Fatalf("no-op must return the stored entry unchanged: %+v", same)
	}

	next := validPolicy()
	next.TargetStock = 80
	next.Version = "policy-v2"
	revised, changed, err := created.Revise(next, catalogueNow.Add(time.Hour))
	if err != nil || !changed {
		t.Fatalf("revise: changed=%v err=%v", changed, err)
	}
	if revised.Revision != 2 || revised.Policy != next || !revised.UpdatedAt.Equal(catalogueNow.Add(time.Hour)) {
		t.Fatalf("revised = %+v", revised)
	}
}

func TestRebalancePolicyRevisionRefusals(t *testing.T) {
	created, err := NewRebalancePolicy(validPolicy(), catalogueNow)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	moved := validPolicy()
	moved.Site = "WH9"
	if _, _, err := created.Revise(moved, catalogueNow); !errors.Is(err, ErrInvalidCatalogueEntry) {
		t.Fatalf("a revision may not change the entry's key: %v", err)
	}
	bad := validPolicy()
	bad.TargetStock = 1
	if _, _, err := created.Revise(bad, catalogueNow); !errors.Is(err, ErrInvalidCatalogueEntry) {
		t.Fatalf("an invalid revision must be refused: %v", err)
	}
	if _, err := NewRebalancePolicy(bad, catalogueNow); !errors.Is(err, ErrInvalidCatalogueEntry) {
		t.Fatalf("an invalid entry must be refused: %v", err)
	}
}

func TestTransferLaneLifecycle(t *testing.T) {
	created, err := NewTransferLane(validLane(), catalogueNow)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	if created.Revision != 1 || created.Lane != validLane() {
		t.Fatalf("created = %+v", created)
	}
	if _, changed, err := created.Revise(validLane(), catalogueNow); err != nil || changed {
		t.Fatalf("identical lane PUT must be a no-op: changed=%v err=%v", changed, err)
	}
	off := validLane()
	off.Enabled = false
	revised, changed, err := created.Revise(off, catalogueNow.Add(time.Minute))
	if err != nil || !changed || revised.Revision != 2 || revised.Lane.Enabled {
		t.Fatalf("disable lane: %+v changed=%v err=%v", revised, changed, err)
	}
	reversed := validLane()
	reversed.Origin, reversed.Destination = "WH2", "WH1"
	if _, _, err := created.Revise(reversed, catalogueNow); !errors.Is(err, ErrInvalidCatalogueEntry) {
		t.Fatalf("a revision may not change the lane's direction: %v", err)
	}
	bad := validLane()
	bad.UnitHandlingCost = -3
	if _, err := NewTransferLane(bad, catalogueNow); !errors.Is(err, ErrInvalidCatalogueEntry) {
		t.Fatalf("invalid lane: %v", err)
	}
	if _, _, err := created.Revise(bad, catalogueNow); !errors.Is(err, ErrInvalidCatalogueEntry) {
		t.Fatalf("invalid lane revision: %v", err)
	}
}
