package memory

import (
	"context"
	"sync"

	"github.com/claudioed/network-inventory-planning/internal/domain/packlevel"
)

type packLevelKey struct {
	sku   string
	level packlevel.Level
}

// PackLevelView is the in-memory ports.PackLevelView, for unit tests only:
// the runtime wiring is Postgres (cmd/network-inventory-planning).
type PackLevelView struct {
	mu   sync.Mutex
	rows map[packLevelKey]packlevel.PackLevel
}

// NewPackLevelView constructs an empty PackLevelView.
func NewPackLevelView() *PackLevelView {
	return &PackLevelView{rows: map[packLevelKey]packlevel.PackLevel{}}
}

// Get implements ports.PackLevelView.
func (v *PackLevelView) Get(_ context.Context, sku string, level packlevel.Level) (packlevel.PackLevel, bool, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	l, ok := v.rows[packLevelKey{sku, level}]
	return l, ok, nil
}

// Save implements ports.PackLevelView.
func (v *PackLevelView) Save(_ context.Context, l packlevel.PackLevel) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.rows[packLevelKey{l.SKU, l.Level}] = l
	return nil
}
