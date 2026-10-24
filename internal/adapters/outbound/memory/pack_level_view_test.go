package memory

import (
	"testing"

	"github.com/claudioed/network-inventory-planning/internal/application/ports"
	"github.com/claudioed/network-inventory-planning/internal/testing/viewcontract"
)

func TestPackLevelViewContract(t *testing.T) {
	viewcontract.PackLevelView(t, func(*testing.T) ports.PackLevelView { return NewPackLevelView() })
}
