package main

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	"github.com/claudioed/network-inventory-planning/internal/adapters/outbound/postgres"
)

// TestOutboxMaxAttemptsFromEnv pins OUTBOX_MAX_ATTEMPTS (gap fix D12, ADR
// 0016): unset means the fleet default of 20, 0 means unlimited, and an
// invalid or negative value falls back to the default with a warning instead
// of silently disabling the cap.
func TestOutboxMaxAttemptsFromEnv(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want int
		warn bool
	}{
		{name: "unset uses the default", raw: "", want: 20},
		{name: "explicit value", raw: "5", want: 5},
		{name: "zero means unlimited", raw: "0", want: 0},
		{name: "negative falls back", raw: "-1", want: 20, warn: true},
		{name: "garbage falls back", raw: "many", want: 20, warn: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(envOutboxMaxAttempts, tt.raw)
			var buf bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&buf, nil))
			if got := outboxMaxAttemptsFromEnv(logger); got != tt.want {
				t.Fatalf("outboxMaxAttemptsFromEnv(%q) = %d, want %d", tt.raw, got, tt.want)
			}
			if warned := strings.Contains(buf.String(), "level=WARN"); warned != tt.warn {
				t.Fatalf("warned = %v, want %v (log: %s)", warned, tt.warn, buf.String())
			}
		})
	}
}

// TestDefaultOutboxMaxAttemptsIsTwenty keeps the fleet default in one place:
// the postgres package constant the composition root falls back to.
func TestDefaultOutboxMaxAttemptsIsTwenty(t *testing.T) {
	if postgres.DefaultOutboxMaxAttempts != 20 {
		t.Fatalf("DefaultOutboxMaxAttempts = %d, want 20 (fleet convention, ADR 0016)", postgres.DefaultOutboxMaxAttempts)
	}
	if envOutboxMaxAttempts != "OUTBOX_MAX_ATTEMPTS" {
		t.Fatalf("env var = %q, want OUTBOX_MAX_ATTEMPTS (fleet convention)", envOutboxMaxAttempts)
	}
}
