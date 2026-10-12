package main

import (
	"log/slog"
	"testing"
)

// TestLogLevelFromEnv: the API binary reads LOG_LEVEL like the fleet's
// other binaries (mcp, nip-projector, nip-reports): `warning` is accepted
// as an alias of `warn`, case-insensitively; unknown or empty means info.
func TestLogLevelFromEnv(t *testing.T) {
	for raw, want := range map[string]slog.Level{
		"debug":   slog.LevelDebug,
		"info":    slog.LevelInfo,
		"warn":    slog.LevelWarn,
		"warning": slog.LevelWarn,
		"WARNING": slog.LevelWarn,
		"Warn":    slog.LevelWarn,
		"error":   slog.LevelError,
		"":        slog.LevelInfo,
		"bogus":   slog.LevelInfo,
	} {
		if got := logLevel(raw); got != want {
			t.Errorf("logLevel(%q) = %v, want %v", raw, got, want)
		}
	}
}
