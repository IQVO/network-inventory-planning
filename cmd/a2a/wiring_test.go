package main

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"
)

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// TestBuildDeps_UsesMigrationsDatabaseURLNotDatabaseURLForMigrations proves
// buildDeps threads migrationsDatabaseURL into the migration step and
// databaseURL into the pgx pool, never conflating them (ADR 0006: migrations
// over a direct connection). databaseURL points at an address nothing listens
// on (the pool, opened AFTER migrations succeed, would fail with a dial error
// if reached) while migrationsDatabaseURL is a schemeless string migrate.New
// rejects immediately with a distinctive parse error: a dial/"connection
// refused" error here would mean migrations ran against databaseURL.
func TestBuildDeps_UsesMigrationsDatabaseURLNotDatabaseURLForMigrations(t *testing.T) {
	const (
		bogusMigrationsURL = "not-a-valid-connection-string"
		unreachableAppURL  = "postgres://u:p@127.0.0.1:1/db?sslmode=disable&connect_timeout=1"
	)
	_, closeFn, err := buildDeps(context.Background(), quietLogger(), unreachableAppURL, bogusMigrationsURL, "../../internal/adapters/outbound/postgres/migrations")
	closeFn()
	if err == nil {
		t.Fatal("a malformed MIGRATIONS_DATABASE_URL must fail boot")
	}
	if !strings.Contains(err.Error(), "run migrations") || !strings.Contains(err.Error(), "parse scheme") {
		t.Fatalf("err = %v -- want the bogus-URL parse error from the migration step, not a dial error from the pool", err)
	}
}

func TestMigrationsDatabaseURLFallback(t *testing.T) {
	const databaseURL = "postgres://u:p@pgbouncer.example:6432/db"
	t.Setenv(envMigrationsDBURL, "")
	if got := getenv(envMigrationsDBURL, databaseURL); got != databaseURL {
		t.Fatalf("fallback = %q, want DATABASE_URL", got)
	}
	t.Setenv(envMigrationsDBURL, "postgres://u:p@postgres.example:5432/db")
	if got := getenv(envMigrationsDBURL, databaseURL); got != "postgres://u:p@postgres.example:5432/db" {
		t.Fatalf("MIGRATIONS_DATABASE_URL must win, got %q", got)
	}
}

func TestGetenv(t *testing.T) {
	t.Setenv("NIP_A2A_TEST_SET", "v")
	t.Setenv("NIP_A2A_TEST_EMPTY", "")
	if got := getenv("NIP_A2A_TEST_SET", "d"); got != "v" {
		t.Errorf("set: %q", got)
	}
	if got := getenv("NIP_A2A_TEST_EMPTY", "d"); got != "d" {
		t.Errorf("empty: %q", got)
	}
}

func TestNewLoggerLevels(t *testing.T) {
	for level, want := range map[string]slog.Level{"debug": slog.LevelDebug, "WARN": slog.LevelWarn, "warning": slog.LevelWarn, "error": slog.LevelError, "": slog.LevelInfo, "bogus": slog.LevelInfo} {
		if got := newLogger(level); !got.Handler().Enabled(context.Background(), want) || (want > slog.LevelDebug && got.Handler().Enabled(context.Background(), want-1)) {
			t.Errorf("newLogger(%q): level %v not the minimum enabled", level, want)
		}
	}
}

func TestDefaultListenAddressIsTheA2APort(t *testing.T) {
	if defaultAddr != ":8093" {
		t.Fatalf("defaultAddr = %q, want :8093 (8090 mcp, 8091 projector admin, 8092 reports)", defaultAddr)
	}
}
