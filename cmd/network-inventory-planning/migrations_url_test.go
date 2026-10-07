package main

import "testing"

func TestMigrationsURL(t *testing.T) {
	const runtimeDSN = "postgres://u@pgbouncer:6432/db"
	const directDSN = "postgres://u@postgres:5432/db"

	t.Run("falls back to the runtime DSN when unset", func(t *testing.T) {
		t.Setenv(envMigrationsDBURL, "")
		if got := migrationsURL(runtimeDSN); got != runtimeDSN {
			t.Fatalf("got %q, want %q", got, runtimeDSN)
		}
	})

	t.Run("prefers the direct DSN when set", func(t *testing.T) {
		t.Setenv(envMigrationsDBURL, directDSN)
		if got := migrationsURL(runtimeDSN); got != directDSN {
			t.Fatalf("got %q, want %q", got, directDSN)
		}
	})
}
