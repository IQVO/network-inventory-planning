package main

import (
	"testing"
	"time"
)

func TestOptionalIntervalFromEnv(t *testing.T) {
	t.Run("unset means the default", func(t *testing.T) {
		t.Setenv(envHealthInterval, "")
		d, ok, err := optionalIntervalFromEnv(envHealthInterval, 5*time.Minute)
		if err != nil || !ok || d != 5*time.Minute {
			t.Fatalf("got %v %v %v, want 5m true nil", d, ok, err)
		}
	})

	t.Run("zero disables", func(t *testing.T) {
		t.Setenv(envHealthInterval, "0")
		d, ok, err := optionalIntervalFromEnv(envHealthInterval, 5*time.Minute)
		if err != nil || ok || d != 0 {
			t.Fatalf("got %v %v %v, want 0 false nil (disabled)", d, ok, err)
		}
	})

	t.Run("0s also disables", func(t *testing.T) {
		t.Setenv(envHealthInterval, "0s")
		_, ok, err := optionalIntervalFromEnv(envHealthInterval, 5*time.Minute)
		if err != nil || ok {
			t.Fatalf("got %v %v, want false nil", ok, err)
		}
	})

	t.Run("parses a duration", func(t *testing.T) {
		t.Setenv(envHealthInterval, "90s")
		d, ok, err := optionalIntervalFromEnv(envHealthInterval, 5*time.Minute)
		if err != nil || !ok || d != 90*time.Second {
			t.Fatalf("got %v %v %v, want 90s true nil", d, ok, err)
		}
	})

	t.Run("garbage is an error (never a silently changed cadence)", func(t *testing.T) {
		t.Setenv(envHealthInterval, "every-five-minutes")
		if _, _, err := optionalIntervalFromEnv(envHealthInterval, 5*time.Minute); err == nil {
			t.Fatal("want an error for an unparsable interval")
		}
	})

	t.Run("negative is an error", func(t *testing.T) {
		t.Setenv(envRebalSchedule, "-5m")
		if _, _, err := optionalIntervalFromEnv(envRebalSchedule, 0); err == nil {
			t.Fatal("want an error for a negative interval")
		}
	})
}

func TestRebalanceScheduleDefaultIsOff(t *testing.T) {
	// NIP_REBALANCE_SCHEDULE has NO default: unset means the loop must
	// not run (a planner pass on a shared fleet is a deliberate
	// deployment choice).
	t.Setenv(envRebalSchedule, "")
	_, ok, err := optionalIntervalFromEnv(envRebalSchedule, 0)
	if err != nil {
		t.Fatalf("unset schedule: %v", err)
	}
	if ok {
		t.Fatal("unset NIP_REBALANCE_SCHEDULE must disable the loop (default off)")
	}
}
