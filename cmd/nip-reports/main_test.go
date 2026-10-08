package main

import (
	"context"
	"errors"
	"testing"
)

func env(m map[string]string) func(string) string { return func(k string) string { return m[k] } }

func TestLoadConfig_ReaderDSNWinsAndTheProjectorDSNIsTheFallback(t *testing.T) {
	c, err := loadConfig(env(map[string]string{"ANALYTICS_READER_DATABASE_URL": "postgres://ro@h/db", "ANALYTICS_DATABASE_URL": "postgres://rw@h/db"}))
	if err != nil || c.analyticsURL != "postgres://ro@h/db" {
		t.Fatalf("config = %+v, %v; want the reader DSN", c, err)
	}
	c, err = loadConfig(env(map[string]string{"ANALYTICS_DATABASE_URL": "postgres://rw@h/db"}))
	if err != nil || c.analyticsURL != "postgres://rw@h/db" {
		t.Fatalf("config = %+v, %v; want the fallback DSN", c, err)
	}
}

func TestLoadConfig_Defaults(t *testing.T) {
	c, err := loadConfig(env(map[string]string{"ANALYTICS_DATABASE_URL": "postgres://u@h/db"}))
	if err != nil {
		t.Fatal(err)
	}
	if c.httpAddr != ":8092" || c.logLevel != "info" {
		t.Fatalf("config = %+v", c)
	}
	c, err = loadConfig(env(map[string]string{"ANALYTICS_DATABASE_URL": "postgres://u@h/db", "HTTP_ADDR": ":1", "LOG_LEVEL": "debug"}))
	if err != nil || c.httpAddr != ":1" || c.logLevel != "debug" {
		t.Fatalf("config = %+v, %v", c, err)
	}
}

func TestLoadConfig_RequiresADatabase(t *testing.T) {
	if _, err := loadConfig(env(nil)); !errors.Is(err, errMissingAnalyticsURL) {
		t.Fatalf("err = %v", err)
	}
}

func TestRun_FailsFastWithoutTheAnalyticalDatabase(t *testing.T) {
	t.Setenv("ANALYTICS_READER_DATABASE_URL", "")
	t.Setenv("ANALYTICS_DATABASE_URL", "")
	if err := run(); !errors.Is(err, errMissingAnalyticsURL) {
		t.Fatalf("run = %v", err)
	}
}

func TestNewLogger_LevelMapping(t *testing.T) {
	for level, enabledDebug := range map[string]bool{"debug": true, "info": false, "": false} {
		if got := newLogger(level).Enabled(context.Background(), -4); got != enabledDebug {
			t.Errorf("level %q: debug enabled = %v, want %v", level, got, enabledDebug)
		}
	}
	if newLogger("warn").Enabled(context.Background(), 0) || !newLogger("warning").Enabled(context.Background(), 4) || newLogger("error").Enabled(context.Background(), 4) {
		t.Error("warn/error level mapping is wrong")
	}
}
