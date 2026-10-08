package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync/atomic"
	"testing"
)

func env(m map[string]string) func(string) string { return func(k string) string { return m[k] } }

func TestLoadConfig_Defaults(t *testing.T) {
	c, err := loadConfig(env(map[string]string{"ANALYTICS_DATABASE_URL": "postgres://u@h/db", "KAFKA_BROKERS": "k1:9092, k2:9092,,"}))
	if err != nil {
		t.Fatal(err)
	}
	want := config{
		analyticsURL: "postgres://u@h/db", brokers: []string{"k1:9092", "k2:9092"},
		group: "network-inventory-planning-analytics", migrationsPath: "analytics/migrations", adminAddr: ":8091", logLevel: "info",
	}
	if !reflect.DeepEqual(c, want) {
		t.Fatalf("config = %+v, want %+v", c, want)
	}
}

func TestLoadConfig_EnvironmentOverridesEverything(t *testing.T) {
	c, err := loadConfig(env(map[string]string{
		"ANALYTICS_DATABASE_URL": "postgres://u@h/db", "KAFKA_BROKERS": "kafka:9092",
		"ANALYTICS_CONSUMER_GROUP": "g-from-env", "ANALYTICS_MIGRATIONS_PATH": "/app/analytics/migrations",
		"ADMIN_ADDR": ":9999", "LOG_LEVEL": "debug",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if c.group != "g-from-env" || c.migrationsPath != "/app/analytics/migrations" || c.adminAddr != ":9999" || c.logLevel != "debug" {
		t.Fatalf("config = %+v", c)
	}
}

func TestLoadConfig_RequiresTheAnalyticalDatabaseAndKafka(t *testing.T) {
	if _, err := loadConfig(env(map[string]string{"KAFKA_BROKERS": "k:9092"})); !errors.Is(err, errMissingAnalyticsURL) {
		t.Fatalf("err = %v", err)
	}
	if _, err := loadConfig(env(map[string]string{"ANALYTICS_DATABASE_URL": "postgres://u@h/db"})); !errors.Is(err, errMissingBrokers) {
		t.Fatalf("err = %v", err)
	}
	if _, err := loadConfig(env(map[string]string{"ANALYTICS_DATABASE_URL": "postgres://u@h/db", "KAFKA_BROKERS": " , "})); !errors.Is(err, errMissingBrokers) {
		t.Fatalf("blank brokers: err = %v", err)
	}
}

func TestRun_FailsFastWithoutTheAnalyticalDatabase(t *testing.T) {
	t.Setenv("ANALYTICS_DATABASE_URL", "")
	t.Setenv("KAFKA_BROKERS", "k:9092")
	if err := run(); !errors.Is(err, errMissingAnalyticsURL) {
		t.Fatalf("run = %v", err)
	}
}

func TestAdminMux_HealthzIsLivenessAndReadyzFlipsOnShutdown(t *testing.T) {
	var notReady atomic.Bool
	mux := newAdminMux(&notReady)
	get := func(path string) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		return rec
	}
	if rec := get("/healthz"); rec.Code != 200 || rec.Body.String() != `{"status":"ok"}` {
		t.Fatalf("healthz = %d %s", rec.Code, rec.Body)
	}
	if rec := get("/readyz"); rec.Code != 200 || rec.Body.String() != `{"status":"ready"}` {
		t.Fatalf("readyz = %d %s", rec.Code, rec.Body)
	}
	notReady.Store(true)
	if rec := get("/readyz"); rec.Code != 503 || rec.Body.String() != `{"status":"not_ready"}` {
		t.Fatalf("readyz during shutdown = %d %s", rec.Code, rec.Body)
	}
	if rec := get("/healthz"); rec.Code != 200 {
		t.Fatalf("healthz must stay up during shutdown, got %d", rec.Code)
	}
}

func TestNewLogger_LevelMapping(t *testing.T) {
	for level, enabledDebug := range map[string]bool{"debug": true, "info": false, "": false, "nonsense": false} {
		if got := newLogger(level).Enabled(context.Background(), -4); got != enabledDebug {
			t.Errorf("level %q: debug enabled = %v, want %v", level, got, enabledDebug)
		}
	}
	if newLogger("warn").Enabled(context.Background(), 0) || !newLogger("warning").Enabled(context.Background(), 4) || newLogger("error").Enabled(context.Background(), 4) {
		t.Error("warn/error level mapping is wrong")
	}
}
