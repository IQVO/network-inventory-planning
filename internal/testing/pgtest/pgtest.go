// Package pgtest boots ONE real Postgres through testcontainers per test
// binary and hands every test its own database, cloned from a template that
// already has the migrations applied. It never reads DATABASE_URL and never
// skips: a missing Docker is a test failure.
package pgtest

import (
	"context"
	"fmt"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/claudioed/network-inventory-planning/internal/adapters/outbound/postgres"
)

const (
	user         = "nip_pgtest"
	password     = "nip_pgtest"
	adminDB      = "postgres"
	templateName = "nip_pgtest_template"
)

var (
	once      sync.Once
	container *tcpostgres.PostgresContainer
	errStart  error
	seq       atomic.Int64
)

func start() {
	ctx := context.Background()
	container, errStart = tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase(adminDB),
		tcpostgres.WithUsername(user),
		tcpostgres.WithPassword(password),
		testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(90*time.Second)),
	)
	if errStart != nil {
		return
	}
	errStart = buildTemplate(ctx)
}

// migrationsDir resolves the repo's migrations directory relative to THIS
// FILE (go test's working directory varies by runner).
func migrationsDir() (string, error) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		return "", fmt.Errorf("cannot locate test source file")
	}
	return filepath.Join(filepath.Dir(thisFile), "..", "..", "adapters", "outbound", "postgres", "migrations"), nil
}

// buildTemplate creates the template database and applies the migrations to
// it (the same pass boot runs on every start; running it twice must be a
// no-op, which the migration tooling guarantees).
func buildTemplate(ctx context.Context) error {
	if err := adminExec(ctx, `CREATE DATABASE `+templateName); err != nil {
		return err
	}
	url, err := container.ConnectionString(ctx, "sslmode=disable", "dbname="+templateName)
	if err != nil {
		return err
	}
	dir, err := migrationsDir()
	if err != nil {
		return err
	}
	return postgres.RunMigrations(url, dir)
}

func adminExec(ctx context.Context, sql string) error {
	url, err := container.ConnectionString(ctx, "sslmode=disable", "dbname="+adminDB)
	if err != nil {
		return err
	}
	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(ctx) }()
	_, err = conn.Exec(ctx, sql)
	return err
}

// NewPool starts the shared container on first use and returns a pool over
// a FRESH database cloned from the migrated template: every caller starts
// from a state it owns, isolated from every other test.
func NewPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	once.Do(start)
	if errStart != nil {
		t.Fatalf("start postgres container: %v", errStart)
	}
	ctx := context.Background()
	name := fmt.Sprintf("nip_pgtest_%d", seq.Add(1))
	if err := adminExec(ctx, fmt.Sprintf(`CREATE DATABASE %s TEMPLATE %s`, name, templateName)); err != nil {
		t.Fatalf("clone test database: %v", err)
	}
	url, err := container.ConnectionString(ctx, "sslmode=disable", "dbname="+name)
	if err != nil {
		t.Fatalf("resolve connection string: %v", err)
	}
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatalf("connect pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}
