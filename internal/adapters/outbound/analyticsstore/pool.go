// Package analyticsstore holds the Postgres adapters of the analytics read
// side (ADR 0009) over the SEPARATE analytical database: Projection (the
// writer used by cmd/nip-projector), Reader (the read-only reports queries
// used by cmd/nip-reports), and an in-memory twin of both for tests. No OLTP
// package may import this one (arch-test).
package analyticsstore

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	// MaxConns is the projector's connection ceiling: one consumer loop
	// issuing single-row inserts needs very few.
	MaxConns = 5
	// ReportsMaxConns is the reports process's ceiling (stateless reads,
	// horizontally scalable by the chart).
	ReportsMaxConns = 5
	// StatementTimeout bounds one projector statement.
	StatementTimeout = "10s"
	// ReportsStatementTimeout bounds one report query: aggregates over a
	// caller-chosen range (at most 366 days) get more headroom than a
	// single-row insert, but never run unbounded.
	ReportsStatementTimeout = "15s"
)

// NewPool opens the WRITER pool over the analytical database.
func NewPool(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	return newPool(ctx, databaseURL, MaxConns, StatementTimeout, false)
}

// NewReadOnlyPool opens the READER pool: every connection defaults to
// read-only transactions, so a bug in cmd/nip-reports cannot mutate the
// model even if its database role could (defence in depth on top of the
// role).
func NewReadOnlyPool(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	return newPool(ctx, databaseURL, ReportsMaxConns, ReportsStatementTimeout, true)
}

func newPool(ctx context.Context, databaseURL string, maxConns int32, statementTimeout string, readOnly bool) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = maxConns
	if readOnly {
		if cfg.ConnConfig.RuntimeParams == nil {
			cfg.ConnConfig.RuntimeParams = map[string]string{}
		}
		cfg.ConnConfig.RuntimeParams["default_transaction_read_only"] = "on"
	}
	cfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		_, err := conn.Exec(ctx, fmt.Sprintf("SET statement_timeout = '%s'", statementTimeout))
		return err
	}
	return pgxpool.NewWithConfig(ctx, cfg)
}
