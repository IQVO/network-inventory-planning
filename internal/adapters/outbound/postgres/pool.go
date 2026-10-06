// Package postgres provides pgxpool-backed implementations of the
// outbound ports, plus a golang-migrate runner for the SQL migrations in
// this package's migrations/ directory.
package postgres

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// MaxConns is this adapter's per-process connection ceiling. Phase 1 has a
// single read/write path (the planning read models); kept modest until a
// real production composition root and its own connection-budget
// accounting exist (see inventory-storage's pgxpool-tuning ADR for the
// fleet's general approach).
const MaxConns = 10

// StatementTimeout bounds how long a single query may hold a connection.
const StatementTimeout = "5s"

// NewPool opens a connection pool against databaseURL with MaxConns and
// StatementTimeout applied to every connection.
func NewPool(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, err
	}

	config.MaxConns = MaxConns
	config.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		_, err := conn.Exec(ctx, "SET statement_timeout = '"+StatementTimeout+"'")
		return err
	}

	return pgxpool.NewWithConfig(ctx, config)
}
