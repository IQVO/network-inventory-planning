package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/claudioed/network-inventory-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/network-inventory-planning/internal/application/ports"
	"github.com/claudioed/network-inventory-planning/internal/application/usecases"
	"github.com/claudioed/network-inventory-planning/internal/domain/transfer"
	"github.com/jackc/pgx/v5/pgxpool"
)

// optionalIntervalFromEnv parses an interval env var. Unset means def;
// "0" means DISABLED (ok=false); anything unparsable or negative is a
// boot failure (a typo'd ticker interval would silently change cadence).
// A def of 0 with the variable unset ALSO disables (the rebalance
// schedule's default-off contract).
func optionalIntervalFromEnv(name string, def time.Duration) (d time.Duration, ok bool, err error) {
	raw := os.Getenv(name)
	if raw == "" {
		if def <= 0 {
			return 0, false, nil
		}
		return def, true, nil
	}
	parsed, perr := time.ParseDuration(raw)
	if perr != nil || parsed < 0 {
		return 0, false, fmt.Errorf("%s must be a non-negative duration, got %q", name, raw)
	}
	if parsed == 0 {
		return 0, false, nil
	}
	return parsed, true, nil
}

// wireHealthTicker starts the bounded saga-health check loop (ADR 0007):
// every NIP_HEALTH_CHECK_INTERVAL (default 5m, 0=off) it reads the
// non-terminal transfers and publishes TransferStuckDetected occurrences
// for those past their per-state threshold (NIP_STUCK_THRESHOLDS). The
// loop ONLY reads and publishes — it never mutates saga state.
func wireHealthTicker(logger *slog.Logger, pool *pgxpool.Pool, events ports.TransferEventPublisher, out *sagaWiring) {
	interval, ok, err := optionalIntervalFromEnv(envHealthInterval, defaultHealthInterval)
	if err != nil {
		logger.Error("invalid health check interval; check disabled", "error", err)
		return
	}
	if !ok {
		logger.Info("saga health check disabled", "env", envHealthInterval)
		return
	}
	if events == nil {
		logger.Warn("saga health check disabled: no outbox publisher configured (occurrences could never be drained)",
			"enable_with", strings.Join([]string{envKafkaBrokers, envOutboxRelay}, ","))
		return
	}
	thresholds, err := transfer.ParseStuckThresholds(os.Getenv(envStuckThresholds))
	if err != nil {
		logger.Error("invalid stuck thresholds; check disabled", "error", err, "env", envStuckThresholds)
		return
	}
	check := usecases.CheckStuckTransfers{
		Reader: postgres.NewTransferRepo(pool),
		Events: events,
		Check:  transfer.StuckCheck{Thresholds: thresholds},
		Limit:  500,
		Now:    time.Now,
	}
	runner := usecases.PeriodicRunner{
		Name:     "saga health check",
		Interval: interval,
		Tick: func(ctx context.Context) error {
			_, err := check.Execute(ctx)
			return err
		},
		Logger: logger,
	}
	hcCtx, hcStop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	out.runners = append(out.runners, func() error { defer hcStop(); return runner.Run(hcCtx) })
	logger.Info("saga health check running",
		"interval", interval.String(), "thresholds_env", envStuckThresholds)
}

// rebalanceCatalogue groups the two operator-managed catalogues the
// scheduled pass plans from (ADR 0015).
type rebalanceCatalogue struct {
	policies ports.RebalancePolicyRepository
	lanes    ports.TransferLaneRepository
}

// wireCatalogue builds the Postgres-backed Rebalance Policy and Transfer
// Lane catalogues and their REST use cases (ADR 0015).
func wireCatalogue(pool *pgxpool.Pool, out *sagaWiring) rebalanceCatalogue {
	catalogue := rebalanceCatalogue{
		policies: postgres.NewRebalancePolicyRepo(pool),
		lanes:    postgres.NewTransferLaneRepo(pool),
	}
	out.putPolicy = &usecases.PutRebalancePolicy{Policies: catalogue.policies, Now: time.Now}
	out.listPolicies = &usecases.ListRebalancePolicies{Policies: catalogue.policies}
	out.putLane = &usecases.PutTransferLane{Lanes: catalogue.lanes, Now: time.Now}
	out.listLanes = &usecases.ListTransferLanes{Lanes: catalogue.lanes}
	return catalogue
}

// wireRebalanceTicker starts the scheduled rebalance loop (ADR 0007):
// every NIP_REBALANCE_SCHEDULE (NO default — unset means off) it runs the
// SAME fail-closed snapshot build as the simulation, plans from the
// Rebalance Policy and Transfer Lane catalogues (ADR 0015), persists a
// rebalance_runs row and publishes a RebalanceRunCompleted occurrence.
// Observe-only: no approval, no allocation command — ever.
func wireRebalanceTicker(
	logger *slog.Logger,
	snapshots ports.PlanningSnapshotRepository,
	packLevels ports.PackLevelView,
	runs ports.RebalanceRunRepository,
	catalogue rebalanceCatalogue,
	events ports.TransferEventPublisher,
	maxStaleness time.Duration,
	out *sagaWiring,
) {
	interval, ok, err := optionalIntervalFromEnv(envRebalSchedule, 0)
	if err != nil {
		logger.Error("invalid rebalance schedule; scheduled runs disabled", "error", err)
		return
	}
	if !ok {
		logger.Info("scheduled rebalance disabled (no default; deliberate deployment choice)",
			"enable_with", envRebalSchedule)
		return
	}
	if events == nil {
		logger.Warn("scheduled rebalance disabled: no outbox publisher configured",
			"enable_with", strings.Join([]string{envKafkaBrokers, envOutboxRelay, envRebalSchedule}, ","))
		return
	}
	run := usecases.RunScheduledRebalance{
		Snapshot:     snapshots,
		Planner:      transfer.Planner{},
		Packs:        packLevels,
		Policies:     catalogue.policies,
		Lanes:        catalogue.lanes,
		Runs:         runs,
		Events:       events,
		MaxStaleness: maxStaleness,
		Now:          time.Now,
	}
	runner := usecases.PeriodicRunner{
		Name:     "scheduled rebalance",
		Interval: interval,
		Tick: func(ctx context.Context) error {
			_, err := run.Execute(ctx)
			return err
		},
		Logger: logger,
	}
	rbCtx, rbStop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	out.runners = append(out.runners, func() error { defer rbStop(); return runner.Run(rbCtx) })
	logger.Info("scheduled rebalance running", "interval", interval.String())
}
