package main

import (
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	inboundkafka "github.com/claudioed/network-inventory-planning/internal/adapters/inbound/kafka"
	"github.com/claudioed/network-inventory-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/network-inventory-planning/internal/application/ports"
	"github.com/claudioed/network-inventory-planning/internal/application/usecases"
	"github.com/jackc/pgx/v5/pgxpool"
)

// defaultCPTOffset is the release CPT horizon used when a leg's
// TRANSFER_*_CPT_OFFSET is unset: 2h out for the pick, 3h for the
// dispatch (the dispatch happens after the pick, so its CPT sits further
// out to preserve the pick-before-dispatch slack).
const (
	defaultPickCPTOffset   = 2 * time.Hour
	defaultDispatchCPTOffs = 3 * time.Hour
)

// cptOffsetFromEnv resolves one TRANSFER_*_CPT_OFFSET (default d).
func cptOffsetFromEnv(envName string, def time.Duration) (time.Duration, error) {
	raw := os.Getenv(envName)
	if raw == "" {
		return def, nil
	}
	parsed, err := time.ParseDuration(raw)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("%s must be a positive duration, got %q", envName, raw)
	}
	return parsed, nil
}

// workReleaseConfigFromEnv resolves the ADR 0005 release configuration.
// TRANSFER_PICK_PATH_ID deliberately has NO default: unset means the
// approval endpoint answers 503 config-incomplete (fail-closed) rather
// than minting transfers whose pick demand could not be released.
func workReleaseConfigFromEnv(logger *slog.Logger) usecases.WorkReleaseConfig {
	pickOffset, err := cptOffsetFromEnv(envPickCPTOffset, defaultPickCPTOffset)
	if err != nil {
		logger.Warn("invalid TRANSFER_PICK_CPT_OFFSET; using default", "error", err, "default", defaultPickCPTOffset)
		pickOffset = defaultPickCPTOffset
	}
	dispatchOffset, err := cptOffsetFromEnv(envDispatchCPTOffs, defaultDispatchCPTOffs)
	if err != nil {
		logger.Warn("invalid TRANSFER_DISPATCH_CPT_OFFSET; using default", "error", err, "default", defaultDispatchCPTOffs)
		dispatchOffset = defaultDispatchCPTOffs
	}
	return usecases.WorkReleaseConfig{
		PickPathID:        os.Getenv(envPickPathID),
		PickCPTOffset:     pickOffset,
		DispatchPathID:    os.Getenv(envDispatchPathID),
		DispatchCPTOffset: dispatchOffset,
	}
}

// sagaWiring is what wireSagas assembles: the two read/approve use cases
// plus the runner/closer funcs of the consumers and the outbox relay.
type sagaWiring struct {
	simulate *usecases.SimulateTransferOptions
	generate usecases.GenerateTransferProposals
	approve  *usecases.ApproveTransfer
	cancel   *usecases.CancelTransfer
	runs     *usecases.ListRebalanceRuns
	// The operator-managed rebalance catalogues (ADR 0015).
	putPolicy    *usecases.PutRebalancePolicy
	listPolicies *usecases.ListRebalancePolicies
	putLane      *usecases.PutTransferLane
	listLanes    *usecases.ListTransferLanes
	runners      []func() error
	closers      []func() error
}

// wireSagas wires the Phase-1 simulation, the Phase-2 approval saga, the
// transactional outbox relay and every inbound consumer over the pool.
func wireSagas(logger *slog.Logger, pool *pgxpool.Pool, maxStaleness time.Duration) (sagaWiring, error) {
	var out sagaWiring

	uow := postgres.NewUnitOfWork(pool)
	processedEvents := postgres.NewProcessedEventRepo(pool)
	snapshots := postgres.NewSnapshotRepo(pool)
	transfers := postgres.NewTransferRepo(pool)
	// The local copy of product-master's pack levels (ADR 0017): written by
	// the pack-level consumer, read by every planning path that rounds a
	// case SKU to whole cases. No planning path calls product-master.
	packLevels := postgres.NewPackLevelView(pool)
	out.generate = usecases.GenerateTransferProposals{Packs: packLevels}
	out.simulate = &usecases.SimulateTransferOptions{Snapshot: snapshots, MaxStaleness: maxStaleness, Now: time.Now}

	// Transactional outbox: the approval writes TransferPlanApproved +
	// TransferAllocationRequested rows in its own transaction; the relay
	// drains them onto warehouse.network-inventory-planning.events (and,
	// since ADR 0007, the analytics occurrences onto
	// warehouse.network-inventory-planning.analytics — the same relay,
	// the rows carry their own topic).
	outboxPublisher, relayRunner := wireOutbox(logger, pool)
	if relayRunner != nil {
		out.runners = append(out.runners, relayRunner)
	}

	// Work-release configuration (ADR 0005): the pick leg's path has no
	// default, so an unconfigured deployment fails approval closed (503
	// config-incomplete) instead of minting transfers whose pick demand
	// could never be released.
	release := workReleaseConfigFromEnv(logger)

	// The approval endpoint requires the outbox AND a valid pick-leg
	// release config: without either the saga would persist state and
	// never emit the allocation command or the pick demand. Unconfigured
	// means the endpoint answers 503 (fail-closed), exactly like Simulate.
	if outboxPublisher != nil {
		approve := &usecases.ApproveTransfer{
			Transfers:    transfers,
			Events:       outboxPublisher,
			Snapshot:     snapshots,
			UoW:          uow,
			MaxStaleness: maxStaleness,
			Packs:        packLevels,
			Release:      release,
			Now:          time.Now,
		}
		if err := approve.Release.Validate(); err != nil {
			logger.Warn("work release not configured; POST /v1/transfer-approvals answers 503 (no transfer may be approved without a releasable pick leg)",
				"error", err, "enable_with", strings.Join([]string{envPickPathID, envPickCPTOffset}, ","))
		}
		// Non-nil even when unconfigured: the endpoint answers the typed 503
		// with the missing-variable detail.
		out.approve = approve
		// Cancel (ADR 0022) needs the outbox for the ALLOCATED path's
		// TransferAllocationReleaseRequested command — without it a
		// transfer could enter RELEASING with no command ever sent. The
		// pre-release path shares the same use case, so unconfigured
		// means the whole endpoint answers 503 (fail-closed, exactly
		// like approval).
		out.cancel = &usecases.CancelTransfer{
			Transfers: transfers,
			Events:    outboxPublisher,
			UoW:       uow,
			Now:       time.Now,
		}
	} else {
		logger.Warn("KAFKA_BROKERS not configured; POST /v1/transfer-approvals answers 503 (the saga cannot emit its allocation command without the outbox)")
		logger.Warn("KAFKA_BROKERS not configured; POST /v1/transfers/{id}/cancellation answers 503 (an ALLOCATED cancel cannot emit its release command without the outbox)")
	}

	replyApplier, factApplier := wireTransferConsumers(transfers, outboxPublisher, uow, release)

	consumerRunners, consumerClosers, err := startConsumers(logger, consumerDeps{
		uow:             uow,
		processedEvents: processedEvents,
		capabilities:    postgres.NewSiteCapabilityRepo(pool),
		demands:         postgres.NewSiteSkuDemandRepo(pool),
		plans:           postgres.NewPublishedCapacityPlanRepo(pool),
		replyApplier:    replyApplier,
		factApplier:     factApplier,
		packLevels:      usecases.ApplyPackLevel{PackLevels: packLevels},
	})
	if err != nil {
		return out, err
	}
	out.runners = append(out.runners, consumerRunners...)
	out.closers = append(out.closers, consumerClosers...)

	// Phase-4 observability + scheduled runs (ADR 0007). Both tickers are
	// OFF unless their env var is set (the health check defaults to 5m;
	// the rebalance schedule has NO default — a planner pass on a shared
	// fleet must be a deliberate deployment choice).
	rebalanceRuns := postgres.NewRebalanceRunRepo(pool)
	out.runs = &usecases.ListRebalanceRuns{Runs: rebalanceRuns}
	catalogue := wireCatalogue(pool, &out)
	wireHealthTicker(logger, pool, outboxPublisher, &out)
	wireRebalanceTicker(logger, snapshots, packLevels, rebalanceRuns, catalogue, outboxPublisher, maxStaleness, &out)
	return out, nil
}

// wireTransferConsumers builds the reply consumer's use cases (the
// inventory-storage replies incl. the release fact, ADR 0022) and the
// fulfillment-execution fact consumer's, over the same transfers store,
// outbox publisher and UnitOfWork.
func wireTransferConsumers(transfers *postgres.TransferRepo, outboxPublisher ports.TransferEventPublisher, uow ports.UnitOfWork, release usecases.WorkReleaseConfig) (inboundkafka.ReplyUseCases, inboundkafka.FactUseCases) {
	replyAllocate := &usecases.ApplyTransferAllocation{
		Transfers: transfers,
		Events:    outboxPublisher,
		UoW:       uow,
		Release:   release,
		Now:       time.Now,
	}
	replyReject := &usecases.ApplyTransferRejection{Transfers: transfers, Events: outboxPublisher, UoW: uow}
	replyStaged := &usecases.ApplyTransferReceiptStaged{Transfers: transfers, Events: outboxPublisher, UoW: uow}
	replyStow := &usecases.ApplyTransferStow{Transfers: transfers, Events: outboxPublisher, UoW: uow}
	replyReleased := &usecases.ApplyTransferAllocationReleased{Transfers: transfers, Events: outboxPublisher, UoW: uow}
	replyApplier := inboundkafka.ReplyUseCases{Allocate: *replyAllocate, Reject: *replyReject, Staged: *replyStaged, Stow: *replyStow, Released: *replyReleased}

	factPick := &usecases.ApplyTransferPick{Transfers: transfers, Events: outboxPublisher, UoW: uow, Release: release}
	factDispatched := &usecases.ApplyTransferDispatched{Transfers: transfers, Events: outboxPublisher, UoW: uow}
	factArrival := &usecases.ApplyTransferArrival{Transfers: transfers, Events: outboxPublisher, UoW: uow}
	factApplier := inboundkafka.FactUseCases{Pick: *factPick, Dispatched: *factDispatched, Arrival: *factArrival}
	return replyApplier, factApplier
}
