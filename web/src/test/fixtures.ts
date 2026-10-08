import type {
  ApproveTransferResponse,
  AuditEntry,
  RebalanceRun,
  SimulationResponse,
  SimulationSite,
  TransferDetail,
  TransferList,
  TransferView,
} from "../types";

/**
 * Fixtures shaped like the REAL responses of the Go handlers
 * (internal/adapters/inbound/http). contract.test.ts fails when any of them
 * drifts from src/test/realShapes.ts.
 */

export function transfer(over: Partial<TransferView> = {}): TransferView {
  return {
    id: "t-1001",
    state: "ALLOCATING",
    sku: "SKU-RED-42",
    quantity: 120,
    originSiteId: "DC-EAST",
    destinationSiteId: "DC-WEST",
    policyVersion: "v7",
    expiresAt: "2026-10-08T10:00:00Z",
    createdAt: "2026-10-07T10:00:00Z",
    updatedAt: "2026-10-07T10:05:00Z",
    version: 3,
    ...over,
  };
}

export const RECEIVED = transfer({
  id: "t-0999",
  state: "RECEIVED",
  quantity: 40,
  pickedQuantity: 38,
  reservationId: "res-77",
  sku: "SKU-BLUE-7",
  originSiteId: "DC-NORTH",
  destinationSiteId: "DC-EAST",
  updatedAt: "2026-10-06T09:00:00Z",
  createdAt: "2026-10-05T09:00:00Z",
  version: 7,
});

export const UNFULFILLABLE = transfer({
  id: "t-0998",
  state: "UNFULFILLABLE",
  rejectionReason: "INSUFFICIENT_USABLE",
  version: 4,
});

export function transferList(items: TransferView[], over: Partial<TransferList> = {}): TransferList {
  return { items, total: items.length, limit: 25, offset: 0, ...over };
}

/** The real creation trail of an approval: DRAFT -> PROPOSED -> APPROVED -> ALLOCATING (saga.go). */
export const AUDIT: AuditEntry[] = [
  { seq: 1, to: "DRAFT", event: "TransferDrafted", cause: "drafted from advisory proposal snapshot", occurredAt: "2026-10-07T10:00:00Z" },
  { seq: 2, from: "DRAFT", to: "PROPOSED", event: "TransferProposed", cause: "rebalance east to west", occurredAt: "2026-10-07T10:00:00Z" },
  { seq: 3, from: "PROPOSED", to: "APPROVED", event: "TransferPlanApproved", cause: "rebalance east to west", occurredAt: "2026-10-07T10:00:01Z" },
  { seq: 4, from: "APPROVED", to: "ALLOCATING", event: "TransferAllocationRequested", cause: "origin allocation requested", occurredAt: "2026-10-07T10:00:02Z" },
];

export function detail(over: Partial<TransferDetail> = {}): TransferDetail {
  return { ...transfer(), audit: AUDIT, ...over };
}

export function simSite(over: Partial<SimulationSite> = {}): SimulationSite {
  return {
    site: "DC-EAST",
    originEnabled: true,
    destinationEnabled: true,
    totalDemand: 400,
    capacityOverWindow: 520.5,
    capacityHeadroom: 120,
    windowStart: "2026-10-07T00:00:00Z",
    windowEnd: "2026-10-14T00:00:00Z",
    ...over,
  };
}

/** In the service's own order (sorted by site id); the screen re-sorts short sites first. */
export const SITES: SimulationSite[] = [
  simSite(),
  simSite({ site: "DC-NORTH", totalDemand: 270, capacityOverWindow: 300, capacityHeadroom: 30, destinationEnabled: false }),
  simSite({ site: "DC-SOUTH", totalDemand: 90, capacityOverWindow: 50, capacityHeadroom: -40, originEnabled: false }),
  simSite({ site: "DC-WEST", totalDemand: 500, capacityOverWindow: 350, capacityHeadroom: -150, originEnabled: false }),
];

export function simulation(over: Partial<SimulationResponse> = {}): SimulationResponse {
  return { advisory: true, asOf: "2026-10-07T09:50:00Z", sites: SITES, ...over };
}

export function approved(over: Partial<ApproveTransferResponse> = {}): ApproveTransferResponse {
  return {
    transferId: "t-2001",
    state: "ALLOCATING",
    originSiteId: "DC-EAST",
    destinationSiteId: "DC-WEST",
    sku: "SKU-RED-42",
    quantity: 120,
    policyVersion: "v7",
    replayed: false,
    expiresAt: "2026-10-08T10:00:00Z",
    transferLineId: "t-2001:1",
    ...over,
  };
}

export const RUNS: RebalanceRun[] = [
  { id: 12, startedAt: "2026-10-07T06:00:00Z", snapshotAsOf: "2026-10-07T05:59:00Z", proposalCount: 4, rejectedCount: 1, outcome: "COMPLETED" },
  { id: 11, startedAt: "2026-10-07T05:00:00Z", proposalCount: 0, rejectedCount: 0, outcome: "FAILED", failClosedReason: "capacity facts are stale" },
];
