import type { AuditEntry, Proposal, RebalanceRun, TransferDetail, TransferList, TransferView } from "../types";

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

export const AUDIT: AuditEntry[] = [
  { seq: 1, to: "PROPOSED", event: "TransferProposed", cause: "operator approval requested", occurredAt: "2026-10-07T10:00:00Z" },
  { seq: 2, from: "PROPOSED", to: "APPROVED", event: "TransferPlanApproved", cause: "rebalance east to west", occurredAt: "2026-10-07T10:00:01Z" },
  { seq: 3, from: "APPROVED", to: "ALLOCATING", event: "TransferAllocationRequested", cause: "awaiting inventory-storage", occurredAt: "2026-10-07T10:00:02Z" },
];

export function detail(over: Partial<TransferDetail> = {}): TransferDetail {
  return { ...transfer(), audit: AUDIT, ...over };
}

export function option(over: Partial<Proposal> = {}): Proposal {
  return {
    Origin: "DC-EAST",
    Destination: "DC-WEST",
    SKU: "SKU-RED-42",
    Quantity: 120,
    PolicyVersion: "v7",
    PositionAsOf: "2026-10-07T09:55:00Z",
    Reasons: ["DESTINATION_BELOW_TARGET", "APPROVED_LANE"],
    ScoreBreakdown: { PriorityBenefit: 100, HandlingPenalty: 10, LeadTimePenalty: 5 },
    ...over,
  };
}

export const OPTIONS: Proposal[] = [
  option(),
  option({ Origin: "DC-NORTH", Destination: "DC-WEST", SKU: "SKU-BLUE-7", Quantity: 30, PolicyVersion: "v3" }),
];

export const RUNS: RebalanceRun[] = [
  { id: 12, startedAt: "2026-10-07T06:00:00Z", snapshotAsOf: "2026-10-07T05:59:00Z", proposalCount: 4, rejectedCount: 1, outcome: "COMPLETED" },
  { id: 11, startedAt: "2026-10-07T05:00:00Z", proposalCount: 0, rejectedCount: 0, outcome: "FAILED", failClosedReason: "capacity facts are stale" },
];
