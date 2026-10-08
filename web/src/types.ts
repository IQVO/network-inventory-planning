/**
 * Wire shapes of network-inventory-planning's REST API.
 *
 * Source of truth: the Go handlers' DTOs in
 * internal/adapters/inbound/http/{handler.go,transfers_read.go} -- NOT
 * apis/openapi.yaml, which described GET /v1/transfer-simulations wrongly
 * (ADR 0010, "Correction"). src/test/realShapes.ts pins these shapes and
 * contract.test.ts fails when a fixture, or the Go DTO tags, drift from them.
 */

export const TRANSFER_STATES = [
  "DRAFT",
  "PROPOSED",
  "APPROVED",
  "ALLOCATING",
  "ALLOCATED",
  "PICKED",
  "IN_TRANSIT",
  "ARRIVED",
  "RECEIVED",
  "UNFULFILLABLE",
  "CANCELLED",
] as const;

export type TransferState = (typeof TRANSFER_STATES)[number];

export type RejectionReason = "ORIGIN_SITE_UNKNOWN" | "INSUFFICIENT_USABLE" | "IDEMPOTENCY_CONFLICT";

/** transferDTO: GET /v1/transfers items and the head of GET /v1/transfers/{id}. */
export interface TransferView {
  id: string;
  state: TransferState;
  sku: string;
  quantity: number;
  /** `omitempty` on the wire: absent until something was picked AND when the picked quantity is 0. */
  pickedQuantity?: number;
  originSiteId: string;
  destinationSiteId: string;
  policyVersion: string;
  /** inventory-storage's reservation id, present once ALLOCATED. */
  reservationId?: string;
  /** Present once UNFULFILLABLE. */
  rejectionReason?: RejectionReason;
  expiresAt: string;
  createdAt: string;
  updatedAt: string;
  version: number;
}

/** auditEntryDTO. */
export interface AuditEntry {
  seq: number;
  /** Absent on the creation entry (DRAFT). */
  from?: string;
  to: TransferState;
  event: string;
  /** May be an empty string (an approval without operator reason). */
  cause: string;
  occurredAt: string;
}

/** transferDetailDTO. */
export interface TransferDetail extends TransferView {
  /** Oldest first. */
  audit: AuditEntry[];
}

/** transferListDTO. */
export interface TransferList {
  items: TransferView[];
  total: number;
  limit: number;
  offset: number;
}

export interface TransferQuery {
  state?: string;
  originSiteId?: string;
  destinationSiteId?: string;
  /** The service accepts 1..200 and defaults to 50. */
  limit?: number;
  offset?: number;
}

/**
 * simulateSiteDTO: one participating site's advisory view. The simulation
 * carries NO proposals and NO quantities; it only says how each site's
 * published capacity over the window compares with its in-window demand.
 */
export interface SimulationSite {
  site: string;
  originEnabled: boolean;
  destinationEnabled: boolean;
  totalDemand: number;
  /** A float on the wire (float64). */
  capacityOverWindow: number;
  /** capacityOverWindow minus totalDemand (integer, truncated); negative means the published plan is already short. */
  capacityHeadroom: number;
  windowStart: string;
  windowEnd: string;
}

/** GET /v1/transfer-simulations (the service sorts `sites` by site id). */
export interface SimulationResponse {
  /** Always true: the simulation never reserves, moves or promises stock. */
  advisory: boolean;
  /** Oldest watermark among the facts the simulation used. */
  asOf: string;
  sites: SimulationSite[];
}

/** approveRequest: POST /v1/transfers:approve body (unknown fields are a 400). */
export interface ApproveTransferRequest {
  originSiteId: string;
  destinationSiteId: string;
  sku: string;
  quantity: number;
  policyVersion: string;
  operatorReason?: string;
  proposalAsOf: string;
}

/** approveTransferDTO: the 200 body (a replay of the same key answers 200 too, never 201). */
export interface ApproveTransferResponse {
  transferId: string;
  state: TransferState;
  originSiteId: string;
  destinationSiteId: string;
  sku: string;
  quantity: number;
  policyVersion: string;
  reservationId?: string;
  rejectionReason?: RejectionReason;
  /** True when the Idempotency-Key already existed (the original transfer is returned). */
  replayed: boolean;
  expiresAt: string;
  transferLineId: string;
}

/** rebalanceRunDTO. */
export interface RebalanceRun {
  id: number;
  startedAt: string;
  /** Absent on a FAILED run that never reached a watermark. */
  snapshotAsOf?: string;
  proposalCount: number;
  rejectedCount: number;
  outcome: "COMPLETED" | "FAILED";
  /** Present only when outcome is FAILED. */
  failClosedReason?: string;
}

export interface RebalanceRunsResponse {
  runs: RebalanceRun[];
}
