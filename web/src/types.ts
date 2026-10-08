/** Wire shapes of network-inventory-planning's REST API (apis/openapi.yaml). */

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

/** GET /v1/transfers items and the head of GET /v1/transfers/{id}. */
export interface TransferView {
  id: string;
  state: TransferState;
  sku: string;
  quantity: number;
  /** Present from PICKED onwards; may be short of `quantity`. */
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

export interface AuditEntry {
  seq: number;
  /** Absent on the creation entry. */
  from?: string;
  to: TransferState;
  event: string;
  cause: string;
  occurredAt: string;
}

export interface TransferDetail extends TransferView {
  /** Oldest first. */
  audit: AuditEntry[];
}

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
  limit?: number;
  offset?: number;
}

/** One advisory option of GET /v1/transfer-simulations (Go-cased wire fields). */
export interface Proposal {
  Origin: string;
  Destination: string;
  SKU: string;
  Quantity: number;
  PolicyVersion: string;
  PositionAsOf: string;
  Reasons: string[];
  ScoreBreakdown: {
    PriorityBenefit: number;
    HandlingPenalty: number;
    LeadTimePenalty: number;
  };
}

export interface SimulationResponse {
  /** Oldest watermark among the facts the simulation used. */
  asOf: string;
  options: Proposal[];
}

/** POST /v1/transfers:approve body. */
export interface ApproveTransferRequest {
  originSiteId: string;
  destinationSiteId: string;
  sku: string;
  quantity: number;
  policyVersion: string;
  operatorReason?: string;
  proposalAsOf: string;
}

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
