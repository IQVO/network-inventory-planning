/**
 * The REAL wire shapes of network-inventory-planning's REST API, pinned to the
 * Go handler DTOs -- not to apis/openapi.yaml, which was wrong about
 * GET /v1/transfer-simulations (ADR 0010, "Correction").
 *
 * Sources (read them when changing a shape):
 *   internal/adapters/inbound/http/handler.go        simulateSiteDTO, the simulate
 *                                                    envelope, approveRequest,
 *                                                    approveTransferDTO, problem,
 *                                                    rebalanceRunDTO + envelope
 *   internal/adapters/inbound/http/transfers_read.go transferDTO, auditEntryDTO,
 *                                                    transferDetailDTO, transferListDTO
 *
 * Notation: key -> kind; a trailing "?" marks a key the service omits when empty
 * (`omitempty`). contract.test.ts (a) validates every fixture against these and
 * (b) parses the Go source and fails when its json tags drift from them.
 */

export type Kind = "string" | "number" | "boolean" | "array";
export type Shape = Record<string, Kind | `${Kind}?`>;

export const SIMULATION_SITE: Shape = {
  site: "string",
  originEnabled: "boolean",
  destinationEnabled: "boolean",
  totalDemand: "number",
  capacityOverWindow: "number",
  capacityHeadroom: "number",
  windowStart: "string",
  windowEnd: "string",
};

/** GET /v1/transfer-simulations 200. There is no `options`/proposal list. */
export const SIMULATION: Shape = {
  advisory: "boolean",
  asOf: "string",
  sites: "array",
};

export const TRANSFER: Shape = {
  id: "string",
  state: "string",
  sku: "string",
  quantity: "number",
  pickedQuantity: "number?",
  originSiteId: "string",
  destinationSiteId: "string",
  policyVersion: "string",
  reservationId: "string?",
  rejectionReason: "string?",
  expiresAt: "string",
  createdAt: "string",
  updatedAt: "string",
  version: "number",
};

export const AUDIT_ENTRY: Shape = {
  seq: "number",
  from: "string?",
  to: "string",
  event: "string",
  cause: "string",
  occurredAt: "string",
};

/** GET /v1/transfers/{id}: the transfer fields plus `audit`. */
export const TRANSFER_DETAIL: Shape = { ...TRANSFER, audit: "array" };

/** GET /v1/transfers. */
export const TRANSFER_LIST: Shape = {
  items: "array",
  total: "number",
  limit: "number",
  offset: "number",
};

/** POST /v1/transfers:approve request (unknown fields are a 400; operatorReason may be left out). */
export const APPROVE_REQUEST: Shape = {
  originSiteId: "string",
  destinationSiteId: "string",
  sku: "string",
  quantity: "number",
  policyVersion: "string",
  operatorReason: "string?",
  proposalAsOf: "string",
};

/** POST /v1/transfers:approve 200 (also a replay of the same key; the service never answers 201). */
export const APPROVE_RESPONSE: Shape = {
  transferId: "string",
  state: "string",
  originSiteId: "string",
  destinationSiteId: "string",
  sku: "string",
  quantity: "number",
  policyVersion: "string",
  reservationId: "string?",
  rejectionReason: "string?",
  replayed: "boolean",
  expiresAt: "string",
  transferLineId: "string",
};

export const REBALANCE_RUN: Shape = {
  id: "number",
  startedAt: "string",
  snapshotAsOf: "string?",
  proposalCount: "number",
  rejectedCount: "number",
  outcome: "string",
  failClosedReason: "string?",
};

/** GET /v1/rebalance-runs 200. */
export const REBALANCE_RUNS: Shape = { runs: "array" };

/** application/problem+json, every error status (handler.go `problem`). */
export const PROBLEM: Shape = {
  type: "string",
  title: "string",
  status: "number",
  detail: "string",
};

/**
 * Where each shape lives in the Go source, for the drift check. `struct` is a
 * named type; `anonymousFirstField` finds an inline response envelope by its
 * first field. `optionality` is false for request bodies (Go cannot say "optional"
 * for a decoded request field).
 */
export const GO_SOURCES: {
  name: string;
  file: "handler.go" | "transfers_read.go";
  shape: Shape;
  struct?: string;
  anonymousFirstField?: string;
  optionality: boolean;
}[] = [
  { name: "simulateSiteDTO", file: "handler.go", struct: "simulateSiteDTO", shape: SIMULATION_SITE, optionality: true },
  { name: "simulate envelope", file: "handler.go", anonymousFirstField: "Advisory", shape: SIMULATION, optionality: true },
  { name: "transferDTO", file: "transfers_read.go", struct: "transferDTO", shape: TRANSFER, optionality: true },
  { name: "auditEntryDTO", file: "transfers_read.go", struct: "auditEntryDTO", shape: AUDIT_ENTRY, optionality: true },
  { name: "transferDetailDTO", file: "transfers_read.go", struct: "transferDetailDTO", shape: TRANSFER_DETAIL, optionality: true },
  { name: "transferListDTO", file: "transfers_read.go", struct: "transferListDTO", shape: TRANSFER_LIST, optionality: true },
  { name: "approveRequest", file: "handler.go", struct: "approveRequest", shape: APPROVE_REQUEST, optionality: false },
  { name: "approveTransferDTO", file: "handler.go", struct: "approveTransferDTO", shape: APPROVE_RESPONSE, optionality: true },
  { name: "rebalanceRunDTO", file: "handler.go", struct: "rebalanceRunDTO", shape: REBALANCE_RUN, optionality: true },
  { name: "rebalance-runs envelope", file: "handler.go", anonymousFirstField: "Runs", shape: REBALANCE_RUNS, optionality: true },
  { name: "problem", file: "handler.go", struct: "problem", shape: PROBLEM, optionality: true },
];
