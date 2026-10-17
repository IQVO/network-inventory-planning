/** Small pure helpers shared by the screens. */
import type { ApiError } from "./api";
import type {
  ApproveTransferRequest,
  GenerateRequestWire,
  LaneWire,
  PolicyWire,
  PositionWire,
  SimulationSite,
  TransferState,
} from "./types";

type Tone = "neutral" | "progress" | "success" | "warning" | "danger";

/** "2026-10-05T08:00:00Z" -> "2026-10-05 08:00Z" (deterministic, always UTC). */
export function utcLabel(iso: string | null | undefined): string {
  if (!iso) return "—";
  const m = /^(\d{4}-\d{2}-\d{2})T(\d{2}:\d{2})/.exec(iso);
  return m ? `${m[1]} ${m[2]}Z` : iso;
}

/** RECEIVED, UNFULFILLABLE and CANCELLED are terminal; every other state is waiting for a next step. */
const TERMINAL: ReadonlySet<string> = new Set(["RECEIVED", "UNFULFILLABLE", "CANCELLED"]);

export function isTerminal(state: string): boolean {
  return TERMINAL.has(state);
}

/** The kit's StatusPill does not know the saga states, so the tone is passed explicitly. */
export function stateTone(state: TransferState | string): Tone {
  switch (state) {
    case "RECEIVED":
      return "success";
    case "UNFULFILLABLE":
      return "danger";
    case "CANCELLED":
    case "DRAFT":
    case "PROPOSED":
      return "neutral";
    default:
      return "progress";
  }
}

/** Seconds between an instant and `nowMs`; null when the instant is missing/unparseable. */
export function lagSeconds(iso: string | null | undefined, nowMs: number): number | null {
  if (!iso) return null;
  const t = Date.parse(iso);
  return Number.isNaN(t) ? null : Math.max(0, (nowMs - t) / 1000);
}

/** "5m ago", "2.0h ago", "just now". */
export function ageLabel(iso: string | null | undefined, nowMs: number): string {
  const lag = lagSeconds(iso, nowMs);
  if (lag === null) return "";
  if (lag < 30) return "just now";
  if (lag < 3600) return `${Math.max(1, Math.round(lag / 60))}m ago`;
  if (lag < 86400) return `${(lag / 3600).toFixed(1)}h ago`;
  return `${Math.round(lag / 86400)}d ago`;
}

/** A fresh key per logical approval (the service replays the FIRST transfer a key created). */
export function newIdempotencyKey(): string {
  const c = globalThis.crypto;
  if (c && typeof c.randomUUID === "function") return c.randomUUID();
  return `nip-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 12)}`;
}

/** A site whose published capacity over the window is below its in-window demand (the service's own "negative = short"). */
export function isShort(site: SimulationSite): boolean {
  return site.capacityHeadroom < 0;
}

/**
 * Display order of the simulation's sites, mirroring warehouse-ops-agent's
 * explain_network_imbalance (internal/domain/policy/transfer_imbalance.go
 * `siteBefore`): short sites first, most short first; then covered sites, most
 * headroom first; ties by site id. Returns a new array.
 */
export function orderSites(sites: readonly SimulationSite[]): SimulationSite[] {
  return [...sites].sort((a, b) => {
    const aShort = isShort(a);
    if (aShort !== isShort(b)) return aShort ? -1 : 1;
    if (a.capacityHeadroom !== b.capacityHeadroom) {
      return aShort ? a.capacityHeadroom - b.capacityHeadroom : b.capacityHeadroom - a.capacityHeadroom;
    }
    return a.site.localeCompare(b.site);
  });
}

/** The approval form's raw (string) fields. */
export interface ApproveDraft {
  originSiteId: string;
  destinationSiteId: string;
  sku: string;
  quantity: string;
  policyVersion: string;
  operatorReason: string;
  proposalAsOf: string;
}

export type ApproveField = keyof ApproveDraft;

/** RFC 3339 with an explicit offset: what the service's time.Time decoding accepts. */
const RFC3339 = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})$/;

/**
 * Client-side checks that mirror what the service refuses on its face
 * (internal/domain/transfer/saga.go ProposalInput.validate): every field but
 * the operator reason is required, origin and destination differ, quantity is a
 * positive integer. Either `request` (ready to POST) or per-field `errors`.
 */
export function buildApproveRequest(d: ApproveDraft): {
  request?: ApproveTransferRequest;
  errors: Partial<Record<ApproveField, string>>;
} {
  const errors: Partial<Record<ApproveField, string>> = {};
  const origin = d.originSiteId.trim();
  const destination = d.destinationSiteId.trim();
  const sku = d.sku.trim();
  const policyVersion = d.policyVersion.trim();
  const proposalAsOf = d.proposalAsOf.trim();
  const reason = d.operatorReason.trim();
  if (!origin) errors.originSiteId = "Choose an origin site.";
  if (!destination) errors.destinationSiteId = "Choose a destination site.";
  if (origin && origin === destination) errors.destinationSiteId = "The destination must differ from the origin.";
  if (!sku) errors.sku = "SKU is required.";
  const quantity = /^[1-9]\d*$/.test(d.quantity.trim()) ? Number(d.quantity.trim()) : NaN;
  if (!Number.isSafeInteger(quantity)) errors.quantity = "Quantity must be a positive whole number.";
  if (!policyVersion) errors.policyVersion = "Policy version is required.";
  if (!RFC3339.test(proposalAsOf) || Number.isNaN(Date.parse(proposalAsOf))) {
    errors.proposalAsOf = "Proposal as-of must be an RFC 3339 timestamp, e.g. 2026-10-07T09:50:00Z.";
  }
  if (Object.keys(errors).length > 0) return { errors };
  return {
    errors,
    request: {
      originSiteId: origin,
      destinationSiteId: destination,
      sku,
      quantity,
      policyVersion,
      proposalAsOf,
      ...(reason && { operatorReason: reason }),
    },
  };
}

/** Operator guidance for the problems this remote can run into. Keyed by HTTP status. */
export const STATUS_LEADS: Record<number, string> = {
  503: "The service is not ready to answer this (read models not ready, stale or incomplete, or the database is unavailable). Nothing was changed; retry shortly.",
};

/**
 * Guidance for the approval failures the service documents (handler.go
 * writeApproveProblem). 422 covers an invalid request AND every fail-closed
 * refusal: facts missing, stale or disabled (including read models that cannot
 * be built), and an expired proposal. 503 is configuration or an unclassified
 * failure, safe to retry under the same key.
 */
export function approveLead(error: ApiError): string | undefined {
  switch (error.status) {
    case 409:
      return "This idempotency key was already used for a different transfer. Close the approval and start it again for a fresh key.";
    case 422:
      return "The approval no longer passes validation against the current facts (missing, stale or disabled). Nothing was approved; refresh the simulation and look again.";
    case 503:
      return "The service cannot approve right now (configuration incomplete or the approval could not be completed). Nothing was approved; you can retry the same approval safely.";
    default:
      return undefined;
  }
}

/* ------------------------------------------------------------------ *
 * Catalogue editors (ADR 0015: PUT /v1/transfer-lanes and
 * /v1/rebalance-policies). The client-side checks mirror what the domain
 * refuses on its face (internal/domain/transfer/catalogue.go Validate), so
 * the operator sees the same rule before the network round-trip; the
 * service stays authoritative and its 422 problem is still surfaced.
 * ------------------------------------------------------------------ */

/** One nanosecond per 3.6e12: the wire carries Go time.Duration (ns). */
export const NS_PER_HOUR = 3.6e12;

/** A lane's LeadTime as an input value: exact hours ("18", "0.25"). */
export function leadTimeHoursValue(ns: number): string {
  const hours = ns / NS_PER_HOUR;
  return Number.isInteger(hours) ? String(hours) : String(Number(hours.toFixed(4)));
}

/** A lane's LeadTime for a table cell: 64800000000000 -> "18 h". */
export function leadTimeLabel(ns: number): string {
  const hours = ns / NS_PER_HOUR;
  return `${Number.isInteger(hours) ? hours : Number(hours.toFixed(2))} h`;
}

/** The lane editor's raw (string) fields. */
export interface LaneDraft {
  origin: string;
  destination: string;
  /** Hours; blank means 0. Sent as nanoseconds. */
  leadTimeHours: string;
  /** Blank means 0. */
  unitHandlingCost: string;
  enabled: "yes" | "no";
}

export type LaneField = keyof LaneDraft;

export function emptyLaneDraft(): LaneDraft {
  return { origin: "", destination: "", leadTimeHours: "", unitHandlingCost: "", enabled: "yes" };
}

export function laneDraftFrom(lane: LaneWire): LaneDraft {
  return {
    origin: lane.Origin,
    destination: lane.Destination,
    leadTimeHours: leadTimeHoursValue(lane.LeadTime),
    unitHandlingCost: String(lane.UnitHandlingCost),
    enabled: lane.Enabled ? "yes" : "no",
  };
}

/** Client-side mirror of transfer.Lane.Validate: keys present and differing,
 *  non-negative lead time and cost. Either `request` or per-field `errors`. */
export function buildLaneRequest(d: LaneDraft): {
  request?: LaneWire;
  errors: Partial<Record<LaneField, string>>;
} {
  const errors: Partial<Record<LaneField, string>> = {};
  const origin = d.origin.trim();
  const destination = d.destination.trim();
  if (!origin) errors.origin = "Origin site is required.";
  if (!destination) errors.destination = "Destination site is required.";
  if (origin && origin === destination) {
    errors.destination = "Origin and destination must differ — a lane is directed; WH1→WH2 and WH2→WH1 are two lanes.";
  }
  const hours = d.leadTimeHours.trim() === "" ? 0 : Number(d.leadTimeHours.trim());
  if (!Number.isFinite(hours) || hours < 0) errors.leadTimeHours = "Lead time must be zero or more hours.";
  const cost = d.unitHandlingCost.trim() === "" ? "0" : d.unitHandlingCost.trim();
  if (!/^\d+$/.test(cost) || !Number.isSafeInteger(Number(cost))) {
    errors.unitHandlingCost = "Unit handling cost must be zero or a positive whole number.";
  }
  if (Object.keys(errors).length > 0) return { errors };
  return {
    errors,
    request: {
      Origin: origin,
      Destination: destination,
      LeadTime: Math.round(hours * NS_PER_HOUR),
      UnitHandlingCost: Number(cost),
      Enabled: d.enabled === "yes",
    },
  };
}

/** The stored entry a PUT answered with, worded for the operator. */
export function catalogueOutcomeNote(key: string, entry: { revision: number; outcome?: string }): string {
  switch (entry.outcome) {
    case "CREATED":
      return `Created ${key} at revision ${entry.revision}.`;
    case "UPDATED":
      return `Updated ${key} to revision ${entry.revision}.`;
    case "UNCHANGED":
      return `${key} is unchanged (the PUT carried exactly the stored entry; nothing was written). Revision ${entry.revision}.`;
    default:
      return `Saved ${key} at revision ${entry.revision}.`;
  }
}

/** The policy editor's raw (string) fields. */
export interface PolicyDraft {
  site: string;
  sku: string;
  version: string;
  /** Blank means 0. */
  safetyStock: string;
  /** Blank means 0 (a target of 0 means "do not stock this SKU here"). */
  targetStock: string;
  /** Blank means 0. */
  unitPriority: string;
}

export type PolicyField = keyof PolicyDraft;

export function emptyPolicyDraft(): PolicyDraft {
  return { site: "", sku: "", version: "", safetyStock: "", targetStock: "", unitPriority: "" };
}

export function policyDraftFrom(policy: PolicyWire): PolicyDraft {
  return {
    site: policy.Site,
    sku: policy.SKU,
    version: policy.Version,
    safetyStock: String(policy.SafetyStock),
    targetStock: String(policy.TargetStock),
    unitPriority: String(policy.UnitPriority),
  };
}

/** Client-side mirror of transfer.Policy.Validate: key and version present,
 *  non-negative integers, target not below the safety stock. Either
 *  `request` or per-field `errors`. */
export function buildPolicyRequest(d: PolicyDraft): {
  request?: PolicyWire;
  errors: Partial<Record<PolicyField, string>>;
} {
  const errors: Partial<Record<PolicyField, string>> = {};
  const site = d.site.trim();
  const sku = d.sku.trim();
  const version = d.version.trim();
  if (!site) errors.site = "Site is required.";
  if (!sku) errors.sku = "SKU is required.";
  if (!version) errors.version = "Policy version is required.";
  const counts: { field: PolicyField; label: string; raw: string }[] = [
    { field: "safetyStock", label: "Safety stock", raw: d.safetyStock },
    { field: "targetStock", label: "Target stock", raw: d.targetStock },
    { field: "unitPriority", label: "Unit priority", raw: d.unitPriority },
  ];
  const parsed: Record<string, number> = {};
  for (const { field, label, raw } of counts) {
    const value = raw.trim() === "" ? "0" : raw.trim();
    if (!/^\d+$/.test(value) || !Number.isSafeInteger(Number(value))) {
      errors[field] = `${label} must be zero or a positive whole number.`;
    } else {
      parsed[field] = Number(value);
    }
  }
  if (parsed.safetyStock !== undefined && parsed.targetStock !== undefined && parsed.targetStock < parsed.safetyStock) {
    errors.targetStock = "Target stock must not be below the safety stock.";
  }
  if (Object.keys(errors).length > 0) return { errors };
  return {
    errors,
    request: {
      Version: version,
      Site: site,
      SKU: sku,
      SafetyStock: parsed.safetyStock,
      TargetStock: parsed.targetStock,
      UnitPriority: parsed.unitPriority,
    },
  };
}

/**
 * Operator guidance for the catalogue problems the service documents
 * (catalogue.go writeCatalogueProblem, parseIfMatch). Nothing is written on
 * any of these: 412/409 mean the entry moved under the operator and the list
 * has been refreshed.
 */
export function catalogueLead(error: ApiError): string | undefined {
  switch (error.status) {
    case 400:
      return "The request was refused before anything was written (a malformed If-Match, or a body field the service does not know).";
    case 412:
      return "The entry moved on since it was loaded (If-Match does not match the current revision). Nothing was written; the list below has been refreshed — edit the entry again from its current revision.";
    case 409:
      return "Another writer changed this entry while yours was being saved. Nothing of yours was written; the list has been refreshed — retry from the current revision.";
    case 422:
      return "The service refused the entry as invalid. Nothing was written; fix the named field and save again.";
    case 503:
      return "This instance runs without the persistence the catalogues require; nothing was written.";
    default:
      return undefined;
  }
}

/* ------------------------------------------------------------------ *
 * Proposal generation (POST /v1/transfer-proposals:generate): the
 * diagnostic planning boundary over an explicit snapshot. The client
 * mirrors the use case's own refusals (generate_transfer_proposals.go):
 * asOf is required and every position's AsOf must equal it.
 * ------------------------------------------------------------------ */

/** One position row's raw (string) fields; AsOf comes from the form's as-of. */
export interface PositionDraft {
  site: string;
  sku: string;
  available: string;
  /** Blank means 0. */
  customerReservations: string;
  /** Blank means 0. */
  confirmedInbound: string;
  /** Blank means 0. */
  committedOutbound: string;
}

export type PositionField = keyof PositionDraft;

export function emptyPositionDraft(): PositionDraft {
  return { site: "", sku: "", available: "", customerReservations: "", confirmedInbound: "", committedOutbound: "" };
}

/** One snapshot position: site + SKU required, counts non-negative integers
 *  (blank = 0). AsOf is stamped by the form, never typed per row. */
export function buildPositionRequest(d: PositionDraft, asOf: string): {
  request?: PositionWire;
  errors: Partial<Record<PositionField, string>>;
} {
  const errors: Partial<Record<PositionField, string>> = {};
  const site = d.site.trim();
  const sku = d.sku.trim();
  if (!site) errors.site = "Site is required.";
  if (!sku) errors.sku = "SKU is required.";
  const counts: { field: PositionField; label: string; raw: string }[] = [
    { field: "available", label: "Available", raw: d.available },
    { field: "customerReservations", label: "Customer reservations", raw: d.customerReservations },
    { field: "confirmedInbound", label: "Confirmed inbound", raw: d.confirmedInbound },
    { field: "committedOutbound", label: "Committed outbound", raw: d.committedOutbound },
  ];
  const parsed: Record<string, number> = {};
  for (const { field, label, raw } of counts) {
    const value = raw.trim() === "" ? "0" : raw.trim();
    if (!/^\d+$/.test(value) || !Number.isSafeInteger(Number(value))) {
      errors[field] = `${label} must be zero or a positive whole number.`;
    } else {
      parsed[field] = Number(value);
    }
  }
  if (Object.keys(errors).length > 0) return { errors };
  return {
    errors,
    request: {
      Site: site,
      SKU: sku,
      Available: parsed.available,
      CustomerReservations: parsed.customerReservations,
      ConfirmedInbound: parsed.confirmedInbound,
      CommittedOutbound: parsed.committedOutbound,
      AsOf: asOf,
    },
  };
}

/** The generate form's whole state: the as-of plus the three snapshot lists. */
export interface GenerateDraft {
  asOf: string;
  positions: PositionDraft[];
  policies: PolicyDraft[];
  lanes: LaneDraft[];
}

/**
 * Assembles the POST body: asOf must be RFC 3339, and every row must pass the
 * same checks the catalogue editors apply (each position is also stamped with
 * the as-of — the service refuses a mixed snapshot). Row errors are prefixed
 * with their list and number, e.g. "Position 2: SKU is required.".
 */
export function buildGenerateRequest(d: GenerateDraft): {
  request?: GenerateRequestWire;
  errors: string[];
} {
  const errors: string[] = [];
  const asOf = d.asOf.trim();
  if (!RFC3339.test(asOf) || Number.isNaN(Date.parse(asOf))) {
    errors.push("As-of must be an RFC 3339 timestamp, e.g. 2026-10-07T09:50:00Z.");
  }
  const positions: PositionWire[] = [];
  d.positions.forEach((row, i) => {
    const out = buildPositionRequest(row, asOf);
    if (out.request) positions.push(out.request);
    else errors.push(...Object.values(out.errors).map((e) => `Position ${i + 1}: ${e}`));
  });
  const policies: PolicyWire[] = [];
  d.policies.forEach((row, i) => {
    const out = buildPolicyRequest(row);
    if (out.request) policies.push(out.request);
    else errors.push(...Object.values(out.errors).map((e) => `Policy ${i + 1}: ${e}`));
  });
  const lanes: LaneWire[] = [];
  d.lanes.forEach((row, i) => {
    const out = buildLaneRequest(row);
    if (out.request) lanes.push(out.request);
    else errors.push(...Object.values(out.errors).map((e) => `Lane ${i + 1}: ${e}`));
  });
  if (errors.length > 0) return { errors };
  return { errors, request: { asOf, positions, policies, lanes } };
}

/**
 * Guidance for the generate endpoint's documented problems (handler.go
 * generate): 400 the body could not be decoded, 422 the snapshot was refused
 * — a missing asOf or a position stamped with another instant (mixed
 * snapshot). Nothing is ever generated on a refusal.
 */
export function generateLead(error: ApiError): string | undefined {
  switch (error.status) {
    case 400:
      return "The service could not read the request body (a field it does not know, or a malformed value). Nothing was generated.";
    case 422:
      return "The snapshot was refused: the as-of is required, and every position must carry that same instant — a mixed snapshot is never planned. Nothing was generated.";
    default:
      return undefined;
  }
}
