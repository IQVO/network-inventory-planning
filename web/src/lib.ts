/** Small pure helpers shared by the screens. */
import type { ApiError } from "./api";
import type { Proposal, TransferState } from "./types";

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

export interface SiteRollup {
  site: string;
  /** Units the site would send out if every option were executed. */
  outbound: number;
  /** Units the site would receive. */
  inbound: number;
  /** inbound - outbound. */
  net: number;
  options: number;
}

/**
 * Per-site view of the advisory options: what each site gives up and gains if
 * every option were executed. The simulation endpoint serves proposals only
 * (no stock positions), so this is demand/headroom DERIVED from the options,
 * not a stock level.
 */
export function siteRollup(options: Proposal[]): SiteRollup[] {
  const sites = new Map<string, SiteRollup>();
  const at = (site: string): SiteRollup => {
    let row = sites.get(site);
    if (!row) {
      row = { site, outbound: 0, inbound: 0, net: 0, options: 0 };
      sites.set(site, row);
    }
    return row;
  };
  for (const o of options) {
    const from = at(o.Origin);
    const to = at(o.Destination);
    from.outbound += o.Quantity;
    from.options += 1;
    to.inbound += o.Quantity;
    to.options += 1;
  }
  for (const row of sites.values()) row.net = row.inbound - row.outbound;
  return [...sites.values()].sort((a, b) => a.site.localeCompare(b.site));
}

/** Operator guidance for the problems this remote can run into. Keyed by HTTP status. */
export const STATUS_LEADS: Record<number, string> = {
  503: "The service is not ready to answer this (read models not ready, stale or incomplete, or the database is unavailable). Nothing was changed; retry shortly.",
};

/** Guidance for the approval failures the service documents (409 / 422 / 503). */
export function approveLead(error: ApiError): string | undefined {
  switch (error.status) {
    case 409:
      return "This idempotency key was already used for a different transfer. Close the approval and start it again for a fresh key.";
    case 422:
      return "The proposal no longer passes validation against the current facts (missing, stale or disabled). Nothing was approved; refresh the simulation and look again.";
    case 503:
      return "The service cannot approve right now (configuration incomplete or read models not ready). Nothing was approved; you can retry the same approval safely.";
    default:
      return undefined;
  }
}
