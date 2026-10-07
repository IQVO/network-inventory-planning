import { NIP_API_BASE } from "./config";
import type {
  ApproveTransferRequest,
  ApproveTransferResponse,
  RebalanceRun,
  RebalanceRunsResponse,
  SimulationResponse,
  TransferDetail,
  TransferList,
  TransferQuery,
} from "./types";

/** RFC 7807 problem+json body every error response from the service returns
 *  (`type` is a stable problem identifier, e.g. `.../read-models-not-ready`). */
export interface ProblemDetails {
  type?: string;
  title?: string;
  status?: number;
  detail?: string;
  instance?: string;
}

/**
 * Every failed call -- an HTTP error with a problem+json body, an HTTP error
 * without one, or a network failure -- surfaces as one ApiError, so a screen
 * has a single shape to render: `title` (the problem's category) and `detail`
 * (what exactly was wrong), plus `slug` to branch on a specific problem.
 */
export class ApiError extends Error {
  readonly status: number;
  readonly problem: ProblemDetails | null;
  readonly title: string;
  readonly detail: string;
  /** Last path segment of problem.type; "" when unknown. */
  readonly slug: string;

  constructor(status: number, problem: ProblemDetails | null, fallbackTitle: string) {
    const title = problem?.title || fallbackTitle;
    const detail = problem?.detail ?? "";
    super(detail || title);
    this.name = "ApiError";
    this.status = status;
    this.problem = problem;
    this.title = title;
    this.detail = detail;
    this.slug = problem?.type ? (problem.type.split("/").pop() ?? "") : "";
  }
}

/** Normalizes anything thrown by a request into an ApiError. */
export function toApiError(err: unknown): ApiError {
  if (err instanceof ApiError) return err;
  const message = err instanceof Error ? err.message : String(err);
  return new ApiError(0, { detail: message }, "Network error");
}

type Query = Record<string, string | number | undefined>;

function withQuery(path: string, query?: Query): string {
  if (!query) return path;
  const params = new URLSearchParams();
  for (const [key, value] of Object.entries(query)) {
    if (value !== undefined && value !== "") params.set(key, String(value));
  }
  const qs = params.toString();
  return qs ? `${path}?${qs}` : path;
}

async function request<T>(
  method: "GET" | "POST",
  path: string,
  opts: { body?: unknown; headers?: Record<string, string> } = {},
): Promise<T> {
  let res: Response;
  try {
    res = await fetch(`${NIP_API_BASE}${path}`, {
      method,
      ...(opts.body === undefined
        ? {}
        : {
            headers: { "Content-Type": "application/json", ...opts.headers },
            body: JSON.stringify(opts.body),
          }),
    });
  } catch (err) {
    throw toApiError(err);
  }
  if (!res.ok) {
    let problem: ProblemDetails | null = null;
    try {
      problem = (await res.json()) as ProblemDetails;
    } catch {
      // non-JSON error body -- fall through with problem = null
    }
    throw new ApiError(res.status, problem, `${res.status} ${res.statusText}`.trim());
  }
  return (await res.json()) as T;
}

const seg = encodeURIComponent;

/** GET /v1/transfers?state=&originSiteId=&destinationSiteId=&limit=&offset= */
export function listTransfers(q: TransferQuery): Promise<TransferList> {
  return request<TransferList>("GET", withQuery("/v1/transfers", { ...q }));
}

/** GET /v1/transfers/{id} (the transfer plus its audit trail). */
export function getTransfer(id: string): Promise<TransferDetail> {
  return request<TransferDetail>("GET", `/v1/transfers/${seg(id)}`);
}

/** GET /v1/transfer-simulations (503 problem+json while the read models are not ready). */
export function getSimulation(): Promise<SimulationResponse> {
  return request<SimulationResponse>("GET", "/v1/transfer-simulations");
}

/**
 * POST /v1/transfers:approve. `idempotencyKey` is REQUIRED by the service:
 * replaying the same key with the same payload returns the ORIGINAL transfer
 * (`replayed: true`), the same key with another payload is a 409.
 */
export function approveTransfer(
  input: ApproveTransferRequest,
  idempotencyKey: string,
): Promise<ApproveTransferResponse> {
  return request<ApproveTransferResponse>("POST", "/v1/transfers:approve", {
    body: input,
    headers: { "Idempotency-Key": idempotencyKey },
  });
}

/** GET /v1/rebalance-runs?limit= (newest first). */
export async function listRebalanceRuns(limit?: number): Promise<RebalanceRun[]> {
  const res = await request<RebalanceRunsResponse>("GET", withQuery("/v1/rebalance-runs", { limit }));
  return res.runs ?? [];
}
