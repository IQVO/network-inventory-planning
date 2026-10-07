import { vi } from "vitest";
import { NIP_API_BASE } from "../config";

/** One request the code under test made through fetch. */
export interface MockCall {
  method: string;
  /** Path under the API base, e.g. "/v1/transfers". */
  path: string;
  query: URLSearchParams;
  /** The full URL as fetched. */
  url: string;
  headers: Record<string, string>;
  /** Parsed JSON body (undefined when none was sent). */
  body: unknown;
}

export type Reply = Response | Promise<Response>;
export type Handler = Reply | ((call: MockCall) => Reply);

/** A 2xx (or any status) application/json reply. */
export function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } });
}

/** An RFC 7807 application/problem+json reply, shaped like the service's. */
export function problem(status: number, slug: string, title: string, detail: string): Response {
  return new Response(
    JSON.stringify({
      type: `https://errors.network-inventory-planning.warehouse-systems.dev/${slug}`,
      title,
      status,
      detail,
      instance: "/x",
    }),
    { status, headers: { "Content-Type": "application/problem+json" } },
  );
}

/** A reply that never arrives, to hold a screen in its loading state. */
export function pending(): Promise<Response> {
  return new Promise<Response>(() => {});
}

/**
 * Replaces global fetch with a router over "METHOD /path" keys (path relative
 * to the API base). A request no route claims REJECTS (and is recorded), so an
 * unexpected call fails the test instead of silently hitting the network.
 * Returns the recorded calls; later calls can swap a route via `set`.
 */
export function mockApi(routes: Record<string, Handler>) {
  const calls: MockCall[] = [];
  const table = new Map(Object.entries(routes));
  const base = new URL(NIP_API_BASE);

  const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
    const url = new URL(String(input));
    const method = (init?.method ?? "GET").toUpperCase();
    const path = url.pathname.slice(base.pathname.replace(/\/$/, "").length) || "/";
    const call: MockCall = {
      method,
      path,
      query: url.searchParams,
      url: String(input),
      headers: (init?.headers ?? {}) as Record<string, string>,
      body: typeof init?.body === "string" ? JSON.parse(init.body) : undefined,
    };
    calls.push(call);
    const handler = table.get(`${method} ${path}`);
    if (!handler) throw new TypeError(`unmocked request: ${method} ${path}`);
    // Static replies are cloned so one Response can answer repeated calls
    // (a body can only be read once).
    const reply = typeof handler === "function" ? handler(call) : handler;
    return reply instanceof Response ? reply.clone() : reply.then((r) => r.clone());
  });
  vi.stubGlobal("fetch", fetchMock);

  return {
    calls,
    set(key: string, handler: Handler) {
      table.set(key, handler);
    },
    /** Calls made to "METHOD /path". */
    to(key: string): MockCall[] {
      return calls.filter((c) => `${c.method} ${c.path}` === key);
    },
  };
}
