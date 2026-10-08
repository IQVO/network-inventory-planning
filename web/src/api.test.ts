import { describe, expect, it } from "vitest";
import { ApiError, approveTransfer, getSimulation, getTransfer, listRebalanceRuns, listTransfers, toApiError } from "./api";
import { json, mockApi, problem } from "./test/fetchMock";
import { RUNS, transferList, transfer } from "./test/fixtures";

describe("listTransfers", () => {
  it("sends only the filters that are set", async () => {
    const api = mockApi({ "GET /v1/transfers": json(transferList([transfer()])) });
    await listTransfers({ state: "ALLOCATING", originSiteId: "", destinationSiteId: "DC-WEST", limit: 25, offset: 0 });
    const q = api.calls[0].query;
    expect(q.get("state")).toBe("ALLOCATING");
    expect(q.get("destinationSiteId")).toBe("DC-WEST");
    expect(q.has("originSiteId")).toBe(false);
    expect(q.get("limit")).toBe("25");
    expect(q.get("offset")).toBe("0");
  });
});

describe("getTransfer", () => {
  it("escapes the id into the path", async () => {
    const api = mockApi({ "GET /v1/transfers/a%2Fb": json({}) });
    await getTransfer("a/b");
    expect(api.calls[0].url).toContain("/v1/transfers/a%2Fb");
  });
});

describe("approveTransfer", () => {
  it("POSTs the snapshot as JSON with the Idempotency-Key header", async () => {
    const api = mockApi({ "POST /v1/transfers:approve": json({ transferId: "t-1", replayed: false }) });
    await approveTransfer(
      { originSiteId: "A", destinationSiteId: "B", sku: "S", quantity: 5, policyVersion: "v1", proposalAsOf: "2026-10-07T09:00:00Z" },
      "key-123",
    );
    const call = api.calls[0];
    expect(call.method).toBe("POST");
    expect(call.headers["Idempotency-Key"]).toBe("key-123");
    expect(call.headers["Content-Type"]).toBe("application/json");
    expect(call.body).toEqual({
      originSiteId: "A",
      destinationSiteId: "B",
      sku: "S",
      quantity: 5,
      policyVersion: "v1",
      proposalAsOf: "2026-10-07T09:00:00Z",
    });
  });
});

describe("errors", () => {
  it("keeps the problem+json title, detail and slug", async () => {
    mockApi({ "GET /v1/transfer-simulations": problem(503, "read-models-incomplete", "Planning read models are incomplete or stale", "capacity facts are stale") });
    const err = await getSimulation().catch((e: unknown) => e);
    expect(err).toBeInstanceOf(ApiError);
    const e = err as ApiError;
    expect(e.status).toBe(503);
    expect(e.title).toBe("Planning read models are incomplete or stale");
    expect(e.detail).toBe("capacity facts are stale");
    expect(e.slug).toBe("read-models-incomplete");
  });

  it("survives a non-JSON error body", async () => {
    mockApi({ "GET /v1/rebalance-runs": new Response("bad gateway", { status: 502, statusText: "Bad Gateway" }) });
    const e = (await listRebalanceRuns().catch((x: unknown) => x)) as ApiError;
    expect(e.status).toBe(502);
    expect(e.title).toBe("502 Bad Gateway");
    expect(e.problem).toBeNull();
  });

  it("turns a network failure into an ApiError", () => {
    const e = toApiError(new TypeError("Failed to fetch"));
    expect(e.status).toBe(0);
    expect(e.title).toBe("Network error");
    expect(e.detail).toBe("Failed to fetch");
  });
});

describe("listRebalanceRuns", () => {
  it("returns the runs array and passes the limit", async () => {
    const api = mockApi({ "GET /v1/rebalance-runs": json({ runs: RUNS }) });
    expect(await listRebalanceRuns(50)).toEqual(RUNS);
    expect(api.calls[0].query.get("limit")).toBe("50");
  });
});
