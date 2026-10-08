import { describe, expect, it } from "vitest";
import { ageLabel, buildApproveRequest, isTerminal, lagSeconds, newIdempotencyKey, orderSites, stateTone, utcLabel } from "./lib";
import type { ApproveDraft } from "./lib";
import { SITES, simSite } from "./test/fixtures";

describe("utcLabel", () => {
  it("renders a deterministic UTC label and a dash for nothing", () => {
    expect(utcLabel("2026-10-07T10:05:09Z")).toBe("2026-10-07 10:05Z");
    expect(utcLabel(undefined)).toBe("—");
    expect(utcLabel("not-a-date")).toBe("not-a-date");
  });
});

describe("isTerminal / stateTone", () => {
  it("knows the three terminal states", () => {
    for (const s of ["RECEIVED", "UNFULFILLABLE", "CANCELLED"]) expect(isTerminal(s)).toBe(true);
    for (const s of ["DRAFT", "PROPOSED", "APPROVED", "ALLOCATING", "ALLOCATED", "PICKED", "IN_TRANSIT", "ARRIVED"])
      expect(isTerminal(s)).toBe(false);
  });

  it("maps states to tones", () => {
    expect(stateTone("RECEIVED")).toBe("success");
    expect(stateTone("UNFULFILLABLE")).toBe("danger");
    expect(stateTone("CANCELLED")).toBe("neutral");
    expect(stateTone("ALLOCATING")).toBe("progress");
    expect(stateTone("IN_TRANSIT")).toBe("progress");
  });
});

describe("lagSeconds / ageLabel", () => {
  const now = Date.parse("2026-10-07T12:00:00Z");
  it("computes lag, clamps the future to zero, and is null for missing input", () => {
    expect(lagSeconds("2026-10-07T11:55:00Z", now)).toBe(300);
    expect(lagSeconds("2026-10-07T12:30:00Z", now)).toBe(0);
    expect(lagSeconds(undefined, now)).toBeNull();
    expect(lagSeconds("garbage", now)).toBeNull();
  });

  it("words the age", () => {
    expect(ageLabel("2026-10-07T11:59:50Z", now)).toBe("just now");
    expect(ageLabel("2026-10-07T11:55:00Z", now)).toBe("5m ago");
    expect(ageLabel("2026-10-07T10:00:00Z", now)).toBe("2.0h ago");
    expect(ageLabel("2026-10-04T12:00:00Z", now)).toBe("3d ago");
    expect(ageLabel(undefined, now)).toBe("");
  });
});

describe("newIdempotencyKey", () => {
  it("is non-empty and fresh on every call", () => {
    const a = newIdempotencyKey();
    const b = newIdempotencyKey();
    expect(a).not.toBe("");
    expect(a).not.toBe(b);
  });
});

describe("orderSites", () => {
  it("puts short sites first (most short first), then covered sites by headroom, like explain_network_imbalance", () => {
    expect(orderSites(SITES).map((s) => [s.site, s.capacityHeadroom])).toEqual([
      ["DC-WEST", -150],
      ["DC-SOUTH", -40],
      ["DC-EAST", 120],
      ["DC-NORTH", 30],
    ]);
  });

  it("treats zero headroom as covered and breaks ties by site id, without mutating the input", () => {
    const input = [simSite({ site: "B", capacityHeadroom: 0 }), simSite({ site: "A", capacityHeadroom: 0 }), simSite({ site: "C", capacityHeadroom: -1 })];
    expect(orderSites(input).map((s) => s.site)).toEqual(["C", "A", "B"]);
    expect(input.map((s) => s.site)).toEqual(["B", "A", "C"]);
  });

  it("is empty for no sites", () => {
    expect(orderSites([])).toEqual([]);
  });
});

describe("buildApproveRequest", () => {
  const draft: ApproveDraft = {
    originSiteId: "DC-EAST",
    destinationSiteId: "DC-WEST",
    sku: " SKU-RED-42 ",
    quantity: "120",
    policyVersion: "v7",
    operatorReason: "  ",
    proposalAsOf: "2026-10-07T09:50:00Z",
  };

  it("builds the request, trimming text and leaving out an empty reason", () => {
    expect(buildApproveRequest(draft)).toEqual({
      errors: {},
      request: {
        originSiteId: "DC-EAST",
        destinationSiteId: "DC-WEST",
        sku: "SKU-RED-42",
        quantity: 120,
        policyVersion: "v7",
        proposalAsOf: "2026-10-07T09:50:00Z",
      },
    });
  });

  it.each(["0", "-3", "1.5", "1e3", "abc", ""])("refuses quantity %j", (quantity) => {
    const out = buildApproveRequest({ ...draft, quantity });
    expect(out.request).toBeUndefined();
    expect(Object.keys(out.errors)).toEqual(["quantity"]);
  });

  it("requires every field but the reason, and different sites", () => {
    const out = buildApproveRequest({ ...draft, originSiteId: "", sku: "", policyVersion: " ", proposalAsOf: "yesterday" });
    expect(Object.keys(out.errors).sort()).toEqual(["originSiteId", "policyVersion", "proposalAsOf", "sku"]);
    expect(Object.keys(buildApproveRequest({ ...draft, destinationSiteId: "DC-EAST" }).errors)).toEqual(["destinationSiteId"]);
  });

  it("accepts fractional seconds and numeric offsets, refuses a timestamp without an offset", () => {
    expect(buildApproveRequest({ ...draft, proposalAsOf: "2026-10-07T09:50:00.123456Z" }).request).toBeDefined();
    expect(buildApproveRequest({ ...draft, proposalAsOf: "2026-10-07T06:50:00-03:00" }).request).toBeDefined();
    expect(buildApproveRequest({ ...draft, proposalAsOf: "2026-10-07T09:50:00" }).request).toBeUndefined();
  });
});
