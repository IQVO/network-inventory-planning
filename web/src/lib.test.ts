import { describe, expect, it } from "vitest";
import { ageLabel, isTerminal, lagSeconds, newIdempotencyKey, siteRollup, stateTone, utcLabel } from "./lib";
import { OPTIONS } from "./test/fixtures";

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

describe("siteRollup", () => {
  it("sums what each site sends and receives, sorted by site", () => {
    expect(siteRollup(OPTIONS)).toEqual([
      { site: "DC-EAST", outbound: 120, inbound: 0, net: -120, options: 1 },
      { site: "DC-NORTH", outbound: 30, inbound: 0, net: -30, options: 1 },
      { site: "DC-WEST", outbound: 0, inbound: 150, net: 150, options: 2 },
    ]);
  });

  it("is empty for no options", () => {
    expect(siteRollup([])).toEqual([]);
  });
});
