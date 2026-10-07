import { describe, expect, it } from "vitest";
import { NIP_API_BASE, resolveNipApiBase } from "./config";

describe("resolveNipApiBase", () => {
  it("builds the production API base from the runtime API origin", () => {
    expect(resolveNipApiBase({ apiOrigin: "http://localhost:8000" }, true)).toBe(
      "http://localhost:8000/api/network-inventory-planning",
    );
  });

  it("normalizes trailing slashes on the runtime API origin", () => {
    expect(resolveNipApiBase({ apiOrigin: "https://warehouse.example/" }, true)).toBe(
      "https://warehouse.example/api/network-inventory-planning",
    );
    expect(resolveNipApiBase({ apiOrigin: "https://warehouse.example///" }, true)).toBe(
      "https://warehouse.example/api/network-inventory-planning",
    );
  });

  it("fails loudly when production runtime configuration has no API origin", () => {
    expect(() => resolveNipApiBase({}, true)).toThrow(
      "window.__WAREHOUSE_CONFIG__.apiOrigin is required in production",
    );
    expect(() => resolveNipApiBase({ apiOrigin: "" }, true)).toThrow(
      "window.__WAREHOUSE_CONFIG__.apiOrigin is required in production",
    );
  });

  it("falls back to the service's own dev port outside production", () => {
    expect(resolveNipApiBase({}, false)).toBe("http://localhost:8080");
  });

  it("still prefers a runtime origin over the dev fallback", () => {
    expect(resolveNipApiBase({ apiOrigin: "http://localhost:8000" }, false)).toBe(
      "http://localhost:8000/api/network-inventory-planning",
    );
  });
});

describe("NIP_API_BASE", () => {
  it("loads without /config.json (window.__WAREHOUSE_CONFIG__ absent) in a non-production build", () => {
    // The module evaluated at import time with no published config: it must
    // not throw and must resolve to the dev base.
    expect(window.__WAREHOUSE_CONFIG__).toBeUndefined();
    expect(NIP_API_BASE).toBe("http://localhost:8080");
  });
});
