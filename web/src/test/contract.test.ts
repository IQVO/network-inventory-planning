import { readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import { buildApproveRequest } from "../lib";
import { problem } from "./fetchMock";
import { APPROVE_REQUEST, APPROVE_RESPONSE, AUDIT_ENTRY, GO_SOURCES, PROBLEM, REBALANCE_RUN, REBALANCE_RUNS, SIMULATION, SIMULATION_SITE, TRANSFER, TRANSFER_DETAIL, TRANSFER_LIST } from "./realShapes";
import type { Shape } from "./realShapes";
import { AUDIT, RECEIVED, RUNS, SITES, UNFULFILLABLE, approved, detail, simSite, simulation, transfer, transferList } from "./fixtures";

/**
 * Contract test: every fixture the screens' tests run against must have exactly
 * the keys (and JSON kinds) of the REAL API shapes pinned in realShapes.ts, and
 * those pins must still match the json tags in the Go handlers. A fixture that
 * invents a field (the old `options`) or forgets one fails here, instead of
 * passing against a fiction.
 */

/** Differences between a value and a shape; [] when it conforms. */
function diff(value: unknown, shape: Shape): string[] {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return ["not an object"];
  const obj = value as Record<string, unknown>;
  const out: string[] = [];
  for (const [key, spec] of Object.entries(shape)) {
    const optional = spec.endsWith("?");
    const kind = optional ? spec.slice(0, -1) : spec;
    if (!(key in obj)) {
      if (!optional) out.push(`missing required key "${key}"`);
      continue;
    }
    const actual = Array.isArray(obj[key]) ? "array" : typeof obj[key];
    if (actual !== kind) out.push(`key "${key}" is ${actual}, the service sends ${kind}`);
  }
  for (const key of Object.keys(obj)) if (!(key in shape)) out.push(`unexpected key "${key}" (the service never sends it)`);
  return out;
}

describe("fixtures match the real API shapes", () => {
  it("GET /v1/transfer-simulations", () => {
    expect(diff(simulation(), SIMULATION)).toEqual([]);
    expect(diff(simulation({ sites: [] }), SIMULATION)).toEqual([]);
    for (const site of [...SITES, simSite({ capacityOverWindow: 99.25 })]) expect(diff(site, SIMULATION_SITE)).toEqual([]);
  });

  it("GET /v1/transfers and /v1/transfers/{id}", () => {
    for (const t of [transfer(), RECEIVED, UNFULFILLABLE]) expect(diff(t, TRANSFER)).toEqual([]);
    expect(diff(transferList([transfer()]), TRANSFER_LIST)).toEqual([]);
    expect(diff(detail(), TRANSFER_DETAIL)).toEqual([]);
    for (const entry of AUDIT) expect(diff(entry, AUDIT_ENTRY)).toEqual([]);
  });

  it("GET /v1/rebalance-runs", () => {
    expect(diff({ runs: RUNS }, REBALANCE_RUNS)).toEqual([]);
    for (const run of RUNS) expect(diff(run, REBALANCE_RUN)).toEqual([]);
  });

  it("POST /v1/transfers:approve response", () => {
    expect(diff(approved(), APPROVE_RESPONSE)).toEqual([]);
    expect(diff(approved({ replayed: true, reservationId: "res-1" }), APPROVE_RESPONSE)).toEqual([]);
  });

  it("problem+json bodies", async () => {
    const body: unknown = await problem(503, "read-models-incomplete", "Planning read models are incomplete or stale", "x").json();
    expect(diff(body, PROBLEM)).toEqual([]);
  });

  it("the approval request the form builds", () => {
    const draft = {
      originSiteId: "DC-EAST",
      destinationSiteId: "DC-WEST",
      sku: "SKU-RED-42",
      quantity: "120",
      policyVersion: "v7",
      operatorReason: "",
      proposalAsOf: "2026-10-07T09:50:00Z",
    };
    expect(diff(buildApproveRequest(draft).request, APPROVE_REQUEST)).toEqual([]);
    expect(diff(buildApproveRequest({ ...draft, operatorReason: "west is short" }).request, APPROVE_REQUEST)).toEqual([]);
  });
});

describe("the check itself bites", () => {
  it("rejects the old fictional simulation (asOf + options)", () => {
    expect(diff({ asOf: "2026-10-07T09:50:00Z", options: [] }, SIMULATION)).toEqual([
      'missing required key "advisory"',
      'missing required key "sites"',
      'unexpected key "options" (the service never sends it)',
    ]);
  });

  it("rejects a wrong kind and a Go-cased field", () => {
    expect(diff({ ...transfer(), quantity: "120", Origin: "x" }, TRANSFER)).toEqual([
      'key "quantity" is string, the service sends number',
      'unexpected key "Origin" (the service never sends it)',
    ]);
  });
});

const GO_DIR = resolve(dirname(fileURLToPath(import.meta.url)), "../../../internal/adapters/inbound/http");

function goSource(file: string): string {
  return readFileSync(resolve(GO_DIR, file), "utf8");
}

/** json tag -> omitted-when-empty, from one struct body (embedded structs are followed). */
function tagsOf(src: string, body: string): Map<string, boolean> {
  const tags = new Map<string, boolean>();
  for (const line of body.split("\n")) {
    const tag = /`json:"([^",]+)(,omitempty)?"`/.exec(line);
    if (tag) {
      tags.set(tag[1], tag[2] !== undefined);
      continue;
    }
    const embedded = /^\s*([A-Za-z_]\w*)\s*$/.exec(line);
    if (embedded) {
      const inner = new RegExp(`type ${embedded[1]} struct \\{\\n([\\s\\S]*?)\\n\\}`).exec(src);
      if (!inner) throw new Error(`embedded struct ${embedded[1]} not found`);
      for (const [k, v] of tagsOf(src, inner[1])) tags.set(k, v);
    }
  }
  return tags;
}

describe("realShapes.ts still matches the Go handlers' json tags", () => {
  it.each(GO_SOURCES)("$name", ({ file, struct, anonymousFirstField, shape, optionality }) => {
    const src = goSource(file);
    const m = struct
      ? new RegExp(`type ${struct} struct \\{\\n([\\s\\S]*?)\\n\\}`).exec(src)
      : new RegExp(`struct \\{\\n(\\s*${anonymousFirstField}\\s[\\s\\S]*?)\\n\\s*\\}\\{`).exec(src);
    expect(m, `${struct ?? anonymousFirstField} not found in ${file}`).not.toBeNull();
    const tags = tagsOf(src, (m as RegExpExecArray)[1]);

    expect([...tags.keys()].sort()).toEqual(Object.keys(shape).sort());
    if (optionality) {
      const goOptional = [...tags].filter(([, omitempty]) => omitempty).map(([k]) => k).sort();
      const pinnedOptional = Object.entries(shape).filter(([, spec]) => spec.endsWith("?")).map(([k]) => k).sort();
      expect(goOptional).toEqual(pinnedOptional);
    }
  });
});
