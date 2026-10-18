import { describe, expect, it } from "vitest";
import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router-dom";
import { RebalancePoliciesScreen } from "./RebalancePoliciesScreen";
import { buildPolicyRequest } from "../lib";
import { json, mockApi, pending, problem } from "../test/fetchMock";
import { policy, policyEntry, policyList } from "../test/fixtures";

const LIST = "GET /v1/rebalance-policies";
const PUT = "PUT /v1/rebalance-policies";

function mount() {
  return render(
    <MemoryRouter initialEntries={["/policies"]}>
      <RebalancePoliciesScreen />
    </MemoryRouter>,
  );
}

function rows(): HTMLElement[] {
  return within(screen.getByRole("table")).getAllByRole("row").slice(1);
}

describe("RebalancePoliciesScreen", () => {
  it("shows a loading state", () => {
    mockApi({ [LIST]: pending() });
    mount();
    expect(screen.getByText("Loading rebalance policies…")).toBeInTheDocument();
  });

  it("lists the policies: key, version, levels, priority and revision", async () => {
    mockApi({ [LIST]: json(policyList()) });
    mount();
    await screen.findByRole("table");
    const r = rows();
    expect(r).toHaveLength(2);
    expect(within(r[0]).getAllByRole("cell").map((c) => c.textContent)).toEqual([
      "DC-WEST",
      "SKU-RED-42",
      "v7",
      "60",
      "200",
      "5",
      "4",
      "2026-10-06 07:00Z",
      "Edit",
    ]);
    // zeros are legal and still rendered (untagged Go structs always marshal)
    expect(within(r[1]).getAllByRole("cell").map((c) => c.textContent)).toEqual([
      "DC-WEST",
      "SKU-BLUE-7",
      "v2",
      "0",
      "0",
      "0",
      "1",
      "2026-10-02 07:00Z",
      "Edit",
    ]);
  });

  it("shows an empty state", async () => {
    mockApi({ [LIST]: json({ policies: [] }) });
    mount();
    expect(await screen.findByText(/The policy catalogue is empty/)).toBeInTheDocument();
  });

  it("surfaces the 503 problem of an instance without catalogue persistence", async () => {
    mockApi({
      [LIST]: problem(
        503,
        "catalogue-unavailable",
        "Rebalance catalogue is not configured",
        "this instance runs without the persistence the policy and lane catalogues require",
      ),
    });
    mount();
    const alert = await screen.findByRole("alert");
    expect(alert).toHaveTextContent("Rebalance catalogue is not configured");
    expect(alert).toHaveTextContent("(HTTP 503)");
  });

  it("refuses an empty create form without calling the service", async () => {
    const api = mockApi({ [LIST]: json(policyList()) });
    const user = userEvent.setup();
    mount();
    await screen.findByRole("form", { name: "Create rebalance policy" });
    await user.click(screen.getByRole("button", { name: "Create policy" }));
    const alert = await screen.findByRole("alert");
    expect(alert).toHaveTextContent("Site is required.");
    expect(alert).toHaveTextContent("SKU is required.");
    expect(alert).toHaveTextContent("Policy version is required.");
    expect(api.to(PUT)).toHaveLength(0);
  });

  it("creates a policy: PUT without If-Match, PascalCase body, blanks as zero", async () => {
    const created = policyEntry({
      policy: policy({ Site: "DC-NORTH", SKU: "SKU-GREEN-1", Version: "v1", SafetyStock: 0, TargetStock: 80, UnitPriority: 0 }),
      revision: 1,
      updatedAt: "2026-10-07T10:00:00Z",
      outcome: "CREATED",
    });
    const api = mockApi({ [LIST]: json(policyList()), [PUT]: json(created, 201) });
    const user = userEvent.setup();
    mount();
    const form = await screen.findByRole("form", { name: "Create rebalance policy" });
    const f = within(form);
    await user.type(f.getByLabelText(/^Site/), "DC-NORTH");
    await user.type(f.getByLabelText(/^SKU/), "SKU-GREEN-1");
    await user.type(f.getByLabelText(/^Policy version/), "v1");
    await user.type(f.getByLabelText(/^Target stock/), "80");
    await user.click(f.getByRole("button", { name: "Create policy" }));

    const status = await screen.findByRole("status");
    expect(status).toHaveTextContent("Created DC-NORTH/SKU-GREEN-1 at revision 1.");
    const puts = api.to(PUT);
    expect(puts).toHaveLength(1);
    expect(puts[0].headers["If-Match"]).toBeUndefined();
    expect(puts[0].body).toEqual({
      Version: "v1",
      Site: "DC-NORTH",
      SKU: "SKU-GREEN-1",
      SafetyStock: 0,
      TargetStock: 80,
      UnitPriority: 0,
    });
    await waitFor(() => expect(api.to(LIST)).toHaveLength(2));
  });

  it("edits a policy: key locked, If-Match carries the loaded revision", async () => {
    const updated = policyEntry({
      policy: policy({ TargetStock: 240 }),
      revision: 5,
      updatedAt: "2026-10-07T10:30:00Z",
      outcome: "UPDATED",
    });
    const api = mockApi({ [LIST]: json(policyList()), [PUT]: json(updated) });
    const user = userEvent.setup();
    mount();
    await screen.findByRole("table");
    await user.click(within(rows()[0]).getByRole("button", { name: "Edit policy DC-WEST SKU-RED-42" }));

    const form = await screen.findByRole("form", { name: "Edit rebalance policy" });
    const f = within(form);
    expect(f.getByLabelText(/^Site/)).toBeDisabled();
    expect(f.getByLabelText(/^SKU/)).toBeDisabled();
    expect(f.getByLabelText(/^SKU/)).toHaveValue("SKU-RED-42");
    expect(f.getByLabelText(/^Target stock/)).toHaveValue(200);
    const target = f.getByLabelText(/^Target stock/);
    await user.clear(target);
    await user.type(target, "240");
    await user.click(f.getByRole("button", { name: "Save policy" }));

    const status = await screen.findByRole("status");
    expect(status).toHaveTextContent("Updated DC-WEST/SKU-RED-42 to revision 5.");
    const puts = api.to(PUT);
    expect(puts).toHaveLength(1);
    expect(puts[0].headers["If-Match"]).toBe('"4"');
    expect(puts[0].body).toEqual(policy({ TargetStock: 240 }));
    await waitFor(() => expect(api.to(LIST)).toHaveLength(2));
  });

  it("shows a 422 refusal with the service's own title and detail", async () => {
    // The client mirrors the service's rules, so this payload passes the local
    // checks -- the refusal below is the service's own word (e.g. a rule the
    // client has not caught up with), and must surface verbatim.
    mockApi({
      [LIST]: json(policyList()),
      [PUT]: problem(
        422,
        "invalid-catalogue-entry",
        "Invalid catalogue entry",
        "policy DC-WEST/SKU-RED-42: target stock 50 is below the safety stock 60",
      ),
    });
    const user = userEvent.setup();
    mount();
    await screen.findByRole("table");
    await user.click(within(rows()[0]).getByRole("button", { name: "Edit policy DC-WEST SKU-RED-42" }));
    const target = screen.getByLabelText(/^Target stock/);
    await user.clear(target);
    await user.type(target, "240");
    await user.click(screen.getByRole("button", { name: "Save policy" }));
    const alert = await screen.findByRole("alert");
    expect(alert).toHaveTextContent("The service refused the entry as invalid");
    expect(alert).toHaveTextContent("Invalid catalogue entry");
    expect(alert).toHaveTextContent("target stock 50 is below the safety stock 60");
    expect(screen.getByRole("form", { name: "Edit rebalance policy" })).toBeInTheDocument();
  });

  it("says so when the PUT was an idempotent replay of the stored policy", async () => {
    mockApi({
      [LIST]: json(policyList()),
      [PUT]: json(policyEntry({ outcome: "UNCHANGED" })),
    });
    const user = userEvent.setup();
    mount();
    await screen.findByRole("table");
    await user.click(within(rows()[0]).getByRole("button", { name: "Edit policy DC-WEST SKU-RED-42" }));
    await user.click(screen.getByRole("button", { name: "Save policy" }));
    const status = await screen.findByRole("status");
    expect(status).toHaveTextContent("nothing was written");
  });
});

describe("buildPolicyRequest (client-side mirror of the service's own rules)", () => {
  it("refuses a target below the safety stock before any request is made", () => {
    const out = buildPolicyRequest({ site: "DC-WEST", sku: "SKU-RED-42", version: "v7", safetyStock: "60", targetStock: "50", unitPriority: "" });
    expect(out.request).toBeUndefined();
    expect(out.errors.targetStock).toMatch(/must not be below the safety stock/);
  });

  it("refuses negatives and missing keys", () => {
    expect(buildPolicyRequest({ site: "", sku: "S", version: "v1", safetyStock: "", targetStock: "", unitPriority: "" }).errors.site).toMatch(/required/);
    expect(
      buildPolicyRequest({ site: "A", sku: "S", version: "v1", safetyStock: "-1", targetStock: "10", unitPriority: "" }).errors.safetyStock,
    ).toMatch(/whole number/);
  });

  it("treats blank counts as the zeros the service would store", () => {
    expect(buildPolicyRequest({ site: "A", sku: "S", version: "v1", safetyStock: "", targetStock: "", unitPriority: "" }).request).toEqual({
      Version: "v1",
      Site: "A",
      SKU: "S",
      SafetyStock: 0,
      TargetStock: 0,
      UnitPriority: 0,
    });
  });
});
