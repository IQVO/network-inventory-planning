import { describe, expect, it } from "vitest";
import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router-dom";
import { TransferLanesScreen } from "./TransferLanesScreen";
import { buildLaneRequest } from "../lib";
import { json, mockApi, pending, problem } from "../test/fetchMock";
import { lane, laneEntry, laneList } from "../test/fixtures";

const LIST = "GET /v1/transfer-lanes";
const PUT = "PUT /v1/transfer-lanes";

function mount() {
  return render(
    <MemoryRouter initialEntries={["/lanes"]}>
      <TransferLanesScreen />
    </MemoryRouter>,
  );
}

function rows(): HTMLElement[] {
  return within(screen.getByRole("table")).getAllByRole("row").slice(1);
}

describe("TransferLanesScreen", () => {
  it("shows a loading state", () => {
    mockApi({ [LIST]: pending() });
    mount();
    expect(screen.getByText("Loading transfer lanes…")).toBeInTheDocument();
  });

  it("lists the directed lanes with lead time in hours, cost, status and revision", async () => {
    mockApi({ [LIST]: json(laneList()) });
    mount();
    await screen.findByRole("table");
    const r = rows();
    expect(r).toHaveLength(2);
    expect(within(r[0]).getAllByRole("cell").map((c) => c.textContent)).toEqual([
      "DC-EAST → DC-WEST",
      "18 h",
      "3",
      "ENABLED",
      "2",
      "2026-10-06 08:00Z",
      "Edit",
    ]);
    expect(within(r[1]).getAllByRole("cell").map((c) => c.textContent)).toEqual([
      "DC-WEST → DC-NORTH",
      "0.25 h",
      "0",
      "DISABLED",
      "1",
      "2026-10-05 08:00Z",
      "Edit",
    ]);
    expect(within(r[0]).getByText("ENABLED")).toHaveAttribute("data-tone", "success");
    expect(within(r[1]).getByText("DISABLED")).toHaveAttribute("data-tone", "neutral");
  });

  it("shows an empty state", async () => {
    mockApi({ [LIST]: json({ lanes: [] }) });
    mount();
    expect(await screen.findByText(/The lane catalogue is empty/)).toBeInTheDocument();
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
    expect(alert).toHaveTextContent("without the persistence");
    expect(alert).toHaveTextContent("(HTTP 503)");
  });

  it("refuses an empty create form without calling the service, and names what is wrong", async () => {
    const api = mockApi({ [LIST]: json(laneList()) });
    const user = userEvent.setup();
    mount();
    await screen.findByRole("form", { name: "Create transfer lane" });
    await user.click(screen.getByRole("button", { name: "Create lane" }));
    const alert = await screen.findByRole("alert");
    expect(alert).toHaveTextContent("Origin site is required.");
    expect(alert).toHaveTextContent("Destination site is required.");
    expect(api.to(PUT)).toHaveLength(0);
  });

  it("creates a lane: PUT without If-Match, PascalCase body, hours as nanoseconds", async () => {
    const created = laneEntry({
      lane: lane({ Origin: "DC-NORTH", Destination: "DC-EAST", LeadTime: 5400000000000, UnitHandlingCost: 2 }),
      revision: 1,
      updatedAt: "2026-10-07T09:00:00Z",
      outcome: "CREATED",
    });
    const api = mockApi({ [LIST]: json(laneList()), [PUT]: json(created, 201) });
    const user = userEvent.setup();
    mount();
    const form = await screen.findByRole("form", { name: "Create transfer lane" });
    const f = within(form);
    await user.type(f.getByLabelText(/^Origin site/), "DC-NORTH");
    await user.type(f.getByLabelText(/^Destination site/), "DC-EAST");
    await user.type(f.getByLabelText(/^Lead time \(hours\)/), "1.5");
    await user.type(f.getByLabelText(/^Unit handling cost/), "2");
    await user.click(f.getByRole("button", { name: "Create lane" }));

    const status = await screen.findByRole("status");
    expect(status).toHaveTextContent("Created DC-NORTH → DC-EAST at revision 1.");
    const puts = api.to(PUT);
    expect(puts).toHaveLength(1);
    expect(puts[0].headers["If-Match"]).toBeUndefined();
    expect(puts[0].headers["Content-Type"]).toBe("application/json");
    expect(puts[0].body).toEqual({
      Origin: "DC-NORTH",
      Destination: "DC-EAST",
      LeadTime: 5400000000000,
      UnitHandlingCost: 2,
      Enabled: true,
    });
    // the list refreshed
    await waitFor(() => expect(api.to(LIST)).toHaveLength(2));
  });

  it("edits a lane: key fields locked, If-Match carries the loaded revision", async () => {
    const updated = laneEntry({
      lane: lane({ LeadTime: 86400000000000 }),
      revision: 3,
      updatedAt: "2026-10-07T09:30:00Z",
      outcome: "UPDATED",
    });
    const api = mockApi({ [LIST]: json(laneList()), [PUT]: json(updated) });
    const user = userEvent.setup();
    mount();
    await screen.findByRole("table");
    await user.click(within(rows()[0]).getByRole("button", { name: "Edit lane DC-EAST to DC-WEST" }));

    const form = await screen.findByRole("form", { name: "Edit transfer lane" });
    const f = within(form);
    expect(f.getByLabelText(/^Origin site/)).toBeDisabled();
    expect(f.getByLabelText(/^Origin site/)).toHaveValue("DC-EAST");
    expect(f.getByLabelText(/^Lead time \(hours\)/)).toHaveValue(18);
    const lead = f.getByLabelText(/^Lead time \(hours\)/);
    await user.clear(lead);
    await user.type(lead, "24");
    await user.selectOptions(f.getByLabelText("Lane status"), "no");
    await user.click(f.getByRole("button", { name: "Save lane" }));

    const status = await screen.findByRole("status");
    expect(status).toHaveTextContent("Updated DC-EAST → DC-WEST to revision 3.");
    const puts = api.to(PUT);
    expect(puts).toHaveLength(1);
    expect(puts[0].headers["If-Match"]).toBe('"2"');
    expect(puts[0].body).toEqual({
      Origin: "DC-EAST",
      Destination: "DC-WEST",
      LeadTime: 86400000000000,
      UnitHandlingCost: 3,
      Enabled: false,
    });
    await waitFor(() => expect(api.to(LIST)).toHaveLength(2));
  });

  it("shows a 422 refusal with the service's own title and detail, and keeps the form", async () => {
    mockApi({
      [LIST]: json(laneList()),
      [PUT]: problem(422, "invalid-catalogue-entry", "Invalid catalogue entry", "lane DC-EAST→DC-EAST: origin and destination must differ"),
    });
    const user = userEvent.setup();
    mount();
    await screen.findByRole("table");
    await user.click(within(rows()[0]).getByRole("button", { name: "Edit lane DC-EAST to DC-WEST" }));
    await user.click(screen.getByRole("button", { name: "Save lane" }));
    const alert = await screen.findByRole("alert");
    expect(alert).toHaveTextContent("The service refused the entry as invalid");
    expect(alert).toHaveTextContent("Invalid catalogue entry");
    expect(alert).toHaveTextContent("origin and destination must differ");
    expect(screen.getByRole("form", { name: "Edit transfer lane" })).toBeInTheDocument();
  });

  it("explains a 412 revision conflict as a moved-on entry and refreshes the list", async () => {
    const api = mockApi({
      [LIST]: json(laneList()),
      [PUT]: problem(412, "version-mismatch", "If-Match does not match the current revision", "re-fetch the entry and retry with its current ETag"),
    });
    const user = userEvent.setup();
    mount();
    await screen.findByRole("table");
    await user.click(within(rows()[0]).getByRole("button", { name: "Edit lane DC-EAST to DC-WEST" }));
    await user.click(screen.getByRole("button", { name: "Save lane" }));
    const alert = await screen.findByRole("alert");
    expect(alert).toHaveTextContent("The entry moved on since it was loaded");
    expect(alert).toHaveTextContent("If-Match does not match the current revision");
    await waitFor(() => expect(api.to(LIST)).toHaveLength(2));
  });

  it("says so when the PUT was an idempotent replay of the stored lane", async () => {
    mockApi({
      [LIST]: json(laneList()),
      [PUT]: json(laneEntry({ outcome: "UNCHANGED" })),
    });
    const user = userEvent.setup();
    mount();
    await screen.findByRole("table");
    await user.click(within(rows()[0]).getByRole("button", { name: "Edit lane DC-EAST to DC-WEST" }));
    await user.click(screen.getByRole("button", { name: "Save lane" }));
    const status = await screen.findByRole("status");
    expect(status).toHaveTextContent("unchanged (the PUT carried exactly the stored entry; nothing was written)");
  });
});

describe("buildLaneRequest (client-side mirror of the service's own rules)", () => {
  it("refuses a lane whose origin equals its destination, and negative inputs", () => {
    expect(buildLaneRequest({ origin: "DC-EAST", destination: "DC-EAST", leadTimeHours: "1", unitHandlingCost: "0", enabled: "yes" }).errors.destination).toMatch(
      /must differ/,
    );
    expect(buildLaneRequest({ origin: "A", destination: "B", leadTimeHours: "-1", unitHandlingCost: "0", enabled: "yes" }).errors.leadTimeHours).toMatch(
      /zero or more/,
    );
    expect(buildLaneRequest({ origin: "A", destination: "B", leadTimeHours: "1", unitHandlingCost: "-2", enabled: "yes" }).errors.unitHandlingCost).toMatch(
      /whole number/,
    );
  });

  it("converts hours to the wire's nanoseconds", () => {
    expect(buildLaneRequest({ origin: "A", destination: "B", leadTimeHours: "0.25", unitHandlingCost: "", enabled: "no" }).request).toEqual({
      Origin: "A",
      Destination: "B",
      LeadTime: 900000000000,
      UnitHandlingCost: 0,
      Enabled: false,
    });
  });
});
