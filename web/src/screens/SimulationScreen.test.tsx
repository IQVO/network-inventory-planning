import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router-dom";
import { SimulationScreen } from "./SimulationScreen";
import { ApiError } from "../api";
import { approveLead } from "../lib";
import { fillApproval } from "../test/approval";
import { json, mockApi, pending, problem } from "../test/fetchMock";
import { approved, simulation } from "../test/fixtures";

const SIM = simulation();
const APPROVE = "POST /v1/transfers:approve";
const SIMULATE = "GET /v1/transfer-simulations";

function mount() {
  return render(
    <MemoryRouter initialEntries={["/simulation"]}>
      <SimulationScreen />
    </MemoryRouter>,
  );
}

function optionTexts(select: HTMLElement): string[] {
  return within(select).getAllByRole("option").map((o) => o.textContent ?? "");
}

async function confirm(user: ReturnType<typeof userEvent.setup>) {
  await user.click(screen.getByRole("button", { name: "Confirm approval" }));
}

describe("SimulationScreen", () => {
  beforeEach(() => {
    // Only Date is faked so waitFor/async keep working.
    vi.useFakeTimers({ toFake: ["Date"] });
    vi.setSystemTime(new Date("2026-10-07T10:00:00Z"));
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  it("shows a loading state", () => {
    mockApi({ [SIMULATE]: pending() });
    mount();
    expect(screen.getByText("Loading simulation…")).toBeInTheDocument();
  });

  it("renders the advisory banner, freshness and the sites: short first, headroom highlighted", async () => {
    mockApi({ [SIMULATE]: json(SIM) });
    mount();
    expect(await screen.findByText("10m behind")).toBeInTheDocument();
    expect(screen.getByText("2026-10-07 09:50Z")).toBeInTheDocument();
    expect(screen.getByText(/Advisory — moves nothing\./)).toBeInTheDocument();

    const rows = within(screen.getByRole("table")).getAllByRole("row").slice(1);
    expect(rows.map((r) => within(r).getAllByRole("cell")[0].textContent)).toEqual(["DC-WEST", "DC-SOUTH", "DC-EAST", "DC-NORTH"]);
    expect(within(rows[0]).getAllByRole("cell").map((c) => c.textContent)).toEqual([
      "DC-WEST",
      "500",
      "350",
      "-150",
      "SHORT",
      "no",
      "yes",
      "2026-10-07 00:00Z → 2026-10-14 00:00Z",
    ]);
    // short sites carry the highlight, covered ones do not
    expect(within(rows[0]).getByText("-150")).toHaveAttribute("data-short", "true");
    expect(within(rows[1]).getByText("-40")).toHaveAttribute("data-short", "true");
    expect(within(rows[2]).getByText("120")).not.toHaveAttribute("data-short");
    expect(within(rows[2]).getByText("COVERED")).toBeInTheDocument();
    expect(within(rows[3]).getAllByRole("cell").map((c) => c.textContent).slice(5, 7)).toEqual(["yes", "no"]);
  });

  it("proposes nothing: there is no option list and no per-row Approve button", async () => {
    mockApi({ [SIMULATE]: json(SIM) });
    mount();
    await screen.findByRole("table");
    expect(screen.queryByRole("button", { name: /^Approve \d/ })).not.toBeInTheDocument();
    expect(screen.queryByText(/Options \(best score first\)/)).not.toBeInTheDocument();
  });

  it("explains a fail-closed 503 as read models not ready, with the problem's title and detail", async () => {
    mockApi({
      [SIMULATE]: problem(503, "read-models-incomplete", "Planning read models are incomplete or stale", "site capability facts are stale"),
    });
    mount();
    const alert = await screen.findByRole("alert");
    expect(alert).toHaveTextContent("Read models not ready — the simulation is fail-closed");
    expect(alert).toHaveTextContent("Planning read models are incomplete or stale");
    expect(alert).toHaveTextContent("(HTTP 503)");
    expect(alert).toHaveTextContent("site capability facts are stale");
    expect(screen.queryByRole("table")).not.toBeInTheDocument();
    expect(screen.queryByRole("form", { name: "Approve transfer" })).not.toBeInTheDocument();
  });

  it("does not present an empty site list as a balanced network", async () => {
    mockApi({ [SIMULATE]: json(simulation({ sites: [] })) });
    mount();
    expect(await screen.findByText(/answered without any site/)).toBeInTheDocument();
    expect(screen.queryByRole("table")).not.toBeInTheDocument();
  });

  describe("the Approve form", () => {
    it("is operator-driven: origin lists origin-enabled sites, destination lists destination-enabled sites other than the origin", async () => {
      mockApi({ [SIMULATE]: json(SIM) });
      const user = userEvent.setup();
      mount();
      const form = await screen.findByRole("form", { name: "Approve transfer" });
      const origin = within(form).getByLabelText(/^Origin site/);
      const destination = within(form).getByLabelText(/^Destination site/);
      expect(optionTexts(origin)).toEqual(["Select…", "DC-EAST (headroom 120)", "DC-NORTH (headroom 30)"]);
      expect(optionTexts(destination)).toEqual(["Select…", "DC-WEST (short by 150)", "DC-SOUTH (short by 40)", "DC-EAST (headroom 120)"]);
      await user.selectOptions(origin, "DC-EAST");
      expect(optionTexts(destination)).toEqual(["Select…", "DC-WEST (short by 150)", "DC-SOUTH (short by 40)"]);
      // nothing is suggested: no pre-filled site, SKU, quantity or policy version; as-of defaults to the simulation's
      expect(destination).toHaveValue("");
      expect(within(form).getByLabelText(/^SKU/)).toHaveValue("");
      expect(within(form).getByLabelText(/^Quantity/)).toHaveValue(null);
      expect(within(form).getByLabelText(/^Policy version/)).toHaveValue("");
      expect(within(form).getByLabelText(/^Proposal as of/)).toHaveValue("2026-10-07T09:50:00Z");
    });

    it("clears the destination when the origin is switched to it", async () => {
      mockApi({ [SIMULATE]: json(simulation({ sites: [{ ...SIM.sites[0] }, { ...SIM.sites[1], destinationEnabled: true }] })) });
      const user = userEvent.setup();
      mount();
      const form = await screen.findByRole("form", { name: "Approve transfer" });
      await user.selectOptions(within(form).getByLabelText(/^Origin site/), "DC-EAST");
      await user.selectOptions(within(form).getByLabelText(/^Destination site/), "DC-NORTH");
      await user.selectOptions(within(form).getByLabelText(/^Origin site/), "DC-NORTH");
      expect(within(form).getByLabelText(/^Destination site/)).toHaveValue("");
    });

    it("POSTs the typed request with an Idempotency-Key, and leaves operatorReason out when none is typed", async () => {
      const api = mockApi({ [SIMULATE]: json(SIM), [APPROVE]: json(approved()) });
      const user = userEvent.setup();
      mount();
      await fillApproval(user);
      await confirm(user);

      const notice = await screen.findByRole("status");
      expect(notice).toHaveTextContent("Approved.");
      expect(notice).toHaveTextContent("is ALLOCATING");
      expect(within(notice).getByRole("link", { name: "t-2001" })).toHaveAttribute("href", "/transfers/t-2001");
      const post = api.to(APPROVE);
      expect(post).toHaveLength(1);
      expect(post[0].headers["Idempotency-Key"]).toMatch(/\S+/);
      expect(post[0].body).toEqual({
        originSiteId: "DC-EAST",
        destinationSiteId: "DC-WEST",
        sku: "SKU-RED-42",
        quantity: 120,
        policyVersion: "v7",
        proposalAsOf: "2026-10-07T09:50:00Z",
      });
    });

    it("sends the operator reason and an edited proposalAsOf", async () => {
      const api = mockApi({ [SIMULATE]: json(SIM), [APPROVE]: json(approved()) });
      const user = userEvent.setup();
      mount();
      const form = await fillApproval(user, { reason: "west is below target" });
      const asOf = within(form).getByLabelText(/^Proposal as of/);
      await user.clear(asOf);
      await user.type(asOf, "2026-10-07T09:40:00Z");
      await confirm(user);
      await screen.findByRole("status");
      expect(api.to(APPROVE)[0].body).toMatchObject({ operatorReason: "west is below target", proposalAsOf: "2026-10-07T09:40:00Z" });
    });

    it("refuses an incomplete form without calling the service, and names what is wrong", async () => {
      const api = mockApi({ [SIMULATE]: json(SIM), [APPROVE]: json(approved()) });
      const user = userEvent.setup();
      mount();
      const form = await fillApproval(user, { quantity: "0", policyVersion: "x" });
      await user.clear(within(form).getByLabelText(/^Policy version/));
      await confirm(user);
      const alert = await screen.findByRole("alert");
      expect(alert).toHaveTextContent("Quantity must be a positive whole number.");
      expect(alert).toHaveTextContent("Policy version is required.");
      expect(api.to(APPROVE)).toHaveLength(0);
    });

    it("says so when the key was replayed (the original transfer is shown)", async () => {
      mockApi({ [SIMULATE]: json(SIM), [APPROVE]: json(approved({ replayed: true })) });
      const user = userEvent.setup();
      mount();
      await fillApproval(user);
      await confirm(user);
      expect(await screen.findByRole("status")).toHaveTextContent("already recorded; showing the original transfer");
    });

    it("shows a 422 fail-closed refusal with title and detail, and keeps the form open", async () => {
      mockApi({
        [SIMULATE]: json(SIM),
        [APPROVE]: problem(
          422,
          "facts-incomplete",
          "Planning facts are incomplete or stale for approval",
          "transfer: planning facts incomplete for approval: destination site DC-WEST shows no in-window demand for SKU SKU-RED-42",
        ),
      });
      const user = userEvent.setup();
      mount();
      await fillApproval(user);
      await confirm(user);
      const alert = await screen.findByRole("alert");
      expect(alert).toHaveTextContent("no longer passes validation");
      expect(alert).toHaveTextContent("Planning facts are incomplete or stale for approval");
      expect(alert).toHaveTextContent("shows no in-window demand for SKU SKU-RED-42");
      expect(screen.getByRole("form", { name: "Approve transfer" })).toBeInTheDocument();
    });

    it("shows a 400 problem as the service words it", async () => {
      mockApi({ [SIMULATE]: json(SIM), [APPROVE]: problem(400, "invalid-request", "Invalid request", 'json: unknown field "x"') });
      const user = userEvent.setup();
      mount();
      await fillApproval(user);
      await confirm(user);
      const alert = await screen.findByRole("alert");
      expect(alert).toHaveTextContent("Invalid request");
      expect(alert).toHaveTextContent('json: unknown field "x"');
    });

    it("shows a 503 config-incomplete problem and retries identical input with the SAME Idempotency-Key", async () => {
      const api = mockApi({
        [SIMULATE]: json(SIM),
        [APPROVE]: problem(503, "config-incomplete", "Work release is not configured", "set TRANSFER_PICK_PATH_ID and TRANSFER_PICK_CPT_OFFSET"),
      });
      const user = userEvent.setup();
      mount();
      await fillApproval(user);
      await confirm(user);
      const alert = await screen.findByRole("alert");
      expect(alert).toHaveTextContent("Work release is not configured");
      expect(alert).toHaveTextContent("TRANSFER_PICK_PATH_ID");
      expect(alert).toHaveTextContent("retry the same approval safely");

      api.set(APPROVE, json(approved()));
      await confirm(user);
      await screen.findByText(/Approved\./);
      const posts = api.to(APPROVE);
      expect(posts).toHaveLength(2);
      expect(posts[1].headers["Idempotency-Key"]).toBe(posts[0].headers["Idempotency-Key"]);
    });

    it("mints a new Idempotency-Key when any field changes", async () => {
      const api = mockApi({ [SIMULATE]: json(SIM), [APPROVE]: problem(503, "approval-unavailable", "Approval could not be completed", "x") });
      const user = userEvent.setup();
      mount();
      const form = await fillApproval(user);
      await confirm(user);
      await screen.findByRole("alert");
      await user.type(within(form).getByLabelText(/^Quantity/), "5");
      await confirm(user);
      await waitFor(() => expect(api.to(APPROVE)).toHaveLength(2));
      const [a, b] = api.to(APPROVE);
      expect(b.headers["Idempotency-Key"]).not.toBe(a.headers["Idempotency-Key"]);
      expect((b.body as { quantity: number }).quantity).toBe(1205);
    });

    it("starts the next approval from a clean form with a fresh key", async () => {
      const api = mockApi({ [SIMULATE]: json(SIM), [APPROVE]: json(approved()) });
      const user = userEvent.setup();
      mount();
      await fillApproval(user);
      await confirm(user);
      await screen.findByRole("status");
      expect(screen.getByLabelText(/^SKU/)).toHaveValue("");
      await fillApproval(user);
      await confirm(user);
      await waitFor(() => expect(api.to(APPROVE)).toHaveLength(2));
      const [a, b] = api.to(APPROVE);
      expect(b.headers["Idempotency-Key"]).not.toBe(a.headers["Idempotency-Key"]);
    });
  });
});

describe("approveLead", () => {
  it("has guidance for 409, 422 and 503 and none for other statuses", () => {
    expect(approveLead(new ApiError(409, null, "x"))).toMatch(/idempotency key was already used/);
    expect(approveLead(new ApiError(422, null, "x"))).toMatch(/no longer passes validation/);
    expect(approveLead(new ApiError(503, null, "x"))).toMatch(/retry the same approval safely/);
    expect(approveLead(new ApiError(400, null, "x"))).toBeUndefined();
  });
});
