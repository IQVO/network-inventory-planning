import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router-dom";
import { SimulationScreen } from "./SimulationScreen";
import { ApiError } from "../api";
import { approveLead } from "../lib";
import { json, mockApi, pending, problem } from "../test/fetchMock";
import { OPTIONS } from "../test/fixtures";

const SIM = { asOf: "2026-10-07T09:50:00Z", options: OPTIONS };

function mount() {
  return render(
    <MemoryRouter initialEntries={["/simulation"]}>
      <SimulationScreen />
    </MemoryRouter>,
  );
}

const APPROVED = {
  transferId: "t-2001",
  state: "ALLOCATING",
  originSiteId: "DC-EAST",
  destinationSiteId: "DC-WEST",
  sku: "SKU-RED-42",
  quantity: 120,
  policyVersion: "v7",
  replayed: false,
  expiresAt: "2026-10-08T10:00:00Z",
  transferLineId: "t-2001:1",
};

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
    mockApi({ "GET /v1/transfer-simulations": pending() });
    mount();
    expect(screen.getByText("Loading simulation…")).toBeInTheDocument();
  });

  it("renders freshness, the per-site effect and the options", async () => {
    mockApi({ "GET /v1/transfer-simulations": json(SIM) });
    mount();
    expect(await screen.findByText("10m behind")).toBeInTheDocument();
    expect(screen.getByText("2026-10-07 09:50Z")).toBeInTheDocument();

    const sites = screen.getAllByRole("table")[0];
    const rows = within(sites).getAllByRole("row").slice(1);
    expect(rows).toHaveLength(3);
    expect(within(rows[0]).getAllByRole("cell").map((c) => c.textContent)).toEqual(["DC-EAST", "120", "0", "-120", "1"]);
    expect(within(rows[2]).getAllByRole("cell").map((c) => c.textContent)).toEqual(["DC-WEST", "0", "150", "+150", "2"]);

    const options = screen.getAllByRole("table")[1];
    const opt = within(within(options).getAllByRole("row")[1]);
    expect(opt.getByText("DC-EAST → DC-WEST")).toBeInTheDocument();
    expect(opt.getByText("DESTINATION_BELOW_TARGET, APPROVED_LANE")).toBeInTheDocument();
    expect(opt.getByText("85")).toBeInTheDocument(); // 100 - 10 - 5
  });

  it("explains a fail-closed 503 as read models not ready, with the problem's title and detail", async () => {
    mockApi({
      "GET /v1/transfer-simulations": problem(503, "read-models-not-ready", "Read models not ready", "site capability facts are stale"),
    });
    mount();
    const alert = await screen.findByRole("alert");
    expect(alert).toHaveTextContent("Read models not ready — the simulation is fail-closed");
    expect(alert).toHaveTextContent("(HTTP 503)");
    expect(alert).toHaveTextContent("site capability facts are stale");
    expect(screen.queryByRole("table")).not.toBeInTheDocument();
  });

  it("distinguishes 'no option advisable' from an error", async () => {
    mockApi({ "GET /v1/transfer-simulations": json({ asOf: SIM.asOf, options: [] }) });
    mount();
    expect(await screen.findByText(/no transfer is advisable right now/)).toBeInTheDocument();
    expect(screen.queryByRole("alert")).not.toBeInTheDocument();
  });

  it("approves an option: POST with an Idempotency-Key, the option's snapshot and the operator reason", async () => {
    const api = mockApi({
      "GET /v1/transfer-simulations": json(SIM),
      "POST /v1/transfers:approve": json(APPROVED),
    });
    const user = userEvent.setup();
    mount();
    await user.click(await screen.findByRole("button", { name: "Approve 120 SKU-RED-42 from DC-EAST to DC-WEST" }));
    const form = screen.getByRole("form", { name: "Approve transfer" });
    await user.type(within(form).getByLabelText("Operator reason"), "west is below target");
    await user.click(within(form).getByRole("button", { name: "Confirm approval" }));

    const notice = await screen.findByRole("status");
    expect(notice).toHaveTextContent("Approved.");
    expect(notice).toHaveTextContent("is ALLOCATING");
    expect(within(notice).getByRole("link", { name: "t-2001" })).toHaveAttribute("href", "/transfers/t-2001");

    const post = api.to("POST /v1/transfers:approve");
    expect(post).toHaveLength(1);
    expect(post[0].headers["Idempotency-Key"]).toMatch(/\S+/);
    expect(post[0].body).toEqual({
      originSiteId: "DC-EAST",
      destinationSiteId: "DC-WEST",
      sku: "SKU-RED-42",
      quantity: 120,
      policyVersion: "v7",
      proposalAsOf: "2026-10-07T09:55:00Z",
      operatorReason: "west is below target",
    });
    expect(screen.queryByRole("form", { name: "Approve transfer" })).not.toBeInTheDocument();
  });

  it("omits operatorReason when none is typed", async () => {
    const api = mockApi({ "GET /v1/transfer-simulations": json(SIM), "POST /v1/transfers:approve": json(APPROVED) });
    const user = userEvent.setup();
    mount();
    await user.click(await screen.findByRole("button", { name: /^Approve 120/ }));
    await user.click(screen.getByRole("button", { name: "Confirm approval" }));
    await screen.findByRole("status");
    expect(api.to("POST /v1/transfers:approve")[0].body).not.toHaveProperty("operatorReason");
  });

  it("says so when the key was replayed (the original transfer is shown)", async () => {
    mockApi({
      "GET /v1/transfer-simulations": json(SIM),
      "POST /v1/transfers:approve": json({ ...APPROVED, replayed: true }),
    });
    const user = userEvent.setup();
    mount();
    await user.click(await screen.findByRole("button", { name: /^Approve 120/ }));
    await user.click(screen.getByRole("button", { name: "Confirm approval" }));
    expect(await screen.findByRole("status")).toHaveTextContent("already recorded; showing the original transfer");
  });

  it("shows a 422 fail-closed validation problem with title and detail, and keeps the form open", async () => {
    mockApi({
      "GET /v1/transfer-simulations": json(SIM),
      "POST /v1/transfers:approve": problem(422, "proposal-stale", "Proposal no longer valid", "destination lane is disabled"),
    });
    const user = userEvent.setup();
    mount();
    await user.click(await screen.findByRole("button", { name: /^Approve 120/ }));
    await user.click(screen.getByRole("button", { name: "Confirm approval" }));
    const alert = await screen.findByRole("alert");
    expect(alert).toHaveTextContent("no longer passes validation");
    expect(alert).toHaveTextContent("Proposal no longer valid");
    expect(alert).toHaveTextContent("destination lane is disabled");
    expect(screen.getByRole("form", { name: "Approve transfer" })).toBeInTheDocument();
  });

  it("shows a 503 config-incomplete problem and retries with the SAME Idempotency-Key", async () => {
    const api = mockApi({
      "GET /v1/transfer-simulations": json(SIM),
      "POST /v1/transfers:approve": problem(503, "config-incomplete", "Configuration incomplete", "no allocation topic configured"),
    });
    const user = userEvent.setup();
    mount();
    await user.click(await screen.findByRole("button", { name: /^Approve 120/ }));
    await user.click(screen.getByRole("button", { name: "Confirm approval" }));
    const alert = await screen.findByRole("alert");
    expect(alert).toHaveTextContent("Configuration incomplete");
    expect(alert).toHaveTextContent("no allocation topic configured");
    expect(alert).toHaveTextContent("retry the same approval safely");

    api.set("POST /v1/transfers:approve", json(APPROVED));
    await user.click(screen.getByRole("button", { name: "Confirm approval" }));
    await screen.findByText(/Approved\./);
    const posts = api.to("POST /v1/transfers:approve");
    expect(posts).toHaveLength(2);
    expect(posts[1].headers["Idempotency-Key"]).toBe(posts[0].headers["Idempotency-Key"]);
  });

  it("uses a fresh key for a different option", async () => {
    const api = mockApi({
      "GET /v1/transfer-simulations": json(SIM),
      "POST /v1/transfers:approve": problem(503, "config-incomplete", "Configuration incomplete", "x"),
    });
    const user = userEvent.setup();
    mount();
    await user.click(await screen.findByRole("button", { name: /^Approve 120/ }));
    await user.click(screen.getByRole("button", { name: "Confirm approval" }));
    await screen.findByRole("alert");
    await user.click(screen.getByRole("button", { name: /^Approve 30/ }));
    await user.click(screen.getByRole("button", { name: "Confirm approval" }));
    await waitFor(() => expect(api.to("POST /v1/transfers:approve")).toHaveLength(2));
    const [a, b] = api.to("POST /v1/transfers:approve");
    expect(b.headers["Idempotency-Key"]).not.toBe(a.headers["Idempotency-Key"]);
    expect((b.body as { sku: string }).sku).toBe("SKU-BLUE-7");
  });

  it("cancel closes the approval without sending anything", async () => {
    const api = mockApi({ "GET /v1/transfer-simulations": json(SIM) });
    const user = userEvent.setup();
    mount();
    await user.click(await screen.findByRole("button", { name: /^Approve 120/ }));
    await user.click(screen.getByRole("button", { name: "Cancel" }));
    expect(screen.queryByRole("form", { name: "Approve transfer" })).not.toBeInTheDocument();
    expect(api.to("POST /v1/transfers:approve")).toHaveLength(0);
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
