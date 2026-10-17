import { describe, expect, it } from "vitest";
import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router-dom";
import { ProposalsScreen } from "./ProposalsScreen";
import { buildGenerateRequest } from "../lib";
import { json, mockApi, problem } from "../test/fetchMock";
import { SNAPSHOT_AS_OF, generateResponse, lane, policy, position } from "../test/fixtures";

const POST = "POST /v1/transfer-proposals:generate";

function mount() {
  return render(
    <MemoryRouter initialEntries={["/proposals"]}>
      <ProposalsScreen />
    </MemoryRouter>,
  );
}

/** Fills the minimal snapshot: as-of, one position, one policy, one lane. */
async function fillSnapshot(user: ReturnType<typeof userEvent.setup>) {
  await user.type(screen.getByLabelText(/^Snapshot as of/), SNAPSHOT_AS_OF);

  const position1 = within(screen.getByRole("group", { name: "position 1" }));
  await user.type(position1.getByLabelText(/^Site/), "DC-EAST");
  await user.type(position1.getByLabelText(/^SKU/), "SKU-RED-42");
  await user.type(position1.getByLabelText(/^Available/), "500");
  await user.type(position1.getByLabelText(/Customer reservations/), "20");
  await user.type(position1.getByLabelText(/Committed outbound/), "30");

  const policy1 = within(screen.getByRole("group", { name: "policy 1" }));
  await user.type(policy1.getByLabelText(/^Site/), "DC-WEST");
  await user.type(policy1.getByLabelText(/^SKU/), "SKU-RED-42");
  await user.type(policy1.getByLabelText(/^Policy version/), "v7");
  await user.type(policy1.getByLabelText(/^Safety stock/), "60");
  await user.type(policy1.getByLabelText(/^Target stock/), "200");
  await user.type(policy1.getByLabelText(/^Unit priority/), "5");

  const lane1 = within(screen.getByRole("group", { name: "lane 1" }));
  await user.type(lane1.getByLabelText(/^Origin site/), "DC-EAST");
  await user.type(lane1.getByLabelText(/^Destination site/), "DC-WEST");
  await user.type(lane1.getByLabelText(/^Lead time \(hours\)/), "18");
  await user.type(lane1.getByLabelText(/^Unit handling cost/), "3");
}

describe("ProposalsScreen", () => {
  it("renders the form and fetches nothing on mount (the screen is POST-only)", () => {
    const api = mockApi({});
    mount();
    expect(screen.getByRole("form", { name: "Generate proposals" })).toBeInTheDocument();
    expect(screen.getByRole("group", { name: "position 1" })).toBeInTheDocument();
    expect(screen.getByRole("group", { name: "policy 1" })).toBeInTheDocument();
    expect(screen.getByRole("group", { name: "lane 1" })).toBeInTheDocument();
    expect(api.calls).toHaveLength(0);
  });

  it("refuses an empty snapshot without calling the service, and names the rows", async () => {
    const api = mockApi({});
    const user = userEvent.setup();
    mount();
    await user.click(screen.getByRole("button", { name: "Generate proposals" }));
    const alert = await screen.findByRole("alert");
    expect(alert).toHaveTextContent("As-of must be an RFC 3339 timestamp");
    expect(alert).toHaveTextContent("Position 1: Site is required.");
    expect(alert).toHaveTextContent("Policy 1: Policy version is required.");
    expect(alert).toHaveTextContent("Lane 1: Origin site is required.");
    expect(api.calls).toHaveLength(0);
  });

  it("adds and removes snapshot rows", async () => {
    mockApi({});
    const user = userEvent.setup();
    mount();
    await user.click(screen.getByRole("button", { name: "Add position" }));
    expect(screen.getAllByRole("group", { name: /position \d/ })).toHaveLength(2);
    await user.click(screen.getByRole("button", { name: "Remove position 2" }));
    expect(screen.getAllByRole("group", { name: /position \d/ })).toHaveLength(1);
    await user.click(screen.getByRole("button", { name: "Remove position 1" }));
    expect(screen.queryByRole("group", { name: /position \d/ })).not.toBeInTheDocument();
  });

  it("POSTs the exact contract body with an Idempotency-Key header, positions stamped with the as-of", async () => {
    const api = mockApi({ [POST]: json(generateResponse()) });
    const user = userEvent.setup();
    mount();
    await fillSnapshot(user);
    await user.click(screen.getByRole("button", { name: "Generate proposals" }));

    await screen.findByRole("table");
    const posts = api.to(POST);
    expect(posts).toHaveLength(1);
    expect(posts[0].headers["Idempotency-Key"]).toMatch(/\S+/);
    expect(posts[0].headers["Content-Type"]).toBe("application/json");
    expect(posts[0].body).toEqual({
      asOf: SNAPSHOT_AS_OF,
      positions: [position()],
      policies: [policy()],
      lanes: [lane()],
    });
  });

  it("renders what the answer contains: count, the score-ordered moves, reasons and breakdown", async () => {
    mockApi({ [POST]: json(generateResponse()) });
    const user = userEvent.setup();
    mount();
    await fillSnapshot(user);
    await user.click(screen.getByRole("button", { name: "Generate proposals" }));

    await screen.findByRole("table");
    expect(screen.getByText("Proposals returned")).toBeInTheDocument();
    const rows = within(screen.getByRole("table")).getAllByRole("row").slice(1);
    expect(rows).toHaveLength(2);
    expect(within(rows[0]).getAllByRole("cell").map((c) => c.textContent)).toEqual([
      "DC-EAST → DC-WEST",
      "SKU-RED-42",
      "140",
      "v7",
      "2026-10-07 09:50Z",
      "DESTINATION_BELOW_TARGET, ORIGIN_ABOVE_SAFETY_STOCK, APPROVED_LANE",
      "700",
      "3",
      "18",
      "679",
    ]);
    // the raw answer stays reachable, not dropped
    expect(screen.getByText("Raw response JSON")).toBeInTheDocument();
  });

  it("does not present an empty answer as anything but zero proposals", async () => {
    mockApi({ [POST]: json({ proposals: [] }) });
    const user = userEvent.setup();
    mount();
    await fillSnapshot(user);
    await user.click(screen.getByRole("button", { name: "Generate proposals" }));
    expect(await screen.findByText(/The planner proposed no transfer from this snapshot/)).toBeInTheDocument();
    expect(screen.queryByRole("table")).not.toBeInTheDocument();
  });

  it("shows the 422 mixed-snapshot refusal with the service's own words", async () => {
    mockApi({
      [POST]: problem(422, "invalid-planning-snapshot", "Invalid planning snapshot", "generate transfer proposals: mixed position snapshot"),
    });
    const user = userEvent.setup();
    mount();
    await fillSnapshot(user);
    await user.click(screen.getByRole("button", { name: "Generate proposals" }));
    const alert = await screen.findByRole("alert");
    expect(alert).toHaveTextContent("a mixed snapshot is never planned");
    expect(alert).toHaveTextContent("Invalid planning snapshot");
    expect(alert).toHaveTextContent("mixed position snapshot");
    expect(screen.getByRole("form", { name: "Generate proposals" })).toBeInTheDocument();
  });

  it("retries the SAME snapshot with the SAME Idempotency-Key, and remints when it changes", async () => {
    const api = mockApi({ [POST]: problem(422, "invalid-planning-snapshot", "Invalid planning snapshot", "x") });
    const user = userEvent.setup();
    mount();
    await fillSnapshot(user);
    await user.click(screen.getByRole("button", { name: "Generate proposals" }));
    await screen.findByRole("alert");

    api.set(POST, json(generateResponse()));
    await user.click(screen.getByRole("button", { name: "Generate proposals" }));
    await screen.findByRole("table");
    expect(api.to(POST)).toHaveLength(2);
    expect(api.to(POST)[1].headers["Idempotency-Key"]).toBe(api.to(POST)[0].headers["Idempotency-Key"]);

    await user.clear(screen.getByLabelText(/^Snapshot as of/));
    await user.type(screen.getByLabelText(/^Snapshot as of/), "2026-10-07T10:00:00Z");
    await user.click(screen.getByRole("button", { name: "Generate proposals" }));
    await waitFor(() => expect(api.to(POST)).toHaveLength(3));
    expect(api.to(POST)[2].headers["Idempotency-Key"]).not.toBe(api.to(POST)[1].headers["Idempotency-Key"]);
    expect((api.to(POST)[2].body as { asOf: string }).asOf).toBe("2026-10-07T10:00:00Z");
  });
});

describe("buildGenerateRequest (the use case's own refusals, mirrored)", () => {
  it("requires the as-of and stamps every position with it", () => {
    const row = { site: "DC-EAST", sku: "SKU-RED-42", available: "10", customerReservations: "", confirmedInbound: "", committedOutbound: "" };
    const missing = buildGenerateRequest({ asOf: "", positions: [row], policies: [], lanes: [] });
    expect(missing.request).toBeUndefined();
    expect(missing.errors[0]).toMatch(/RFC 3339/);

    const built = buildGenerateRequest({ asOf: SNAPSHOT_AS_OF, positions: [row], policies: [], lanes: [] });
    expect(built.request?.positions[0].AsOf).toBe(SNAPSHOT_AS_OF);
    expect(built.request?.policies).toEqual([]);
    expect(built.request?.lanes).toEqual([]);
  });

  it("names the row that is wrong", () => {
    const out = buildGenerateRequest({
      asOf: SNAPSHOT_AS_OF,
      positions: [
        { site: "DC-EAST", sku: "SKU-RED-42", available: "5", customerReservations: "", confirmedInbound: "", committedOutbound: "" },
        { site: "", sku: "SKU-BLUE-7", available: "-1", customerReservations: "", confirmedInbound: "", committedOutbound: "" },
      ],
      policies: [],
      lanes: [],
    });
    expect(out.request).toBeUndefined();
    expect(out.errors).toEqual(["Position 2: Site is required.", "Position 2: Available must be zero or a positive whole number."]);
  });
});
