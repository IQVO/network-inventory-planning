import { describe, expect, it } from "vitest";
import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router-dom";
import { TransfersScreen } from "./TransfersScreen";
import { json, mockApi, pending, problem } from "../test/fetchMock";
import { RECEIVED, UNFULFILLABLE, transfer, transferList } from "../test/fixtures";

function mount() {
  return render(
    <MemoryRouter>
      <TransfersScreen />
    </MemoryRouter>,
  );
}

describe("TransfersScreen", () => {
  it("shows a loading state", () => {
    mockApi({ "GET /v1/transfers": pending() });
    mount();
    expect(screen.getByText("Loading transfers…")).toBeInTheDocument();
  });

  it("lists transfers with state pills, route, quantities, last change and a detail link", async () => {
    mockApi({ "GET /v1/transfers": json(transferList([transfer(), RECEIVED, UNFULFILLABLE])) });
    mount();
    const table = await screen.findByRole("table");
    const rows = within(table).getAllByRole("row").slice(1);
    expect(rows).toHaveLength(3);

    const first = within(rows[0]);
    expect(first.getByRole("link", { name: "t-1001" })).toHaveAttribute("href", "/transfers/t-1001");
    expect(first.getByText("ALLOCATING")).toHaveAttribute("data-tone", "progress");
    expect(first.getByText("DC-EAST → DC-WEST")).toBeInTheDocument();
    expect(first.getByText("120")).toBeInTheDocument();
    expect(first.getByText(/2026-10-07 10:05Z/)).toBeInTheDocument();

    const received = within(rows[1]);
    expect(received.getByText("RECEIVED")).toHaveAttribute("data-tone", "success");
    expect(received.getByText("38 of 40 picked")).toBeInTheDocument();
    expect(within(rows[2]).getByText("UNFULFILLABLE")).toHaveAttribute("data-tone", "danger");
  });

  it("stamps when the page was loaded", async () => {
    mockApi({ "GET /v1/transfers": json(transferList([transfer()])) });
    mount();
    expect(await screen.findByText(/^Loaded \d{4}-\d{2}-\d{2} \d{2}:\d{2}Z$/)).toBeInTheDocument();
  });

  it("shows an empty state", async () => {
    mockApi({ "GET /v1/transfers": json(transferList([])) });
    mount();
    expect(await screen.findByText("No transfers match this selection.")).toBeInTheDocument();
    expect(screen.queryByRole("table")).not.toBeInTheDocument();
  });

  it("surfaces BOTH title and detail of a problem+json failure (503 without a database)", async () => {
    mockApi({ "GET /v1/transfers": problem(503, "service-unavailable", "Service unavailable", "transfer store is not configured") });
    mount();
    const alert = await screen.findByRole("alert");
    expect(alert).toHaveTextContent("Service unavailable");
    expect(alert).toHaveTextContent("(HTTP 503)");
    expect(alert).toHaveTextContent("transfer store is not configured");
  });

  it("applies the state and site filters to the query and resets paging", async () => {
    const api = mockApi({ "GET /v1/transfers": json(transferList([transfer()])) });
    const user = userEvent.setup();
    mount();
    await screen.findByRole("table");
    expect(api.calls[0].query.has("state")).toBe(false);
    expect(api.calls[0].query.get("limit")).toBe("25");

    const form = screen.getByRole("form", { name: "Transfer filters" });
    await user.selectOptions(within(form).getByLabelText("State"), "ALLOCATING");
    await user.type(within(form).getByLabelText("Origin site"), " DC-EAST ");
    await user.type(within(form).getByLabelText("Destination site"), "DC-WEST");
    await user.click(within(form).getByRole("button", { name: "Apply filters" }));

    await waitFor(() => expect(api.calls.length).toBe(2));
    const q = api.calls[1].query;
    expect(q.get("state")).toBe("ALLOCATING");
    expect(q.get("originSiteId")).toBe("DC-EAST");
    expect(q.get("destinationSiteId")).toBe("DC-WEST");
    expect(q.get("offset")).toBe("0");

    await user.click(within(form).getByRole("button", { name: "Clear" }));
    await waitFor(() => expect(api.calls.length).toBe(3));
    expect(api.calls[2].query.has("state")).toBe(false);
    expect(api.calls[2].query.has("originSiteId")).toBe(false);
  });

  it("pages forward and back by the page size", async () => {
    const api = mockApi({
      "GET /v1/transfers": (call) => {
        const offset = Number(call.query.get("offset"));
        return json(transferList([transfer({ id: `t-${offset}` })], { total: 51, offset }));
      },
    });
    const user = userEvent.setup();
    mount();
    await screen.findByRole("link", { name: "t-0" });
    const pager = screen.getByRole("navigation", { name: "Transfers paging" });
    expect(within(pager).getByRole("button", { name: "Previous" })).toBeDisabled();
    expect(within(pager).getByText("Showing 1–1 of 51")).toBeInTheDocument();

    await user.click(within(pager).getByRole("button", { name: "Next" }));
    await screen.findByRole("link", { name: "t-25" });
    expect(api.calls[1].query.get("offset")).toBe("25");

    await user.click(within(pager).getByRole("button", { name: "Next" }));
    await screen.findByRole("link", { name: "t-50" });
    expect(within(pager).getByRole("button", { name: "Next" })).toBeDisabled();

    await user.click(within(pager).getByRole("button", { name: "Previous" }));
    await screen.findByRole("link", { name: "t-25" });
  });

  it("refreshes on demand", async () => {
    const api = mockApi({ "GET /v1/transfers": json(transferList([transfer()])) });
    const user = userEvent.setup();
    mount();
    await screen.findByRole("table");
    await user.click(screen.getByRole("button", { name: "Refresh" }));
    await waitFor(() => expect(api.calls.length).toBe(2));
  });
});
