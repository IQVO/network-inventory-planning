import { describe, expect, it } from "vitest";
import { render, screen, within } from "@testing-library/react";
import { RebalanceRunsScreen } from "./RebalanceRunsScreen";
import { json, mockApi, pending, problem } from "../test/fetchMock";
import { RUNS } from "../test/fixtures";

describe("RebalanceRunsScreen", () => {
  it("shows a loading state", () => {
    mockApi({ "GET /v1/rebalance-runs": pending() });
    render(<RebalanceRunsScreen />);
    expect(screen.getByText("Loading rebalance runs…")).toBeInTheDocument();
  });

  it("lists runs with outcome pills and the fail-closed reason", async () => {
    const api = mockApi({ "GET /v1/rebalance-runs": json({ runs: RUNS }) });
    render(<RebalanceRunsScreen />);
    const rows = within(await screen.findByRole("table")).getAllByRole("row").slice(1);
    expect(rows).toHaveLength(2);
    expect(within(rows[0]).getByText("COMPLETED")).toHaveAttribute("data-tone", "success");
    expect(within(rows[0]).getByText("2026-10-07 05:59Z")).toBeInTheDocument();
    expect(within(rows[1]).getByText("FAILED")).toHaveAttribute("data-tone", "danger");
    expect(within(rows[1]).getByText("capacity facts are stale")).toBeInTheDocument();
    expect(api.calls[0].query.get("limit")).toBe("50");
  });

  it("shows an empty state", async () => {
    mockApi({ "GET /v1/rebalance-runs": json({ runs: [] }) });
    render(<RebalanceRunsScreen />);
    expect(await screen.findByText(/No rebalance run has been recorded yet/)).toBeInTheDocument();
  });

  it("surfaces title and detail of a 503", async () => {
    mockApi({ "GET /v1/rebalance-runs": problem(503, "service-unavailable", "Service unavailable", "no database") });
    render(<RebalanceRunsScreen />);
    const alert = await screen.findByRole("alert");
    expect(alert).toHaveTextContent("Service unavailable");
    expect(alert).toHaveTextContent("no database");
  });
});
