import { describe, expect, it } from "vitest";
import { render, screen, within } from "@testing-library/react";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { TransferDetailScreen } from "./TransferDetailScreen";
import { auditSteps } from "../audit";
import { json, mockApi, pending, problem } from "../test/fetchMock";
import { AUDIT, detail } from "../test/fixtures";

function mount(id: string) {
  return render(
    <MemoryRouter initialEntries={[`/transfers/${id}`]}>
      <Routes>
        <Route path="/" element={<div>list</div>} />
        <Route path="transfers/:id" element={<TransferDetailScreen />} />
      </Routes>
    </MemoryRouter>,
  );
}

describe("TransferDetailScreen", () => {
  it("shows a loading state", () => {
    mockApi({ "GET /v1/transfers/t-1001": pending() });
    mount("t-1001");
    expect(screen.getByText("Loading transfer…")).toBeInTheDocument();
  });

  it("renders state, quantities, no reservation yet and the audit trail as a timeline", async () => {
    const api = mockApi({ "GET /v1/transfers/t-1001": json(detail()) });
    mount("t-1001");
    expect(await screen.findByRole("heading", { name: "Transfer t-1001" })).toBeInTheDocument();
    expect(api.calls[0].path).toBe("/v1/transfers/t-1001");
    expect(screen.getByText("Planned quantity")).toBeInTheDocument();
    expect(screen.getByText("120")).toBeInTheDocument();
    expect(screen.getByText("not picked yet")).toBeInTheDocument();
    expect(screen.getByText("No reservation yet")).toBeInTheDocument();
    expect(screen.getByText("DC-EAST → DC-WEST")).toBeInTheDocument();
    expect(screen.getByText(/waiting for its next step/)).toBeInTheDocument();

    const steps = screen.getAllByRole("listitem");
    expect(steps).toHaveLength(4);
    expect(within(steps[0]).getByText("created as DRAFT")).toBeInTheDocument();
    expect(within(steps[0]).getByText(/TransferDrafted/)).toBeInTheDocument();
    expect(within(steps[1]).getByText(/TransferProposed — rebalance east to west/)).toBeInTheDocument();
    expect(within(steps[3]).getByText("ALLOCATING")).toBeInTheDocument();
    expect(within(steps[3]).getByText("APPROVED → ALLOCATING")).toBeInTheDocument();
    expect(within(steps[3]).getByText(/origin allocation requested/)).toBeInTheDocument();
  });

  it("shows the reservation and picked quantity of an allocated, picked transfer", async () => {
    mockApi({
      "GET /v1/transfers/t-9": json(detail({ id: "t-9", state: "PICKED", quantity: 40, pickedQuantity: 38, reservationId: "res-77" })),
    });
    mount("t-9");
    expect(await screen.findByText("res-77")).toBeInTheDocument();
    expect(screen.getByText("38")).toBeInTheDocument();
    expect(screen.queryByText("No reservation yet")).not.toBeInTheDocument();
  });

  it("does not call a transfer past PICKED 'not picked yet' when the service omits a zero pickedQuantity", async () => {
    mockApi({ "GET /v1/transfers/t-7": json(detail({ id: "t-7", state: "IN_TRANSIT", reservationId: "res-1" })) });
    mount("t-7");
    expect(await screen.findByText("none recorded")).toBeInTheDocument();
    expect(screen.queryByText("not picked yet")).not.toBeInTheDocument();
  });

  it("explains an UNFULFILLABLE transfer's closed rejection reason", async () => {
    mockApi({ "GET /v1/transfers/t-8": json(detail({ id: "t-8", state: "UNFULFILLABLE", rejectionReason: "INSUFFICIENT_USABLE" })) });
    mount("t-8");
    expect(await screen.findByText(/INSUFFICIENT_USABLE — The origin does not have enough usable stock/)).toBeInTheDocument();
    expect(screen.queryByText(/waiting for its next step/)).not.toBeInTheDocument();
  });

  it("shows a 404 problem for an unknown transfer with title and detail", async () => {
    mockApi({ "GET /v1/transfers/nope": problem(404, "transfer-not-found", "Transfer not found", "no transfer nope") });
    mount("nope");
    const alert = await screen.findByRole("alert");
    expect(alert).toHaveTextContent("This transfer does not exist.");
    expect(alert).toHaveTextContent("Transfer not found");
    expect(alert).toHaveTextContent("no transfer nope");
  });

  it("shows a 503 problem with guidance", async () => {
    mockApi({ "GET /v1/transfers/t-1": problem(503, "read-side-unavailable", "Transfer read side is not configured", "no database") });
    mount("t-1");
    const alert = await screen.findByRole("alert");
    expect(alert).toHaveTextContent("Transfer read side is not configured");
    expect(alert).toHaveTextContent("no database");
    expect(alert).toHaveTextContent("Nothing was changed");
  });

  it("links back to all transfers", async () => {
    mockApi({ "GET /v1/transfers/t-1001": json(detail()) });
    mount("t-1001");
    expect(await screen.findByRole("link", { name: "← All transfers" })).toHaveAttribute("href", "/");
  });
});

describe("auditSteps", () => {
  it("marks earlier entries done and the last one active while non-terminal", () => {
    expect(auditSteps(AUDIT, null).map((s) => s.state)).toEqual(["done", "done", "done", "active"]);
  });

  it("ends a RECEIVED trail done, and a failed or cancelled one in error", () => {
    expect(auditSteps(AUDIT, "RECEIVED").map((s) => s.state)).toEqual(["done", "done", "done", "done"]);
    expect(auditSteps(AUDIT, "UNFULFILLABLE").map((s) => s.state)).toEqual(["done", "done", "done", "error"]);
    expect(auditSteps(AUDIT, "CANCELLED")[3].state).toBe("error");
  });
});
