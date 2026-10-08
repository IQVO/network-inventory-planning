import { describe, expect, it } from "vitest";
import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import App from "./App";
import { json, mockApi } from "./test/fetchMock";
import { OPTIONS, detail, transfer, transferList } from "./test/fixtures";

const NAV = "Network inventory sections";
const LABELS = ["Transfers", "Network simulation", "Rebalance runs"];

function mount(path: string) {
  return render(
    <MemoryRouter initialEntries={[path]}>
      <App />
    </MemoryRouter>,
  );
}

/** Mounted the way the console mounts the remote: inside the host's own
 *  splat route. Relative links behave differently there than at the router
 *  root, which root-mounted tests cannot see. */
function mountUnderHost(path: string) {
  return render(
    <MemoryRouter initialEntries={[path]}>
      <Routes>
        <Route path="/network-inventory/*" element={<App />} />
      </Routes>
    </MemoryRouter>,
  );
}

function routes() {
  return mockApi({
    "GET /v1/transfers": json(transferList([transfer()])),
    "GET /v1/transfers/t-1001": json(detail()),
    "GET /v1/transfer-simulations": json({ asOf: "2026-10-07T09:50:00Z", options: OPTIONS }),
    "GET /v1/rebalance-runs": json({ runs: [] }),
  });
}

function navHrefs(): string[] {
  return within(screen.getByRole("navigation", { name: NAV }))
    .getAllByRole("link")
    .map((a) => a.getAttribute("href") ?? "");
}

describe("App (the exposed ./App)", () => {
  it("renders the transfers list at the index route with a three-way sub-nav", async () => {
    routes();
    mount("/");
    expect(screen.getByRole("heading", { level: 1, name: "Transfers" })).toBeInTheDocument();
    const nav = screen.getByRole("navigation", { name: NAV });
    for (const label of LABELS) expect(nav).toHaveTextContent(label);
    expect(await screen.findByRole("link", { name: "t-1001" })).toBeInTheDocument();
  });

  it("routes relatively: every screen works under any mount prefix", async () => {
    routes();
    const a = mount("/simulation");
    expect(await screen.findByRole("heading", { name: "Network simulation" })).toBeInTheDocument();
    a.unmount();
    const b = mount("/rebalance-runs");
    expect(await screen.findByRole("heading", { name: "Rebalance runs" })).toBeInTheDocument();
    b.unmount();
    mount("/transfers/t-1001");
    expect(await screen.findByRole("heading", { name: "Transfer t-1001" })).toBeInTheDocument();
  });

  describe("mounted under the console's /network-inventory/* splat route", () => {
    it.each(["/network-inventory", "/network-inventory/simulation", "/network-inventory/rebalance-runs", "/network-inventory/transfers/t-1001"])(
      "sub-nav links resolve against the mount point, not the current URL (%s)",
      async (start) => {
        routes();
        mountUnderHost(start);
        await screen.findByRole("navigation", { name: NAV });
        expect(navHrefs()).toEqual(["/network-inventory", "/network-inventory/simulation", "/network-inventory/rebalance-runs"]);
      },
    );

    it.each([
      ["/network-inventory", "Transfers"],
      ["/network-inventory/simulation", "Network simulation"],
      ["/network-inventory/rebalance-runs", "Rebalance runs"],
    ])("marks only the current section active (%s)", async (start, active) => {
      routes();
      mountUnderHost(start);
      const nav = await screen.findByRole("navigation", { name: NAV });
      for (const name of LABELS) {
        const link = within(nav).getByRole("link", { name });
        if (name === active) expect(link).toHaveAttribute("aria-current", "page");
        else expect(link).not.toHaveAttribute("aria-current");
      }
    });

    it("navigates between all screens and into a transfer, hop after hop", async () => {
      routes();
      const user = userEvent.setup();
      mountUnderHost("/network-inventory");

      const row = await screen.findByRole("link", { name: "t-1001" });
      expect(row).toHaveAttribute("href", "/network-inventory/transfers/t-1001");
      await user.click(row);
      expect(await screen.findByRole("heading", { name: "Transfer t-1001" })).toBeInTheDocument();
      expect(screen.getByRole("link", { name: "← All transfers" })).toHaveAttribute("href", "/network-inventory");

      await user.click(screen.getByRole("link", { name: "Network simulation" }));
      expect(await screen.findByRole("heading", { name: "Network simulation" })).toBeInTheDocument();

      await user.click(screen.getByRole("link", { name: "Rebalance runs" }));
      expect(await screen.findByRole("heading", { name: "Rebalance runs" })).toBeInTheDocument();

      await user.click(screen.getByRole("link", { name: "Transfers" }));
      expect(await screen.findByRole("heading", { level: 1, name: "Transfers" })).toBeInTheDocument();
      expect(navHrefs()).toEqual(["/network-inventory", "/network-inventory/simulation", "/network-inventory/rebalance-runs"]);
    });

    it("the approved-transfer link from the simulation resolves under the mount point", async () => {
      mockApi({
        "GET /v1/transfer-simulations": json({ asOf: "2026-10-07T09:50:00Z", options: OPTIONS }),
        "POST /v1/transfers:approve": json({
          transferId: "t-2001", state: "ALLOCATING", originSiteId: "DC-EAST", destinationSiteId: "DC-WEST", sku: "SKU-RED-42",
          quantity: 120, policyVersion: "v7", replayed: false, expiresAt: "2026-10-08T10:00:00Z", transferLineId: "t-2001:1",
        }),
      });
      const user = userEvent.setup();
      mountUnderHost("/network-inventory/simulation");
      await user.click(await screen.findByRole("button", { name: /^Approve 120/ }));
      await user.click(screen.getByRole("button", { name: "Confirm approval" }));
      const notice = await screen.findByRole("status");
      expect(within(notice).getByRole("link", { name: "t-2001" })).toHaveAttribute("href", "/network-inventory/transfers/t-2001");
    });
  });
});
