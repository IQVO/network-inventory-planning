import { NavLink, Outlet, Route, Routes } from "react-router-dom";
import { RebalanceRunsScreen } from "./screens/RebalanceRunsScreen";
import { SimulationScreen } from "./screens/SimulationScreen";
import { TransferDetailScreen } from "./screens/TransferDetailScreen";
import { TransfersScreen } from "./screens/TransfersScreen";

const SUB_NAV = [
  { to: ".", label: "Transfers", end: true },
  { to: "simulation", label: "Network simulation" },
  { to: "rebalance-runs", label: "Rebalance runs" },
];

const linkStyle = ({ isActive }: { isActive: boolean }) => ({
  display: "inline-flex",
  padding: "6px 12px",
  borderRadius: "var(--wh-radius-pill)",
  fontSize: "var(--wh-font-size-sm)",
  fontWeight: isActive ? 600 : 500,
  color: isActive ? "var(--wh-color-text)" : "var(--wh-color-text-muted)",
  background: isActive ? "var(--wh-color-accent-muted)" : "transparent",
  textDecoration: "none",
});

/** The sub-nav, rendered by a layout route with the path "/".
 *
 *  Why a layout route and not a nav placed above `<Routes>`: the console mounts
 *  this component inside its own `<Route path="/network-inventory/*">`. A
 *  relative `<NavLink>` rendered straight in that splat route resolves against
 *  the FULL current URL (react-router 7 uses the leaf match's `pathname`, splat
 *  included), so hops after the first pointed at URLs no route matches. The
 *  layout route MUST carry a non-empty path: react-router drops pathless
 *  routes from relative-link resolution, so a pathless layout changes nothing.
 *  With path "/" the layout's own match is the one links resolve against: the
 *  mount point, under any prefix and standalone at `/`. */
function NipLayout() {
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "var(--wh-space-5)" }}>
      <nav aria-label="Network inventory sections" style={{ display: "flex", gap: "var(--wh-space-2)" }}>
        {SUB_NAV.map((item) => (
          <NavLink key={item.to} to={item.to} end={item.end} style={linkStyle}>
            {item.label}
          </NavLink>
        ))}
      </nav>
      <Outlet />
    </div>
  );
}

/** Exposed as nip_mfe/App via Module Federation. Takes NO props: the console
 *  mounts it under a route prefix of its choosing (`/<prefix>/*`) and provides
 *  the BrowserRouter, the design tokens and `window.__WAREHOUSE_CONFIG__`.
 *  Routes are RELATIVE, so the component works identically mounted under that
 *  prefix (in the shell) or at / (standalone dev, see main.tsx).
 *
 *   - Transfers (index): filterable, paged transfer list.
 *   - transfers/:id: one transfer, quantities, reservation and audit timeline.
 *   - simulation: advisory options, per-site effect, and the Approve action.
 *   - rebalance-runs: the scheduled observe-only run history. */
export default function App() {
  return (
    <Routes>
      <Route path="/" element={<NipLayout />}>
        <Route index element={<TransfersScreen />} />
        <Route path="transfers/:id" element={<TransferDetailScreen />} />
        <Route path="simulation" element={<SimulationScreen />} />
        <Route path="rebalance-runs" element={<RebalanceRunsScreen />} />
      </Route>
    </Routes>
  );
}
