import { useRef, useState } from "react";
import { Link } from "react-router-dom";
import { Card, DataTable, FreshnessBadge, KpiStat, StatusPill, formatNumber } from "@warehouse/ui-kit";
import { approveTransfer, getSimulation, toApiError } from "../api";
import type { ApiError } from "../api";
import { Form, FormRow, InlineError, InlineSuccess, SelectField, SubmitButton, TextField } from "../components/formkit";
import { PageHeader, Stack } from "../components/layout";
import { EmptyNote, ProblemAlert, RequestView } from "../components/ProblemAlert";
import { useRequest } from "../hooks/useRequest";
import { approveLead, buildApproveRequest, isShort, lagSeconds, newIdempotencyKey, orderSites, STATUS_LEADS, utcLabel } from "../lib";
import type { ApproveDraft } from "../lib";
import type { ApproveTransferResponse, SimulationResponse, SimulationSite } from "../types";

/**
 * Network simulation: the service's advisory per-site view served from its
 * local read models (GET /v1/transfer-simulations) -- demand, capacity over the
 * window and headroom per site, short sites first -- and the operator-driven
 * Approve form (POST /v1/transfers:approve).
 *
 * The simulation proposes NOTHING: no routes, no SKUs, no quantities. The
 * operator decides the transfer and types it; the service then re-validates it
 * against the CURRENT fail-closed read models.
 *
 * The endpoint is fail-closed: while the read models are unconfigured,
 * incomplete or stale it answers 503 problem+json, which is shown as "read
 * models not ready" with the problem's own title and detail -- never as an
 * empty simulation.
 */
export function SimulationScreen() {
  const simulation = useRequest(
    async () => ({ sim: await getSimulation(), loadedAtMs: Date.now() }),
    "simulation",
  );

  return (
    <Stack gap={5}>
      <PageHeader
        title="Network simulation"
        subtitle="network-inventory-planning · per-site capacity against demand from the current read models; an approval starts the allocation saga"
      />
      <div>
        <SubmitButton type="button" onClick={simulation.reload} disabled={simulation.status === "loading"}>
          Refresh simulation
        </SubmitButton>
      </div>
      {simulation.status === "error" ? (
        <ProblemAlert
          error={simulation.error}
          lead={
            simulation.error.status === 503
              ? "Read models not ready — the simulation is fail-closed and will not answer from partial or stale facts."
              : STATUS_LEADS[simulation.error.status]
          }
        />
      ) : (
        <RequestView state={simulation} what="simulation" empty="">
          {(r) => <SimulationBody sim={r.sim} loadedAtMs={r.loadedAtMs} />}
        </RequestView>
      )}
    </Stack>
  );
}

function SimulationBody({ sim, loadedAtMs }: { sim: SimulationResponse; loadedAtMs: number }) {
  const sites = orderSites(sim.sites);
  const shortCount = sites.filter(isShort).length;
  const [approved, setApproved] = useState<ApproveTransferResponse | null>(null);
  // Bumped after a successful approval so the next one starts from a clean form (and a fresh key).
  const [formEpoch, setFormEpoch] = useState(0);
  return (
    <Stack gap={5}>
      <div role="note" style={{ fontSize: "var(--wh-font-size-sm)", color: "var(--wh-color-text-muted)" }}>
        <strong>Advisory — moves nothing.</strong> This view only compares each site's published capacity over its window with the
        demand in that window. It proposes no transfer; any move starts only when an operator approves one below.
        {!sim.advisory && " (The service did not flag this answer as advisory; treat it with care.)"}
      </div>
      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(160px, 1fr))", gap: "var(--wh-space-4)" }}>
        <KpiStat label="Sites" value={sites.length} />
        <KpiStat label="Short sites" value={shortCount} caption="headroom below zero" />
        <KpiStat label="Facts as of" value={utcLabel(sim.asOf)} caption="oldest watermark used" />
      </div>
      <div>
        {/* Display-only staleness hint; the service itself refuses (503) once its own freshness budget is exceeded. */}
        <FreshnessBadge lagSeconds={lagSeconds(sim.asOf, loadedAtMs)} staleThresholdSeconds={900} />
      </div>
      {approved && <ApprovedNotice approved={approved} />}
      <Card title="Sites (short first, most short first)">
        {sites.length === 0 ? (
          <EmptyNote>The service answered without any site. That is not a statement that the network is balanced.</EmptyNote>
        ) : (
          <DataTable
            rowKey={(s) => s.site}
            rows={sites}
            columns={[
              { key: "site", header: "Site", render: (s) => s.site },
              { key: "demand", header: "Demand", align: "right", render: (s) => formatNumber(s.totalDemand) },
              { key: "capacity", header: "Capacity over window", align: "right", render: (s) => formatNumber(s.capacityOverWindow) },
              {
                key: "headroom",
                header: "Headroom",
                align: "right",
                render: (s) => (
                  <span
                    data-short={isShort(s) ? "true" : undefined}
                    style={isShort(s) ? { color: "var(--wh-color-status-danger)", fontWeight: 700 } : undefined}
                  >
                    {formatNumber(s.capacityHeadroom)}
                  </span>
                ),
              },
              {
                key: "balance",
                header: "Balance",
                render: (s) => <StatusPill status={isShort(s) ? "SHORT" : "COVERED"} tone={isShort(s) ? "danger" : "success"} size="sm" />,
              },
              { key: "origin", header: "Origin enabled", render: (s) => (s.originEnabled ? "yes" : "no") },
              { key: "destination", header: "Destination enabled", render: (s) => (s.destinationEnabled ? "yes" : "no") },
              { key: "window", header: "Window", render: (s) => `${utcLabel(s.windowStart)} → ${utcLabel(s.windowEnd)}` },
            ]}
          />
        )}
      </Card>
      <ApproveForm
        key={formEpoch}
        sites={sites}
        defaultAsOf={sim.asOf}
        onApproved={(a) => {
          setApproved(a);
          setFormEpoch((n) => n + 1);
        }}
      />
    </Stack>
  );
}

function siteLabel(s: SimulationSite): string {
  return isShort(s) ? `${s.site} (short by ${formatNumber(-s.capacityHeadroom)})` : `${s.site} (headroom ${formatNumber(s.capacityHeadroom)})`;
}

/**
 * Operator-driven approval. The operator picks the sites and types the SKU,
 * quantity and policy version; nothing is pre-filled except the proposal as-of
 * (the simulation's own watermark, editable).
 *
 * Idempotency: the key is minted per distinct request. Retrying identical input
 * (after a 503 or a network failure) reuses the key, so it cannot create a second
 * transfer; changing ANY field makes it a different approval and mints a new key.
 */
function ApproveForm({
  sites,
  defaultAsOf,
  onApproved,
}: {
  sites: SimulationSite[];
  defaultAsOf: string;
  onApproved: (a: ApproveTransferResponse) => void;
}) {
  const [draft, setDraft] = useState<ApproveDraft>({
    originSiteId: "",
    destinationSiteId: "",
    sku: "",
    quantity: "",
    policyVersion: "",
    operatorReason: "",
    proposalAsOf: defaultAsOf,
  });
  const [saving, setSaving] = useState(false);
  const [problem, setProblem] = useState<ApiError | null>(null);
  const [invalid, setInvalid] = useState<string[]>([]);
  const attempt = useRef<{ fingerprint: string; key: string } | null>(null);
  const [lastKey, setLastKey] = useState<string | null>(null);

  const set = (patch: Partial<ApproveDraft>) => setDraft((d) => ({ ...d, ...patch }));

  const origins = sites.filter((s) => s.originEnabled);
  const destinations = sites.filter((s) => s.destinationEnabled && s.site !== draft.originSiteId);

  const submit = async () => {
    setProblem(null);
    const { request, errors } = buildApproveRequest(draft);
    if (!request) {
      setInvalid(Object.values(errors));
      return;
    }
    setInvalid([]);
    const fingerprint = JSON.stringify(request);
    if (attempt.current?.fingerprint !== fingerprint) attempt.current = { fingerprint, key: newIdempotencyKey() };
    const key = attempt.current.key;
    setLastKey(key);
    setSaving(true);
    try {
      onApproved(await approveTransfer(request, key));
    } catch (err) {
      setProblem(toApiError(err));
    } finally {
      setSaving(false);
    }
  };

  return (
    <Card title="Approve a transfer">
      <Form label="Approve transfer" onSubmit={submit}>
        <EmptyNote>
          You decide the transfer; the simulation does not suggest one. A short site is a natural destination and a covered site with
          headroom that is origin-enabled a natural origin, but the service has the last word: it re-checks the request against the
          current read models and refuses it (422) if they are stale or incomplete, a site is not enabled for its direction, the
          destination shows no in-window demand for the SKU, or the origin's capacity cannot cover it. Approving starts the allocation
          saga at the origin.
        </EmptyNote>
        <FormRow>
          <SelectField
            label="Origin site"
            required
            value={draft.originSiteId}
            onChange={(originSiteId) =>
              set({ originSiteId, ...(originSiteId === draft.destinationSiteId && { destinationSiteId: "" }) })
            }
            options={origins.map((s) => ({ value: s.site, label: siteLabel(s) }))}
          />
          <SelectField
            label="Destination site"
            required
            value={draft.destinationSiteId}
            onChange={(destinationSiteId) => set({ destinationSiteId })}
            options={destinations.map((s) => ({ value: s.site, label: siteLabel(s) }))}
          />
        </FormRow>
        <FormRow>
          <TextField label="SKU" required value={draft.sku} onChange={(sku) => set({ sku })} />
          <TextField
            label="Quantity"
            required
            type="number"
            min={1}
            step={1}
            value={draft.quantity}
            onChange={(quantity) => set({ quantity })}
            hint="Whole units, at least 1."
          />
          <TextField
            label="Policy version"
            required
            value={draft.policyVersion}
            onChange={(policyVersion) => set({ policyVersion })}
            hint="The policy version the transfer was planned under; recorded on the transfer."
          />
        </FormRow>
        <FormRow>
          <TextField
            label="Proposal as of"
            required
            value={draft.proposalAsOf}
            onChange={(proposalAsOf) => set({ proposalAsOf })}
            hint="RFC 3339. Defaults to the simulation's as-of."
          />
          <TextField
            label="Operator reason"
            value={draft.operatorReason}
            onChange={(operatorReason) => set({ operatorReason })}
            hint="Optional. Kept in the transfer's audit trail."
          />
        </FormRow>
        <FormRow>
          <SubmitButton disabled={saving}>{saving ? "Approving…" : "Confirm approval"}</SubmitButton>
        </FormRow>
        {lastKey && <EmptyNote>Last attempt recorded under idempotency key {lastKey}.</EmptyNote>}
        {invalid.length > 0 && <InlineError message={invalid.join(" ")} />}
        {problem && <ProblemAlert error={problem} lead={approveLead(problem)} />}
      </Form>
    </Card>
  );
}

function ApprovedNotice({ approved }: { approved: ApproveTransferResponse }) {
  return (
    <InlineSuccess
      message={
        <span>
          {approved.replayed
            ? "This approval was already recorded; showing the original transfer. "
            : "Approved. "}
          Transfer <Link to={`../transfers/${encodeURIComponent(approved.transferId)}`}>{approved.transferId}</Link> is{" "}
          {approved.state} ({formatNumber(approved.quantity)} × {approved.sku}, {approved.originSiteId} →{" "}
          {approved.destinationSiteId}).
        </span>
      }
    />
  );
}
