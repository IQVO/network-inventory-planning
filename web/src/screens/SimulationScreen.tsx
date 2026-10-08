import { useState } from "react";
import { Link } from "react-router-dom";
import { Card, DataTable, FreshnessBadge, KpiStat, formatNumber } from "@warehouse/ui-kit";
import { approveTransfer, getSimulation, toApiError } from "../api";
import type { ApiError } from "../api";
import { Form, FormRow, InlineSuccess, SubmitButton, TextField } from "../components/formkit";
import { PageHeader, Stack } from "../components/layout";
import { EmptyNote, ProblemAlert, RequestView } from "../components/ProblemAlert";
import { useRequest } from "../hooks/useRequest";
import { approveLead, lagSeconds, newIdempotencyKey, siteRollup, STATUS_LEADS, utcLabel } from "../lib";
import type { ApproveTransferResponse, Proposal, SimulationResponse } from "../types";

/**
 * Network simulation: the advisory options served from the service's local
 * read models (GET /v1/transfer-simulations), a per-site roll-up, and the
 * Approve action (POST /v1/transfers:approve).
 *
 * The endpoint is fail-closed: while the read models are unconfigured,
 * incomplete or stale it answers 503 problem+json, which is shown as "read
 * models not ready" with the problem's own title and detail -- never as an
 * empty simulation. The simulation carries proposals only (no stock levels),
 * so the per-site table is what the OPTIONS would move, not what is on hand.
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
        subtitle="network-inventory-planning · advisory transfer options from the current read models; approval starts the allocation saga"
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
              ? "Read models not ready — the simulation is fail-closed and will not propose from partial or stale facts."
              : STATUS_LEADS[simulation.error.status]
          }
        />
      ) : (
        <RequestView
          state={simulation}
          what="simulation"
          isEmpty={(r) => r.sim.options.length === 0}
          empty="The read models are ready, but no transfer is advisable right now: every site is within its policy."
        >
          {(r) => <SimulationBody sim={r.sim} loadedAtMs={r.loadedAtMs} />}
        </RequestView>
      )}
    </Stack>
  );
}

function SimulationBody({ sim, loadedAtMs }: { sim: SimulationResponse; loadedAtMs: number }) {
  const sites = siteRollup(sim.options);
  const [approved, setApproved] = useState<ApproveTransferResponse | null>(null);
  return (
    <Stack gap={5}>
      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(160px, 1fr))", gap: "var(--wh-space-4)" }}>
        <KpiStat label="Advisory options" value={sim.options.length} />
        <KpiStat label="Sites involved" value={sites.length} />
        <KpiStat label="Facts as of" value={utcLabel(sim.asOf)} caption="oldest watermark used" />
      </div>
      <div>
        <FreshnessBadge lagSeconds={lagSeconds(sim.asOf, loadedAtMs)} staleThresholdSeconds={900} />
      </div>
      {approved && <ApprovedNotice approved={approved} />}
      <Card title="Per-site effect if every option were executed">
        <DataTable
          rowKey={(s) => s.site}
          rows={sites}
          columns={[
            { key: "site", header: "Site", render: (s) => s.site },
            { key: "out", header: "Units out", align: "right", render: (s) => formatNumber(s.outbound) },
            { key: "in", header: "Units in", align: "right", render: (s) => formatNumber(s.inbound) },
            { key: "net", header: "Net", align: "right", render: (s) => (s.net > 0 ? `+${formatNumber(s.net)}` : formatNumber(s.net)) },
            { key: "opts", header: "Options", align: "right", render: (s) => formatNumber(s.options) },
          ]}
        />
      </Card>
      <Card title="Options (best score first)">
        <OptionsTable options={sim.options} onApproved={setApproved} />
      </Card>
    </Stack>
  );
}

function optionKey(o: Proposal): string {
  return `${o.Origin}|${o.Destination}|${o.SKU}|${o.PolicyVersion}|${o.PositionAsOf}`;
}

function OptionsTable({ options, onApproved }: { options: Proposal[]; onApproved: (a: ApproveTransferResponse) => void }) {
  const [selected, setSelected] = useState<Proposal | null>(null);
  return (
    <Stack>
      <DataTable
        rowKey={optionKey}
        rows={options}
        columns={[
          { key: "route", header: "Route", render: (o) => `${o.Origin} → ${o.Destination}` },
          { key: "sku", header: "SKU", render: (o) => o.SKU },
          { key: "qty", header: "Quantity", align: "right", render: (o) => formatNumber(o.Quantity) },
          { key: "why", header: "Why", render: (o) => o.Reasons.join(", ") },
          {
            key: "score",
            header: "Score",
            align: "right",
            render: (o) => formatNumber(o.ScoreBreakdown.PriorityBenefit - o.ScoreBreakdown.HandlingPenalty - o.ScoreBreakdown.LeadTimePenalty),
          },
          { key: "policy", header: "Policy", render: (o) => o.PolicyVersion },
          { key: "asof", header: "Position as of", render: (o) => utcLabel(o.PositionAsOf) },
          {
            key: "act",
            header: "",
            render: (o) => (
              <SubmitButton
                type="button"
                ariaLabel={`Approve ${o.Quantity} ${o.SKU} from ${o.Origin} to ${o.Destination}`}
                onClick={() => setSelected(o)}
              >
                Approve…
              </SubmitButton>
            ),
          },
        ]}
      />
      {selected && (
        // key: a different option is a different logical approval, so it remounts with a fresh Idempotency-Key.
        <ApproveForm
          key={optionKey(selected)}
          option={selected}
          onCancel={() => setSelected(null)}
          onApproved={(a) => {
            setSelected(null);
            onApproved(a);
          }}
        />
      )}
    </Stack>
  );
}

/**
 * The approval of ONE option. The Idempotency-Key is minted when the form
 * opens and reused for every retry of this same approval (a 503 or a network
 * error can be retried without risking a second transfer); a different option
 * or reopening the form gets a new key.
 */
function ApproveForm({
  option,
  onCancel,
  onApproved,
}: {
  option: Proposal;
  onCancel: () => void;
  onApproved: (a: ApproveTransferResponse) => void;
}) {
  const [key] = useState(newIdempotencyKey);
  const [reason, setReason] = useState("");
  const [saving, setSaving] = useState(false);
  const [problem, setProblem] = useState<ApiError | null>(null);

  const submit = async () => {
    setProblem(null);
    setSaving(true);
    try {
      const out = await approveTransfer(
        {
          originSiteId: option.Origin,
          destinationSiteId: option.Destination,
          sku: option.SKU,
          quantity: option.Quantity,
          policyVersion: option.PolicyVersion,
          proposalAsOf: option.PositionAsOf,
          ...(reason.trim() && { operatorReason: reason.trim() }),
        },
        key,
      );
      onApproved(out);
    } catch (err) {
      setProblem(toApiError(err));
    } finally {
      setSaving(false);
    }
  };

  return (
    <Card title={`Approve ${option.Quantity} × ${option.SKU}: ${option.Origin} → ${option.Destination}`}>
      <Form label="Approve transfer" onSubmit={submit}>
        <EmptyNote>
          Approving reserves stock at {option.Origin} through inventory-storage and starts the allocation saga. It is
          recorded under idempotency key {key}.
        </EmptyNote>
        <FormRow>
          <TextField label="Operator reason" value={reason} onChange={setReason} hint="Optional. Kept in the transfer's audit trail." />
          <SubmitButton disabled={saving}>{saving ? "Approving…" : "Confirm approval"}</SubmitButton>
          <SubmitButton type="button" onClick={onCancel} disabled={saving}>
            Cancel
          </SubmitButton>
        </FormRow>
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
