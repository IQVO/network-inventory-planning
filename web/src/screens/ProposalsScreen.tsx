import { useRef, useState } from "react";
import type { ReactNode } from "react";
import { Card, DataTable, KpiStat, formatNumber } from "@warehouse/ui-kit";
import { generateTransferProposals, toApiError } from "../api";
import type { ApiError } from "../api";
import { Form, FormRow, InlineError, SelectField, SubmitButton, TextField } from "../components/formkit";
import { PageHeader, Stack } from "../components/layout";
import { EmptyNote, ProblemAlert } from "../components/ProblemAlert";
import {
  buildGenerateRequest,
  emptyLaneDraft,
  emptyPolicyDraft,
  emptyPositionDraft,
  generateLead,
  newIdempotencyKey,
  utcLabel,
} from "../lib";
import type { GenerateDraft, LaneDraft, PolicyDraft, PositionDraft } from "../lib";
import type { GenerateResponseWire, ProposalWire } from "../types";

/** One editable snapshot row (the key only exists to keep React list state). */
interface Row<T> {
  key: number;
  draft: T;
}

/** Row identity counter: module-scoped so no ref is touched during render. */
let nextRowKey = 0;
const newRow = <T,>(draft: T): Row<T> => ({ key: ++nextRowKey, draft });

/**
 * Proposal generation (POST /v1/transfer-proposals:generate): the DIAGNOSTIC
 * planning boundary. The endpoint plans deterministically over an explicit,
 * caller-provided snapshot — it never reaches into sibling contexts, and
 * production planning uses the simulation endpoint instead. The form asks for
 * exactly what the contract requires (asOf, positions, policies, lanes) and
 * renders exactly what the answer contains: the proposals, highest score
 * first. Nothing here reserves or moves stock.
 *
 * Snapshot coherence: the service refuses a snapshot whose positions are
 * stamped with different instants, so each position's AsOf is not typed per
 * row — the form stamps every row with the snapshot as-of.
 *
 * Idempotency follows the remote's convention: a key per logical run, reused
 * while the same snapshot is retried, reminted when anything changes.
 */
export function ProposalsScreen() {
  const [asOf, setAsOf] = useState("");
  const [positions, setPositions] = useState<Row<PositionDraft>[]>([newRow(emptyPositionDraft())]);
  const [policies, setPolicies] = useState<Row<PolicyDraft>[]>([newRow(emptyPolicyDraft())]);
  const [lanes, setLanes] = useState<Row<LaneDraft>[]>([newRow(emptyLaneDraft())]);
  const [generating, setGenerating] = useState(false);
  const [problem, setProblem] = useState<ApiError | null>(null);
  const [invalid, setInvalid] = useState<string[]>([]);
  const [result, setResult] = useState<GenerateResponseWire | null>(null);
  const attempt = useRef<{ fingerprint: string; key: string } | null>(null);
  const [lastKey, setLastKey] = useState<string | null>(null);

  const draft: GenerateDraft = { asOf, positions: positions.map((r) => r.draft), policies: policies.map((r) => r.draft), lanes: lanes.map((r) => r.draft) };

  const submit = async () => {
    setProblem(null);
    const { request, errors } = buildGenerateRequest(draft);
    if (!request) {
      setInvalid(errors);
      return;
    }
    setInvalid([]);
    const fingerprint = JSON.stringify(request);
    if (attempt.current?.fingerprint !== fingerprint) attempt.current = { fingerprint, key: newIdempotencyKey() };
    const key = attempt.current.key;
    setLastKey(key);
    setGenerating(true);
    try {
      setResult(await generateTransferProposals(request, key));
    } catch (err) {
      setProblem(toApiError(err));
    } finally {
      setGenerating(false);
    }
  };

  return (
    <Stack gap={5}>
      <PageHeader
        title="Proposal generation"
        subtitle="network-inventory-planning · the diagnostic planner: deterministic, explainable proposals over an explicit snapshot (advisory — nothing is reserved or moved)"
      />
      <Card title="Generate from an explicit snapshot">
        <Form label="Generate proposals" onSubmit={submit}>
          <EmptyNote>
            The planner consumes ONLY what you type here: positions (per site + SKU), the policies that name target
            levels, and the lanes transfers may take. Every position is stamped with the snapshot as-of — the service
            refuses a mixed snapshot (422), so there is no per-row timestamp. The answer is advisory and moves
            nothing; an operator approves an actual transfer from the simulation screen.
          </EmptyNote>
          <FormRow>
            <TextField
              label="Snapshot as of"
              required
              value={asOf}
              onChange={setAsOf}
              hint="RFC 3339, e.g. 2026-10-07T09:50:00Z. Stamped onto every position."
            />
          </FormRow>

          <SnapshotList
            title="Positions"
            what="position"
            rows={positions}
            onChange={setPositions}
            newDraft={emptyPositionDraft}
            render={(draft, set) => (
              <>
                <TextField label="Site" required value={draft.site} onChange={(site) => set({ site })} />
                <TextField label="SKU" required value={draft.sku} onChange={(sku) => set({ sku })} />
                <TextField
                  label="Available"
                  required
                  type="number"
                  min={0}
                  step={1}
                  value={draft.available}
                  onChange={(available) => set({ available })}
                  hint="Blank means 0."
                />
                <TextField
                  label="Customer reservations"
                  type="number"
                  min={0}
                  step={1}
                  value={draft.customerReservations}
                  onChange={(customerReservations) => set({ customerReservations })}
                  hint="Blank means 0."
                />
                <TextField
                  label="Confirmed inbound"
                  type="number"
                  min={0}
                  step={1}
                  value={draft.confirmedInbound}
                  onChange={(confirmedInbound) => set({ confirmedInbound })}
                  hint="Blank means 0."
                />
                <TextField
                  label="Committed outbound"
                  type="number"
                  min={0}
                  step={1}
                  value={draft.committedOutbound}
                  onChange={(committedOutbound) => set({ committedOutbound })}
                  hint="Blank means 0."
                />
              </>
            )}
          />

          <SnapshotList
            title="Policies"
            what="policy"
            rows={policies}
            onChange={setPolicies}
            newDraft={emptyPolicyDraft}
            note="The same shape the Rebalance policies catalogue stores — target levels per site + SKU."
            render={(draft, set) => (
              <>
                <TextField label="Site" required value={draft.site} onChange={(site) => set({ site })} />
                <TextField label="SKU" required value={draft.sku} onChange={(sku) => set({ sku })} />
                <TextField label="Policy version" required value={draft.version} onChange={(version) => set({ version })} />
                <TextField
                  label="Safety stock"
                  type="number"
                  min={0}
                  step={1}
                  value={draft.safetyStock}
                  onChange={(safetyStock) => set({ safetyStock })}
                  hint="Blank means 0."
                />
                <TextField
                  label="Target stock"
                  type="number"
                  min={0}
                  step={1}
                  value={draft.targetStock}
                  onChange={(targetStock) => set({ targetStock })}
                  hint="Must not be below the safety stock; blank means 0."
                />
                <TextField
                  label="Unit priority"
                  type="number"
                  min={0}
                  step={1}
                  value={draft.unitPriority}
                  onChange={(unitPriority) => set({ unitPriority })}
                  hint="Blank means 0."
                />
              </>
            )}
          />

          <SnapshotList
            title="Lanes"
            what="lane"
            rows={lanes}
            onChange={setLanes}
            newDraft={emptyLaneDraft}
            note="Only enabled lanes can carry a proposal."
            render={(draft, set) => (
              <>
                <TextField label="Origin site" required value={draft.origin} onChange={(origin) => set({ origin })} />
                <TextField
                  label="Destination site"
                  required
                  value={draft.destination}
                  onChange={(destination) => set({ destination })}
                />
                <TextField
                  label="Lead time (hours)"
                  required
                  type="number"
                  min={0}
                  step="any"
                  value={draft.leadTimeHours}
                  onChange={(leadTimeHours) => set({ leadTimeHours })}
                  hint="Blank means 0."
                />
                <TextField
                  label="Unit handling cost"
                  required
                  type="number"
                  min={0}
                  step={1}
                  value={draft.unitHandlingCost}
                  onChange={(unitHandlingCost) => set({ unitHandlingCost })}
                  hint="Blank means 0."
                />
                <SelectField
                  label="Lane status"
                  value={draft.enabled}
                  onChange={(enabled) => set({ enabled: enabled === "no" ? "no" : "yes" })}
                  options={[
                    { value: "yes", label: "Enabled" },
                    { value: "no", label: "Disabled" },
                  ]}
                />
              </>
            )}
          />

          <FormRow>
            <SubmitButton disabled={generating}>{generating ? "Generating…" : "Generate proposals"}</SubmitButton>
          </FormRow>
          {lastKey && <EmptyNote>Last attempt recorded under idempotency key {lastKey}.</EmptyNote>}
          {invalid.length > 0 && <InlineError message={invalid.join(" ")} />}
          {problem && <ProblemAlert error={problem} lead={generateLead(problem)} />}
        </Form>
      </Card>
      {result && <ProposalsResult result={result} />}
    </Stack>
  );
}

/** One titled, repeatable snapshot list (positions / policies / lanes) with
 *  add and remove. Rows are groups so repeated field labels stay addressable. */
function SnapshotList<T>({
  title,
  what,
  rows,
  onChange,
  newDraft,
  render,
  note,
}: {
  title: string;
  what: string;
  rows: Row<T>[];
  onChange: (rows: Row<T>[]) => void;
  newDraft: () => T;
  render: (draft: T, set: (patch: Partial<T>) => void) => ReactNode;
  note?: string;
}) {
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "var(--wh-space-3)" }}>
      <div style={{ display: "flex", alignItems: "baseline", gap: "var(--wh-space-3)" }}>
        <strong>{title}</strong>
        {note && <span style={{ fontSize: "var(--wh-font-size-xs)", color: "var(--wh-color-text-faint)" }}>{note}</span>}
      </div>
      {rows.map((r, i) => (
        <div key={r.key} role="group" aria-label={`${what} ${i + 1}`} style={{ display: "flex", flexDirection: "column", gap: "var(--wh-space-2)" }}>
          <FormRow>
            {render(r.draft, (patch) => {
              const next = rows.map((row) => (row.key === r.key ? { key: row.key, draft: { ...row.draft, ...patch } } : row));
              onChange(next as Row<T>[]);
            })}
            <SubmitButton
              type="button"
              ariaLabel={`Remove ${what} ${i + 1}`}
              onClick={() => onChange(rows.filter((row) => row.key !== r.key))}
            >
              Remove
            </SubmitButton>
          </FormRow>
        </div>
      ))}
      <div>
        <SubmitButton type="button" onClick={() => onChange([...rows, newRow(newDraft())])}>
          Add {what}
        </SubmitButton>
      </div>
    </div>
  );
}

/**
 * The answer: ONLY the proposals the contract returns (there are no
 * considered/rejected counters to invent). Rendered highest score first in
 * the service's own order, with the full score breakdown; the raw JSON stays
 * one disclosure away so nothing the service adds can be silently dropped.
 */
function ProposalsResult({ result }: { result: GenerateResponseWire }) {
  const count = result.proposals.length;
  return (
    <Card title="Proposals (highest score first)">
      <Stack gap={4}>
        <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(180px, 1fr))", gap: "var(--wh-space-4)" }}>
          <KpiStat label="Proposals returned" value={count} caption="advisory — the answer carries only this list" />
        </div>
        {count === 0 ? (
          <EmptyNote>The planner proposed no transfer from this snapshot.</EmptyNote>
        ) : (
          <DataTable
            rowKey={(p) => `${p.Origin}→${p.Destination}:${p.SKU}`}
            rows={result.proposals}
            columns={[
              { key: "move", header: "Move", render: (p) => `${p.Origin} → ${p.Destination}` },
              { key: "sku", header: "SKU", render: (p) => p.SKU },
              { key: "qty", header: "Quantity", align: "right", render: (p) => formatNumber(p.Quantity) },
              { key: "version", header: "Policy version", render: (p) => p.PolicyVersion },
              { key: "asof", header: "Position as of", render: (p) => utcLabel(p.PositionAsOf) },
              { key: "reasons", header: "Reasons", render: (p) => p.Reasons.join(", ") },
              { key: "benefit", header: "Priority benefit", align: "right", render: (p) => formatNumber(p.ScoreBreakdown.PriorityBenefit) },
              { key: "handling", header: "Handling penalty", align: "right", render: (p) => formatNumber(p.ScoreBreakdown.HandlingPenalty) },
              { key: "lead", header: "Lead time penalty", align: "right", render: (p) => formatNumber(p.ScoreBreakdown.LeadTimePenalty) },
              {
                key: "score",
                header: "Score (benefit − penalties)",
                align: "right",
                render: (p) => formatNumber(proposalScore(p)),
              },
            ]}
          />
        )}
        <details>
          <summary style={{ fontSize: "var(--wh-font-size-sm)", color: "var(--wh-color-text-muted)" }}>
            Raw response JSON
          </summary>
          <pre style={{ fontSize: "var(--wh-font-size-xs)", overflowX: "auto" }}>{JSON.stringify(result, null, 2)}</pre>
        </details>
      </Stack>
    </Card>
  );
}

/** The service's own ordering formula (ScoreBreakdown.Total in the domain):
 *  priority benefit minus the handling and lead-time penalties. */
function proposalScore(p: ProposalWire): number {
  return p.ScoreBreakdown.PriorityBenefit - p.ScoreBreakdown.HandlingPenalty - p.ScoreBreakdown.LeadTimePenalty;
}
