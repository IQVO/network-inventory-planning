import { useState } from "react";
import { Link } from "react-router-dom";
import { Card, DataTable, StatusPill, formatNumber } from "@warehouse/ui-kit";
import { listTransfers } from "../api";
import { Form, FormRow, SelectField, SubmitButton, TextField } from "../components/formkit";
import { PageHeader, Stack } from "../components/layout";
import { EmptyNote, ProblemAlert, RequestView } from "../components/ProblemAlert";
import { useRequest } from "../hooks/useRequest";
import { ageLabel, stateTone, STATUS_LEADS, utcLabel } from "../lib";
import { TRANSFER_STATES } from "../types";
import type { TransferList, TransferQuery, TransferView } from "../types";

const PAGE_SIZE = 25;

interface Filters {
  state: string;
  origin: string;
  destination: string;
}

const NO_FILTERS: Filters = { state: "", origin: "", destination: "" };

/**
 * Transfers: a read-only page of inter-warehouse transfers (GET /v1/transfers),
 * newest first, filtered by lifecycle state and by origin / destination site.
 * Each row links to the transfer's detail. The "Loaded" stamp says when this
 * page was fetched, and every row says how long ago its state last changed
 * (a non-terminal transfer whose last change is old is stuck).
 */
export function TransfersScreen() {
  const [draft, setDraft] = useState<Filters>(NO_FILTERS);
  const [applied, setApplied] = useState<Filters>(NO_FILTERS);
  // Keyset paging: cursors[i] is the cursor that opens page i (page 0 has none).
  const [cursors, setCursors] = useState<string[]>([]);
  const cursor = cursors[cursors.length - 1];

  const query: TransferQuery = {
    state: applied.state,
    originSiteId: applied.origin,
    destinationSiteId: applied.destination,
    limit: PAGE_SIZE,
    cursor,
  };
  const key = `transfers|${applied.state}|${applied.origin}|${applied.destination}|${cursor ?? ""}`;
  const transfers = useRequest(
    async () => ({ page: await listTransfers(query), loadedAt: new Date().toISOString() }),
    key,
  );

  const apply = () => {
    setCursors([]);
    setApplied({ state: draft.state, origin: draft.origin.trim(), destination: draft.destination.trim() });
  };
  const clear = () => {
    setDraft(NO_FILTERS);
    setApplied(NO_FILTERS);
    setCursors([]);
  };

  return (
    <Stack gap={5}>
      <PageHeader
        title="Transfers"
        subtitle="network-inventory-planning · inter-warehouse transfers and where each one is in its lifecycle"
      />
      <Card title="Filters">
        <Form label="Transfer filters" onSubmit={apply}>
          <FormRow>
            <SelectField
              label="State"
              value={draft.state}
              onChange={(state) => setDraft({ ...draft, state })}
              options={TRANSFER_STATES.map((s) => ({ value: s, label: s }))}
              placeholder="All states"
              clearable
            />
            <TextField
              label="Origin site"
              value={draft.origin}
              onChange={(origin) => setDraft({ ...draft, origin })}
              placeholder="e.g. DC-EAST"
            />
            <TextField
              label="Destination site"
              value={draft.destination}
              onChange={(destination) => setDraft({ ...draft, destination })}
              placeholder="e.g. DC-WEST"
            />
            <SubmitButton>Apply filters</SubmitButton>
            <SubmitButton type="button" onClick={clear}>
              Clear
            </SubmitButton>
          </FormRow>
        </Form>
      </Card>
      <Card
        title="Transfers"
        actions={
          transfers.status === "success" ? (
            <span style={{ fontSize: "var(--wh-font-size-xs)", color: "var(--wh-color-text-muted)" }}>
              Loaded {utcLabel(transfers.data.loadedAt)}
            </span>
          ) : undefined
        }
      >
        <Stack>
          {transfers.status === "error" ? (
            <ProblemAlert error={transfers.error} lead={STATUS_LEADS[transfers.error.status]} />
          ) : (
            <RequestView
              state={transfers}
              what="transfers"
              isEmpty={(r) => r.page.items.length === 0}
              empty="No transfers match this selection."
            >
              {(r) => <TransfersTable page={r.page} loadedAt={r.loadedAt} />}
            </RequestView>
          )}
          <Pager
            page={transfers.status === "success" ? transfers.data.page : null}
            pageNumber={cursors.length}
            onNext={(next) => setCursors((c) => [...c, next])}
            onPrevious={() => setCursors((c) => c.slice(0, -1))}
            onRefresh={transfers.reload}
            busy={transfers.status === "loading"}
          />
        </Stack>
      </Card>
    </Stack>
  );
}

function TransfersTable({ page, loadedAt }: { page: TransferList; loadedAt: string }) {
  const now = Date.parse(loadedAt);
  return (
    <DataTable<TransferView>
      rowKey={(t) => t.id}
      rows={page.items}
      columns={[
        { key: "id", header: "Transfer", render: (t) => <Link to={`transfers/${encodeURIComponent(t.id)}`}>{t.id}</Link> },
        { key: "state", header: "State", render: (t) => <StatusPill status={t.state} tone={stateTone(t.state)} size="sm" /> },
        { key: "sku", header: "SKU", render: (t) => t.sku },
        { key: "route", header: "Route", render: (t) => `${t.originSiteId} → ${t.destinationSiteId}` },
        {
          key: "qty",
          header: "Quantity",
          align: "right",
          render: (t) =>
            t.pickedQuantity === undefined
              ? formatNumber(t.quantity)
              : `${formatNumber(t.pickedQuantity)} of ${formatNumber(t.quantity)} picked`,
        },
        { key: "policy", header: "Policy", render: (t) => t.policyVersion },
        {
          key: "changed",
          header: "Last change",
          render: (t) => (
            <span>
              {utcLabel(t.updatedAt)}{" "}
              <small style={{ color: "var(--wh-color-text-muted)" }}>{ageLabel(t.updatedAt, now)}</small>
            </span>
          ),
        },
        { key: "expires", header: "Expires", render: (t) => utcLabel(t.expiresAt) },
      ]}
    />
  );
}

function Pager({
  page,
  pageNumber,
  onNext,
  onPrevious,
  onRefresh,
  busy,
}: {
  page: TransferList | null;
  pageNumber: number;
  onNext: (cursor: string) => void;
  onPrevious: () => void;
  onRefresh: () => void;
  busy: boolean;
}) {
  const shown = page?.items.length ?? 0;
  const hasPrev = pageNumber > 0;
  const nextCursor = page?.nextCursor;
  const hasNext = nextCursor !== undefined && nextCursor !== "";
  return (
    <nav
      aria-label="Transfers paging"
      style={{ display: "flex", gap: "var(--wh-space-3)", alignItems: "center", flexWrap: "wrap" }}
    >
      <SubmitButton type="button" disabled={!hasPrev || busy} onClick={onPrevious}>
        Previous
      </SubmitButton>
      <SubmitButton type="button" disabled={!hasNext || busy} onClick={() => nextCursor && onNext(nextCursor)}>
        Next
      </SubmitButton>
      <SubmitButton type="button" disabled={busy} onClick={onRefresh}>
        Refresh
      </SubmitButton>
      <EmptyNote>
        {page && shown > 0 ? `Page ${pageNumber + 1}: ${formatNumber(shown)} transfers` : ""}
      </EmptyNote>
    </nav>
  );
}
