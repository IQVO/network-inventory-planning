import { Link, useParams } from "react-router-dom";
import { Card, KpiStat, StatusPill, Timeline } from "@warehouse/ui-kit";
import { getTransfer } from "../api";
import { auditSteps } from "../audit";
import { PageHeader, Stack } from "../components/layout";
import { EmptyNote, ProblemAlert, RequestView } from "../components/ProblemAlert";
import { useRequest } from "../hooks/useRequest";
import { isTerminal, STATUS_LEADS, stateTone, utcLabel } from "../lib";
import type { TransferDetail } from "../types";

const REJECTION_TEXT: Record<string, string> = {
  ORIGIN_SITE_UNKNOWN: "The origin site is not known to inventory-storage.",
  INSUFFICIENT_USABLE: "The origin does not have enough usable stock to reserve.",
  IDEMPOTENCY_CONFLICT: "The idempotency key was already used for another transfer.",
};

const PICKED_ONWARDS: ReadonlySet<string> = new Set(["PICKED", "IN_TRANSIT", "ARRIVED", "RECEIVED"]);

/**
 * Transfer detail (GET /v1/transfers/{id}): current state, quantities,
 * reservation and the immutable audit trail as a timeline. Read-only.
 */
export function TransferDetailScreen() {
  const { id = "" } = useParams();
  const transfer = useRequest(() => getTransfer(id), `transfer|${id}`);

  return (
    <Stack gap={5}>
      <PageHeader title={`Transfer ${id}`} subtitle="network-inventory-planning · state, quantities, reservation and audit trail" />
      <div>
        <Link to="..">← All transfers</Link>
      </div>
      {transfer.status === "error" ? (
        <ProblemAlert
          error={transfer.error}
          lead={transfer.error.status === 404 ? "This transfer does not exist." : STATUS_LEADS[transfer.error.status]}
        />
      ) : (
        <RequestView state={transfer} what="transfer" empty="">
          {(t) => <Detail t={t} />}
        </RequestView>
      )}
    </Stack>
  );
}

function Detail({ t }: { t: TransferDetail }) {
  const terminal = isTerminal(t.state);
  return (
    <Stack gap={5}>
      <Card title="Overview" actions={<StatusPill status={t.state} tone={stateTone(t.state)} />}>
        <Stack>
          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(160px, 1fr))", gap: "var(--wh-space-4)" }}>
            <KpiStat label="Planned quantity" value={t.quantity} />
            <KpiStat
              label="Picked quantity"
              value={t.pickedQuantity ?? null}
              // The service omits a zero pickedQuantity, so "absent" past PICKED means none was picked, not "not yet".
              caption={t.pickedQuantity === undefined ? (PICKED_ONWARDS.has(t.state) ? "none recorded" : "not picked yet") : undefined}
            />
          </div>
          <dl style={{ display: "grid", gridTemplateColumns: "max-content 1fr", gap: "6px var(--wh-space-4)", margin: 0 }}>
            <Fact label="SKU" value={t.sku} />
            <Fact label="Route" value={`${t.originSiteId} → ${t.destinationSiteId}`} />
            <Fact label="Policy version" value={t.policyVersion} />
            <Fact label="Reservation" value={t.reservationId ?? "No reservation yet"} />
            {t.rejectionReason && (
              <Fact
                label="Rejection reason"
                value={`${t.rejectionReason} — ${REJECTION_TEXT[t.rejectionReason] ?? "see the audit trail"}`}
              />
            )}
            <Fact label="Created" value={utcLabel(t.createdAt)} />
            <Fact label="Last change" value={utcLabel(t.updatedAt)} />
            <Fact label="Expires" value={utcLabel(t.expiresAt)} />
            <Fact label="Version" value={String(t.version)} />
          </dl>
          {!terminal && (
            <EmptyNote>This transfer is waiting for its next step; it advances only through replies and facts from sibling contexts.</EmptyNote>
          )}
        </Stack>
      </Card>
      <Card title="State timeline">
        {t.audit.length === 0 ? (
          <EmptyNote>No audit entries recorded.</EmptyNote>
        ) : (
          <Timeline steps={auditSteps(t.audit, terminal ? t.state : null)} />
        )}
      </Card>
    </Stack>
  );
}

function Fact({ label, value }: { label: string; value: string }) {
  return (
    <>
      <dt style={{ color: "var(--wh-color-text-muted)", fontSize: "var(--wh-font-size-sm)" }}>{label}</dt>
      <dd style={{ margin: 0, fontFamily: "var(--wh-font-mono)", fontSize: "var(--wh-font-size-sm)" }}>{value}</dd>
    </>
  );
}
