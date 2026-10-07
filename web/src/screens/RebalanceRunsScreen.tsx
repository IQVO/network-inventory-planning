import { Card, DataTable, StatusPill, formatNumber } from "@warehouse/ui-kit";
import { listRebalanceRuns } from "../api";
import { SubmitButton } from "../components/formkit";
import { PageHeader, Stack } from "../components/layout";
import { ProblemAlert, RequestView } from "../components/ProblemAlert";
import { useRequest } from "../hooks/useRequest";
import { STATUS_LEADS, utcLabel } from "../lib";

/**
 * Rebalance runs (GET /v1/rebalance-runs): the observe-only scheduled run
 * history, newest first. A run never approves anything; a fail-closed
 * snapshot refusal is a FAILED run carrying its reason.
 */
export function RebalanceRunsScreen() {
  const runs = useRequest(() => listRebalanceRuns(50), "rebalance-runs");
  return (
    <Stack gap={5}>
      <PageHeader
        title="Rebalance runs"
        subtitle="network-inventory-planning · scheduled, observe-only rebalance passes (they never approve or allocate)"
      />
      <Card
        title="Recent runs"
        actions={
          <SubmitButton type="button" onClick={runs.reload} disabled={runs.status === "loading"}>
            Refresh
          </SubmitButton>
        }
      >
        {runs.status === "error" ? (
          <ProblemAlert error={runs.error} lead={STATUS_LEADS[runs.error.status]} />
        ) : (
          <RequestView
            state={runs}
            what="rebalance runs"
            isEmpty={(r) => r.length === 0}
            empty="No rebalance run has been recorded yet (the schedule may be off)."
          >
            {(list) => (
              <DataTable
                rowKey={(r) => String(r.id)}
                rows={list}
                columns={[
                  { key: "id", header: "Run", render: (r) => r.id },
                  { key: "started", header: "Started", render: (r) => utcLabel(r.startedAt) },
                  { key: "asof", header: "Snapshot as of", render: (r) => utcLabel(r.snapshotAsOf) },
                  { key: "proposals", header: "Proposals", align: "right", render: (r) => formatNumber(r.proposalCount) },
                  { key: "rejected", header: "Rejected", align: "right", render: (r) => formatNumber(r.rejectedCount) },
                  {
                    key: "outcome",
                    header: "Outcome",
                    render: (r) => <StatusPill status={r.outcome} tone={r.outcome === "COMPLETED" ? "success" : "danger"} size="sm" />,
                  },
                  { key: "reason", header: "Fail-closed reason", render: (r) => r.failClosedReason ?? "—" },
                ]}
              />
            )}
          </RequestView>
        )}
      </Card>
    </Stack>
  );
}
