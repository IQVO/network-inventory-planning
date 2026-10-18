import { useState } from "react";
import { Card, DataTable, StatusPill, formatNumber } from "@warehouse/ui-kit";
import { listTransferLanes, putTransferLane, toApiError } from "../api";
import type { ApiError } from "../api";
import { Form, FormRow, InlineError, InlineSuccess, SelectField, SubmitButton, TextField } from "../components/formkit";
import { PageHeader, Stack } from "../components/layout";
import { EmptyNote, ProblemAlert, RequestView } from "../components/ProblemAlert";
import { useRequest } from "../hooks/useRequest";
import {
  catalogueLead,
  catalogueOutcomeNote,
  buildLaneRequest,
  emptyLaneDraft,
  laneDraftFrom,
  leadTimeLabel,
  utcLabel,
} from "../lib";
import type { LaneDraft } from "../lib";
import type { TransferLaneEntry } from "../types";

/**
 * Transfer lanes (ADR 0015): the operator-managed inter-warehouse lane
 * catalogue (GET / PUT /v1/transfer-lanes). One entry per DIRECTED origin →
 * destination (WH1→WH2 and WH2→WH1 are two lanes); the scheduled rebalance
 * pass uses a lane only when it is enabled. There is no DELETE — a lane is
 * disabled with Enabled: false.
 *
 * The editor PUTs the typed lane; editing an existing entry sends its loaded
 * revision as If-Match (optimistic concurrency: an entry that moved on
 * answers 412 and the list refreshes), creating one PUTs without If-Match.
 * Validation problems come back as RFC 7807 problem+json and are shown with
 * the service's own title and detail.
 */
export function TransferLanesScreen() {
  const lanes = useRequest(() => listTransferLanes(), "transfer-lanes");
  // The entry the editor is loaded from (null = the create form), plus the
  // note of the last accepted PUT; both live HERE so the editor can remount
  // (fresh values, fresh revision) without losing the note.
  const [focus, setFocus] = useState<TransferLaneEntry | null>(null);
  const [newEpoch, setNewEpoch] = useState(0);
  const [saved, setSaved] = useState<TransferLaneEntry | null>(null);

  return (
    <Stack gap={5}>
      <PageHeader
        title="Transfer lanes"
        subtitle="network-inventory-planning · the operator-managed directed lane catalogue (ADR 0015); the scheduled rebalance pass uses a lane only when it is enabled"
      />
      <Card
        title="Lanes"
        actions={
          <SubmitButton type="button" onClick={lanes.reload} disabled={lanes.status === "loading"}>
            Refresh
          </SubmitButton>
        }
      >
        {lanes.status === "error" ? (
          <ProblemAlert error={lanes.error} lead={catalogueLead(lanes.error)} />
        ) : (
          <RequestView
            state={lanes}
            what="transfer lanes"
            isEmpty={(l) => l.lanes.length === 0}
            empty="The lane catalogue is empty. Create the first directed lane below."
          >
            {(l) => (
              <DataTable
                rowKey={(e) => `${e.lane.Origin}→${e.lane.Destination}`}
                rows={l.lanes}
                columns={[
                  { key: "lane", header: "Lane (directed)", render: (e) => `${e.lane.Origin} → ${e.lane.Destination}` },
                  { key: "lead", header: "Lead time", align: "right", render: (e) => leadTimeLabel(e.lane.LeadTime) },
                  {
                    key: "cost",
                    header: "Unit handling cost",
                    align: "right",
                    render: (e) => formatNumber(e.lane.UnitHandlingCost),
                  },
                  {
                    key: "enabled",
                    header: "Status",
                    render: (e) => (
                      <StatusPill status={e.lane.Enabled ? "ENABLED" : "DISABLED"} tone={e.lane.Enabled ? "success" : "neutral"} size="sm" />
                    ),
                  },
                  { key: "revision", header: "Revision", align: "right", render: (e) => e.revision },
                  { key: "updated", header: "Updated", render: (e) => utcLabel(e.updatedAt) },
                  {
                    key: "edit",
                    header: "",
                    render: (e) => (
                      <SubmitButton type="button" onClick={() => setFocus(e)} ariaLabel={`Edit lane ${e.lane.Origin} to ${e.lane.Destination}`}>
                        Edit
                      </SubmitButton>
                    ),
                  },
                ]}
              />
            )}
          </RequestView>
        )}
      </Card>
      {saved && (
        <InlineSuccess message={catalogueOutcomeNote(`${saved.lane.Origin} → ${saved.lane.Destination}`, saved)} />
      )}
      <LaneEditor
        key={focus ? `${focus.lane.Origin}→${focus.lane.Destination}@${focus.revision}` : `new-${newEpoch}`}
        focus={focus}
        onSaved={(entry) => {
          setFocus(entry);
          setSaved(entry);
          lanes.reload();
        }}
        onFailed={() => lanes.reload()}
        onNew={() => {
          setFocus(null);
          setSaved(null);
          setNewEpoch((n) => n + 1);
        }}
      />
    </Stack>
  );
}

/**
 * The lane upsert form. Remounts (via its key) whenever the edited entry
 * changes, so its state is always initialized from `focus` — the create form
 * starts empty. The key fields are locked while editing: a revise must keep
 * the entry's Origin → Destination (the service refuses a key change with a
 * 422).
 */
function LaneEditor({
  focus,
  onSaved,
  onFailed,
  onNew,
}: {
  focus: TransferLaneEntry | null;
  onSaved: (entry: TransferLaneEntry) => void;
  /** Called after any failed PUT, so a 412/409 refreshes the list. */
  onFailed: () => void;
  onNew: () => void;
}) {
  const [draft, setDraft] = useState<LaneDraft>(focus ? laneDraftFrom(focus.lane) : emptyLaneDraft());
  const [saving, setSaving] = useState(false);
  const [problem, setProblem] = useState<ApiError | null>(null);
  const [invalid, setInvalid] = useState<string[]>([]);

  const set = (patch: Partial<LaneDraft>) => setDraft((d) => ({ ...d, ...patch }));
  const editing = focus !== null;

  const submit = async () => {
    setProblem(null);
    const { request, errors } = buildLaneRequest(draft);
    if (!request) {
      setInvalid(Object.values(errors));
      return;
    }
    setInvalid([]);
    setSaving(true);
    try {
      onSaved(await putTransferLane(request, focus ? `"${focus.revision}"` : undefined));
    } catch (err) {
      setProblem(toApiError(err));
      onFailed();
    } finally {
      setSaving(false);
    }
  };

  return (
    <Card title={editing ? `Edit lane ${focus.lane.Origin} → ${focus.lane.Destination}` : "Create a lane"}>
      <Form label={editing ? "Edit transfer lane" : "Create transfer lane"} onSubmit={submit}>
        <EmptyNote>
          The PUT is idempotent: sending exactly the stored lane changes nothing (UNCHANGED). A changed lane bumps its
          revision; a new lane is created at revision 1. Editing sends the loaded revision in If-Match, so an entry
          that moved on under you answers 412 instead of being overwritten. Lead time is sent as nanoseconds (Go's
          time.Duration wire shape); hours here are exact — 0.25 is fifteen minutes.
        </EmptyNote>
        <FormRow>
          <TextField
            label="Origin site"
            required
            disabled={editing}
            value={draft.origin}
            onChange={(origin) => set({ origin })}
            hint={editing ? "The key of an existing lane cannot change." : undefined}
          />
          <TextField
            label="Destination site"
            required
            disabled={editing}
            value={draft.destination}
            onChange={(destination) => set({ destination })}
            hint={editing ? undefined : "Must differ from the origin — a lane is directed."}
          />
        </FormRow>
        <FormRow>
          <TextField
            label="Lead time (hours)"
            required
            type="number"
            min={0}
            step="any"
            value={draft.leadTimeHours}
            onChange={(leadTimeHours) => set({ leadTimeHours })}
            hint="Zero or more; blank means 0."
          />
          <TextField
            label="Unit handling cost"
            required
            type="number"
            min={0}
            step={1}
            value={draft.unitHandlingCost}
            onChange={(unitHandlingCost) => set({ unitHandlingCost })}
            hint="Whole units; blank means 0."
          />
          <SelectField
            label="Lane status"
            value={draft.enabled}
            onChange={(enabled) => set({ enabled: enabled === "no" ? "no" : "yes" })}
            options={[
              { value: "yes", label: "Enabled" },
              { value: "no", label: "Disabled (no DELETE exists)" },
            ]}
          />
        </FormRow>
        <FormRow>
          <SubmitButton disabled={saving}>{saving ? "Saving…" : editing ? "Save lane" : "Create lane"}</SubmitButton>
          <SubmitButton type="button" onClick={onNew}>
            {editing ? "New lane" : "Clear"}
          </SubmitButton>
        </FormRow>
        {invalid.length > 0 && <InlineError message={invalid.join(" ")} />}
        {problem && <ProblemAlert error={problem} lead={catalogueLead(problem)} />}
      </Form>
    </Card>
  );
}
