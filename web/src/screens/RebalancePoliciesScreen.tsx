import { useState } from "react";
import { Card, DataTable, formatNumber } from "@warehouse/ui-kit";
import { listRebalancePolicies, putRebalancePolicy, toApiError } from "../api";
import type { ApiError } from "../api";
import { Form, FormRow, InlineError, InlineSuccess, SubmitButton, TextField } from "../components/formkit";
import { PageHeader, Stack } from "../components/layout";
import { EmptyNote, ProblemAlert, RequestView } from "../components/ProblemAlert";
import { useRequest } from "../hooks/useRequest";
import {
  buildPolicyRequest,
  catalogueLead,
  catalogueOutcomeNote,
  emptyPolicyDraft,
  policyDraftFrom,
  utcLabel,
} from "../lib";
import type { PolicyDraft } from "../lib";
import type { RebalancePolicyEntry } from "../types";

/**
 * Rebalance policies (ADR 0015): the operator-managed target-levels catalogue
 * (GET / PUT /v1/rebalance-policies), one entry per site + SKU. The scheduled
 * rebalance pass plans from this catalogue; each entry embeds the SAME Policy
 * shape POST /v1/transfer-proposals:generate accepts, plus its revision.
 *
 * Same edit contract as the lanes screen: an edit PUTs with the loaded
 * revision in If-Match (412 when it moved on), a create PUTs without it, and
 * the service's problem+json title/detail surface verbatim on a refusal.
 */
export function RebalancePoliciesScreen() {
  const policies = useRequest(() => listRebalancePolicies(), "rebalance-policies");
  const [focus, setFocus] = useState<RebalancePolicyEntry | null>(null);
  const [newEpoch, setNewEpoch] = useState(0);
  const [saved, setSaved] = useState<RebalancePolicyEntry | null>(null);

  return (
    <Stack gap={5}>
      <PageHeader
        title="Rebalance policies"
        subtitle="network-inventory-planning · the operator-managed target-levels catalogue (ADR 0015); the scheduled rebalance pass plans from these entries"
      />
      <Card
        title="Policies"
        actions={
          <SubmitButton type="button" onClick={policies.reload} disabled={policies.status === "loading"}>
            Refresh
          </SubmitButton>
        }
      >
        {policies.status === "error" ? (
          <ProblemAlert error={policies.error} lead={catalogueLead(policies.error)} />
        ) : (
          <RequestView
            state={policies}
            what="rebalance policies"
            isEmpty={(l) => l.policies.length === 0}
            empty="The policy catalogue is empty. Create the first site + SKU policy below."
          >
            {(l) => (
              <DataTable
                rowKey={(e) => `${e.policy.Site}/${e.policy.SKU}`}
                rows={l.policies}
                columns={[
                  { key: "site", header: "Site", render: (e) => e.policy.Site },
                  { key: "sku", header: "SKU", render: (e) => e.policy.SKU },
                  { key: "version", header: "Version", render: (e) => e.policy.Version },
                  {
                    key: "safety",
                    header: "Safety stock",
                    align: "right",
                    render: (e) => formatNumber(e.policy.SafetyStock),
                  },
                  {
                    key: "target",
                    header: "Target stock",
                    align: "right",
                    render: (e) => formatNumber(e.policy.TargetStock),
                  },
                  {
                    key: "priority",
                    header: "Unit priority",
                    align: "right",
                    render: (e) => formatNumber(e.policy.UnitPriority),
                  },
                  { key: "revision", header: "Revision", align: "right", render: (e) => e.revision },
                  { key: "updated", header: "Updated", render: (e) => utcLabel(e.updatedAt) },
                  {
                    key: "edit",
                    header: "",
                    render: (e) => (
                      <SubmitButton
                        type="button"
                        onClick={() => setFocus(e)}
                        ariaLabel={`Edit policy ${e.policy.Site} ${e.policy.SKU}`}
                      >
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
      {saved && <InlineSuccess message={catalogueOutcomeNote(`${saved.policy.Site}/${saved.policy.SKU}`, saved)} />}
      <PolicyEditor
        key={focus ? `${focus.policy.Site}/${focus.policy.SKU}@${focus.revision}` : `new-${newEpoch}`}
        focus={focus}
        onSaved={(entry) => {
          setFocus(entry);
          setSaved(entry);
          policies.reload();
        }}
        onFailed={() => policies.reload()}
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
 * The policy upsert form. Remounts (via its key) whenever the edited entry
 * changes, so its state is always initialized from `focus`. Site + SKU are
 * the entry's key and lock while editing (the service refuses a key change
 * with a 422).
 */
function PolicyEditor({
  focus,
  onSaved,
  onFailed,
  onNew,
}: {
  focus: RebalancePolicyEntry | null;
  onSaved: (entry: RebalancePolicyEntry) => void;
  onFailed: () => void;
  onNew: () => void;
}) {
  const [draft, setDraft] = useState<PolicyDraft>(focus ? policyDraftFrom(focus.policy) : emptyPolicyDraft());
  const [saving, setSaving] = useState(false);
  const [problem, setProblem] = useState<ApiError | null>(null);
  const [invalid, setInvalid] = useState<string[]>([]);

  const set = (patch: Partial<PolicyDraft>) => setDraft((d) => ({ ...d, ...patch }));
  const editing = focus !== null;

  const submit = async () => {
    setProblem(null);
    const { request, errors } = buildPolicyRequest(draft);
    if (!request) {
      setInvalid(Object.values(errors));
      return;
    }
    setInvalid([]);
    setSaving(true);
    try {
      onSaved(await putRebalancePolicy(request, focus ? `"${focus.revision}"` : undefined));
    } catch (err) {
      setProblem(toApiError(err));
      onFailed();
    } finally {
      setSaving(false);
    }
  };

  return (
    <Card title={editing ? `Edit policy ${focus.policy.Site}/${focus.policy.SKU}` : "Create a policy"}>
      <Form label={editing ? "Edit rebalance policy" : "Create rebalance policy"} onSubmit={submit}>
        <EmptyNote>
          One entry per site + SKU. The PUT is idempotent: sending exactly the stored policy changes nothing
          (UNCHANGED); a changed policy bumps its revision; a new one is created at revision 1. A target stock of 0
          is legal and means “do not stock this SKU here”. Editing sends the loaded revision in If-Match, so an entry
          that moved on under you answers 412 instead of being overwritten.
        </EmptyNote>
        <FormRow>
          <TextField
            label="Site"
            required
            disabled={editing}
            value={draft.site}
            onChange={(site) => set({ site })}
            hint={editing ? "The key of an existing policy cannot change." : undefined}
          />
          <TextField
            label="SKU"
            required
            disabled={editing}
            value={draft.sku}
            onChange={(sku) => set({ sku })}
            hint={editing ? undefined : "With the site, the entry's key."}
          />
          <TextField
            label="Policy version"
            required
            value={draft.version}
            onChange={(version) => set({ version })}
            hint="Recorded on every transfer planned under this policy."
          />
        </FormRow>
        <FormRow>
          <TextField
            label="Safety stock"
            required
            type="number"
            min={0}
            step={1}
            value={draft.safetyStock}
            onChange={(safetyStock) => set({ safetyStock })}
            hint="Whole units; blank means 0."
          />
          <TextField
            label="Target stock"
            required
            type="number"
            min={0}
            step={1}
            value={draft.targetStock}
            onChange={(targetStock) => set({ targetStock })}
            hint="Must not be below the safety stock; 0 means “do not stock here”."
          />
          <TextField
            label="Unit priority"
            required
            type="number"
            min={0}
            step={1}
            value={draft.unitPriority}
            onChange={(unitPriority) => set({ unitPriority })}
            hint="Higher raises a proposal's priority benefit; blank means 0."
          />
        </FormRow>
        <FormRow>
          <SubmitButton disabled={saving}>{saving ? "Saving…" : editing ? "Save policy" : "Create policy"}</SubmitButton>
          <SubmitButton type="button" onClick={onNew}>
            {editing ? "New policy" : "Clear"}
          </SubmitButton>
        </FormRow>
        {invalid.length > 0 && <InlineError message={invalid.join(" ")} />}
        {problem && <ProblemAlert error={problem} lead={catalogueLead(problem)} />}
      </Form>
    </Card>
  );
}
