import type { TimelineStep } from "@warehouse/ui-kit";
import { utcLabel } from "./lib";
import type { AuditEntry } from "./types";

/** The audit trail as timeline steps: every entry is a completed transition,
 *  except the last one, which is the transfer's current position. */
export function auditSteps(audit: AuditEntry[], terminalState: string | null): TimelineStep[] {
  return audit.map((a, i) => {
    const last = i === audit.length - 1;
    let state: TimelineStep["state"] = "done";
    if (last) {
      if (terminalState === "UNFULFILLABLE" || terminalState === "CANCELLED") state = "error";
      else if (terminalState === "RECEIVED") state = "done";
      else state = "active";
    }
    return {
      id: String(a.seq),
      context: a.from ? `${a.from} → ${a.to}` : `created as ${a.to}`,
      title: a.to,
      state,
      timestamp: utcLabel(a.occurredAt),
      detail: (
        <span>
          {a.event}
          {a.cause ? ` — ${a.cause}` : ""}
        </span>
      ),
    };
  });
}
