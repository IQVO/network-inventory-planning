import type { ReactNode } from "react";
import type { ApiError } from "../api";
import type { RequestState } from "../hooks/useRequest";

const mutedStyle = {
  color: "var(--wh-color-text-muted)",
  fontSize: "var(--wh-font-size-sm)",
};

/**
 * Renders an RFC 7807 problem: `title` (the category) in bold and `detail`
 * (what exactly was wrong) beneath it. `lead` is optional operator guidance
 * for one specific problem, shown first.
 */
export function ProblemAlert({ error, lead }: { error: ApiError; lead?: string }) {
  return (
    <div
      role="alert"
      style={{
        color: "var(--wh-color-status-danger)",
        background: "var(--wh-color-status-danger-bg)",
        borderRadius: "var(--wh-radius-md)",
        padding: "10px 14px",
        fontSize: "var(--wh-font-size-sm)",
        display: "flex",
        flexDirection: "column",
        gap: 4,
      }}
    >
      {lead && <div style={{ fontWeight: 600 }}>{lead}</div>}
      <div>
        <strong>{error.title}</strong>
        {error.status > 0 && <span style={{ opacity: 0.8 }}> (HTTP {error.status})</span>}
      </div>
      {error.detail && <div>{error.detail}</div>}
    </div>
  );
}

export function EmptyNote({ children }: { children: ReactNode }) {
  return <div style={mutedStyle}>{children}</div>;
}

export function LoadingNote({ what }: { what: string }) {
  return (
    <div role="status" style={mutedStyle}>
      Loading {what}…
    </div>
  );
}

/**
 * The one place a request's four states are rendered: loading, error (with
 * the problem's title and detail), empty, and success. `idle` renders
 * `whenIdle` (a prompt for the input the request still needs).
 */
export function RequestView<T>({
  state,
  what,
  isEmpty,
  empty,
  whenIdle,
  children,
}: {
  state: RequestState<T>;
  /** Noun for the loading note: "transfers". */
  what: string;
  isEmpty?: (data: T) => boolean;
  empty: ReactNode;
  whenIdle?: ReactNode;
  children: (data: T) => ReactNode;
}) {
  switch (state.status) {
    case "idle":
      return whenIdle ? <EmptyNote>{whenIdle}</EmptyNote> : null;
    case "loading":
      return <LoadingNote what={what} />;
    case "error":
      return <ProblemAlert error={state.error} />;
    case "success":
      return isEmpty?.(state.data) ? <EmptyNote>{empty}</EmptyNote> : <>{children(state.data)}</>;
  }
}
