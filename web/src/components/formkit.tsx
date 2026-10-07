import { useId } from "react";
import type { ChangeEvent, FormEvent, ReactNode } from "react";

/**
 * Minimal styled form primitives for this remote's screens. Copied per
 * remote on purpose (not promoted to @warehouse/ui-kit: each remote's copy is
 * allowed to diverge) and styled purely off the shared design tokens so it
 * looks native inside the console shell. Every control is labelled by its
 * visible text, which is also how the tests find it.
 */

const fieldWrapStyle = {
  display: "flex",
  flexDirection: "column" as const,
  gap: 4,
};

const labelStyle = {
  fontSize: "var(--wh-font-size-xs)",
  color: "var(--wh-color-text-muted)",
  fontWeight: 600,
};

const inputStyle = {
  padding: "8px 10px",
  borderRadius: "var(--wh-radius-md)",
  border: "1px solid var(--wh-color-border)",
  background: "var(--wh-color-bg-sunken)",
  color: "var(--wh-color-text)",
  fontFamily: "var(--wh-font-mono)",
  fontSize: "var(--wh-font-size-sm)",
};

export function TextField({
  label,
  value,
  onChange,
  placeholder,
  type = "text",
  required,
  hint,
  min,
  step,
}: {
  label: string;
  value: string;
  onChange: (value: string) => void;
  placeholder?: string;
  type?: "text" | "number" | "datetime-local";
  required?: boolean;
  /** Short helper text under the input. */
  hint?: string;
  min?: number;
  step?: number | "any";
}) {
  const hintId = useId();
  return (
    <div style={fieldWrapStyle}>
      <label style={fieldWrapStyle}>
        <span style={labelStyle}>
          {label}
          {required && " *"}
        </span>
        <input
          type={type}
          value={value}
          placeholder={placeholder}
          min={min}
          step={step}
          aria-describedby={hint ? hintId : undefined}
          onChange={(e: ChangeEvent<HTMLInputElement>) => onChange(e.target.value)}
          style={inputStyle}
        />
      </label>
      {hint && (
        <span id={hintId} style={{ fontSize: "var(--wh-font-size-xs)", color: "var(--wh-color-text-faint)" }}>
          {hint}
        </span>
      )}
    </div>
  );
}

export function SelectField({
  label,
  value,
  onChange,
  options,
  placeholder = "Select…",
  required,
  disabled,
  clearable,
}: {
  label: string;
  value: string;
  onChange: (value: string) => void;
  options: { value: string; label: string }[];
  placeholder?: string;
  required?: boolean;
  disabled?: boolean;
  /** The placeholder option stays selectable (a filter's "All"). */
  clearable?: boolean;
}) {
  return (
    <label style={fieldWrapStyle}>
      <span style={labelStyle}>
        {label}
        {required && " *"}
      </span>
      <select
        value={value}
        disabled={disabled}
        onChange={(e: ChangeEvent<HTMLSelectElement>) => onChange(e.target.value)}
        style={{ ...inputStyle, fontFamily: "var(--wh-font-sans)" }}
      >
        <option value="" disabled={!clearable}>
          {placeholder}
        </option>
        {options.map((opt) => (
          <option key={opt.value} value={opt.value}>
            {opt.label}
          </option>
        ))}
      </select>
    </label>
  );
}

export function FormRow({ children }: { children: ReactNode }) {
  return (
    <div style={{ display: "flex", gap: "var(--wh-space-3)", flexWrap: "wrap", alignItems: "end" }}>
      {children}
    </div>
  );
}

/** A <form> that suppresses the browser's own validation bubbles (screens
 *  validate and word their own messages) and the default page reload. */
export function Form({
  onSubmit,
  children,
  label,
}: {
  onSubmit: () => void;
  children: ReactNode;
  label: string;
}) {
  return (
    <form
      aria-label={label}
      noValidate
      onSubmit={(e: FormEvent) => {
        e.preventDefault();
        onSubmit();
      }}
      style={{ display: "flex", flexDirection: "column", gap: "var(--wh-space-3)" }}
    >
      {children}
    </form>
  );
}

export function SubmitButton({
  children,
  disabled,
  onClick,
  type = "submit",
  ariaLabel,
}: {
  children: ReactNode;
  disabled?: boolean;
  onClick?: () => void;
  type?: "submit" | "button";
  ariaLabel?: string;
}) {
  return (
    <button
      type={type}
      onClick={onClick}
      disabled={disabled}
      aria-label={ariaLabel}
      style={{
        padding: "9px 16px",
        borderRadius: "var(--wh-radius-md)",
        border: "none",
        background: disabled ? "var(--wh-color-border)" : "var(--wh-color-accent)",
        color: "#fff",
        fontWeight: 600,
        fontSize: "var(--wh-font-size-sm)",
        cursor: disabled ? "not-allowed" : "pointer",
        height: 36,
      }}
    >
      {children}
    </button>
  );
}

export function InlineError({ message }: { message: string | null }) {
  if (!message) return null;
  return (
    <div
      role="alert"
      style={{
        color: "var(--wh-color-status-danger)",
        background: "var(--wh-color-status-danger-bg)",
        borderRadius: "var(--wh-radius-md)",
        padding: "8px 12px",
        fontSize: "var(--wh-font-size-sm)",
      }}
    >
      {message}
    </div>
  );
}

export function InlineSuccess({ message }: { message: ReactNode | null }) {
  if (!message) return null;
  return (
    <div
      role="status"
      style={{
        color: "var(--wh-color-status-success)",
        background: "var(--wh-color-status-success-bg)",
        borderRadius: "var(--wh-radius-md)",
        padding: "8px 12px",
        fontSize: "var(--wh-font-size-sm)",
      }}
    >
      {message}
    </div>
  );
}
