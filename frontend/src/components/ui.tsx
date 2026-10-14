import type { ReactNode } from "react";

import { statusColor } from "../services/presentation";

// Reusable UI primitives shared by every page.

export function Card({
  title,
  actions,
  children,
  className,
}: {
  title?: ReactNode;
  actions?: ReactNode;
  children: ReactNode;
  className?: string;
}) {
  return (
    <section className={`card ${className ?? ""}`}>
      {(title || actions) && (
        <div className="card__title">
          {typeof title === "string" ? <h2>{title}</h2> : title}
          {actions && <div className="toolbar">{actions}</div>}
        </div>
      )}
      {children}
    </section>
  );
}

export function Badge({
  tone = "muted",
  children,
}: {
  tone?: "ok" | "warn" | "err" | "info" | "muted" | "violet";
  children: ReactNode;
}) {
  return <span className={`badge badge--${tone}`}>{children}</span>;
}

export function StatusDot({ status }: { status: string }) {
  return <span className="dot" style={{ background: statusColor(status) }} />;
}

export function Loading({ label = "Loading…" }: { label?: string }) {
  return (
    <div className="state" role="status" aria-live="polite">
      <div className="spinner" />
      <span>{label}</span>
    </div>
  );
}

export function ErrorState({
  message,
  onRetry,
}: {
  message: string;
  onRetry?: () => void;
}) {
  return (
    <div className="state" role="alert">
      <span className="state__title">Could not load data</span>
      <span className="muted small">{message}</span>
      {onRetry && (
        <button className="btn btn--sm" onClick={onRetry} type="button">
          Retry
        </button>
      )}
    </div>
  );
}

export function EmptyState({
  title = "Nothing here yet",
  detail,
  action,
}: {
  title?: string;
  detail?: string;
  action?: ReactNode;
}) {
  return (
    <div className="state">
      <span className="state__title">{title}</span>
      {detail && <span className="muted small">{detail}</span>}
      {action}
    </div>
  );
}

export function KpiCard({
  label,
  value,
  unit,
  kind,
  available = true,
  detail,
}: {
  label: string;
  value: string;
  unit?: string | null;
  kind?: string;
  available?: boolean;
  detail?: string | null;
}) {
  const kindLabel =
    kind === "total"
      ? "total"
      : kind === "rate"
        ? "rate"
        : kind === "gauge"
          ? "gauge"
          : kind === "duration"
            ? "latency"
            : undefined;
  return (
    <div className="kpi">
      <div className="kpi__label">
        <span>{label}</span>
        {kindLabel && <span className="kpi__kind">{kindLabel}</span>}
      </div>
      {available ? (
        <div className="kpi__value">
          {value}
          {unit && <span className="kpi__unit">{unit}</span>}
        </div>
      ) : (
        <div className="kpi__value kpi__value--na">Unavailable</div>
      )}
      {detail && <div className="kpi__detail">{detail}</div>}
    </div>
  );
}
