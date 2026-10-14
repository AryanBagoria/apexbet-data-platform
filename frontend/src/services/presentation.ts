// Shared presentation helpers (kept out of component files so React Fast
// Refresh stays happy).

export type Tone = "ok" | "warn" | "err" | "info" | "muted" | "violet";

export function severityTone(severity: string): Tone {
  switch (severity) {
    case "critical":
    case "error":
      return "err";
    case "warning":
    case "warn":
      return "warn";
    case "info":
      return "info";
    default:
      return "muted";
  }
}

export function statusColor(status: string): string {
  switch (status) {
    case "ok":
    case "healthy":
    case "succeeded":
      return "var(--ok)";
    case "warn":
    case "warning":
    case "processing":
    case "running":
      return "var(--warn)";
    case "err":
    case "error":
    case "failed":
    case "critical":
      return "var(--err)";
    case "disabled":
    case "unknown":
      return "var(--disabled)";
    default:
      return "var(--info)";
  }
}
