// Small formatting helpers shared across pages.

export function formatNumber(
  value: number | null | undefined,
  opts: { compact?: boolean } = {},
): string {
  if (value === null || value === undefined || Number.isNaN(value)) return "Unavailable";
  if (opts.compact && Math.abs(value) >= 1000) {
    return new Intl.NumberFormat("en-US", {
      notation: "compact",
      maximumFractionDigits: 1,
    }).format(value);
  }
  return new Intl.NumberFormat("en-US").format(value);
}

export function formatBytes(bytes: number | null | undefined): string {
  if (bytes === null || bytes === undefined) return "Unavailable";
  if (bytes === 0) return "0 B";
  const units = ["B", "KB", "MB", "GB"];
  const i = Math.min(Math.floor(Math.log(bytes) / Math.log(1024)), units.length - 1);
  const val = bytes / Math.pow(1024, i);
  return `${val.toFixed(i === 0 ? 0 : 1)} ${units[i]}`;
}

export function formatTime(iso: string | null | undefined): string {
  if (!iso) return "Unavailable";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  return d.toLocaleString("en-US", {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}

export function relativeTime(iso: string | null | undefined): string {
  if (!iso) return "Unavailable";
  const d = new Date(iso).getTime();
  if (Number.isNaN(d)) return "Unavailable";
  const diff = Date.now() - d;
  const s = Math.round(diff / 1000);
  if (s < 60) return `${s}s ago`;
  const m = Math.round(s / 60);
  if (m < 60) return `${m}m ago`;
  const h = Math.round(m / 60);
  if (h < 24) return `${h}h ago`;
  return `${Math.round(h / 24)}d ago`;
}

export function formatMinor(minor: number | null | undefined, currency = "USD"): string {
  if (minor === null || minor === undefined) return "Unavailable";
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency,
  }).format(minor / 100);
}

export function shortId(id: string | null | undefined, len = 10): string {
  if (!id) return "—";
  return id.length > len ? `${id.slice(0, len)}…` : id;
}
