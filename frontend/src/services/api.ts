// Thin typed API client over the FastAPI backend.
// Every call hits the real backend; there are no frontend-only counters or
// hardcoded fixtures in live mode.

import type {
  AnomalyAlert,
  DemoJob,
  EnrichmentRecord,
  EnrichmentSummary,
  LatencyRow,
  LayerStat,
  LineageGraph,
  MetricsSnapshot,
  PipelineNode,
  PlatformHealth,
  QualityRule,
  ReconciliationRow,
  RecordPage,
  ThroughputResponse,
} from "../types/api";

export const API_BASE =
  import.meta.env.VITE_API_BASE ?? "/api/v1/dashboard";

class ApiError extends Error {
  status: number;
  constructor(message: string, status: number) {
    super(message);
    this.name = "ApiError";
    this.status = status;
  }
}

async function request<T>(
  path: string,
  init?: RequestInit & { timeoutMs?: number },
): Promise<T> {
  const { timeoutMs = 15000, ...rest } = init ?? {};
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const res = await fetch(`${API_BASE}${path}`, {
      ...rest,
      headers: { "Content-Type": "application/json", ...(rest.headers ?? {}) },
      signal: controller.signal,
    });
    if (!res.ok) {
      let detail = res.statusText;
      try {
        const body = (await res.json()) as { detail?: string };
        if (body.detail) detail = body.detail;
      } catch {
        /* non-JSON error body */
      }
      throw new ApiError(detail, res.status);
    }
    return (await res.json()) as T;
  } catch (err) {
    if (err instanceof ApiError) throw err;
    if ((err as Error).name === "AbortError") {
      throw new ApiError("Request timed out", 408);
    }
    throw new ApiError(
      "Cannot reach the backend. Is it running on :8000?",
      0,
    );
  } finally {
    clearTimeout(timer);
  }
}

export const api = {
  health: () => request<PlatformHealth>("/health"),
  overview: () => request<MetricsSnapshot>("/overview"),
  throughput: (minutes = 180) =>
    request<ThroughputResponse>(`/throughput?minutes=${minutes}`),
  latency: () =>
    request<{ available: boolean; detail?: string; latency: LatencyRow[] }>(
      "/latency",
    ),
  layers: () => request<LayerStat[]>("/layers"),
  datasets: () =>
    request<{ datasets: { dataset: string; layer: string; objects: number }[] }>(
      "/datasets",
    ),
  preview: (dataset: string, offset = 0, limit = 25) =>
    request<RecordPage>(
      `/preview?dataset=${encodeURIComponent(dataset)}&offset=${offset}&limit=${limit}`,
    ),
  quality: () => request<QualityRule[]>("/quality"),
  reconciliation: () => request<ReconciliationRow[]>("/reconciliation"),
  enrichment: (offset = 0, limit = 25) =>
    request<RecordPage>(`/enrichment?offset=${offset}&limit=${limit}`),
  enrichmentSummary: () =>
    request<EnrichmentSummary>("/enrichment/summary"),
  anomalies: () => request<AnomalyAlert[]>("/anomalies"),
  lineage: () => request<LineageGraph>("/lineage"),
  node: (id: string) => request<PipelineNode>(`/nodes/${id}`),
  startDemo: (kind: string, count: number, seed = 1337) =>
    request<DemoJob>(`/demo/${kind}`, {
      method: "POST",
      body: JSON.stringify({ count, seed }),
    }),
  demoJob: (id: string) => request<DemoJob>(`/demo/job/${id}`),
  demoLatest: () => request<DemoJob | null>("/demo/latest"),
};

export type { EnrichmentRecord };
