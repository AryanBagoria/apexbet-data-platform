// Typed frontend models mirroring the FastAPI dashboard response contracts.
// Keep these in sync with src/apexbet/serving/dashboard.py.

export type MetricKind = "total" | "rate" | "gauge" | "duration";

export interface Kpi {
  key: string;
  label: string;
  value: number | null;
  unit: string | null;
  kind: MetricKind;
  available: boolean;
  detail: string | null;
}

export interface MetricsSnapshot {
  available: boolean;
  detail: string | null;
  run_id: string | null;
  finished_at: string | null;
  kpis: Kpi[];
}

export interface LayerStat {
  layer: string;
  record_count: number | null;
  object_count: number | null;
  bytes: number | null;
  latest_modified: string | null;
  available: boolean;
  detail: string | null;
}

export interface RecordPage {
  dataset: string;
  columns: string[];
  rows: Record<string, unknown>[];
  total: number;
  offset: number;
  limit: number;
  available: boolean;
  detail: string | null;
}

export interface QualityRule {
  rule_id: string;
  dataset: string | null;
  severity: string;
  record_count: number | null;
  failure_count: number | null;
  description: string | null;
  passed: boolean | null;
  sample: unknown[];
}

export interface ReconciliationRow {
  rule_id: string;
  description: string | null;
  severity: string;
  expected: unknown;
  actual: unknown;
  difference: unknown;
  resolution_status: string | null;
}

export interface EnrichmentRecord {
  enrichment_id: string | null;
  event_id: string | null;
  model_name: string | null;
  model_version: string | null;
  enrichment_type: string | null;
  processing_status: string | null;
  confidence: number | null;
  created_at: string | null;
  enrichment_payload: unknown;
}

export interface AnomalyAlert {
  alert_id: string | null;
  detector: string | null;
  severity: string;
  metric: string | null;
  observed: number | null;
  expected: number | null;
  threshold: number | null;
  explanation: string | null;
  evidence: unknown;
  method: string;
  detected_at: string | null;
}

export interface EnrichmentSummary {
  available: boolean;
  detail: string | null;
  total: number;
  succeeded: number;
  failed: number;
  skipped: number;
  methods: Record<string, number>;
  models: Record<string, number>;
}

export type NodeStatus =
  | "healthy"
  | "processing"
  | "delayed"
  | "failed"
  | "disabled"
  | "unknown";

export interface PipelineNode {
  id: string;
  label: string;
  technology: string;
  status: NodeStatus;
  description: string | null;
  inputs: string[];
  outputs: string[];
  endpoint: string | null;
  doc_link: string | null;
  records_in: number | null;
  records_out: number | null;
  errors: number | null;
  last_processed: string | null;
  metrics: Record<string, unknown>;
}

export interface PipelineEdge {
  id: string;
  source: string;
  target: string;
  label: string | null;
}

export interface LineageGraph {
  available: boolean;
  detail: string | null;
  nodes: PipelineNode[];
  edges: PipelineEdge[];
}

export interface ThroughputPoint {
  minute: string;
  event_type: string;
  count: number;
}

export interface ThroughputResponse {
  available: boolean;
  detail: string | null;
  points: ThroughputPoint[];
}

export interface LatencyRow {
  event_type: string;
  n: number;
  p50_ms: number | null;
  p95_ms: number | null;
  p99_ms: number | null;
  max_ms: number | null;
}

export interface DemoJob {
  job_id: string;
  kind: string;
  status: "queued" | "running" | "succeeded" | "failed";
  started_at: string | null;
  finished_at: string | null;
  params: Record<string, unknown>;
  result: Record<string, unknown> | null;
  error: string | null;
  log: string[];
}

export interface PlatformHealth {
  pipeline: string;
  last_success: string | null;
  transport: string;
  storage: string;
  layers: LayerStat[];
}
