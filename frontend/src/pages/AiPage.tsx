import { useState } from "react";

import { Badge, Card, EmptyState, ErrorState, Loading } from "../components/ui";
import { severityTone } from "../services/presentation";
import { useAnomalies, useEnrichment, useEnrichmentSummary, useHealth } from "../hooks/useDashboard";
import { formatNumber, formatTime, shortId } from "../services/format";

// Page F — Enrichment & Anomalies. Enrichment adds derived fields to Silver
// records; anomaly detection raises alerts from aggregate metrics. Both are
// operational aids for triage and are not used to make account or financial
// decisions.

export function AiPage() {
  const [offset, setOffset] = useState(0);
  const limit = 20;
  const enrichment = useEnrichment(offset, limit);
  const summary = useEnrichmentSummary();
  const anomalies = useAnomalies();
  const health = useHealth();

  if (anomalies.isLoading) return <Loading label="Loading enrichment and anomaly data…" />;
  if (anomalies.isError) {
    return (
      <ErrorState
        message={(anomalies.error as Error).message}
        onRetry={() => anomalies.refetch()}
      />
    );
  }

  const rows = enrichment.data?.rows ?? [];

  return (
    <div className="stack">
      <div className="page-head">
        <div>
          <h1>Enrichment & Anomaly Detection</h1>
          <p>
            Enrichment adds derived fields (categories, descriptions) to cleaned
            records. Anomaly detection raises alerts from aggregate metrics. Rules
            run by default; conventional ML and a local LLM are optional.
          </p>
        </div>
        <div className="pill-row">
          <Badge tone="info">Transport: {health.data?.transport ?? "—"}</Badge>
          <Badge tone="muted">Enrichment: rules / local_ml / llm</Badge>
        </div>
      </div>

      <div className="alert alert--warn">
        Alerts on this page are operational signals for engineering triage. They
        are not fraud determinations and do not restrict or settle any account.
      </div>

      <div className="kpi-grid">
        <Mini label="Enrichment records" value={summary.data?.total ?? enrichment.data?.total ?? 0} />
        <Mini label="Succeeded" value={summary.data?.succeeded ?? 0} />
        <Mini label="Skipped (optional modes off)" value={summary.data?.skipped ?? 0} />
        <Mini label="Failed" value={summary.data?.failed ?? 0} />
        <Mini label="Anomaly alerts" value={anomalies.data?.length ?? 0} />
        <Mini
          label="Critical alerts"
          value={(anomalies.data ?? []).filter((a) => a.severity === "critical").length}
        />
      </div>

      <div className="grid-2">
        <Card title="Enrichment results">
          {enrichment.isLoading ? (
            <Loading />
          ) : !enrichment.data?.available ? (
            <EmptyState
              title="No enrichment records"
              detail={enrichment.data?.detail ?? "Run `make enrich`."}
            />
          ) : (
            <>
              <div className="table-wrap" style={{ maxHeight: 380 }}>
                <table className="data">
                  <thead>
                    <tr>
                      <th>Enrichment</th>
                      <th>Type</th>
                      <th>Model</th>
                      <th>Status</th>
                    </tr>
                  </thead>
                  <tbody>
                    {rows.map((r, i) => (
                      <tr key={i}>
                        <td className="cell-mono">{shortId(String(r.enrichment_id ?? ""))}</td>
                        <td>{String(r.enrichment_type ?? "—")}</td>
                        <td className="cell-mono">{String(r.model_name ?? "—")}</td>
                        <td>
                          <Badge tone={statusTone(String(r.processing_status ?? ""))}>
                            {String(r.processing_status ?? "—")}
                          </Badge>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              <div className="toolbar" style={{ marginTop: 10, justifyContent: "space-between" }}>
                <span className="small faint">
                  {formatNumber(offset + 1)}–
                  {formatNumber(Math.min(offset + limit, enrichment.data.total))} of{" "}
                  {formatNumber(enrichment.data.total)}
                </span>
                <div className="toolbar">
                  <button
                    className="btn btn--sm"
                    disabled={offset === 0}
                    onClick={() => setOffset(Math.max(0, offset - limit))}
                    type="button"
                  >
                    Prev
                  </button>
                  <button
                    className="btn btn--sm"
                    disabled={enrichment.data.total <= offset + limit}
                    onClick={() => setOffset(offset + limit)}
                    type="button"
                  >
                    Next
                  </button>
                </div>
              </div>
            </>
          )}
        </Card>

        <Card title="Enrichment modes">
          <div className="stack" style={{ gap: 12 }}>
            <Method
              tone="ok"
              title="1 · Rules (default)"
              detail="Runs with no model and no extra dependency. Produces stable categories and descriptions."
            />
            <Method
              tone="info"
              title="2 · Conventional ML (scikit-learn)"
              detail="Optional local model (for example TF-IDF + Naive Bayes). Enabled when a model is present."
            />
            <Method
              tone="violet"
              title="3 · Local LLM (Ollama)"
              detail="Optional. If Ollama is not running, records are marked skipped and the pipeline keeps working."
            />
          </div>
        </Card>
      </div>

      <Card
        title="Anomaly alerts"
        actions={<span className="small faint">rules-first; Isolation Forest optional</span>}
      >
        {(anomalies.data ?? []).length === 0 ? (
          <EmptyState
            title="No anomalies detected"
            detail="Inject a controlled anomaly from the Pipeline Controls page."
          />
        ) : (
          <div className="table-wrap">
            <table className="data">
              <thead>
                <tr>
                  <th>Severity</th>
                  <th>Detector</th>
                  <th>Method</th>
                  <th>Metric</th>
                  <th>Observed</th>
                  <th>Expected</th>
                  <th>Detected</th>
                  <th>Explanation</th>
                </tr>
              </thead>
              <tbody>
                {(anomalies.data ?? []).map((a, i) => (
                  <tr key={a.alert_id ?? i}>
                    <td>
                      <Badge tone={severityTone(a.severity)}>{a.severity}</Badge>
                    </td>
                    <td>{a.detector ?? "—"}</td>
                    <td>
                      <Badge tone={a.method === "isolation_forest" ? "violet" : "muted"}>
                        {a.method}
                      </Badge>
                    </td>
                    <td className="cell-mono">{a.metric ?? "—"}</td>
                    <td className="cell-mono">{fmtNum(a.observed)}</td>
                    <td className="cell-mono">{fmtNum(a.expected)}</td>
                    <td className="cell-mono">{formatTime(a.detected_at)}</td>
                    <td title={a.explanation ?? ""}>{a.explanation ?? "—"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </Card>

      <Card title="Suggested investigation steps">
        <ol className="small muted" style={{ margin: 0, paddingLeft: 18 }}>
          <li>Confirm the metric against the throughput chart on the Overview page.</li>
          <li>Check Data Quality for corresponding failures.</li>
          <li>Open the Pipeline Lineage graph and inspect the affected stage.</li>
          <li>Treat the finding as a signal to investigate, not a conclusion.</li>
        </ol>
      </Card>
    </div>
  );
}

function fmtNum(v: number | null | undefined) {
  return v == null ? "—" : formatNumber(Number(v.toFixed?.(2) ?? v));
}

function statusTone(s: string): "ok" | "warn" | "err" | "muted" {
  if (s === "success") return "ok";
  if (s === "skipped") return "warn";
  if (s === "failed") return "err";
  return "muted";
}

function Mini({ label, value }: { label: string; value: number }) {
  return (
    <div className="kpi">
      <div className="kpi__label">
        <span>{label}</span>
      </div>
      <div className="kpi__value">{formatNumber(value)}</div>
    </div>
  );
}

function Method({
  title,
  detail,
  tone,
}: {
  title: string;
  detail: string;
  tone: "ok" | "info" | "violet";
}) {
  return (
    <div>
      <Badge tone={tone}>{title}</Badge>
      <div className="small muted" style={{ marginTop: 4 }}>
        {detail}
      </div>
    </div>
  );
}
