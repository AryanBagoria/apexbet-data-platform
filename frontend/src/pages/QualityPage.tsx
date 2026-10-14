import { useMemo, useState } from "react";

import { Badge, Card, EmptyState, ErrorState, Loading } from "../components/ui";
import { severityTone } from "../services/presentation";
import { useQuality, useReconciliation } from "../hooks/useDashboard";
import { formatNumber } from "../services/format";

// Page E — Data Quality & Reconciliation. Searchable, filterable tables over
// real DQ rule results and financial reconciliation findings.

export function QualityPage() {
  const quality = useQuality();
  const recon = useReconciliation();
  const [query, setQuery] = useState("");
  const [severity, setSeverity] = useState("all");

  const rules = useMemo(() => {
    let rows = quality.data ?? [];
    if (severity !== "all") rows = rows.filter((r) => r.severity === severity);
    if (query.trim()) {
      const q = query.toLowerCase();
      rows = rows.filter(
        (r) =>
          r.rule_id.toLowerCase().includes(q) ||
          (r.description ?? "").toLowerCase().includes(q) ||
          (r.dataset ?? "").toLowerCase().includes(q),
      );
    }
    return rows;
  }, [quality.data, query, severity]);

  if (quality.isLoading) return <Loading label="Loading quality results…" />;
  if (quality.isError) {
    return (
      <ErrorState
        message={(quality.error as Error).message}
        onRetry={() => quality.refetch()}
      />
    );
  }

  const totalFailures = (quality.data ?? []).reduce((a, r) => a + (r.failure_count ?? 0), 0);
  const failedRules = (quality.data ?? []).filter((r) => (r.failure_count ?? 0) > 0).length;

  return (
    <div className="stack">
      <div className="page-head">
        <div>
          <h1>Data Quality & Reconciliation</h1>
          <p>
            Deterministic validation rules over Silver events plus source-to-target
            financial reconciliation. Every finding carries a rule id, severity and
            reason.
          </p>
        </div>
      </div>

      <div className="kpi-grid">
        <Mini label="Rules evaluated" value={quality.data?.length ?? 0} />
        <Mini label="Rules with failures" value={failedRules} />
        <Mini label="Total failures" value={totalFailures} />
        <Mini
          label="Reconciliation findings"
          value={recon.data?.length ?? 0}
        />
      </div>

      <Card
        title="Validation rule outcomes"
        actions={
          <div className="toolbar">
            <input
              className="input"
              placeholder="Search rules…"
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              aria-label="Search rules"
            />
            <select
              className="select"
              value={severity}
              onChange={(e) => setSeverity(e.target.value)}
              aria-label="Filter by severity"
            >
              <option value="all">All severities</option>
              <option value="critical">Critical</option>
              <option value="error">Error</option>
              <option value="warning">Warning</option>
              <option value="info">Info</option>
            </select>
          </div>
        }
      >
        {rules.length === 0 ? (
          <EmptyState
            title="No matching rules"
            detail="Adjust the search or severity filter."
          />
        ) : (
          <div className="table-wrap" style={{ maxHeight: 460 }}>
            <table className="data">
              <thead>
                <tr>
                  <th>Rule</th>
                  <th>Dataset</th>
                  <th>Severity</th>
                  <th>Records</th>
                  <th>Failures</th>
                  <th>Status</th>
                  <th>Description</th>
                </tr>
              </thead>
              <tbody>
                {rules.map((r) => (
                  <tr key={r.rule_id}>
                    <td className="cell-mono">{r.rule_id}</td>
                    <td>{r.dataset ?? "—"}</td>
                    <td>
                      <Badge tone={severityTone(r.severity)}>{r.severity}</Badge>
                    </td>
                    <td>{formatNumber(r.record_count)}</td>
                    <td>{formatNumber(r.failure_count)}</td>
                    <td>
                      {(r.failure_count ?? 0) > 0 ? (
                        <Badge tone="warn">attention</Badge>
                      ) : (
                        <Badge tone="ok">pass</Badge>
                      )}
                    </td>
                    <td title={r.description ?? ""}>{r.description ?? "—"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </Card>

      <Card
        title="Financial reconciliation"
        actions={
          <span className="small faint">
            amounts shown as stored (integer minor units)
          </span>
        }
      >
        {recon.isLoading ? (
          <Loading />
        ) : (recon.data ?? []).length === 0 ? (
          <EmptyState
            title="No reconciliation findings"
            detail="Run `make reconcile` to produce a report."
          />
        ) : (
          <div className="table-wrap" style={{ maxHeight: 460 }}>
            <table className="data">
              <thead>
                <tr>
                  <th>Rule</th>
                  <th>Severity</th>
                  <th>Expected</th>
                  <th>Actual</th>
                  <th>Difference</th>
                  <th>Status</th>
                  <th>Description</th>
                </tr>
              </thead>
              <tbody>
                {(recon.data ?? []).map((r) => (
                  <tr key={r.rule_id}>
                    <td className="cell-mono">{r.rule_id}</td>
                    <td>
                      <Badge tone={severityTone(r.severity)}>{r.severity}</Badge>
                    </td>
                    <td className="cell-mono">{String(r.expected ?? "—")}</td>
                    <td className="cell-mono">{String(r.actual ?? "—")}</td>
                    <td className="cell-mono">{String(r.difference ?? "—")}</td>
                    <td>{r.resolution_status ?? "—"}</td>
                    <td title={r.description ?? ""}>{r.description ?? "—"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </Card>

      <div className="alert alert--info">
        Reprocessing actions are intentionally not exposed from the browser. Any
        replay or backfill is run through explicit, audited CLI commands, never via
        arbitrary shell execution from the UI.
      </div>
    </div>
  );
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
