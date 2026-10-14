import { useMemo } from "react";
import { Link } from "react-router-dom";

import { Card, ErrorState, KpiCard, Loading, StatusDot } from "../components/ui";
import { LatencyChart, ThroughputChart } from "../components/charts";
import { useHealth, useLatency, useOverview, useThroughput } from "../hooks/useDashboard";
import { formatNumber, formatTime, relativeTime } from "../services/format";

// Page A — Overview. Covers every required KPI plus throughput and
// latency time-series, driven entirely by real backend responses.

export function OverviewPage() {
  const overview = useOverview();
  const throughput = useThroughput();
  const latency = useLatency();
  const health = useHealth();

  const series = useMemo(() => {
    const pts = throughput.data?.points ?? [];
    const byMinute = new Map<string, number>();
    for (const p of pts) {
      byMinute.set(p.minute, (byMinute.get(p.minute) ?? 0) + p.count);
    }
    return Array.from(byMinute.entries())
      .sort((a, b) => a[0].localeCompare(b[0]))
      .map(([minute, total]) => ({ minute: minute.slice(11), total }));
  }, [throughput.data]);

  const latencyData = (latency.data?.latency ?? []).slice(0, 10);

  if (overview.isLoading) return <Loading label="Loading platform overview…" />;

  if (overview.isError) {
    return (
      <ErrorState
        message={(overview.error as Error).message}
        onRetry={() => overview.refetch()}
      />
    );
  }

  const data = overview.data!;

  return (
    <div className="stack">
      <div className="page-head">
        <div>
          <h1>Overview</h1>
          <p>
            Live status of the synthetic sportsbook data platform. Counts are read
            from the latest pipeline run; rates are measured per second. This is a
            local demonstration using synthetic events only.
          </p>
        </div>
        <div className="toolbar">
          <StatusDot status={health.data?.pipeline ?? "unknown"} />
          <span className="small muted">
            Pipeline: {health.data?.pipeline ?? "unknown"}
          </span>
          <span className="small faint">
            last success {relativeTime(health.data?.last_success)}
          </span>
        </div>
      </div>

      {!data.available && (
        <div className="alert alert--info">
          {data.detail ?? "No pipeline run yet."} Open{" "}
          <Link to="/demo">Pipeline Controls</Link> to generate a dataset.
        </div>
      )}

      <div className="kpi-grid">
        {data.kpis.map((k) => (
          <KpiCard
            key={k.key}
            label={k.label}
            kind={k.kind}
            available={k.available}
            detail={k.detail}
            unit={k.unit}
            value={
              k.value === null
                ? ""
                : k.kind === "rate"
                  ? formatNumber(Math.round(k.value))
                  : formatNumber(k.value, { compact: k.kind === "total" })
            }
          />
        ))}
      </div>

      <div className="grid-2">
        <Card
          title="Event throughput"
          actions={
            <span className="small faint">
              {series.length} minutes · events/minute
            </span>
          }
        >
          {throughput.isLoading ? (
            <Loading />
          ) : series.length === 0 ? (
            <div className="state">
              <span className="state__title">No throughput data</span>
              <span className="muted small">
                Run a pipeline to populate Gold event-volume marts.
              </span>
            </div>
          ) : (
            <ThroughputChart data={series} />
          )}
        </Card>

        <Card
          title="Processing latency by event type"
          actions={<span className="small faint">milliseconds</span>}
        >
          {latency.isLoading ? (
            <Loading />
          ) : latencyData.length === 0 ? (
            <div className="state">
              <span className="state__title">No latency data</span>
              <span className="muted small">
                Processing latency is derived from Silver records.
              </span>
            </div>
          ) : (
            <LatencyChart data={latencyData} />
          )}
        </Card>
      </div>

      <div className="grid-3">
        <Card title="Pipeline health">
          <div className="stack" style={{ gap: 10 }}>
            <Row label="Transport" value={health.data?.transport ?? "—"} />
            <Row label="Storage" value={health.data?.storage ?? "—"} />
            <Row
              label="Last successful run"
              value={formatTime(health.data?.last_success)}
            />
            <Row label="Run id" value={data.run_id ?? "—"} mono />
          </div>
        </Card>

        <Card title="Layer object counts">
          <div className="stack" style={{ gap: 10 }}>
            {(health.data?.layers ?? []).map((l) => (
              <Row
                key={l.layer}
                label={l.layer.toUpperCase()}
                value={
                  l.available
                    ? `${formatNumber(l.object_count)} objects`
                    : "Not built"
                }
              />
            ))}
          </div>
        </Card>

        <Card title="Interpreting these metrics">
          <ul className="small muted" style={{ margin: 0, paddingLeft: 18 }}>
            <li>
              <b>Totals</b> are cumulative counts for the latest run.
            </li>
            <li>
              <b>Rates</b> (throughput) are measured events/second.
            </li>
            <li>
              <b>Latency</b> is processing latency from Silver, not wall-clock
              end-to-end.
            </li>
            <li>Updates are eventually consistent across layers.</li>
          </ul>
        </Card>
      </div>
    </div>
  );
}

function Row({
  label,
  value,
  mono,
}: {
  label: string;
  value: string;
  mono?: boolean;
}) {
  return (
    <div style={{ display: "flex", justifyContent: "space-between", gap: 12 }}>
      <span className="muted small">{label}</span>
      <span className={`small ${mono ? "mono faint" : ""}`}>{value}</span>
    </div>
  );
}
