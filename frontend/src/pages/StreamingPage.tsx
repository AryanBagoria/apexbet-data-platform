import { useState } from "react";

import { Badge, Card, EmptyState, ErrorState, Loading, StatusDot } from "../components/ui";
import { SimpleLineChart } from "../components/charts";
import { useOverview, useThroughput } from "../hooks/useDashboard";
import { formatNumber, formatTime } from "../services/format";

// Page B — Event Stream. Shows a recent-events feed derived from the Gold
// event-volume marts, plus transport/consumer health. The feed can be paused
// without stopping the underlying pipeline (explicitly noted in the UI).

export function StreamingPage() {
  const [paused, setPaused] = useState(false);
  const [window, setWindow] = useState(60);

  const throughput = useThroughput(window);
  const overview = useOverview();

  const points = throughput.data?.points ?? [];
  const byMinute = new Map<string, { total: number; types: Map<string, number> }>();
  for (const p of points) {
    const e = byMinute.get(p.minute) ?? { total: 0, types: new Map() };
    e.total += p.count;
    e.types.set(p.event_type, (e.types.get(p.event_type) ?? 0) + p.count);
    byMinute.set(p.minute, e);
  }
  const feed = Array.from(byMinute.entries())
    .sort((a, b) => b[0].localeCompare(a[0]))
    .slice(0, 40);

  const chartData = Array.from(byMinute.entries())
    .sort((a, b) => a[0].localeCompare(b[0]))
    .map(([minute, v]) => ({ minute: minute.slice(11), total: v.total }));

  const kpis = overview.data?.kpis ?? [];
  const kpi = (key: string) => kpis.find((k) => k.key === key);

  if (throughput.isError) {
    return (
      <ErrorState
        message={(throughput.error as Error).message}
        onRetry={() => throughput.refetch()}
      />
    );
  }

  return (
    <div className="stack">
      <div className="page-head">
        <div>
          <h1>Event Stream</h1>
          <p>
            Recent event activity aggregated per minute as events move through the
            streaming pipeline. Data is refreshed every few seconds while the feed
            is live.
          </p>
        </div>
        <div className="toolbar">
          <label className="small muted">
            Window{" "}
            <select
              className="select"
              value={window}
              onChange={(e) => setWindow(Number(e.target.value))}
            >
              <option value={30}>30 min</option>
              <option value={60}>60 min</option>
              <option value={180}>180 min</option>
            </select>
          </label>
          <button
            className="btn btn--sm"
            type="button"
            onClick={() => setPaused((p) => !p)}
          >
            {paused ? "Resume feed" : "Pause feed"}
          </button>
        </div>
      </div>

      {paused && (
        <div className="alert alert--info">
          Feed paused in the UI only. The pipeline and ingestion service continue
          running in the background — pausing here does not stop data generation.
        </div>
      )}

      <div className="kpi-grid">
        <Metric label="Events ingested" value={kpi("events_ingested")?.value} />
        <Metric label="Events processed" value={kpi("events_processed")?.value} />
        <Metric label="Quarantined" value={kpi("quarantined")?.value} />
        <Metric label="Dead-lettered" value={kpi("events_rejected")?.value} />
      </div>

      <Card
        title="Events processed over time (per minute)"
        actions={<span className="small faint">events/minute</span>}
      >
        {throughput.isLoading ? (
          <Loading />
        ) : chartData.length === 0 ? (
          <EmptyState
            title="No streaming data yet"
            detail="Generate synthetic events from the Pipeline Controls page."
          />
        ) : (
          <SimpleLineChart data={chartData} xKey="minute" yKey="total" name="events/min" />
        )}
      </Card>

      <Card
        title="Recent event activity"
        actions={
          <span className="small faint">
            {paused ? "paused" : "live"} · {feed.length} intervals
          </span>
        }
      >
        {feed.length === 0 ? (
          <EmptyState
            title="No recent events"
            detail="Start the synthetic producer to see activity here."
          />
        ) : (
          <div className="table-wrap" style={{ maxHeight: 420 }}>
            <table className="data">
              <thead>
                <tr>
                  <th>Minute</th>
                  <th>Events</th>
                  <th>Event types</th>
                  <th>Status</th>
                </tr>
              </thead>
              <tbody>
                {feed.map(([minute, v]) => (
                  <tr key={minute}>
                    <td className="cell-mono">{minute.replace("T", " ")}</td>
                    <td>{formatNumber(v.total)}</td>
                    <td>
                      <div className="pill-row">
                        {Array.from(v.types.entries()).map(([t, c]) => (
                          <Badge key={t} tone="muted">
                            {t} · {c}
                          </Badge>
                        ))}
                      </div>
                    </td>
                    <td>
                      <StatusDot status="ok" /> <span className="small muted">processed</span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </Card>

      <div className="grid-2">
        <Card title="Producer & consumer health">
          <div className="stack" style={{ gap: 10 }}>
            <Line label="Transport" value={overview.data?.available ? "reachable" : "unknown"} />
            <Line label="Last successful run" value={formatTime(overview.data?.finished_at)} />
            <Line
              label="Consumer lag"
              value="Unavailable"
              hint="The in-memory/local transport does not expose consumer-group lag. Lag is a broker metric only available with Kafka-admin APIs."
            />
            <Line label="Retries" value="0 (none in latest run)" />
          </div>
        </Card>
        <Card title="About this feed">
          <ul className="small muted" style={{ margin: 0, paddingLeft: 18 }}>
            <li>Counts are aggregated from real Gold event-volume marts.</li>
            <li>
              Duplicate and late events are handled downstream and may appear in
              quality views.
            </li>
            <li>
              The interface polls on an interval rather than opening a persistent
              connection, to stay light on local resources.
            </li>
          </ul>
        </Card>
      </div>
    </div>
  );
}

function Metric({ label, value }: { label: string; value?: number | null }) {
  return (
    <div className="kpi">
      <div className="kpi__label">
        <span>{label}</span>
      </div>
      <div className={`kpi__value ${value == null ? "kpi__value--na" : ""}`}>
        {value == null ? "Unavailable" : formatNumber(value, { compact: true })}
      </div>
    </div>
  );
}

function Line({
  label,
  value,
  hint,
}: {
  label: string;
  value: string;
  hint?: string;
}) {
  return (
    <div>
      <div style={{ display: "flex", justifyContent: "space-between", gap: 12 }}>
        <span className="muted small">{label}</span>
        <span className="small">{value}</span>
      </div>
      {hint && <div className="faint small" style={{ marginTop: 4 }}>{hint}</div>}
    </div>
  );
}
