import { useState } from "react";

import { Badge, Card, EmptyState, ErrorState, Loading } from "../components/ui";
import { useDatasets, useLayers, usePreview, useQuality } from "../hooks/useDashboard";
import { formatBytes, formatNumber, formatTime, relativeTime } from "../services/format";

// Page D — Data Pipeline. Bronze/Silver/Gold as distinct sections with real object
// stats and a dataset preview browser backed by DuckDB.

export function LayersPage() {
  const layers = useLayers();
  const datasets = useDatasets();
  const quality = useQuality();

  const [selected, setSelected] = useState("gold_event_counts");
  const [offset, setOffset] = useState(0);
  const limit = 20;
  const preview = usePreview(selected, offset, limit);

  if (layers.isLoading) return <Loading label="Loading ETL layers…" />;
  if (layers.isError) {
    return (
      <ErrorState
        message={(layers.error as Error).message}
        onRetry={() => layers.refetch()}
      />
    );
  }

  const byLayer = Object.fromEntries((layers.data ?? []).map((l) => [l.layer, l]));
  const silverFailures = (quality.data ?? []).reduce(
    (acc, r) => acc + (r.failure_count ?? 0),
    0,
  );

  return (
    <div className="stack">
      <div className="page-head">
        <div>
          <h1>Data Pipeline</h1>
          <p>
            Records move through three storage layers: Bronze (raw), Silver
            (cleaned) and Gold (analytics-ready). Each layer is stored as
            partitioned Parquet; Gold marts are queryable via DuckDB. Counts are
            read from the data lake as it exists on disk.
          </p>
        </div>
      </div>

      <div className="grid-3">
        <LayerCard
          tone="warn"
          name="Bronze"
          subtitle="Raw partitioned events"
          rows={[
            ["Objects", fmt(byLayer.bronze?.object_count)],
            ["Volume", formatBytes(byLayer.bronze?.bytes ?? null)],
            ["Latest write", relativeTime(byLayer.bronze?.latest_modified)],
            ["Event types", "15 (see schemas)"],
          ]}
          available={byLayer.bronze?.available}
        />
        <LayerCard
          tone="info"
          name="Silver"
          subtitle="Typed, deduped, late-aware"
          rows={[
            ["Objects", fmt(byLayer.silver?.object_count)],
            ["Volume", formatBytes(byLayer.silver?.bytes ?? null)],
            ["Latest write", relativeTime(byLayer.silver?.latest_modified)],
            ["Quality failures", formatNumber(silverFailures)],
          ]}
          available={byLayer.silver?.available}
        />
        <LayerCard
          tone="ok"
          name="Gold"
          subtitle="9 DuckDB analytical marts"
          rows={[
            ["Objects", fmt(byLayer.gold?.object_count)],
            ["Volume", formatBytes(byLayer.gold?.bytes ?? null)],
            ["Latest write", relativeTime(byLayer.gold?.latest_modified)],
            ["Marts", "9"],
          ]}
          available={byLayer.gold?.available}
        />
      </div>

      <Card title="Layer flow">
        <div
          style={{
            display: "flex",
            alignItems: "center",
            gap: 12,
            flexWrap: "wrap",
            justifyContent: "center",
            padding: "8px 0",
          }}
        >
          <FlowChip label={`Bronze · ${fmt(byLayer.bronze?.object_count)} objects`} tone="warn" />
          <Arrow />
          <FlowChip label={`Silver · ${fmt(byLayer.silver?.object_count)} objects`} tone="info" />
          <Arrow />
          <FlowChip label={`Gold · ${fmt(byLayer.gold?.object_count)} objects`} tone="ok" />
        </div>
      </Card>

      <Card
        title="Dataset browser"
        actions={
          <div className="toolbar">
            <select
              className="select"
              value={selected}
              onChange={(e) => {
                setSelected(e.target.value);
                setOffset(0);
              }}
              aria-label="Select dataset"
            >
              {(datasets.data?.datasets ?? []).map((d) => (
                <option key={d.dataset} value={d.dataset}>
                  {d.dataset}
                </option>
              ))}
            </select>
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
              disabled={(preview.data?.total ?? 0) <= offset + limit}
              onClick={() => setOffset(offset + limit)}
              type="button"
            >
              Next
            </button>
          </div>
        }
      >
        {preview.isLoading ? (
          <Loading />
        ) : !preview.data?.available ? (
          <EmptyState
            title="Dataset unavailable"
            detail={preview.data?.detail ?? "Select another dataset or run a pipeline."}
          />
        ) : preview.data.rows.length === 0 ? (
          <EmptyState title="No rows" detail="This dataset is empty." />
        ) : (
          <>
            <div className="table-wrap" style={{ maxHeight: 460 }}>
              <table className="data">
                <thead>
                  <tr>
                    {preview.data.columns.map((c) => (
                      <th key={c}>{c}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {preview.data.rows.map((row, i) => (
                    <tr key={i}>
                      {preview.data!.columns.map((c) => (
                        <td key={c} className="cell-mono" title={String(row[c] ?? "")}>
                          {String(row[c] ?? "—")}
                        </td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            <div className="toolbar" style={{ marginTop: 10, justifyContent: "space-between" }}>
              <span className="small faint">
                {formatNumber(offset + 1)}–{formatNumber(Math.min(offset + limit, preview.data.total))} of{" "}
                {formatNumber(preview.data.total)} rows
              </span>
              <span className="small faint">
                Showing bounded pages only — never the full dataset.
              </span>
            </div>
          </>
        )}
      </Card>

      <Card title="Layer freshness">
        <div className="stack" style={{ gap: 8 }}>
          {(layers.data ?? []).map((l) => (
            <div key={l.layer} style={{ display: "flex", justifyContent: "space-between" }}>
              <span className="muted small">{l.layer.toUpperCase()}</span>
              <span className="small">
                {l.available ? formatTime(l.latest_modified) : "Not built"}
              </span>
            </div>
          ))}
        </div>
      </Card>
    </div>
  );
}

function fmt(v: number | null | undefined) {
  return v == null ? "—" : formatNumber(v);
}

function Arrow() {
  return <span className="faint" aria-hidden="true">→</span>;
}

function FlowChip({ label, tone }: { label: string; tone: "ok" | "warn" | "info" }) {
  return <Badge tone={tone}>{label}</Badge>;
}

function LayerCard({
  name,
  subtitle,
  rows,
  tone,
  available,
}: {
  name: string;
  subtitle: string;
  rows: [string, string][];
  tone: "ok" | "warn" | "info";
  available?: boolean;
}) {
  return (
    <Card>
      <div className="card__title">
        <h2>{name}</h2>
        <Badge tone={available ? tone : "muted"}>
          {available ? "available" : "not built"}
        </Badge>
      </div>
      <div className="faint small" style={{ marginTop: -6, marginBottom: 10 }}>
        {subtitle}
      </div>
      <div className="stack" style={{ gap: 7 }}>
        {rows.map(([k, v]) => (
          <div key={k} style={{ display: "flex", justifyContent: "space-between" }}>
            <span className="muted small">{k}</span>
            <span className="small">{v}</span>
          </div>
        ))}
      </div>
    </Card>
  );
}
