import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import ReactFlow, {
  Background,
  Controls,
  Handle,
  MarkerType,
  Position,
  type Edge,
  type Node,
  type NodeProps,
} from "reactflow";

import { Badge, Card, ErrorState, Loading } from "../components/ui";
import { useLineage } from "../hooks/useDashboard";
import { formatNumber, formatTime } from "../services/format";
import type { LineageGraph, NodeStatus, PipelineNode } from "../types/api";

// Page C — Pipeline Lineage.
//
// The graph is built entirely from the backend /lineage response, whose node
// statuses are derived from real artefact state. Clicking a node opens a details
// panel. Animated edges reflect *real* recent pipeline activity (a run exists);
// when there is no data the edges are static and a notice explains why.

const STATUS_META: Record<
  NodeStatus,
  { color: string; ring: string; label: string }
> = {
  healthy: { color: "#34d399", ring: "#1d5b48", label: "Healthy" },
  processing: { color: "#fbbf24", ring: "#6b5416", label: "Processing" },
  delayed: { color: "#fb923c", ring: "#6b3f16", label: "Delayed" },
  failed: { color: "#f87171", ring: "#6b2a2f", label: "Failed" },
  disabled: { color: "#64748b", ring: "#33415f", label: "Disabled / not configured" },
  unknown: { color: "#475569", ring: "#33415f", label: "Unknown" },
};

type FlowNodeData = { node: PipelineNode; selected: boolean; onSelect: (id: string) => void };

function StageNode({ data }: NodeProps<FlowNodeData>) {
  const { node, selected, onSelect } = data;
  const meta = STATUS_META[node.status] ?? STATUS_META.unknown;
  return (
    <div
      onClick={() => onSelect(node.id)}
      style={{
        minWidth: 160,
        maxWidth: 180,
        background: "var(--bg-elev)",
        border: `1.5px solid ${selected ? "var(--accent)" : meta.ring}`,
        borderRadius: 10,
        padding: "10px 12px",
        cursor: "pointer",
        boxShadow: selected ? "0 0 0 3px var(--accent-soft)" : "var(--shadow)",
      }}
      role="button"
      tabIndex={0}
      aria-label={`${node.label}, status ${node.status}`}
      onKeyDown={(e) => e.key === "Enter" && onSelect(node.id)}
    >
      <Handle type="target" position={Position.Left} style={{ background: meta.color }} />
      <div style={{ display: "flex", alignItems: "center", gap: 7 }}>
        <span
          className="dot"
          style={{ background: meta.color, boxShadow: `0 0 8px ${meta.color}` }}
        />
        <span style={{ fontWeight: 600, fontSize: 13 }}>{node.label}</span>
      </div>
      <div className="faint" style={{ fontSize: 10.5, marginTop: 4 }}>
        {node.technology}
      </div>
      <div
        style={{
          display: "flex",
          justifyContent: "space-between",
          marginTop: 7,
          fontSize: 10.5,
          color: "var(--text-muted)",
        }}
      >
        <span>in {node.records_in == null ? "—" : formatNumber(node.records_in, { compact: true })}</span>
        <span>out {node.records_out == null ? "—" : formatNumber(node.records_out, { compact: true })}</span>
      </div>
      {node.errors != null && node.errors > 0 && (
        <div style={{ marginTop: 4, fontSize: 10.5, color: "var(--err)" }}>
          {node.errors} errors
        </div>
      )}
      <Handle type="source" position={Position.Right} style={{ background: meta.color }} />
    </div>
  );
}

const nodeTypes = { stage: StageNode };

// Imperative fit: measure the container after mount, then frame the graph. This
// is more reliable than the declarative fitView prop when data arrives asynchronously.
// Imperative fit: measure the container after mount, then frame the graph. This
// avoids the classic React Flow issue where fitView runs before the container
// has its final size. Graph bounds are known from POSITIONS.
function FlowCanvas({
  nodes,
  edges,
}: {
  nodes: Node<FlowNodeData>[];
  edges: Edge[];
}) {
  const wrapRef = useRef<HTMLDivElement>(null);
  const [viewport, setViewport] = useState<{ x: number; y: number; zoom: number } | null>(null);

  useEffect(() => {
    const el = wrapRef.current;
    if (!el || nodes.length === 0) return;
    const measure = () => {
      const w = el.clientWidth;
      const h = el.clientHeight;
      if (!w || !h) return;
      // Known graph bounds from POSITIONS (node box ≈ 180×120).
      const minX = 0;
      const maxX = 1670;
      const minY = 0;
      const maxY = 475;
      const gw = maxX - minX;
      const gh = maxY - minY;
      const zoom = Math.min((w - 40) / gw, (h - 40) / gh, 1.15);
      const x = (w - gw * zoom) / 2 - minX * zoom;
      const y = (h - gh * zoom) / 2 - minY * zoom;
      setViewport({ x, y, zoom });
    };
    measure();
    const ro = new ResizeObserver(measure);
    ro.observe(el);
    return () => ro.disconnect();
  }, [nodes.length]);

  return (
    <div ref={wrapRef} style={{ width: "100%", height: "100%" }}>
      {viewport && (
        <ReactFlow
          nodes={nodes}
          edges={edges}
          nodeTypes={nodeTypes}
          defaultViewport={viewport}
          minZoom={0.25}
          maxZoom={1.6}
          style={{ width: "100%", height: "100%" }}
          proOptions={{ hideAttribution: true }}
          nodesConnectable={false}
          elementsSelectable
        >
          <Background color="#1a2233" gap={22} />
          <Controls style={{ background: "var(--bg-elev)", border: "1px solid var(--border)" }} />
        </ReactFlow>
      )}
    </div>
  );
}

// Manual layout: compact left-to-right flow. Kept tight so the whole graph
// reads well even in a modestly-sized browser window. All coordinates are
// non-negative so the computed viewport frames the graph predictably.
const POSITIONS: Record<string, { x: number; y: number }> = {
  generator: { x: 0, y: 210 },
  ingestion: { x: 180, y: 210 },
  broker: { x: 360, y: 210 },
  validation: { x: 540, y: 210 },
  emulation: { x: 360, y: 40 },
  storage: { x: 540, y: 40 },
  bronze: { x: 730, y: 120 },
  quarantine: { x: 730, y: 320 },
  silver: { x: 920, y: 120 },
  enrichment: { x: 920, y: 300 },
  gold: { x: 1110, y: 120 },
  orchestration: { x: 1110, y: 15 },
  dq: { x: 1300, y: 40 },
  recon: { x: 1300, y: 145 },
  anomaly: { x: 1300, y: 250 },
  duckdb: { x: 1300, y: 355 },
  dashboard: { x: 1490, y: 265 },
};

export function LineagePage() {
  const lineage = useLineage();
  const [selectedId, setSelectedId] = useState<string | null>(null);

  const onSelect = useCallback((id: string) => setSelectedId(id), []);

  const graph: LineageGraph | undefined = lineage.data;
  const active = Boolean(graph?.nodes.some((n) => n.status === "healthy"));

  const nodes: Node<FlowNodeData>[] = useMemo(() => {
    if (!graph) return [];
    return graph.nodes.map((n) => ({
      id: n.id,
      type: "stage",
      position: POSITIONS[n.id] ?? { x: 0, y: 0 },
      data: { node: n, selected: n.id === selectedId, onSelect },
      draggable: true,
    }));
  }, [graph, selectedId, onSelect]);

  const edges: Edge[] = useMemo(() => {
    if (!graph) return [];
    return graph.edges.map((e) => {
      const healthyEdge =
        active &&
        graph.nodes.find((n) => n.id === e.source)?.status === "healthy" &&
        graph.nodes.find((n) => n.id === e.target)?.status === "healthy";
      return {
        id: e.id,
        source: e.source,
        target: e.target,
        label: e.label ?? undefined,
        animated: healthyEdge,
        style: {
          stroke: healthyEdge ? "#5b8def" : "#33415f",
          strokeWidth: healthWidth(healthyEdge),
        },
        labelStyle: { fill: "#64748b", fontSize: 10 },
        labelBgStyle: { fill: "#0b0f1a" },
        markerEnd: {
          type: MarkerType.ArrowClosed,
          color: healthyEdge ? "#5b8def" : "#33415f",
        },
      };
    });
  }, [graph, active]);

  const selected = graph?.nodes.find((n) => n.id === selectedId) ?? null;

  if (lineage.isLoading) return <Loading label="Loading pipeline lineage…" />;
  if (lineage.isError) {
    return (
      <ErrorState
        message={(lineage.error as Error).message}
        onRetry={() => lineage.refetch()}
      />
    );
  }

  return (
    <div className="stack" style={{ height: "100%" }}>
      <div className="page-head">
        <div>
          <h1>Pipeline Lineage</h1>
          <p>
            The path data follows through the system, from event intake to the
            analytics layer. Node status reflects current pipeline state; edges
            animate only while recent data is flowing. Click any node for details.
          </p>
        </div>
        <div className="toolbar">
          <span className="small faint">
            {active ? "Live activity detected" : "No recent activity"}
          </span>
        </div>
      </div>

      {!active && (
        <div className="alert alert--warn">
          No recent healthy data flow detected, so edges are shown static. Run the
          pipeline or generate events to see movement — the app never animates a
          stopped pipeline.
        </div>
      )}

      <Legend />

      <div className="grid-2" style={{ gridTemplateColumns: selected ? "1fr 360px" : "1fr" }}>
        <Card className="lineage-canvas" >
          <div style={{ height: 440, borderRadius: 8, overflow: "hidden", border: "1px solid var(--border)" }}>
            <FlowCanvas nodes={nodes} edges={edges} />
          </div>
        </Card>

        {selected && (
          <NodePanel node={selected} onClose={() => setSelectedId(null)} />
        )}
      </div>
    </div>
  );
}

function healthWidth(active: boolean) {
  return active ? 2 : 1.2;
}

function Legend() {
  return (
    <div className="toolbar" style={{ gap: 16 }}>
      {(
        ["healthy", "processing", "delayed", "failed", "disabled"] as NodeStatus[]
      ).map((s) => (
        <span key={s} className="small muted" style={{ display: "flex", gap: 6, alignItems: "center" }}>
          <span className="dot" style={{ background: STATUS_META[s].color }} />
          {STATUS_META[s].label}
        </span>
      ))}
    </div>
  );
}

function NodePanel({ node, onClose }: { node: PipelineNode; onClose: () => void }) {
  return (
    <Card
      title={<h2>{node.label}</h2>}
      actions={
        <button className="btn btn--sm btn--ghost" onClick={onClose} type="button">
          Close
        </button>
      }
    >
      <div className="stack" style={{ gap: 12 }}>
        <div className="pill-row">
          <Badge tone={statusTone(node.status)}>
            {STATUS_META[node.status]?.label ?? node.status}
          </Badge>
          <Badge tone="violet">{node.technology}</Badge>
        </div>

        {node.description && <p className="small muted" style={{ margin: 0 }}>{node.description}</p>}

        <div className="stack" style={{ gap: 6 }}>
          <Detail label="Records in" value={formatNumber(node.records_in)} />
          <Detail label="Records out" value={formatNumber(node.records_out)} />
          <Detail label="Errors" value={formatNumber(node.errors)} />
          <Detail label="Last processed" value={formatTime(node.last_processed)} />
        </div>

        {Object.keys(node.metrics).length > 0 && (
          <div className="stack" style={{ gap: 6 }}>
            {Object.entries(node.metrics).map(([k, v]) => (
              <Detail key={k} label={k} value={String(v)} />
            ))}
          </div>
        )}

        {node.endpoint && (
          <div>
            <div className="faint small">API endpoint</div>
            <code className="cell-mono">{node.endpoint}</code>
          </div>
        )}
        {node.doc_link && (
          <div className="small">
            <a href={node.doc_link} target="_blank" rel="noreferrer">
              Documentation ↗
            </a>
          </div>
        )}
      </div>
    </Card>
  );
}

function Detail({ label, value }: { label: string; value: string }) {
  return (
    <div style={{ display: "flex", justifyContent: "space-between", gap: 12 }}>
      <span className="faint small">{label}</span>
      <span className="small">{value}</span>
    </div>
  );
}

function statusTone(s: NodeStatus): "ok" | "warn" | "err" | "muted" | "info" {
  if (s === "healthy") return "ok";
  if (s === "processing" || s === "delayed") return "warn";
  if (s === "failed") return "err";
  if (s === "disabled" || s === "unknown") return "muted";
  return "info";
}
