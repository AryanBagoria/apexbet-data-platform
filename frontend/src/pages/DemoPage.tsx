import { useEffect, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";

import { Badge, Card, Loading } from "../components/ui";
import { api } from "../services/api";
import { formatNumber } from "../services/format";
import type { DemoJob } from "../types/api";

// Page H — Pipeline Controls.
//
// Every action maps to a bounded, validated FastAPI endpoint. There is no shell
// access: the only things a reviewer can do are generate bounded synthetic data
// and trigger the predefined scenarios.

type Action = {
  kind: string;
  title: string;
  detail: string;
  tone: "ok" | "info" | "warn" | "err";
  count?: number;
};

const PRESETS: Action[] = [
  { kind: "generate_small", title: "Generate small dataset", detail: "≈2–3k clean events through the full pipeline.", tone: "ok", count: 2500 },
  { kind: "generate_medium", title: "Generate medium dataset", detail: "≈10k events with injected anomalies.", tone: "info", count: 10000 },
  { kind: "full_pipeline", title: "Run full pipeline", detail: "Generate → stream → Bronze/Silver/Gold → enrichment.", tone: "info", count: 5000 },
  { kind: "inject_duplicates", title: "Inject duplicate events", detail: "High duplicate rate to exercise dedup.", tone: "warn", count: 3000 },
  { kind: "inject_invalid", title: "Inject invalid records", detail: "Invalid amounts, missing refs, schema drift.", tone: "warn", count: 3000 },
  { kind: "traffic_spike", title: "Simulate traffic spike", detail: "Bursty volume to trigger volume anomalies.", tone: "warn", count: 4000 },
  { kind: "settlement_discrepancy", title: "Introduce settlement discrepancy", detail: "Mismatched settlement amounts for reconciliation.", tone: "err", count: 3000 },
];

export function DemoPage() {
  const qc = useQueryClient();
  const [count, setCount] = useState(4000);
  const [countCap] = useState(50000);
  const [seed, setSeed] = useState(1337);
  const [activeJob, setActiveJob] = useState<DemoJob | null>(null);

  const start = useMutation({
    mutationFn: ({ kind, n }: { kind: string; n: number }) =>
      api.startDemo(kind, n, seed),
    onSuccess: (job) => setActiveJob(job),
  });

  const job = useQuery({
    queryKey: ["demo-job", activeJob?.job_id],
    queryFn: () => api.demoJob(activeJob!.job_id),
    enabled: Boolean(activeJob?.job_id),
    refetchInterval: (q) => {
      const status = q.state.data?.status;
      return status === "running" || status === "queued" ? 1500 : false;
    },
  });

  // When a job finishes, refresh the pages that read pipeline artefacts so the
  // whole dashboard reflects the new run.
  const current = job.data ?? activeJob;
  useEffect(() => {
    if (current?.status === "succeeded") {
      qc.invalidateQueries();
    }
  }, [current?.status, qc]);

  const running = current?.status === "running" || current?.status === "queued";

  return (
    <div className="stack">
      <div className="page-head">
        <div>
          <h1>Pipeline Controls</h1>
          <p>
            Run bounded pipeline jobs from the UI. Every action is validated and
            executed server-side; there is no shell access. Only synthetic data is
            generated.
          </p>
        </div>
        <Badge tone="warn">SYNTHETIC ONLY</Badge>
      </div>

      <Card title="Workload settings">
        <div className="toolbar" style={{ gap: 20 }}>
          <label className="small muted">
            Events{" "}
            <input
              className="input"
              type="number"
              min={100}
              max={countCap}
              value={count}
              onChange={(e) =>
                setCount(Math.max(100, Math.min(countCap, Number(e.target.value) || 100)))
              }
              style={{ width: 120 }}
              aria-label="Event count"
            />
          </label>
          <label className="small muted">
            Seed{" "}
            <input
              className="input"
              type="number"
              min={0}
              value={seed}
              onChange={(e) => setSeed(Math.max(0, Number(e.target.value) || 0))}
              style={{ width: 100 }}
              aria-label="Seed"
            />
          </label>
          <span className="small faint">
            Maximum {formatNumber(countCap)} events per job. Larger workloads are run
            from the CLI (`make bench-1m`), not the browser.
          </span>
        </div>
      </Card>

      <div className="grid-3">
        {PRESETS.map((p) => (
          <Card key={p.kind}>
            <div className="stack" style={{ gap: 10 }}>
              <Badge tone={p.tone}>{p.kind}</Badge>
              <h3>{p.title}</h3>
              <p className="small muted" style={{ margin: 0 }}>{p.detail}</p>
              <button
                className="btn btn--primary"
                disabled={running || start.isPending}
                onClick={() => start.mutate({ kind: p.kind, n: p.count ?? count })}
                type="button"
              >
                {running ? "Running…" : "Run"}
              </button>
            </div>
          </Card>
        ))}
      </div>

      <Card title="Latest run result">
        {start.isError && (
          <div className="alert alert--err">
            {(start.error as Error).message}
          </div>
        )}
        {!current ? (
          <div className="state">
            <span className="state__title">No run yet</span>
            <span className="muted small">
              Choose an action above. Results appear here.
            </span>
          </div>
        ) : (
          <div className="stack" style={{ gap: 14 }}>
            <div className="toolbar" style={{ gap: 14 }}>
              <Badge tone={statusTone(current.status)}>{current.status}</Badge>
              <span className="small faint mono">{current.job_id}</span>
              {running && <Loading label="Working…" />}
            </div>

            {current.error && <div className="alert alert--err">{current.error}</div>}

            {current.result && (
              <ResultSummary result={current.result as Record<string, unknown>} />
            )}

            <div>
              <div className="faint small" style={{ marginBottom: 6 }}>
                Job log
              </div>
              <div
                className="cell-mono"
                style={{
                  background: "var(--bg-inset)",
                  border: "1px solid var(--border)",
                  borderRadius: 8,
                  padding: 12,
                  maxHeight: 200,
                  overflowY: "auto",
                }}
              >
                {current.log.length === 0 ? "—" : current.log.map((l, i) => <div key={i}>{l}</div>)}
              </div>
            </div>
          </div>
        )}
      </Card>
    </div>
  );
}

function ResultSummary({ result }: { result: Record<string, unknown> }) {
  const pm = (result.pipeline_metrics ?? {}) as Record<string, number>;
  const gold = (result.gold_tables ?? {}) as Record<string, number>;
  const goldTotal = Object.values(gold).reduce((a, b) => a + (Number(b) || 0), 0);
  return (
    <div className="kpi-grid">
      <Stat label="Consumed" value={pm.consumed} />
      <Stat label="Bronze written" value={pm.bronze_written} />
      <Stat label="Silver written" value={pm.silver_written} />
      <Stat label="Quarantined" value={pm.quarantined} />
      <Stat label="Duplicates" value={pm.duplicates} />
      <Stat label="Throughput (eps)" value={pm.throughput_eps} />
      <Stat label="Gold mart rows" value={goldTotal} />
      <Stat label="Enrichment records" value={result.enrichment_records as number} />
    </div>
  );
}

function Stat({ label, value }: { label: string; value?: number | null }) {
  return (
    <div className="kpi">
      <div className="kpi__label">
        <span>{label}</span>
      </div>
      <div className="kpi__value">
        {value == null ? "—" : formatNumber(Number(value))}
      </div>
    </div>
  );
}

function statusTone(s: string): "ok" | "warn" | "err" | "info" {
  if (s === "succeeded") return "ok";
  if (s === "failed") return "err";
  if (s === "running" || s === "queued") return "warn";
  return "info";
}
