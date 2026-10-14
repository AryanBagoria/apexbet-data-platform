import { useMemo } from "react";

import { Badge, Card, EmptyState, ErrorState, Loading } from "../components/ui";
import {
  DistributionChart,
  LatencyChart,
  MiniBarChart,
  SimpleLineChart,
} from "../components/charts";
import { usePreview, useLatency, useThroughput } from "../hooks/useDashboard";
import { formatMinor, formatNumber } from "../services/format";

// Page G — Analytics. Charts and tables over the actual Gold datasets. All
// financial values are synthetic demonstration data.

export function AnalyticsPage() {
  const throughput = useThroughput();
  const outcomes = usePreview("gold_bet_outcomes", 0, 50);
  const volume = usePreview("gold_event_volume_by_minute", 0, 200);
  const ledger = usePreview("gold_financial_ledger", 0, 50);

  const typeDist = useMemo(() => {
    const pts = throughput.data?.points ?? [];
    const byType = new Map<string, number>();
    for (const p of pts) {
      byType.set(p.event_type, (byType.get(p.event_type) ?? 0) + p.count);
    }
    return Array.from(byType.entries())
      .map(([name, value]) => ({ name, value }))
      .sort((a, b) => b.value - a.value)
      .slice(0, 8);
  }, [throughput.data]);

  const volumeSeries = useMemo(() => {
    const rows = volume.data?.rows ?? [];
    const byMinute = new Map<string, number>();
    for (const r of rows) {
      const m = String(r.event_minute ?? "");
      byMinute.set(m, (byMinute.get(m) ?? 0) + Number(r.event_count ?? 0));
    }
    return Array.from(byMinute.entries())
      .sort((a, b) => a[0].localeCompare(b[0]))
      .map(([minute, count]) => ({ minute: minute.slice(11), count }));
  }, [volume.data]);

  const outcomeRows = (outcomes.data?.rows ?? []).map((r) => ({
    result: String(r.result ?? "—"),
    bet_count: Number(r.bet_count ?? 0),
    settled_stake_minor: Number(r.settled_stake_minor ?? 0),
    payout_minor: Number(r.payout_minor ?? 0),
    gross_margin_minor: Number(r.gross_margin_minor ?? 0),
  }));

  if (throughput.isLoading) return <Loading label="Loading analytics…" />;
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
          <h1>Analytics</h1>
          <p>
            Charts built directly from Gold marts. Every betting and financial
            figure here is <b>synthetic demonstration data</b>.
          </p>
        </div>
        <Badge tone="warn">SYNTHETIC DATA</Badge>
      </div>

      <div className="grid-2">
        <Card title="Events per minute">
          {volumeSeries.length === 0 ? (
            <EmptyState title="No volume data" detail="Run a pipeline first." />
          ) : (
            <SimpleLineChart
              data={volumeSeries}
              xKey="minute"
              yKey="count"
              name="events/min"
              color="#5b8def"
              height={260}
            />
          )}
        </Card>

        <Card title="Event type distribution">
          {typeDist.length === 0 ? (
            <EmptyState title="No distribution data" />
          ) : (
            <DistributionChart data={typeDist} height={260} />
          )}
        </Card>
      </div>

      <div className="grid-2">
        <Card title="Settlement outcomes (synthetic)">
          {outcomeRows.length === 0 ? (
            <EmptyState title="No settlement data" detail="Run a pipeline first." />
          ) : (
            <MiniBarChart
              data={outcomeRows}
              xKey="result"
              yKey="bet_count"
              name="bets"
              color="#a78bfa"
              height={260}
            />
          )}
        </Card>

        <Card title="Processing latency trends">
          <LatencyTrends />
        </Card>
      </div>

      <Card
        title="Synthetic ledger summary"
        actions={<Badge tone="warn">synthetic</Badge>}
      >
        {ledger.isLoading ? (
          <Loading />
        ) : (ledger.data?.rows ?? []).length === 0 ? (
          <EmptyState title="No ledger rows" />
        ) : (
          <div className="table-wrap">
            <table className="data">
              <thead>
                <tr>
                  <th>Event type</th>
                  <th>Currency</th>
                  <th>Transactions</th>
                  <th>Total (minor units)</th>
                  <th>Total (display)</th>
                </tr>
              </thead>
              <tbody>
                {(ledger.data?.rows ?? []).map((r, i) => (
                  <tr key={i}>
                    <td>{String(r.event_type ?? "—")}</td>
                    <td>{String(r.currency ?? "—")}</td>
                    <td>{formatNumber(Number(r.txn_count ?? 0))}</td>
                    <td className="cell-mono">{formatNumber(Number(r.total_minor ?? 0))}</td>
                    <td>{formatMinor(Number(r.total_minor ?? 0), String(r.currency ?? "USD"))}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </Card>

      <Card title="Bet outcomes detail (synthetic)">
        {outcomeRows.length === 0 ? (
          <EmptyState title="No outcome rows" />
        ) : (
          <div className="table-wrap">
            <table className="data">
              <thead>
                <tr>
                  <th>Result</th>
                  <th>Bets</th>
                  <th>Staked</th>
                  <th>Payout</th>
                  <th>Gross margin</th>
                </tr>
              </thead>
              <tbody>
                {outcomeRows.map((r) => (
                  <tr key={r.result}>
                    <td>{r.result}</td>
                    <td>{formatNumber(r.bet_count)}</td>
                    <td>{formatMinor(r.settled_stake_minor)}</td>
                    <td>{formatMinor(r.payout_minor)}</td>
                    <td
                      style={{
                        color: r.gross_margin_minor >= 0 ? "var(--ok)" : "var(--err)",
                      }}
                    >
                      {formatMinor(r.gross_margin_minor)}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </Card>
    </div>
  );
}

function LatencyTrends() {
  const latency = useLatencyRows();
  if (latency.length === 0) {
    return <EmptyState title="No latency data" detail="Run a pipeline first." />;
  }
  return <LatencyChart data={latency.slice(0, 10)} height={260} />;
}

// small wrapper to reuse the shared hook without an extra import cycle in tests
function useLatencyRows() {
  const q = useLatency();
  return (q.data?.latency ?? []).map((r) => ({
    event_type: r.event_type,
    p50_ms: r.p50_ms,
    p95_ms: r.p95_ms,
    p99_ms: r.p99_ms,
  }));
}
