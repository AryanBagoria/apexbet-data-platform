import {
  Area,
  AreaChart,
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  Legend,
  Line,
  LineChart,
  Pie,
  PieChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

// Chart primitives with a consistent dark theme. All data is passed in from
// real API responses; these components never invent values.

import { CHART_COLORS } from "../services/chartTheme";
import { formatNumber } from "../services/format";

const axisStyle = { fill: "#94a3b8", fontSize: 11 };
const gridStyle = { stroke: "#243049" };

// Compact duration label for latency axes/tooltips. Values in the millions of
// milliseconds are unreadable, so switch to seconds above 1000 ms.
function formatDurationMs(ms: number): string {
  if (!Number.isFinite(ms)) return "—";
  if (Math.abs(ms) >= 1000) return `${formatNumber(ms / 1000)}s`;
  return `${formatNumber(ms)}ms`;
}

const tooltipStyle = {
  background: "#121826",
  border: "1px solid #243049",
  borderRadius: 8,
  color: "#e6ebf5",
  fontSize: 12,
};

export function ThroughputChart({
  data,
  height = 260,
}: {
  data: { minute: string; total: number; [k: string]: number | string }[];
  height?: number;
}) {
  return (
    <ResponsiveContainer width="100%" height={height}>
      <AreaChart data={data} margin={{ top: 8, right: 12, left: -12, bottom: 0 }}>
        <defs>
          <linearGradient id="tpFill" x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor="#5b8def" stopOpacity={0.55} />
            <stop offset="100%" stopColor="#5b8def" stopOpacity={0.04} />
          </linearGradient>
        </defs>
        <CartesianGrid {...gridStyle} vertical={false} />
        <XAxis dataKey="minute" tick={axisStyle} tickLine={false} axisLine={false} />
        <YAxis tick={axisStyle} tickLine={false} axisLine={false} width={48} />
        <Tooltip contentStyle={tooltipStyle} />
        <Area
          type="monotone"
          dataKey="total"
          name="events/min"
          stroke="#5b8def"
          strokeWidth={2}
          fill="url(#tpFill)"
        />
      </AreaChart>
    </ResponsiveContainer>
  );
}

export function LatencyChart({
  data,
  height = 260,
}: {
  data: { event_type: string; p50_ms: number | null; p95_ms: number | null; p99_ms: number | null }[];
  height?: number;
}) {
  return (
    <ResponsiveContainer width="100%" height={height}>
      <BarChart data={data} margin={{ top: 8, right: 12, left: 4, bottom: 0 }}>
        <CartesianGrid {...gridStyle} vertical={false} />
        <XAxis dataKey="event_type" tick={axisStyle} tickLine={false} axisLine={false} interval={0} angle={-18} height={48} textAnchor="end" />
        <YAxis
          tick={axisStyle}
          tickLine={false}
          axisLine={false}
          width={72}
          tickFormatter={(v: number) => formatDurationMs(v)}
        />
        <Tooltip
          contentStyle={tooltipStyle}
          formatter={(v: number) => formatDurationMs(v)}
        />
        <Legend wrapperStyle={{ fontSize: 11, color: "#94a3b8" }} />
        <Bar dataKey="p50_ms" name="p50" fill="#34d399" radius={[3, 3, 0, 0]} />
        <Bar dataKey="p95_ms" name="p95" fill="#fbbf24" radius={[3, 3, 0, 0]} />
        <Bar dataKey="p99_ms" name="p99" fill="#f87171" radius={[3, 3, 0, 0]} />
      </BarChart>
    </ResponsiveContainer>
  );
}

export function SimpleLineChart({
  data,
  xKey,
  yKey,
  name,
  color = "#22d3ee",
  height = 220,
}: {
  data: Record<string, unknown>[];
  xKey: string;
  yKey: string;
  name: string;
  color?: string;
  height?: number;
}) {
  return (
    <ResponsiveContainer width="100%" height={height}>
      <LineChart data={data} margin={{ top: 8, right: 12, left: -12, bottom: 0 }}>
        <CartesianGrid {...gridStyle} vertical={false} />
        <XAxis dataKey={xKey} tick={axisStyle} tickLine={false} axisLine={false} />
        <YAxis tick={axisStyle} tickLine={false} axisLine={false} width={48} />
        <Tooltip contentStyle={tooltipStyle} />
        <Line type="monotone" dataKey={yKey} name={name} stroke={color} strokeWidth={2} dot={false} />
      </LineChart>
    </ResponsiveContainer>
  );
}

export function DistributionChart({
  data,
  height = 240,
}: {
  data: { name: string; value: number }[];
  height?: number;
}) {
  return (
    <ResponsiveContainer width="100%" height={height}>
      <PieChart>
        <Pie
          data={data}
          dataKey="value"
          nameKey="name"
          innerRadius={52}
          outerRadius={90}
          paddingAngle={2}
        >
          {data.map((_, i) => (
            <Cell key={i} fill={CHART_COLORS[i % CHART_COLORS.length]} />
          ))}
        </Pie>
        <Tooltip contentStyle={tooltipStyle} />
        <Legend wrapperStyle={{ fontSize: 11, color: "#94a3b8" }} />
      </PieChart>
    </ResponsiveContainer>
  );
}

export function MiniBarChart({
  data,
  xKey,
  yKey,
  name,
  color = "#a78bfa",
  height = 220,
}: {
  data: Record<string, unknown>[];
  xKey: string;
  yKey: string;
  name: string;
  color?: string;
  height?: number;
}) {
  return (
    <ResponsiveContainer width="100%" height={height}>
      <BarChart data={data} margin={{ top: 8, right: 12, left: -12, bottom: 0 }}>
        <CartesianGrid {...gridStyle} vertical={false} />
        <XAxis dataKey={xKey} tick={axisStyle} tickLine={false} axisLine={false} />
        <YAxis tick={axisStyle} tickLine={false} axisLine={false} width={48} />
        <Tooltip contentStyle={tooltipStyle} />
        <Bar dataKey={yKey} name={name} fill={color} radius={[3, 3, 0, 0]} />
      </BarChart>
    </ResponsiveContainer>
  );
}
