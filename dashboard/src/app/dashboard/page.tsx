"use client";

import { useEffect, useMemo, useState } from "react";
import ReactECharts from "echarts-for-react";
import type { EChartsOption } from "echarts";

// ─── Types ────────────────────────────────────────────────────────────────────

type RunInfo = { id: string; label: string };
type Scope = "univariate" | "multivariate";

type ExpScatterPoint = {
  time_ms: number;
  y_value: number | null;
  original_value: number | null;
  y_true: number;
  y_pred: number;
  shift_pct: number;
};

type WeatherPoint = {
  x: number;
  y: number;
  isAnomaly: boolean;
  anomalyHours: number | null;
};

type WeatherScatterResp = {
  rowCount: number;
  anomalyCount: number;
  anomalyRate: number;
  points: WeatherPoint[];
  error?: string;
};

type SummaryRow = {
  city: string;
  scope: string;
  model_name: string;
  target_column: string;
  precision: number;
  recall: number;
  f1: number;
  f2: number;
  n_rows: number;
  n_positive_true: number;
  n_positive_pred: number;
};

type CityTiming = { city: string; trainTimeSec: number; avgMsPerModel: number; n_rows: number; msPerKRows: number };

type FormulaParams = {
  alpha: number; beta: number; delta: number; gamma: number; epsilon: number;
  r2: number; beta_ci95: [number, number]; delta_ci95: [number, number]; gamma_ci95: [number, number];
  n_obs: number;
};

type DashStats = {
  latestRun: string;
  totalRuns: number;
  totalModels: number;
  avgF2: number;
  avgRecall: number;
  avgPrecision: number;
  modelDistribution: Record<string, number>;
  cityStats: { city: string; avgF2: number; avgRecall: number; n_rows: number; models: number }[];
  details: {
    city: string; scope: string; column: string; model: string;
    f2: number; precision: number; recall: number; n_rows: number;
    n_positive_true: number; n_positive_pred: number;
    trainTimeSec?: number | null;
  }[];
  overhead: {
    totalTrainingRows: number;
    totalTrialsActual: number;
    citiesCount: number;
    columnsCount: number;
    totalTrainTimeSec: number;
    avgTrainTimeMsPerModel: number;
    throughputRowsPerSec: number;
    msPerKRows: number;
    cityTiming: CityTiming[];
  };
  formulaParams?: FormulaParams | null;
  noRuns?: boolean;
  error?: string;
};

// ─── Helpers ──────────────────────────────────────────────────────────────────

const CITIES = ["amsterdam", "london", "new_york", "paris", "tokyo"];
const COLUMNS = [
  "temperature_2m", "apparent_temperature", "precipitation",
  "surface_pressure", "soil_temperature_7_to_28cm", "soil_moisture_7_to_28cm",
];
const MODEL_COLORS: Record<string, string> = {
  IForest: "#6366f1", LOF: "#f59e0b", HBOS: "#10b981", COPOD: "#3b82f6", ECOD: "#ec4899",
};

function fmtDate(v: number) {
  return new Date(v).toLocaleDateString(undefined, { month: "short", day: "numeric", year: "numeric" });
}
function fmtDateTime(v: number) {
  return new Date(v).toLocaleString(undefined, { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit" });
}
function avg(arr: number[]) { return arr.length ? arr.reduce((a, b) => a + b, 0) / arr.length : 0; }
function stddev(arr: number[]) {
  if (arr.length < 2) return 0;
  const m = avg(arr);
  return Math.sqrt(arr.reduce((s, v) => s + (v - m) ** 2, 0) / arr.length);
}

// ─── DAG Visualization ────────────────────────────────────────────────────────

function DagDiagram() {
  const NODE_W = 148;
  const NODE_H = 40;
  const TALL_H = 50;   // 2-line node
  const GAP_X = 44;
  const GAP_Y = 10;
  const CITIES_SHORT = ["amsterdam", "london", "new_york", "paris", "tokyo"];

  const nCities = CITIES_SHORT.length;
  const groupH = nCities * TALL_H + (nCities - 1) * GAP_Y;
  const SVG_H = groupH + 60;
  const cy = SVG_H / 2;  // vertical center

  // X positions for each stage
  const x: number[] = [];
  x[0] = 0;                                      // install
  x[1] = x[0] + NODE_W + GAP_X;                 // select
  x[2] = x[1] + NODE_W + GAP_X;                 // ingest×5
  x[3] = x[2] + NODE_W + GAP_X;                 // detect×5
  x[4] = x[3] + NODE_W + GAP_X;                 // trigger_dbt
  x[5] = x[4] + NODE_W + GAP_X;                 // log
  const SVG_W = x[5] + NODE_W;

  const groupTop = cy - groupH / 2;
  const cityY = (i: number) => groupTop + i * (TALL_H + GAP_Y);
  const cityCY = (i: number) => cityY(i) + TALL_H / 2;

  const singleY = cy - NODE_H / 2;
  const singleCY = cy;

  // Arrow head helper
  const arrowTip = (x2: number, y2: number, dir: "right" | "left" = "right") =>
    dir === "right"
      ? `M${x2 - 6},${y2 - 4} L${x2},${y2} L${x2 - 6},${y2 + 4}`
      : `M${x2 + 6},${y2 - 4} L${x2},${y2} L${x2 + 6},${y2 + 4}`;

  return (
    <div className="overflow-x-auto rounded-xl">
      <svg
        viewBox={`0 0 ${SVG_W} ${SVG_H}`}
        width={SVG_W}
        height={SVG_H}
        style={{ minWidth: SVG_W, fontFamily: "inherit" }}
      >
        {/* ── Arrows ─────────────────────────────────────── */}
        <g stroke="#94a3b8" strokeWidth="1.5" fill="#94a3b8">
          {/* install → select */}
          <line x1={x[0] + NODE_W} y1={singleCY} x2={x[1]} y2={singleCY} />
          <path d={arrowTip(x[1], singleCY)} fill="#94a3b8" stroke="none" />

          {/* select → each ingest (fan-out) */}
          {CITIES_SHORT.map((_, i) => {
            const ty = cityCY(i);
            const mx = (x[1] + NODE_W + x[2]) / 2;
            return (
              <g key={i}>
                <line x1={x[1] + NODE_W} y1={singleCY} x2={mx} y2={singleCY} />
                <line x1={mx} y1={singleCY} x2={mx} y2={ty} />
                <line x1={mx} y1={ty} x2={x[2]} y2={ty} />
                <path d={arrowTip(x[2], ty)} fill="#94a3b8" stroke="none" />
              </g>
            );
          })}

          {/* ingest → detect (1:1) */}
          {CITIES_SHORT.map((_, i) => {
            const ty = cityCY(i);
            return (
              <g key={i}>
                <line x1={x[2] + NODE_W} y1={ty} x2={x[3]} y2={ty} />
                <path d={arrowTip(x[3], ty)} fill="#94a3b8" stroke="none" />
              </g>
            );
          })}

          {/* detect → trigger_dbt (fan-in) */}
          {CITIES_SHORT.map((_, i) => {
            const ty = cityCY(i);
            const mx = (x[3] + NODE_W + x[4]) / 2;
            return (
              <g key={i}>
                <line x1={x[3] + NODE_W} y1={ty} x2={mx} y2={ty} />
                <line x1={mx} y1={ty} x2={mx} y2={singleCY} />
                <line x1={mx} y1={singleCY} x2={x[4]} y2={singleCY} />
              </g>
            );
          })}
          <path d={arrowTip(x[4], singleCY)} fill="#94a3b8" stroke="none" />

          {/* trigger_dbt → log */}
          <line x1={x[4] + NODE_W} y1={singleCY} x2={x[5]} y2={singleCY} />
          <path d={arrowTip(x[5], singleCY)} fill="#94a3b8" stroke="none" />
        </g>

        {/* ── Nodes ──────────────────────────────────────── */}

        {/* install_deps */}
        <DagNode x={x[0]} y={singleY} w={NODE_W} h={NODE_H} color="#0ea5e9" bg="#e0f2fe"
          label="install_deps" sub="BashOperator" />

        {/* select_cities */}
        <DagNode x={x[1]} y={singleY} w={NODE_W} h={NODE_H} color="#8b5cf6" bg="#ede9fe"
          label="select_cities" sub="PythonOperator" />

        {/* ingest × 5 */}
        {CITIES_SHORT.map((city, i) => (
          <DagNode key={city} x={x[2]} y={cityY(i)} w={NODE_W} h={TALL_H}
            color="#0ea5e9" bg="#e0f2fe"
            label={`ingest_${city}`} sub="BashOperator" />
        ))}

        {/* detect × 5 — the new AutoML tasks */}
        {CITIES_SHORT.map((city, i) => (
          <DagNode key={city} x={x[3]} y={cityY(i)} w={NODE_W} h={TALL_H}
            color="#10b981" bg="#d1fae5"
            label={`detect_${city}`} sub="AutoML · new" highlight />
        ))}

        {/* trigger_dbt */}
        <DagNode x={x[4]} y={singleY} w={NODE_W} h={NODE_H} color="#8b5cf6" bg="#ede9fe"
          label="trigger_dbt" sub="PythonOperator" />

        {/* log */}
        <DagNode x={x[5]} y={singleY} w={NODE_W} h={NODE_H} color="#6b7280" bg="#f3f4f6"
          label="log_done" sub="BashOperator" />
      </svg>
    </div>
  );
}

function DagNode({
  x, y, w, h, color, bg, label, sub, highlight = false,
}: {
  x: number; y: number; w: number; h: number;
  color: string; bg: string; label: string; sub: string; highlight?: boolean;
}) {
  const cy = y + h / 2;
  return (
    <g>
      <rect x={x} y={y} width={w} height={h} rx={7} ry={7}
        fill={bg} stroke={highlight ? color : "#cbd5e1"}
        strokeWidth={highlight ? 2 : 1} />
      {highlight && (
        <rect x={x} y={y} width={4} height={h} rx={2} ry={2} fill={color} />
      )}
      {/* success dot */}
      <circle cx={x + w - 10} cy={y + 10} r={4} fill="#22c55e" />
      <text x={x + (highlight ? 10 : 8)} y={cy - (h > 42 ? 7 : 0)}
        fontSize={9} fontWeight={600} fill={color} fontFamily="ui-monospace, monospace">
        {label.length > 18 ? label.slice(0, 17) + "…" : label}
      </text>
      {h > 42 && (
        <text x={x + (highlight ? 10 : 8)} y={cy + 9}
          fontSize={8} fill="#64748b" fontFamily="inherit">
          {sub}
        </text>
      )}
      {h <= 42 && (
        <text x={x + (highlight ? 10 : 8)} y={cy + 10}
          fontSize={7.5} fill="#64748b" fontFamily="inherit">
          {sub}
        </text>
      )}
    </g>
  );
}

function TrainingDag() {
  const W = 148, H = 40, GAP = 44;
  const stages = [
    { label: "install_deps", sub: "BashOperator", color: "#0ea5e9", bg: "#e0f2fe" },
    { label: "train_all_models", sub: "BashOperator · AutoML", color: "#10b981", bg: "#d1fae5", highlight: true },
    { label: "log_complete", sub: "BashOperator", color: "#6b7280", bg: "#f3f4f6" },
  ];
  const SVG_W = stages.length * W + (stages.length - 1) * GAP;
  const SVG_H = H + 20;

  return (
    <div className="overflow-x-auto">
      <svg viewBox={`0 0 ${SVG_W} ${SVG_H}`} width={SVG_W} height={SVG_H} style={{ minWidth: SVG_W }}>
        <g stroke="#94a3b8" strokeWidth="1.5" fill="#94a3b8">
          {stages.slice(0, -1).map((_, i) => {
            const x1 = i * (W + GAP) + W, x2 = (i + 1) * (W + GAP);
            return (
              <g key={i}>
                <line x1={x1} y1={SVG_H / 2} x2={x2} y2={SVG_H / 2} />
                <path d={`M${x2 - 6},${SVG_H / 2 - 4} L${x2},${SVG_H / 2} L${x2 - 6},${SVG_H / 2 + 4}`} stroke="none" />
              </g>
            );
          })}
        </g>
        {stages.map((s, i) => (
          <DagNode key={i} x={i * (W + GAP)} y={10} w={W} h={H}
            color={s.color} bg={s.bg} label={s.label} sub={s.sub}
            highlight={s.highlight ?? false} />
        ))}
      </svg>
    </div>
  );
}

// ─── Page ─────────────────────────────────────────────────────────────────────

export default function DashboardPage() {
  // ── Section 1: Live Detection ──────────────────────────────────────────
  const [runs, setRuns] = useState<RunInfo[]>([]);
  const [selectedRun, setSelectedRun] = useState("");
  const [s1City, setS1City] = useState("amsterdam");
  const [s1Col, setS1Col] = useState("temperature_2m");
  const [scope, setScope] = useState<Scope>("univariate");
  const [expPts, setExpPts] = useState<ExpScatterPoint[]>([]);
  const [summaryRows, setSummaryRows] = useState<SummaryRow[]>([]);
  const [s1Range, setS1Range] = useState<{ from: number; to: number } | null>(null);
  const [loadingS1, setLoadingS1] = useState(false);

  // ── Section 2: Architecture ────────────────────────────────────────────
  const [dashStats, setDashStats] = useState<DashStats | null>(null);
  const [s2City, setS2City] = useState("all");

  // ── Section 3: Comparison ─────────────────────────────────────────────
  const [s3City, setS3City] = useState("amsterdam");
  const [s3Col, setS3Col] = useState("temperature_2m");
  const [withData, setWithData] = useState<WeatherPoint[]>([]);
  const [withoutData, setWithoutData] = useState<WeatherPoint[]>([]);
  const [loadingS3, setLoadingS3] = useState(false);

  // ── Load runs ──────────────────────────────────────────────────────────
  useEffect(() => {
    fetch("/api/experiments/runs")
      .then((r) => r.json())
      .then((d: { runs?: RunInfo[] }) => {
        const list = d.runs ?? [];
        setRuns(list);
        if (list.length) setSelectedRun(list[0].id);
      })
      .catch(() => {});
  }, []);

  // ── Load experiment scatter (Section 1) ───────────────────────────────
  useEffect(() => {
    if (!selectedRun || !s1City || !s1Col) return;
    let cancelled = false;
    setLoadingS1(true);
    const col = scope === "multivariate" ? "ALL_FEATURES" : s1Col;
    fetch(`/api/experiments/scatter?run=${selectedRun}&city=${s1City}&scope=${scope}&column=${col}`)
      .then((r) => r.json())
      .then((d: { points?: ExpScatterPoint[] }) => {
        if (cancelled) return;
        const pts = d.points ?? [];
        setExpPts(pts);
        if (pts.length) setS1Range({ from: pts[0].time_ms, to: pts[pts.length - 1].time_ms });
      })
      .catch(() => { if (!cancelled) setExpPts([]); })
      .finally(() => { if (!cancelled) setLoadingS1(false); });
    return () => { cancelled = true; };
  }, [selectedRun, s1City, s1Col, scope]);

  // ── Load summary (Section 1 stats) ─────────────────────────────────────
  useEffect(() => {
    if (!selectedRun) return;
    fetch(`/api/experiments/summary?run=${selectedRun}`)
      .then((r) => r.json())
      .then((d: { rows?: SummaryRow[] }) => setSummaryRows(d.rows ?? []))
      .catch(() => {});
  }, [selectedRun]);

  // ── Load dashboard stats (Section 2) ───────────────────────────────────
  useEffect(() => {
    fetch("/api/dashboard/stats")
      .then((r) => r.json())
      .then((d: DashStats) => setDashStats(d))
      .catch(() => {});
  }, []);

  // ── Load comparison data (Section 3) ───────────────────────────────────
  useEffect(() => {
    if (!s3City || !s3Col) return;
    let cancelled = false;
    setLoadingS3(true);
    const q = (dataset: string) =>
      fetch(`/api/weather/scatter?dataset=${dataset}&city=${s3City}&yColumn=${s3Col}`)
        .then((r) => r.json()) as Promise<WeatherScatterResp>;

    Promise.all([q("daily_with_anomalies"), q("daily_without_anomalies")])
      .then(([w, wo]) => {
        if (cancelled) return;
        setWithData(w.points ?? []);
        setWithoutData(wo.points ?? []);
      })
      .catch(() => {})
      .finally(() => { if (!cancelled) setLoadingS3(false); });
    return () => { cancelled = true; };
  }, [s3City, s3Col]);

  // ── Derived: Section 1 scatter categories ─────────────────────────────
  const { normalPts, detectedPts, missedPts } = useMemo(() => {
    const normal: [number, number][] = [];
    const detected: [number, number][] = [];
    const missed: [number, number][] = [];
    expPts.forEach((p) => {
      const pt: [number, number] = [p.time_ms, p.y_value ?? 0];
      if (p.y_true === 0) normal.push(pt);
      else if (p.y_pred === 1) detected.push(pt);
      else missed.push(pt);
    });
    return { normalPts: normal, detectedPts: detected, missedPts: missed };
  }, [expPts]);

  const detStats = useMemo(() => {
    if (!expPts.length) return null;
    const injected = expPts.filter((p) => p.y_true === 1).length;
    const detected = expPts.filter((p) => p.y_true === 1 && p.y_pred === 1).length;
    return { injected, detected, rate: injected > 0 ? detected / injected : 0 };
  }, [expPts]);

  const citySummary = useMemo(
    () => summaryRows.filter((r) => r.city === s1City && r.scope === scope),
    [summaryRows, s1City, scope],
  );

  // ── Scatter chart option (Section 1) ──────────────────────────────────
  const s1ChartOption = useMemo((): EChartsOption => ({
    animation: false,
    grid: { left: 58, right: 24, top: 40, bottom: 70 },
    legend: { top: 4, textStyle: { color: "#34556a" } },
    tooltip: {
      trigger: "item",
      borderColor: "#a5b4c7",
      backgroundColor: "#fff",
      textStyle: { color: "#20394a" },
      formatter: (params: unknown) => {
        const p = params as { data?: { value: [number, number]; _tip: string } };
        if (!p.data) return "";
        const [t, v] = p.data.value;
        return [`<b>${p.data._tip}</b>`, fmtDateTime(t), `${s1Col}: ${Number(v).toFixed(3)}`].join("<br/>");
      },
    },
    xAxis: {
      type: "time", name: "time", nameLocation: "middle", nameGap: 34,
      axisLabel: { color: "#325163", formatter: (v: number) => fmtDate(v) },
      min: s1Range?.from ?? undefined, max: s1Range?.to ?? undefined,
    },
    yAxis: { type: "value", name: s1Col, nameLocation: "middle", nameGap: 50, axisLabel: { color: "#325163" } },
    dataZoom: [
      { type: "inside", xAxisIndex: 0, filterMode: "none", zoomOnMouseWheel: true, moveOnMouseMove: true },
      { type: "slider", xAxisIndex: 0, filterMode: "none", height: 28, bottom: 14, borderColor: "#c9d7e4", handleSize: "95%", moveHandleSize: 4 },
    ],
    series: [
      {
        name: "Normal", type: "scatter", symbolSize: 6, large: true,
        itemStyle: { color: "#1f8eb5" },
        data: normalPts.map((v) => ({ value: v, _tip: "Normal" })) as any[], // eslint-disable-line
      },
      {
        name: "Detected by AutoML", type: "scatter", symbolSize: 12,
        itemStyle: { color: "#d7265a", borderColor: "#22c55e", borderWidth: 2.5 },
        data: detectedPts.map((v) => ({ value: v, _tip: "Detected (TP)" })) as any[], // eslint-disable-line
      },
      {
        name: "Missed by AutoML", type: "scatter", symbolSize: 12,
        itemStyle: { color: "#d7265a", borderColor: "#1e3a8a", borderWidth: 2.5 },
        data: missedPts.map((v) => ({ value: v, _tip: "Missed (FN)" })) as any[], // eslint-disable-line
      },
    ],
  }), [normalPts, detectedPts, missedPts, s1Col, s1Range]);

  // ── Model distribution chart (Section 2) ──────────────────────────────
  const modelDistChart = useMemo((): EChartsOption => {
    if (!dashStats?.modelDistribution) return {};
    const entries = Object.entries(dashStats.modelDistribution);
    return {
      animation: false,
      tooltip: { trigger: "item", formatter: "{b}: {c} models ({d}%)" },
      legend: { orient: "vertical", right: 0, top: "center", textStyle: { color: "#334155", fontSize: 11 } },
      series: [{
        type: "pie",
        radius: ["45%", "72%"],
        center: ["40%", "50%"],
        data: entries.map(([name, value]) => ({
          name, value, itemStyle: { color: MODEL_COLORS[name] ?? "#6b7280" },
        })),
        label: { show: false },
        emphasis: { itemStyle: { shadowBlur: 10, shadowColor: "rgba(0,0,0,0.2)" } },
      }],
    };
  }, [dashStats]);

  // ── F2 by city chart (Section 2) ──────────────────────────────────────
  const cityF2Chart = useMemo((): EChartsOption => {
    if (!dashStats?.cityStats?.length) return {};
    const data = [...dashStats.cityStats].sort((a, b) => b.avgF2 - a.avgF2);
    return {
      animation: false,
      grid: { left: 80, right: 20, top: 24, bottom: 24 },
      tooltip: { trigger: "axis", formatter: (p: unknown) => {
        const items = p as { name: string; value: number }[];
        return items.map((i) => `${i.name}: F2 ${i.value.toFixed(3)}`).join("<br/>");
      }},
      xAxis: { type: "value", min: 0, max: 1, axisLabel: { color: "#64748b", formatter: (v: number) => v.toFixed(1) } },
      yAxis: { type: "category", data: data.map((d) => d.city), axisLabel: { color: "#334155", fontSize: 11 } },
      series: [{
        type: "bar", barMaxWidth: 28,
        data: data.map((d) => ({
          value: parseFloat(d.avgF2.toFixed(3)),
          itemStyle: { color: d.avgF2 >= 0.5 ? "#10b981" : d.avgF2 >= 0.25 ? "#f59e0b" : "#6366f1", borderRadius: [0, 4, 4, 0] },
        })),
        label: { show: true, position: "right", color: "#334155", formatter: (p: unknown) => (p as { value: number }).value.toFixed(2) },
      }],
    };
  }, [dashStats]);

  // ── Comparison chart (Section 3) ──────────────────────────────────────
  const cmpChartOption = useMemo((): EChartsOption => {
    if (!withData.length && !withoutData.length) return {};

    // Align by time → residual (with − without)
    const withMap = new Map(withData.map((p) => [p.x, p.y]));
    const withoutMap = new Map(withoutData.map((p) => [p.x, p.y]));
    const diffPts: [number, number][] = [];
    withMap.forEach((wy, t) => {
      const woy = withoutMap.get(t);
      if (woy !== undefined) diffPts.push([t, parseFloat((wy - woy).toFixed(4))]);
    });
    diffPts.sort((a, b) => a[0] - b[0]);

    // 14-day rolling mean of |residual| — highlights clustered anomaly periods
    const rollingMean: [number, number][] = diffPts.map((_, i) => {
      const window = diffPts.slice(Math.max(0, i - 6), i + 7);
      const avg = window.reduce((s, p) => s + Math.abs(p[1]), 0) / window.length;
      return [diffPts[i][0], parseFloat(avg.toFixed(4))];
    });

    const maxAbs = Math.max(...diffPts.map((p) => Math.abs(p[1])), 0.001);

    return {
      animation: false,
      grid: { left: 68, right: 24, top: 48, bottom: 72 },
      legend: {
        top: 8,
        data: ["Anomaly Impact (Δ)", "14-day Avg Impact"],
        textStyle: { color: "#475569", fontSize: 12 },
      },
      tooltip: {
        trigger: "axis",
        axisPointer: { type: "line", lineStyle: { color: "#cbd5e1" } },
        formatter: (params: unknown) => {
          const items = params as { seriesName: string; value: [number, number]; color: string }[];
          if (!items.length) return "";
          const date = `<b>${fmtDate(items[0].value[0])}</b>`;
          const lines = items
            .filter((i) => i.seriesName !== "14-day Avg Impact" || Math.abs(i.value[1]) > 0)
            .map((i) => {
              const v = Number(i.value[1]);
              const sign = v >= 0 ? "+" : "";
              return `<span style="color:${i.color}">●</span> ${i.seriesName}: <b>${sign}${v.toFixed(4)}</b>`;
            });
          return [date, ...lines].join("<br/>");
        },
      },
      xAxis: {
        type: "time",
        axisLabel: { color: "#64748b", formatter: (v: number) => fmtDate(v) },
        axisLine: { lineStyle: { color: "#e2e8f0" } },
        splitLine: { show: false },
      },
      yAxis: {
        type: "value",
        name: `Δ ${s3Col}`,
        nameLocation: "middle",
        nameGap: 58,
        nameTextStyle: { color: "#64748b", fontSize: 11 },
        axisLabel: { color: "#64748b", formatter: (v: number) => v.toFixed(2) },
        splitLine: { lineStyle: { color: "#f1f5f9", type: "dashed" } },
        max: maxAbs * 1.2,
        min: -maxAbs * 1.2,
      },
      dataZoom: [
        { type: "inside", filterMode: "none" },
        { type: "slider", filterMode: "none", height: 22, bottom: 8, borderColor: "#c9d7e4", fillerColor: "rgba(16,185,129,0.08)" },
      ],
      series: [
        {
          name: "Anomaly Impact (Δ)",
          type: "bar",
          data: diffPts,
          barMaxWidth: 5,
          itemStyle: {
            color: (p: unknown) => {
              const v = (p as { value: [number, number] }).value[1];
              return v >= 0 ? "rgba(239,68,68,0.75)" : "rgba(59,130,246,0.75)";
            },
            borderRadius: 2,
          },
          emphasis: { itemStyle: { opacity: 1 } },
          markLine: {
            silent: true,
            symbol: "none",
            data: [{ yAxis: 0, lineStyle: { color: "#94a3b8", width: 1 } }],
          },
        },
        {
          name: "14-day Avg Impact",
          type: "line",
          data: rollingMean,
          symbol: "none",
          smooth: true,
          lineStyle: { color: "#f59e0b", width: 2 },
          itemStyle: { color: "#f59e0b" },
          areaStyle: { color: "rgba(245,158,11,0.08)" },
          z: 10,
        },
      ],
    };
  }, [withData, withoutData, s3Col]);

  // ── Smoothness stats (Section 3) ──────────────────────────────────────
  const cmpStats = useMemo(() => {
    if (!withData.length || !withoutData.length) return null;
    const withMap = new Map(withData.map((p) => [p.x, p.y]));
    const withoutMap = new Map(withoutData.map((p) => [p.x, p.y]));
    const residuals: number[] = [];
    withMap.forEach((wy, t) => {
      const woy = withoutMap.get(t);
      if (woy !== undefined) residuals.push(wy - woy);
    });
    const absResiduals = residuals.map(Math.abs);
    const maxImpact = Math.max(...absResiduals, 0);
    const meanImpact = absResiduals.reduce((a, b) => a + b, 0) / (absResiduals.length || 1);
    const anomalousDays = withData.filter((p) => p.isAnomaly).length;
    const affectedDays = residuals.filter((r) => Math.abs(r) > 0.001).length;
    return { maxImpact, meanImpact, anomalousDays, affectedDays };
  }, [withData, withoutData]);

  // ── Time range buttons for scatter ────────────────────────────────────
  const s1TimeButtons = useMemo(() => {
    if (!expPts.length) return [];
    const ts = expPts.map((p) => p.time_ms);
    const min = Math.min(...ts), max = Math.max(...ts), D = 86400000;
    return [
      { label: "7D", from: Math.max(min, max - 7 * D), to: max },
      { label: "30D", from: Math.max(min, max - 30 * D), to: max },
      { label: "ALL", from: min, to: max },
    ];
  }, [expPts]);

  // ─────────────────────────────────────────────────────────────────────
  return (
    <div className="min-h-screen bg-grid">
      <main className="mx-auto flex w-full max-w-7xl flex-col gap-6 px-5 py-8 md:px-8 lg:px-10">

        {/* ── Header ──────────────────────────────────────────────────── */}
        <header className="rounded-2xl border border-white/25 bg-[linear-gradient(125deg,#064e3b_0%,#065f46_45%,#0d9488_100%)] p-6 text-white shadow-[0_18px_40px_rgba(0,0,0,0.25)] md:p-8">
          <div className="flex items-start justify-between gap-4">
            <div>
              <p className="text-xs uppercase tracking-[0.24em] text-emerald-200/90">MSc Thesis · Adaptive Data Profiling</p>
              <h1 className="mt-2 text-2xl font-semibold tracking-tight md:text-4xl">AutoML ETL Pipeline</h1>
              <p className="mt-3 max-w-3xl text-sm text-emerald-50/90 md:text-base">
                End-to-end adaptive data profiling — synthetic anomaly injection, AutoML detection via PyOD + Optuna,
                and measured quality impact on daily weather aggregates across 5 cities.
              </p>
            </div>
            <div className="flex shrink-0 flex-col gap-2">
              <a href="/" className="rounded-xl border border-white/30 bg-white/10 px-4 py-2 text-center text-sm font-medium text-white hover:bg-white/20">
                ← Observatory
              </a>
              <a href="/experiments" className="rounded-xl border border-white/30 bg-white/10 px-4 py-2 text-center text-sm font-medium text-white hover:bg-white/20">
                Experiments →
              </a>
            </div>
          </div>
        </header>

        {/* ══ SECTION 1: Live Detection ════════════════════════════════ */}
        <SectionLabel number="01" title="Live Pipeline Detection" />

        {/* Controls */}
        <section className="rounded-2xl border border-slate-200/80 bg-white/90 p-4 shadow-sm backdrop-blur md:p-5">
          <div className="grid grid-cols-2 gap-3 sm:grid-cols-3 xl:grid-cols-5">
            <label className="control sm:col-span-2">
              <span>Experiment Run</span>
              <select value={selectedRun} onChange={(e) => setSelectedRun(e.target.value)} disabled={!runs.length} suppressHydrationWarning>
                {runs.map((r) => <option key={r.id} value={r.id}>{r.label}</option>)}
                {!runs.length && <option>No runs found</option>}
              </select>
            </label>
            <label className="control">
              <span>City</span>
              <select value={s1City} onChange={(e) => setS1City(e.target.value)} suppressHydrationWarning>
                {CITIES.map((c) => <option key={c}>{c}</option>)}
              </select>
            </label>
            <label className="control">
              <span>Column</span>
              <select value={s1Col} onChange={(e) => setS1Col(e.target.value)} suppressHydrationWarning>
                {COLUMNS.map((c) => <option key={c}>{c}</option>)}
              </select>
            </label>
            <label className="control">
              <span>Scope</span>
              <select value={scope} onChange={(e) => setScope(e.target.value as Scope)}>
                <option value="univariate">Univariate</option>
                <option value="multivariate">Multivariate</option>
              </select>
            </label>
          </div>
        </section>

        {/* Stat cards */}
        <section className="grid grid-cols-2 gap-4 md:grid-cols-4">
          <MiniStat label="Data Points" value={expPts.length > 0 ? expPts.length.toLocaleString() : "-"} tone="ocean" />
          <MiniStat label="Injected Anomalies" value={detStats ? detStats.injected.toLocaleString() : "-"} tone="rose" />
          <MiniStat label="AutoML Detected" value={detStats ? detStats.detected.toLocaleString() : "-"} tone="green" />
          <MiniStat
            label="Detection Rate"
            value={detStats ? `${(detStats.rate * 100).toFixed(1)}%` : "-"}
            tone={detStats && detStats.rate >= 0.5 ? "green" : "amber"}
          />
        </section>

        {/* Scatter plot */}
        <section className="rounded-2xl border border-slate-200/80 bg-white/95 p-4 shadow-sm md:p-6">
          <div className="mb-3 flex flex-wrap items-center justify-between gap-2">
            <h2 className="text-lg font-semibold text-slate-800">Anomaly Detection Overlay</h2>
            <div className="flex items-center gap-3 text-xs text-slate-500">
              <span className="flex items-center gap-1">
                <span className="inline-block h-3 w-3 rounded-full border-2 border-green-400 bg-[#d7265a]" /> Detected (TP)
              </span>
              <span className="flex items-center gap-1">
                <span className="inline-block h-3 w-3 rounded-full border-2 border-blue-800 bg-[#d7265a]" /> Missed (FN)
              </span>
            </div>
          </div>
          {s1TimeButtons.length > 0 && (
            <div className="mb-3 flex gap-2">
              {s1TimeButtons.map((b) => (
                <button key={b.label} type="button"
                  className="rounded-md border border-slate-300 bg-slate-50 px-3 py-1 text-xs font-medium text-slate-700 hover:bg-slate-100"
                  onClick={() => setS1Range({ from: b.from, to: b.to })}>
                  {b.label}
                </button>
              ))}
            </div>
          )}
          <div className="h-[420px] w-full">
            {loadingS1
              ? <div className="chart-loading">Loading detection data…</div>
              : expPts.length
                ? <ReactECharts option={s1ChartOption} style={{ height: "100%", width: "100%" }} notMerge opts={{ renderer: "svg" }} />
                : <div className="chart-loading">Select an experiment run to show detection overlay.</div>
            }
          </div>
          {/* Inline model perf for selected city */}
          {citySummary.length > 0 && (
            <div className="mt-4 flex flex-wrap gap-3">
              {citySummary.slice(0, 6).map((r) => (
                <div key={r.target_column}
                  className="flex items-center gap-2 rounded-lg border border-slate-200 bg-slate-50 px-3 py-2">
                  <span className="font-mono text-xs text-slate-500">{r.target_column.replace(/_/g, "·")}</span>
                  <span className="rounded-md bg-violet-100 px-1.5 py-0.5 text-xs font-medium text-violet-800">{r.model_name}</span>
                  <span className={`text-xs font-semibold ${r.f2 >= 0.5 ? "text-green-700" : r.f2 > 0 ? "text-amber-700" : "text-slate-400"}`}>
                    F2 {r.f2.toFixed(2)}
                  </span>
                </div>
              ))}
            </div>
          )}
        </section>

        {/* ══ SECTION 2: Pipeline Architecture ════════════════════════ */}
        <SectionLabel number="02" title="Pipeline Architecture & Training" />

        {/* DAG diagrams */}
        <section className="rounded-2xl border border-slate-200/80 bg-white/95 p-4 shadow-sm md:p-6">
          <div className="mb-5">
            <h2 className="text-lg font-semibold text-slate-800">Daily Ingestion DAG</h2>
            <p className="mt-0.5 text-xs text-slate-500">
              weather_ingestion · Schedule: 0 2 * * * (UTC) · 5 cities · 2 new AutoML tasks highlighted in green
            </p>
          </div>
          <div className="overflow-x-auto">
            <DagDiagram />
          </div>

          {/* Legend */}
          <div className="mt-4 flex flex-wrap gap-4 text-xs text-slate-600">
            <span className="flex items-center gap-1.5">
              <span className="h-3 w-3 rounded-sm border border-sky-300 bg-sky-100" /> BashOperator
            </span>
            <span className="flex items-center gap-1.5">
              <span className="h-3 w-3 rounded-sm border border-violet-300 bg-violet-100" /> PythonOperator
            </span>
            <span className="flex items-center gap-1.5">
              <span className="h-3 w-3 rounded-sm border border-emerald-400 bg-emerald-100" /> AutoML Detection (new)
            </span>
            <span className="flex items-center gap-1.5">
              <span className="h-2 w-2 rounded-full bg-green-500" /> Success
            </span>
          </div>

          {/* Training DAG */}
          <div className="mt-6 border-t border-slate-100 pt-5">
            <h3 className="mb-1 text-sm font-semibold text-slate-700">AutoML Training DAG</h3>
            <p className="mb-3 text-xs text-slate-400">weather_automl_train · Manual trigger · Trains all city × column models, saves to S3</p>
            <TrainingDag />
          </div>
        </section>

        {/* Training results */}
        {dashStats && !dashStats.noRuns ? (
          <>
            {/* Row 1: charts side-by-side */}
            <section className="grid grid-cols-1 gap-4 md:grid-cols-2">
              <div className="rounded-2xl border border-slate-200/80 bg-white/95 p-4 shadow-sm md:p-5">
                <h3 className="mb-3 text-sm font-semibold text-slate-700">Best Model Distribution</h3>
                <div className="h-[200px]">
                  <ReactECharts option={modelDistChart} style={{ height: "100%", width: "100%" }} notMerge opts={{ renderer: "svg" }} />
                </div>
                <div className="mt-2 text-center text-xs text-slate-400">
                  {dashStats.totalModels} models trained · {dashStats.overhead.citiesCount} cities · {dashStats.overhead.columnsCount} columns
                </div>
              </div>
              <div className="rounded-2xl border border-slate-200/80 bg-white/95 p-4 shadow-sm md:p-5">
                <h3 className="mb-3 text-sm font-semibold text-slate-700">Avg F2 Score by City</h3>
                <div className="h-[200px]">
                  <ReactECharts option={cityF2Chart} style={{ height: "100%", width: "100%" }} notMerge opts={{ renderer: "svg" }} />
                </div>
                <div className="mt-2 text-center text-xs text-slate-400">
                  Avg F2 {dashStats.avgF2.toFixed(3)} · Recall {dashStats.avgRecall.toFixed(3)}
                </div>
              </div>
            </section>

            {/* Row 2: full-width overhead */}
            <section className="rounded-2xl border border-slate-200/80 bg-white/95 p-4 shadow-sm md:p-5">
              <h3 className="mb-4 text-sm font-semibold text-slate-700">AutoML Integration Overhead</h3>
              {/* Horizontal KPI strip */}
              <div className="grid grid-cols-2 gap-px overflow-hidden rounded-xl border border-slate-200 bg-slate-200 sm:grid-cols-4">
                {[
                  { label: "Total Train Time", value: `${dashStats.overhead.totalTrainTimeSec.toFixed(1)}s`, sub: `${dashStats.totalModels} models · ${dashStats.overhead.totalTrialsActual} trials` },
                  { label: "Avg per Model", value: `${dashStats.overhead.avgTrainTimeMsPerModel.toFixed(0)} ms`, sub: "Optuna HPO + fit" },
                  { label: "Throughput", value: `${dashStats.overhead.throughputRowsPerSec.toFixed(0)} rows/s`, sub: "rows processed per second" },
                  { label: "Cost per 1k Rows", value: `${dashStats.overhead.msPerKRows.toFixed(0)} ms`, sub: "scale unit — linear growth" },
                ].map(({ label, value, sub }) => (
                  <div key={label} className="bg-white px-5 py-4">
                    <p className="text-[10px] font-semibold uppercase tracking-widest text-slate-400">{label}</p>
                    <p className="mt-1 text-2xl font-bold tabular-nums text-slate-800">{value}</p>
                    <p className="mt-0.5 text-xs text-slate-400">{sub}</p>
                  </div>
                ))}
              </div>
              {/* Scalability projection */}
              <div className="mt-5">
                <p className="mb-2 text-xs font-semibold text-slate-500">Training Time Projection</p>
                <ScalabilityChart
                  formula={dashStats.formulaParams ?? null}
                  currentRows={dashStats.overhead.totalTrainingRows}
                  currentP={728}
                />
              </div>
            </section>
          </>
        ) : (
          <div className="rounded-2xl border border-dashed border-slate-300 bg-white/70 p-6 text-center text-sm text-slate-400">
            No experiment runs found. Run <code className="rounded bg-slate-100 px-1 py-0.5">./run_experiment.sh</code> to generate training data.
          </div>
        )}

        {/* Training details table */}
        {dashStats && !dashStats.noRuns && dashStats.details.length > 0 && (
          <section className="rounded-2xl border border-slate-200/80 bg-white/95 p-4 shadow-sm md:p-6">
            <div className="mb-4 flex flex-wrap items-center justify-between gap-3">
              <h2 className="text-lg font-semibold text-slate-800">Model Training Results</h2>
              <span className="text-xs text-slate-400">Run: {dashStats.latestRun} · amsterdam</span>
            </div>
            <div className="overflow-auto">
              <table className="w-full min-w-[700px] border-collapse text-sm">
                <thead>
                  <tr className="border-b border-slate-200 text-left text-xs uppercase tracking-wide text-slate-500">
                    <th className="py-2 pr-4 font-medium">Column</th>
                    <th className="py-2 pr-4 font-medium">Best Model</th>
                    <th className="py-2 pr-4 text-right font-medium">Precision</th>
                    <th className="py-2 pr-4 text-right font-medium">Recall</th>
                    <th className="py-2 pr-4 text-right font-medium">F2</th>
                    <th className="py-2 pr-4 text-right font-medium">Rows</th>
                    <th className="py-2 text-right font-medium">Train Time</th>
                  </tr>
                </thead>
                <tbody>
                  {dashStats.details
                    .filter((d) => d.scope === "univariate" && d.city === "amsterdam")
                    .map((row, i) => (
                      <tr key={i} className="border-b border-slate-100 hover:bg-slate-50/60">
                        <td className="py-1.5 pr-4 font-mono text-xs text-slate-600">{row.column}</td>
                        <td className="py-1.5 pr-4">
                          <span className="rounded-md px-2 py-0.5 text-xs font-medium text-white"
                            style={{ backgroundColor: MODEL_COLORS[row.model] ?? "#6b7280" }}>
                            {row.model}
                          </span>
                        </td>
                        <td className="py-1.5 pr-4 text-right tabular-nums text-slate-600">{(row.precision * 100).toFixed(1)}%</td>
                        <td className="py-1.5 pr-4 text-right tabular-nums text-slate-600">{(row.recall * 100).toFixed(1)}%</td>
                        <td className={`py-1.5 pr-4 text-right tabular-nums font-semibold ${row.f2 >= 0.5 ? "text-green-700" : row.f2 > 0 ? "text-amber-700" : "text-slate-400"}`}>
                          {row.f2.toFixed(3)}
                        </td>
                        <td className="py-1.5 pr-4 text-right tabular-nums text-xs text-slate-400">{row.n_rows.toLocaleString()}</td>
                        <td className="py-1.5 text-right tabular-nums text-xs text-slate-400">
                          {row.trainTimeSec != null ? `${row.trainTimeSec.toFixed(1)}s` : "—"}
                        </td>
                      </tr>
                    ))}
                </tbody>
              </table>
            </div>
          </section>
        )}

        {/* ══ SECTION 3: Data Quality Impact ══════════════════════════ */}
        <SectionLabel number="03" title="Data Quality Impact" />

        <section className="rounded-2xl border border-slate-200/80 bg-white/90 p-4 shadow-sm backdrop-blur md:p-5">
          <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
            <label className="control">
              <span>City</span>
              <select value={s3City} onChange={(e) => setS3City(e.target.value)} suppressHydrationWarning>
                {CITIES.map((c) => <option key={c}>{c}</option>)}
              </select>
            </label>
            <label className="control">
              <span>Column</span>
              <select value={s3Col} onChange={(e) => setS3Col(e.target.value)} suppressHydrationWarning>
                {COLUMNS.map((c) => <option key={c}>{c}</option>)}
              </select>
            </label>
          </div>
        </section>

        {cmpStats && (
          <section className="grid grid-cols-2 gap-4 md:grid-cols-4">
            <MiniStat label="Max Single-Day Impact" value={cmpStats.maxImpact.toFixed(3)} tone="rose" sub={s3Col} />
            <MiniStat label="Mean Daily Shift" value={cmpStats.meanImpact.toFixed(4)} tone="ocean" sub="avg |Δ| across all days" />
            <MiniStat label="Days Affected" value={cmpStats.affectedDays.toLocaleString()} tone="amber" sub="daily mean changed" />
            <MiniStat label="Injected Anomaly Days" value={cmpStats.anomalousDays.toLocaleString()} tone="indigo" sub="synthetic anomaly flag" />
          </section>
        )}

        <section className="rounded-2xl border border-slate-200/80 bg-white/95 p-4 shadow-sm md:p-6">
          <div className="mb-3 flex flex-wrap items-center justify-between gap-2">
            <h2 className="text-lg font-semibold text-slate-800">Anomaly Impact on Daily Aggregates</h2>
            <p className="text-xs text-slate-400">
              Each bar = shift in daily mean of <b>{s3Col}</b> for <b>{s3City}</b> caused by injected anomalies ·{" "}
              <span className="text-red-400">■ inflated</span> / <span className="text-blue-400">■ deflated</span> ·{" "}
              <span className="text-amber-400">— 14-day rolling avg</span>
            </p>
          </div>
          <div className="h-[360px] w-full">
            {loadingS3
              ? <div className="chart-loading">Loading comparison data…</div>
              : withData.length
                ? <ReactECharts option={cmpChartOption} style={{ height: "100%", width: "100%" }} notMerge opts={{ renderer: "svg" }} />
                : <div className="chart-loading">No daily aggregate data available yet.</div>
            }
          </div>
          <p className="mt-3 text-xs text-slate-400">
            Bars show Δ = (daily mean with anomaly) − (daily mean without). Red = anomaly inflated the aggregate, blue = deflated.
            The amber trend line is a 14-day rolling average of absolute impact. AutoML targets each of these per-day deviations.
          </p>
        </section>

      </main>
    </div>
  );
}

// ─── Sub-components ───────────────────────────────────────────────────────────

function SectionLabel({ number, title }: { number: string; title: string }) {
  return (
    <div className="flex items-center gap-3">
      <span className="flex h-7 w-7 items-center justify-center rounded-full bg-emerald-900 text-xs font-bold text-emerald-100">
        {number}
      </span>
      <h2 className="text-base font-semibold uppercase tracking-wider text-slate-600">{title}</h2>
      <div className="h-px flex-1 bg-slate-200" />
    </div>
  );
}

function MiniStat({ label, value, tone, sub }: { label: string; value: string; tone: "ocean" | "rose" | "green" | "indigo" | "amber"; sub?: string }) {
  const cls: Record<typeof tone, string> = {
    ocean: "from-cyan-50 to-sky-100 text-sky-950 border-sky-200",
    rose: "from-rose-50 to-pink-100 text-rose-950 border-rose-200",
    green: "from-green-50 to-emerald-100 text-emerald-950 border-emerald-200",
    indigo: "from-indigo-50 to-violet-100 text-indigo-950 border-indigo-200",
    amber: "from-amber-50 to-orange-100 text-amber-950 border-amber-200",
  };
  return (
    <article className={`rounded-2xl border bg-gradient-to-br p-4 shadow-sm ${cls[tone]}`}>
      <p className="text-xs uppercase tracking-[0.16em] opacity-70">{label}</p>
      <p className="mt-2 text-2xl font-semibold tracking-tight tabular-nums">{value}</p>
      {sub && <p className="mt-1 truncate text-xs opacity-50">{sub}</p>}
    </article>
  );
}

function OverheadRow({ label, value, sub, pct, color }: { label: string; value: string; sub: string; pct: number; color: string }) {
  return (
    <div>
      <div className="mb-1 flex items-baseline justify-between">
        <span className="text-sm font-medium text-slate-700">{label}</span>
        <span className="font-mono text-sm font-semibold text-slate-800">{value}</span>
      </div>
      <div className="mb-0.5 h-1.5 w-full overflow-hidden rounded-full bg-slate-100">
        <div className={`h-full rounded-full ${color}`} style={{ width: `${pct}%` }} />
      </div>
      <p className="text-xs text-slate-400">{sub}</p>
    </div>
  );
}

const MODEL_BETAS = [
  { name: "IForest", beta: 0.307, color: "#10b981" },
  { name: "HBOS",    beta: 0.645, color: "#6366f1" },
  { name: "COPOD",   beta: 0.947, color: "#f59e0b" },
  { name: "ECOD",    beta: 0.945, color: "#f59e0b" },
  { name: "LOF",     beta: 1.175, color: "#ef4444" },
];

function ScalabilityChart({ formula, currentRows, currentP }: {
  formula: FormulaParams | null;
  currentRows: number;
  currentP: number;
}) {
  const [showFormula, setShowFormula] = useState(false);
  const [showInsights, setShowInsights] = useState(true);
  const c0 = 5, m0 = 6, k0 = 25;
  const n0 = currentRows / c0;

  const { alpha, beta, delta, gamma, epsilon, r2, beta_ci95 } = formula ?? {
    alpha: 1, beta: 1, delta: 0, gamma: 1, epsilon: -1, r2: 0,
    beta_ci95: [1, 1] as [number, number], delta_ci95: [0, 0] as [number, number],
    gamma_ci95: [1, 1] as [number, number], n_obs: 0,
  };

  const predict = (n: number) =>
    c0 * alpha * Math.pow(n, beta) * Math.pow(m0, delta) * Math.pow(k0, gamma) * Math.pow(currentP, epsilon);

  const scales = [1, 2, 5, 10, 50, 100];
  const points = scales.map((s) => ({ scale: s, n: n0 * s, sec: predict(n0 * s) }));

  const predictB = (n: number, b: number) =>
    c0 * alpha * Math.pow(n, b) * Math.pow(m0, delta) * Math.pow(k0, gamma) * Math.pow(currentP, epsilon);
  const bandLow  = scales.map((s) => predictB(n0 * s, beta_ci95[0]) / 60);
  const bandHigh = scales.map((s) => predictB(n0 * s, beta_ci95[1]) / 60);

  const option: EChartsOption = {
    animation: false,
    grid: { left: 56, right: 20, top: 20, bottom: 36 },
    tooltip: {
      trigger: "axis",
      formatter: (params: unknown) => {
        const items = params as { dataIndex: number; seriesName: string }[];
        const idx = items.find((i) => i.seriesName === "T(n,m,k,p)")?.dataIndex ?? 0;
        const pt = points[idx];
        const t = pt.sec < 60 ? `${pt.sec.toFixed(1)}s` : `${(pt.sec / 60).toFixed(1)} min`;
        return [
          `<b>${pt.scale}× data (n=${(pt.n / 1000).toFixed(0)}k rows/city)</b>`,
          `T ≈ <b>${t}</b>`,
          `95% CI: [${bandLow[idx].toFixed(1)}m, ${bandHigh[idx].toFixed(1)}m]`,
        ].join("<br/>");
      },
    },
    xAxis: {
      type: "category", data: scales.map((s) => `${s}×`),
      axisLabel: { color: "#64748b", fontSize: 11 },
      axisLine: { lineStyle: { color: "#e2e8f0" } }, splitLine: { show: false },
    },
    yAxis: {
      type: "value", name: "min",
      nameTextStyle: { color: "#94a3b8", fontSize: 10 },
      axisLabel: { color: "#64748b", fontSize: 10, formatter: (v: number) => v < 1 ? `${(v * 60).toFixed(0)}s` : `${v.toFixed(0)}m` },
      splitLine: { lineStyle: { color: "#f1f5f9", type: "dashed" } },
    },
    series: [
      { name: "CI upper",  type: "line", data: bandHigh, symbol: "none", lineStyle: { opacity: 0 }, areaStyle: { color: "rgba(99,102,241,0.12)" }, stack: "ci", z: 1 },
      { name: "CI lower",  type: "line", data: bandLow,  symbol: "none", lineStyle: { opacity: 0 }, areaStyle: { color: "rgba(255,255,255,1)" },  stack: "ci", z: 2 },
      {
        name: "T(n,m,k,p)", type: "line", data: points.map((p) => p.sec / 60),
        symbol: "circle",
        symbolSize: (v: unknown, p: unknown) => (p as { dataIndex: number }).dataIndex === 0 ? 10 : 6,
        lineStyle: { color: "#6366f1", width: 2.5 },
        itemStyle: { color: (p: unknown) => (p as { dataIndex: number }).dataIndex === 0 ? "#10b981" : "#6366f1" },
        z: 3,
        markPoint: { data: [{ name: "now", coord: ["1×", points[0].sec / 60], symbol: "pin", symbolSize: 34,
          label: { show: true, formatter: "now", fontSize: 9, color: "#fff" }, itemStyle: { color: "#10b981" } }] },
      },
    ],
  };

  const scalingFactor = (2 ** beta).toFixed(2);
  const twiceTime = predict(n0 * 2);

  return (
    <div>
      {/* Header row: key stat + formula toggle */}
      <div className="mb-3 flex items-center gap-3">
        <div className="flex items-baseline gap-2 rounded-lg border border-indigo-100 bg-indigo-50 px-4 py-2">
          <span className="text-[10px] font-semibold uppercase tracking-widest text-indigo-400">2× data</span>
          <span className="text-2xl font-bold text-indigo-700">{scalingFactor}×</span>
          <span className="text-xs text-indigo-400">compute time</span>
        </div>
        <div className="flex items-baseline gap-2 rounded-lg border border-slate-200 bg-slate-50 px-4 py-2">
          <span className="text-[10px] font-semibold uppercase tracking-widest text-slate-400">10× data</span>
          <span className="text-2xl font-bold text-slate-700">{(10 ** beta).toFixed(2)}×</span>
          <span className="text-xs text-slate-400">compute time</span>
        </div>
        <div className="flex items-baseline gap-2 rounded-lg border border-slate-200 bg-slate-50 px-4 py-2">
          <span className="text-[10px] font-semibold uppercase tracking-widest text-slate-400">100× data</span>
          <span className="text-2xl font-bold text-slate-700">{(100 ** beta).toFixed(2)}×</span>
          <span className="text-xs text-slate-400">compute time</span>
        </div>
        <button
          onClick={() => setShowFormula((v) => !v)}
          className="ml-auto flex items-center gap-1.5 rounded-lg border border-slate-200 bg-white px-3 py-2 text-[11px] font-medium text-slate-500 hover:border-indigo-200 hover:text-indigo-600 transition-colors"
        >
          <span>{showFormula ? "▲" : "▼"}</span>
          {showFormula ? "Hide formula" : "Show formula"}
        </button>
      </div>

      {/* Collapsible formula block */}
      {showFormula && (
        <div className="mb-4 overflow-hidden rounded-xl border border-indigo-100 bg-gradient-to-br from-indigo-50 to-violet-50">
          <div className="flex items-stretch">
            <div className="flex-1 px-5 py-4">
              <p className="mb-1 text-[10px] font-bold uppercase tracking-widest text-indigo-400">
                Empirical Scaling Law · fitted from {formula?.n_obs ?? 0} benchmark observations (projection/benchmark.py)
              </p>
              <p className="font-mono text-[15px] font-bold text-indigo-900">
                T(n, m, k, p) ≈ α · n<sup>β</sup> · m<sup>δ</sup> · k<sup>γ</sup> · p<sup>ε</sup>
              </p>
              <div className="mt-3 grid grid-cols-3 gap-x-6 gap-y-1.5 text-[11px]">
                {[
                  { sym: "α", val: alpha.toExponential(2), desc: "scale coefficient" },
                  { sym: "β", val: beta.toFixed(3), desc: `row exponent · CI [${beta_ci95[0].toFixed(2)}, ${beta_ci95[1].toFixed(2)}]` },
                  { sym: "δ", val: (delta ?? 0).toFixed(3), desc: "column exponent ≈ 0" },
                  { sym: "γ", val: (gamma ?? 1).toFixed(3), desc: "trial exponent" },
                  { sym: "ε", val: (epsilon ?? -1).toFixed(3), desc: "compute exponent ≈ −1" },
                  { sym: "R²", val: r2.toFixed(3), desc: "goodness of fit" },
                ].map(({ sym, val, desc }) => (
                  <div key={sym} className="flex items-baseline gap-1.5">
                    <span className="min-w-[1.4rem] font-mono text-xs font-bold text-indigo-700">{sym}</span>
                    <span className="font-mono text-xs font-semibold text-slate-800">{val}</span>
                    <span className="text-slate-400 leading-tight">{desc}</span>
                  </div>
                ))}
              </div>
            </div>
            <div className="flex w-36 flex-col items-center justify-center border-l border-indigo-100 bg-indigo-100/40 px-4 py-4 text-center">
              <p className="text-[10px] text-slate-400">sub-linear</p>
              <p className="text-[10px] text-slate-400">{twiceTime < 60 ? `${twiceTime.toFixed(0)}s` : `${(twiceTime/60).toFixed(1)}m`} at 2×</p>
              <p className="mt-2 text-[10px] text-slate-400">R² = {r2.toFixed(3)}</p>
              <p className="mt-1 text-[10px] text-slate-400">{formula?.n_obs ?? 0} obs</p>
            </div>
          </div>
        </div>
      )}

      {/* Chart */}
      <div className="h-[160px]">
        <ReactECharts option={option} style={{ height: "100%", width: "100%" }} notMerge opts={{ renderer: "svg" }} />
      </div>
      <p className="mt-1 text-center text-xs text-slate-400">
        Shaded = 95% CI on β · 10×: ~{(predict(n0 * 10) / 60).toFixed(1)} min · 100×: ~{(predict(n0 * 100) / 60).toFixed(1)} min · p={currentP} GFLOPS/s
      </p>

      {/* Per-model scaling insights */}
      <div className="mt-5 rounded-xl border border-slate-200 bg-slate-50 p-4">
        <button
          onClick={() => setShowInsights((v) => !v)}
          className="mb-3 flex w-full items-center justify-between text-[10px] font-bold uppercase tracking-widest text-slate-400 hover:text-slate-600"
        >
          <span>Scaling by Detector — row exponent β (lower = cheaper at scale)</span>
          <span>{showInsights ? "▲" : "▼"}</span>
        </button>
        {showInsights && (
          <>
            <div className="space-y-2">
              {MODEL_BETAS.map(({ name, beta: b, color }) => {
                const pct = (b / 1.4) * 100;
                const doubling = (2 ** b).toFixed(2);
                return (
                  <div key={name} className="flex items-center gap-3">
                    <span className="w-14 text-right text-[11px] font-mono font-semibold text-slate-700">{name}</span>
                    <div className="relative h-5 flex-1 overflow-hidden rounded bg-slate-200">
                      <div
                        className="h-full rounded transition-all"
                        style={{ width: `${pct}%`, backgroundColor: color, opacity: 0.75 }}
                      />
                      <span className="absolute inset-0 flex items-center pl-2 text-[10px] font-bold mix-blend-multiply" style={{ color: "#1e293b" }}>
                        β = {b.toFixed(3)}
                      </span>
                    </div>
                    <span className="w-24 text-[11px] text-slate-500">2× → {doubling}×</span>
                    {b > 1 && <span className="rounded bg-red-100 px-1.5 py-0.5 text-[10px] font-semibold text-red-600">super-linear</span>}
                    {b < 0.4 && <span className="rounded bg-emerald-100 px-1.5 py-0.5 text-[10px] font-semibold text-emerald-700">best for scale</span>}
                  </div>
                );
              })}
            </div>
            <div className="mt-4 grid grid-cols-3 gap-3 border-t border-slate-200 pt-3">
              <div className="text-center">
                <p className="text-[10px] text-slate-400">Column count effect</p>
                <p className="mt-0.5 text-sm font-bold text-slate-700">δ ≈ 0</p>
                <p className="text-[10px] text-slate-400">adding features is free</p>
              </div>
              <div className="text-center">
                <p className="text-[10px] text-slate-400">Hardware scaling</p>
                <p className="mt-0.5 text-sm font-bold text-slate-700">ε ≈ −0.92</p>
                <p className="text-[10px] text-slate-400">2× faster CPU → 1.9× speedup</p>
              </div>
              <div className="text-center">
                <p className="text-[10px] text-slate-400">Trial budget</p>
                <p className="mt-0.5 text-sm font-bold text-slate-700">γ ≈ 0.87</p>
                <p className="text-[10px] text-slate-400">near-linear in k</p>
              </div>
            </div>
            <p className="mt-3 text-[10px] text-slate-400">
              Run <code className="rounded bg-slate-200 px-1">python projection/quick_profiler.py</code> on your own data to get fitted parameters for this machine.
            </p>
          </>
        )}
      </div>
    </div>
  );
}
