"use client";

import { useEffect, useMemo, useState } from "react";
import ReactECharts from "echarts-for-react";
import type { EChartsOption } from "echarts";

// ─── API types ────────────────────────────────────────────────────────────────

type RunInfo = { id: string; label: string };

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

// Experiment-sourced scatter point (from predictions CSV in artifact)
type ExpScatterPoint = {
  time_ms: number;
  y_value: number | null;
  y_true: number;
  y_pred: number;
};

// ─── Weather scatter types (used as fallback / data quality tab) ──────────────

type DatasetKey = "raw_hourly" | "daily_with_anomalies" | "daily_without_anomalies";
type SourceMode = "raw_hourly" | "daily";
type DailyVariant = "with_anomalies" | "without_anomalies";
type Scope = "univariate" | "multivariate";

type MetaResponse = {
  dataset: DatasetKey;
  cities: string[];
  numericColumns: string[];
};

type ScatterPoint = {
  time: string;
  cityId: string;
  x: number;
  y: number;
  isAnomaly: boolean;
  shiftPct: number | null;
  actualValue: number | null;
  targetColumn: string | null;
  anomalyHours: number | null;
};

type ScatterResponse = {
  city: string;
  rowCount: number;
  anomalyCount: number;
  anomalyRate: number;
  avgShiftPct: number | null;
  points: ScatterPoint[];
};

type ChartDatum = { value: [number, number]; point: ScatterPoint };

// ─── Constants ────────────────────────────────────────────────────────────────

const COLORS = {
  normal: "#1f8eb5",
  detected: "#d7265a",
  missed: "#d7265a",
  detectedBorder: "#22c55e",
  missedBorder: "#1e3a8a",
};

const sourceModeLabels: Record<SourceMode, string> = {
  raw_hourly: "Raw Hourly",
  daily: "Daily Aggregate",
};
const dailyVariantLabels: Record<DailyVariant, string> = {
  with_anomalies: "With Anomalies",
  without_anomalies: "Without Anomalies",
};
const scopeLabels: Record<Scope, string> = {
  univariate: "Univariate (per column)",
  multivariate: "Multivariate (all features)",
};

// ─── Helpers ──────────────────────────────────────────────────────────────────

function fmtDate(v: number) {
  return new Date(v).toLocaleDateString(undefined, { year: "numeric", month: "short", day: "numeric" });
}
function fmtDateTime(v: number) {
  return new Date(v).toLocaleString(undefined, { year: "numeric", month: "short", day: "numeric", hour: "2-digit", minute: "2-digit" });
}
function fmtPct(v: number) { return `${(v * 100).toFixed(1)}%`; }
function fmtF1(v: number) { return v.toFixed(3); }

function f1Color(v: number) {
  if (v >= 0.5) return "text-green-700 font-semibold";
  if (v > 0) return "text-amber-700 font-semibold";
  return "text-slate-400";
}

// Convert an ExpScatterPoint to the generic ScatterPoint format used by the chart
function expToScatterPoint(p: ExpScatterPoint, cityId: string, targetColumn: string): ScatterPoint {
  return {
    time: new Date(p.time_ms).toISOString(),
    cityId,
    x: p.time_ms,
    y: p.y_value ?? 0,
    isAnomaly: p.y_true === 1,
    shiftPct: null,
    actualValue: p.y_value,
    targetColumn,
    anomalyHours: null,
  };
}

// ─── Page ─────────────────────────────────────────────────────────────────────

export default function ExperimentsPage() {
  // ── Dataset controls ──────────────────────────────────────────────────
  const [sourceMode, setSourceMode] = useState<SourceMode>("raw_hourly");
  const [dailyVariant, setDailyVariant] = useState<DailyVariant>("with_anomalies");
  const [meta, setMeta] = useState<MetaResponse | null>(null);
  const [city, setCity] = useState<string>("");
  const [yColumn, setYColumn] = useState<string>("");
  const [scatterData, setScatterData] = useState<ScatterResponse | null>(null);

  // ── Experiment controls ───────────────────────────────────────────────
  const [runs, setRuns] = useState<RunInfo[]>([]);
  const [selectedRun, setSelectedRun] = useState<string>("");
  const [scope, setScope] = useState<Scope>("univariate");
  const [summaryRows, setSummaryRows] = useState<SummaryRow[]>([]);

  // ── Experiment scatter (artifact-sourced, always aligned with table) ──
  const [expScatterPoints, setExpScatterPoints] = useState<ExpScatterPoint[]>([]);
  const [hasYValue, setHasYValue] = useState(false);
  const [isLoadingExpScatter, setIsLoadingExpScatter] = useState(false);

  // ── Loading / error ───────────────────────────────────────────────────
  const [isLoadingMeta, setIsLoadingMeta] = useState(false);
  const [isLoadingData, setIsLoadingData] = useState(false);
  const [isLoadingExp, setIsLoadingExp] = useState(false);
  const [dataError, setDataError] = useState("");
  const [expError, setExpError] = useState("");

  // ── Time range ────────────────────────────────────────────────────────
  const [selectedRange, setSelectedRange] = useState<{ from: number; to: number } | null>(null);

  const dataset: DatasetKey =
    sourceMode === "raw_hourly"
      ? "raw_hourly"
      : dailyVariant === "with_anomalies"
        ? "daily_with_anomalies"
        : "daily_without_anomalies";

  // ── Load runs once ────────────────────────────────────────────────────
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

  // ── Load metadata ─────────────────────────────────────────────────────
  useEffect(() => {
    let cancelled = false;
    setIsLoadingMeta(true);
    setDataError("");
    fetch(`/api/weather/metadata?dataset=${dataset}`)
      .then((r) => r.json())
      .then((p: MetaResponse & { error?: string }) => {
        if (cancelled) return;
        if (p.error) throw new Error(p.error);
        setMeta(p);
        setCity(p.cities[0] ?? "");
        setYColumn(p.numericColumns.includes("temperature_2m") ? "temperature_2m" : (p.numericColumns[1] ?? p.numericColumns[0] ?? ""));
      })
      .catch((e) => { if (!cancelled) { setDataError(e.message); setMeta(null); } })
      .finally(() => { if (!cancelled) setIsLoadingMeta(false); });
    return () => { cancelled = true; };
  }, [dataset]);

  // ── Load weather scatter (used only when no experiment run selected) ──
  useEffect(() => {
    if (selectedRun) return; // experiment scatter takes over when a run is selected
    if (!meta || !yColumn) return;
    if (dataset.startsWith("daily_") && !city) return;
    let cancelled = false;
    setIsLoadingData(true);
    setDataError("");
    fetch(`/api/weather/scatter?${new URLSearchParams({ dataset, city, yColumn })}`)
      .then((r) => r.json())
      .then((p: ScatterResponse & { error?: string }) => {
        if (cancelled) return;
        if (p.error) throw new Error(p.error);
        setScatterData(p);
        if (p.points.length) {
          const ts = p.points.map((pt) => pt.x);
          setSelectedRange({ from: Math.min(...ts), to: Math.max(...ts) });
        }
      })
      .catch((e) => { if (!cancelled) { setDataError(e.message); setScatterData(null); } })
      .finally(() => { if (!cancelled) setIsLoadingData(false); });
    return () => { cancelled = true; };
  }, [meta, dataset, city, yColumn, selectedRun]);

  // ── Load experiment scatter from artifact (aligned with table) ────────
  useEffect(() => {
    if (!selectedRun || !city || !yColumn) {
      setExpScatterPoints([]);
      return;
    }
    let cancelled = false;
    setIsLoadingExpScatter(true);
    const col = scope === "multivariate" ? "ALL_FEATURES" : yColumn;
    fetch(`/api/experiments/scatter?run=${selectedRun}&city=${city}&scope=${scope}&column=${col}`)
      .then((r) => r.json())
      .then((d: { points?: ExpScatterPoint[]; hasYValue?: boolean }) => {
        if (cancelled) return;
        setExpScatterPoints(d.points ?? []);
        setHasYValue(d.hasYValue ?? false);
        const pts = d.points ?? [];
        if (pts.length) {
          setSelectedRange({ from: pts[0].time_ms, to: pts[pts.length - 1].time_ms });
        }
      })
      .catch(() => { if (!cancelled) setExpScatterPoints([]); })
      .finally(() => { if (!cancelled) setIsLoadingExpScatter(false); });
    return () => { cancelled = true; };
  }, [selectedRun, city, scope, yColumn]);

  // ── Load experiment summary ───────────────────────────────────────────
  useEffect(() => {
    if (!selectedRun) return;
    let cancelled = false;
    setIsLoadingExp(true);
    setExpError("");
    fetch(`/api/experiments/summary?run=${selectedRun}`)
      .then((r) => r.json())
      .then((summary: { rows?: SummaryRow[]; error?: string }) => {
        if (cancelled) return;
        if (summary.error) { setExpError(summary.error); setSummaryRows([]); }
        else setSummaryRows(summary.rows ?? []);
      })
      .catch((e) => { if (!cancelled) setExpError(e.message); })
      .finally(() => { if (!cancelled) setIsLoadingExp(false); });
    return () => { cancelled = true; };
  }, [selectedRun]);

  // ── Categorise scatter points ─────────────────────────────────────────
  // When a run is selected: use experiment scatter → perfectly aligned with table.
  // Fallback (no run): use weather API scatter with no overlay.
  const { normalPts, detectedPts, missedPts } = useMemo(() => {
    const normal: ScatterPoint[] = [];
    const detected: ScatterPoint[] = [];
    const missed: ScatterPoint[] = [];

    if (expScatterPoints.length > 0) {
      const col = scope === "multivariate" ? "ALL_FEATURES" : yColumn;
      expScatterPoints.forEach((p) => {
        const sp = expToScatterPoint(p, city, col);
        if (p.y_true === 0) normal.push(sp);
        else if (p.y_pred === 1) detected.push(sp);
        else missed.push(sp);
      });
      return { normalPts: normal, detectedPts: detected, missedPts: missed };
    }

    // Fallback: no predictions available, show raw scatter without overlay
    (scatterData?.points ?? []).forEach((p) => normal.push(p));
    return { normalPts: normal, detectedPts: [], missedPts: [] };
  }, [expScatterPoints, scatterData, city, yColumn, scope]);

  // ── Detection stats from experiment scatter ───────────────────────────
  const detectionStats = useMemo(() => {
    if (expScatterPoints.length === 0) return null;
    const tp = expScatterPoints.filter((p) => p.y_true === 1 && p.y_pred === 1).length;
    const fn = expScatterPoints.filter((p) => p.y_true === 1 && p.y_pred === 0).length;
    const fp = expScatterPoints.filter((p) => p.y_true === 0 && p.y_pred === 1).length;
    const tn = expScatterPoints.filter((p) => p.y_true === 0 && p.y_pred === 0).length;
    return { tp, fn, fp, tn, total: expScatterPoints.length };
  }, [expScatterPoints]);

  // ── Time range buttons ────────────────────────────────────────────────
  const timeButtons = useMemo(() => {
    const pts = expScatterPoints.length > 0
      ? expScatterPoints.map((p) => p.time_ms)
      : (scatterData?.points ?? []).map((p) => p.x);
    if (!pts.length) return [] as { label: string; from: number; to: number }[];
    const min = Math.min(...pts), max = Math.max(...pts), D = 86400000;
    return [
      { label: "7D", from: Math.max(min, max - 7 * D), to: max },
      { label: "30D", from: Math.max(min, max - 30 * D), to: max },
      { label: "90D", from: Math.max(min, max - 90 * D), to: max },
      { label: "ALL", from: min, to: max },
    ];
  }, [expScatterPoints, scatterData]);

  const timeLabel = useMemo(() => {
    const pts = expScatterPoints.length > 0
      ? expScatterPoints.map((p) => p.time_ms)
      : (scatterData?.points ?? []).map((p) => p.x);
    if (!pts.length) return "";
    return `${fmtDate(Math.min(...pts))} to ${fmtDate(Math.max(...pts))}`;
  }, [expScatterPoints, scatterData]);

  // ── Scatter chart option ──────────────────────────────────────────────
  const scatterOption = useMemo((): EChartsOption => {
    const toData = (pts: ScatterPoint[]): ChartDatum[] => pts.map((p) => ({ value: [p.x, p.y], point: p }));
    const usingExpData = expScatterPoints.length > 0;

    const tooltip = (params: unknown) => {
      const p = (params as { data?: ChartDatum })?.data?.point;
      if (!p) return "";
      if (usingExpData) {
        const category = p.isAnomaly
          ? (detectedPts.some((d) => d.x === p.x) ? "✓ Detected" : "✗ Missed")
          : "Normal";
        return [
          `<b>${p.cityId}</b>`,
          `time: ${fmtDateTime(p.x)}`,
          `${yColumn}: ${p.y.toFixed(3)}`,
          `<b>${category}</b>`,
        ].join("<br/>");
      }
      return [
        `<b>${p.cityId}</b>`,
        `time: ${p.time}`,
        `actual: ${p.actualValue != null ? p.actualValue.toFixed(3) : "-"}`,
        `value: ${p.y.toFixed(3)}`,
        `shift: ${p.shiftPct != null ? fmtPct(p.shiftPct) : "-"}`,
        `<b>${p.isAnomaly ? "Anomaly" : "Normal"}</b>`,
      ].join("<br/>");
    };

    const legendData = usingExpData
      ? ["Normal", "Detected by AutoML", "Missed by AutoML"]
      : ["Normal"];

    return {
      animation: false,
      grid: { left: 58, right: 24, top: 40, bottom: 70 },
      legend: { top: 4, data: legendData, textStyle: { color: "#34556a" } },
      tooltip: { trigger: "item", borderColor: "#a5b4c7", backgroundColor: "#fff", textStyle: { color: "#20394a" }, formatter: tooltip },
      xAxis: {
        type: "time", name: "time", nameLocation: "middle", nameGap: 34,
        axisLabel: { color: "#325163", formatter: (v: number) => fmtDate(v) },
        min: selectedRange?.from ?? undefined, max: selectedRange?.to ?? undefined,
      },
      yAxis: { type: "value", name: yColumn, nameLocation: "middle", nameGap: 46, axisLabel: { color: "#325163" } },
      dataZoom: [
        { type: "inside", xAxisIndex: 0, filterMode: "none", zoomOnMouseWheel: true, moveOnMouseMove: true, moveOnMouseWheel: true },
        { type: "slider", xAxisIndex: 0, filterMode: "none", height: 28, bottom: 14, borderColor: "#c9d7e4", handleSize: "95%", moveHandleSize: 4 },
      ],
      series: [
        { name: "Normal", type: "scatter", symbolSize: 7, itemStyle: { color: COLORS.normal }, data: toData(normalPts), large: true },
        {
          name: "Detected by AutoML",
          type: "scatter", symbolSize: 12,
          itemStyle: { color: COLORS.detected, borderColor: COLORS.detectedBorder, borderWidth: 2.5 },
          data: toData(detectedPts),
        },
        {
          name: "Missed by AutoML",
          type: "scatter", symbolSize: 12,
          itemStyle: { color: COLORS.missed, borderColor: COLORS.missedBorder, borderWidth: 2.5 },
          data: toData(missedPts),
        },
      ],
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [normalPts, detectedPts, missedPts, yColumn, selectedRange, expScatterPoints.length]);

  // ── Summary filtered to city ──────────────────────────────────────────
  const citySummary = useMemo(
    () => (city ? summaryRows.filter((r) => r.city === city) : summaryRows),
    [summaryRows, city],
  );

  // ── F1 bar chart option ───────────────────────────────────────────────
  const f1ChartOption = useMemo((): EChartsOption => {
    if (!citySummary.length) return {};
    const univariate = citySummary.filter((r) => r.scope === "univariate");
    const multivariate = citySummary.filter((r) => r.scope === "multivariate");

    const labels = univariate.map((r) => r.target_column.replace(/_/g, " "));
    const f1s = univariate.map((r) => parseFloat(r.f1.toFixed(4)));
    const f2s = univariate.map((r) => parseFloat((r.f2 ?? 0).toFixed(4)));
    const precisions = univariate.map((r) => parseFloat(r.precision.toFixed(4)));
    const recalls = univariate.map((r) => parseFloat(r.recall.toFixed(4)));

    const series: EChartsOption["series"] = [
      { name: "Precision", type: "bar", data: precisions, itemStyle: { color: "#60a5fa" }, barMaxWidth: 22 },
      { name: "Recall", type: "bar", data: recalls, itemStyle: { color: "#34d399" }, barMaxWidth: 22 },
      { name: "F1", type: "bar", data: f1s, itemStyle: { color: "#f472b6" }, barMaxWidth: 22 },
      { name: "F2 (recall-weighted)", type: "bar", data: f2s, itemStyle: { color: "#a78bfa" }, barMaxWidth: 22 },
    ];

    if (multivariate.length) {
      const mv = multivariate[0];
      series.push({
        name: "Multivariate F1",
        type: "line",
        data: Array(labels.length).fill(parseFloat(mv.f1.toFixed(4))),
        lineStyle: { color: "#f97316", type: "dashed", width: 2 },
        itemStyle: { color: "#f97316" },
        symbol: "none",
      });
    }

    return {
      animation: false,
      legend: { top: 4, textStyle: { color: "#34556a" } },
      grid: { left: 50, right: 24, top: 44, bottom: 70 },
      tooltip: { trigger: "axis", axisPointer: { type: "shadow" } },
      xAxis: { type: "category", data: labels, axisLabel: { color: "#325163", rotate: 15, interval: 0 } },
      yAxis: { type: "value", min: 0, max: 1, axisLabel: { color: "#325163", formatter: (v: number) => v.toFixed(1) } },
      series,
    };
  }, [citySummary]);

  // ── Stat card values ──────────────────────────────────────────────────
  const totalPoints = expScatterPoints.length > 0 ? expScatterPoints.length : (scatterData?.rowCount ?? 0);
  const usingExpScatter = expScatterPoints.length > 0;

  // ── Render ────────────────────────────────────────────────────────────
  return (
    <div className="min-h-screen bg-grid">
      <main className="mx-auto flex w-full max-w-7xl flex-col gap-6 px-5 py-8 md:px-8 lg:px-10">

        {/* Header */}
        <header className="rounded-2xl border border-white/25 bg-[linear-gradient(125deg,#1a1060_0%,#2d1b8a_45%,#4f39c6_100%)] p-6 text-white shadow-[0_18px_40px_rgba(0,0,0,0.25)] md:p-8">
          <div className="flex items-start justify-between gap-4">
            <div>
              <p className="text-xs uppercase tracking-[0.24em] text-violet-200/90">Adaptive Data Profiling · Experiment Mode</p>
              <h1 className="mt-2 text-2xl font-semibold tracking-tight md:text-4xl">AutoML Anomaly Detection</h1>
              <p className="mt-3 max-w-3xl text-sm text-violet-100/90 md:text-base">
                Synthetic anomalies (injected at ingestion) vs AutoML detection.
                The model is fit on all data (unsupervised — labels never seen during training), then predictions are
                compared against the known ground truth.{" "}
                <span className="inline-flex items-center gap-1">
                  <span className="inline-block h-3 w-3 rounded-full border-2 border-green-400 bg-[#d7265a]" />
                  <span className="text-violet-100/80">detected</span>
                </span>{" · "}
                <span className="inline-flex items-center gap-1">
                  <span className="inline-block h-3 w-3 rounded-full border-2 border-blue-800 bg-[#d7265a]" />
                  <span className="text-violet-100/80">missed</span>
                </span>
              </p>
            </div>
            <a href="/" className="shrink-0 rounded-xl border border-white/30 bg-white/10 px-4 py-2 text-sm font-medium text-white hover:bg-white/20">
              ← Dashboard
            </a>
          </div>
        </header>

        {/* Controls */}
        <section className="rounded-2xl border border-slate-200/80 bg-white/90 p-4 shadow-sm backdrop-blur md:p-6">
          <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 xl:grid-cols-3 2xl:grid-cols-6">
            <label className="control sm:col-span-2 xl:col-span-2">
              <span>Experiment Run</span>
              <select value={selectedRun} onChange={(e) => setSelectedRun(e.target.value)} disabled={runs.length === 0} suppressHydrationWarning>
                {runs.map((r) => <option key={r.id} value={r.id}>{r.label}</option>)}
                {runs.length === 0 && <option value="">No runs found — run the experiment first</option>}
              </select>
            </label>

            <label className="control">
              <span>City</span>
              <select value={city} onChange={(e) => setCity(e.target.value)} disabled={!meta} suppressHydrationWarning>
                {(meta?.cities ?? []).map((c) => <option key={c} value={c}>{c}</option>)}
              </select>
            </label>

            <label className="control">
              <span>Y Axis / Column</span>
              <select value={yColumn} onChange={(e) => setYColumn(e.target.value)} disabled={!meta} suppressHydrationWarning>
                {(meta?.numericColumns ?? []).map((col) => <option key={col} value={col}>{col}</option>)}
              </select>
            </label>

            <label className="control">
              <span>AutoML Scope</span>
              <select value={scope} onChange={(e) => setScope(e.target.value as Scope)}>
                {Object.entries(scopeLabels).map(([k, l]) => <option key={k} value={k}>{l}</option>)}
              </select>
            </label>

            {/* Dataset selector only shown when no run selected (data quality browsing) */}
            {!selectedRun && (
              <label className="control">
                <span>Dataset</span>
                <select value={sourceMode} onChange={(e) => setSourceMode(e.target.value as SourceMode)}>
                  {Object.entries(sourceModeLabels).map(([k, l]) => <option key={k} value={k}>{l}</option>)}
                </select>
              </label>
            )}
            {!selectedRun && sourceMode === "daily" && (
              <label className="control">
                <span>Daily View</span>
                <select value={dailyVariant} onChange={(e) => setDailyVariant(e.target.value as DailyVariant)}>
                  {Object.entries(dailyVariantLabels).map(([k, l]) => <option key={k} value={k}>{l}</option>)}
                </select>
              </label>
            )}
          </div>

          {usingExpScatter && !hasYValue && (
            <p className="mt-2 text-xs text-amber-700">
              This run was produced before y_value tracking was added — scatter Y axis shows 0 for all points.
              Re-run the experiment to get fully aligned scatter data.
            </p>
          )}
        </section>

        {/* Errors */}
        {dataError && <section className="rounded-2xl border border-rose-200 bg-rose-50 px-4 py-3 text-sm text-rose-800">{dataError}</section>}
        {expError && <section className="rounded-2xl border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-800">Experiment data: {expError}</section>}

        {/* Stat cards */}
        <section className="grid grid-cols-2 gap-4 md:grid-cols-4">
          <StatCard label="Data Points" value={totalPoints > 0 ? totalPoints.toLocaleString() : "-"} tone="ocean" />
          <StatCard label="Synthetic Anomalies" value={(detectedPts.length + missedPts.length).toLocaleString() || "-"} tone="rose" />
          <StatCard label="Detected" value={usingExpScatter ? detectedPts.length.toLocaleString() : "-"} tone="green" />
          <StatCard label="Missed" value={usingExpScatter ? missedPts.length.toLocaleString() : "-"} tone="indigo" />
        </section>

        {/* Scatter plot */}
        <section className="rounded-2xl border border-slate-200/80 bg-white/95 p-4 shadow-sm md:p-6">
          <div className="mb-3 flex flex-wrap items-center justify-between gap-2">
            <h2 className="text-lg font-semibold text-slate-800">Scatter Plot — AutoML Detection Overlay</h2>
            <p className="text-xs text-slate-500">
              {city || "all"} · Y={yColumn} · {scope}
              {selectedRun ? ` · ${runs.find((r) => r.id === selectedRun)?.label ?? selectedRun}` : ""}
            </p>
          </div>
          {timeLabel && <p className="mb-2 text-xs text-slate-500">{timeLabel}</p>}
          {timeButtons.length > 0 && (
            <div className="mb-3 flex flex-wrap gap-2">
              {timeButtons.map((b) => (
                <button key={b.label} type="button"
                  className="rounded-md border border-slate-300 bg-slate-50 px-3 py-1 text-xs font-medium text-slate-700 hover:bg-slate-100"
                  onClick={() => setSelectedRange({ from: b.from, to: b.to })}>
                  {b.label}
                </button>
              ))}
            </div>
          )}
          <div className="h-[420px] w-full">
            {isLoadingMeta || isLoadingData || isLoadingExpScatter
              ? <div className="chart-loading">Loading data…</div>
              : <ReactECharts option={scatterOption} style={{ height: "100%", width: "100%" }} notMerge opts={{ renderer: "svg" }} />
            }
          </div>
          {!usingExpScatter && selectedRun && !isLoadingExpScatter && (
            <p className="mt-2 text-xs text-slate-400">No experiment scatter data for this city/column/scope combination in the selected run.</p>
          )}
        </section>

        {/* Experiment Interpretation */}
        <section className="rounded-2xl border border-slate-200/80 bg-white/95 p-4 shadow-sm md:p-6">
          <div className="mb-5 flex flex-wrap items-center justify-between gap-2">
            <h2 className="text-lg font-semibold text-slate-800">Experiment Interpretation</h2>
            <span className="text-xs text-slate-500">
              {isLoadingExp ? "Loading…" : `${citySummary.length} model(s) · ${city || "all cities"}`}
            </span>
          </div>

          {/* Confusion matrix for selected column */}
          {detectionStats && (
            <div className="mb-6 grid grid-cols-2 gap-3 sm:grid-cols-4">
              <MetricTile
                label="True Positives"
                sub="Detected correctly"
                value={detectionStats.tp}
                total={detectionStats.tp + detectionStats.fn}
                ratioLabel="of all anomalies"
                color="bg-green-50 border-green-200 text-green-900"
                barColor="bg-green-500"
              />
              <MetricTile
                label="False Negatives"
                sub="Anomalies missed"
                value={detectionStats.fn}
                total={detectionStats.tp + detectionStats.fn}
                ratioLabel="of all anomalies"
                color="bg-indigo-50 border-indigo-200 text-indigo-900"
                barColor="bg-indigo-400"
              />
              <MetricTile
                label="False Positives"
                sub="Normal flagged wrong"
                value={detectionStats.fp}
                total={detectionStats.tn + detectionStats.fp}
                ratioLabel="of normal points"
                color="bg-amber-50 border-amber-200 text-amber-900"
                barColor="bg-amber-400"
              />
              <MetricTile
                label="True Negatives"
                sub="Correctly clear"
                value={detectionStats.tn}
                total={detectionStats.tn + detectionStats.fp}
                ratioLabel="of normal points"
                color="bg-slate-50 border-slate-200 text-slate-700"
                barColor="bg-slate-400"
              />
            </div>
          )}

          {/* F1 bar chart */}
          {citySummary.length > 0 && (
            <div className="mb-6 rounded-xl border border-slate-100 bg-slate-50/60 p-4">
              <h3 className="mb-2 text-sm font-semibold text-slate-700">
                Precision / Recall / F1 by Column
                <span className="ml-2 text-xs font-normal text-slate-400">({city || "selected city"} · univariate + multivariate reference line)</span>
              </h3>
              <div className="h-[240px]">
                <ReactECharts option={f1ChartOption} style={{ height: "100%", width: "100%" }} notMerge opts={{ renderer: "svg" }} />
              </div>
            </div>
          )}

          {/* Summary table */}
          <div className="overflow-auto">
            <table className="w-full min-w-[760px] border-collapse text-sm">
              <thead>
                <tr className="border-b border-slate-200 text-left text-slate-500">
                  <th className="py-2 pr-4 font-medium">City</th>
                  <th className="py-2 pr-4 font-medium">Scope</th>
                  <th className="py-2 pr-4 font-medium">Column</th>
                  <th className="py-2 pr-4 font-medium">Best Model</th>
                  <th className="py-2 pr-4 text-right font-medium">Precision</th>
                  <th className="py-2 pr-4 text-right font-medium">Recall</th>
                  <th className="py-2 pr-4 text-right font-medium">F1</th>
                  <th className="py-2 pr-4 text-right font-medium">F2 ↑recall</th>
                  <th className="py-2 pr-4 text-right font-medium">Detected / Total</th>
                  <th className="py-2 text-right font-medium">Predicted</th>
                </tr>
              </thead>
              <tbody>
                {citySummary.map((row, i) => (
                  <tr key={i} className={`border-b border-slate-100 ${row.target_column === yColumn || row.target_column === "ALL_FEATURES" ? "bg-violet-50/60" : ""}`}>
                    <td className="py-2 pr-4 font-medium text-slate-700">{row.city}</td>
                    <td className="py-2 pr-4 text-slate-500">{row.scope}</td>
                    <td className="py-2 pr-4 font-mono text-xs text-slate-700">{row.target_column}</td>
                    <td className="py-2 pr-4">
                      <span className="rounded-md bg-violet-100 px-2 py-0.5 text-xs font-medium text-violet-800">{row.model_name}</span>
                    </td>
                    <td className="py-2 pr-4 text-right tabular-nums">{fmtPct(row.precision)}</td>
                    <td className="py-2 pr-4 text-right tabular-nums">{fmtPct(row.recall)}</td>
                    <td className={`py-2 pr-4 text-right tabular-nums ${f1Color(row.f1)}`}>{fmtF1(row.f1)}</td>
                    <td className={`py-2 pr-4 text-right tabular-nums ${f1Color(row.f2 ?? 0)}`}>{fmtF1(row.f2 ?? 0)}</td>
                    <td className="py-2 pr-4 text-right tabular-nums">
                      <span className="text-slate-700">{fmtPct(row.recall)}</span>
                    </td>
                    <td className="py-2 text-right tabular-nums text-slate-600">{row.n_positive_pred.toLocaleString()}</td>
                  </tr>
                ))}
                {citySummary.length === 0 && (
                  <tr>
                    <td colSpan={9} className="py-6 text-center text-slate-400">
                      {isLoadingExp ? "Loading experiment results…" : selectedRun ? "No results yet — experiment may still be running." : "Select an experiment run above."}
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </section>

      </main>
    </div>
  );
}

// ─── Sub-components ───────────────────────────────────────────────────────────

function StatCard({ label, value, tone }: { label: string; value: string; tone: "ocean" | "rose" | "green" | "indigo" }) {
  const cls: Record<typeof tone, string> = {
    ocean: "from-cyan-50 to-sky-100 text-sky-950 border-sky-200",
    rose: "from-rose-50 to-pink-100 text-rose-950 border-rose-200",
    green: "from-green-50 to-emerald-100 text-emerald-950 border-emerald-200",
    indigo: "from-indigo-50 to-violet-100 text-indigo-950 border-indigo-200",
  };
  return (
    <article className={`rounded-2xl border bg-gradient-to-br p-4 shadow-sm ${cls[tone]}`}>
      <p className="text-xs uppercase tracking-[0.16em] opacity-80">{label}</p>
      <p className="mt-2 text-2xl font-semibold tracking-tight">{value}</p>
    </article>
  );
}

function MetricTile({
  label, sub, value, total, ratioLabel, color, barColor,
}: {
  label: string; sub: string; value: number; total?: number; ratioLabel?: string; color: string; barColor?: string;
}) {
  const pct = total && total > 0 ? (value / total) * 100 : null;
  return (
    <div className={`rounded-xl border p-3 ${color}`}>
      <p className="text-xs font-semibold uppercase tracking-wide opacity-70">{label}</p>
      <p className="text-xs opacity-60">{sub}</p>
      <p className="mt-1 text-2xl font-bold tabular-nums">{value.toLocaleString()}</p>
      {total !== undefined && pct !== null && (
        <div className="mt-2">
          <div className="mb-1 flex justify-between text-xs opacity-70">
            <span>{value.toLocaleString()} / {total.toLocaleString()}</span>
            <span>{pct.toFixed(1)}% {ratioLabel}</span>
          </div>
          <div className="h-1.5 w-full overflow-hidden rounded-full bg-black/10">
            <div className={`h-full rounded-full ${barColor ?? "bg-current"}`} style={{ width: `${Math.min(pct, 100)}%` }} />
          </div>
        </div>
      )}
    </div>
  );
}
