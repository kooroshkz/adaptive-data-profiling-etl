"use client";

import { useEffect, useMemo, useState } from "react";
import ReactECharts from "echarts-for-react";
import type { EChartsOption } from "echarts";

type DatasetKey = "raw_hourly" | "daily_with_anomalies" | "daily_without_anomalies";
type SourceMode = "raw_hourly" | "daily";
type DailyVariant = "with_anomalies" | "without_anomalies";

type MetaResponse = {
  dataset: DatasetKey;
  cities: string[];
  columns: Array<{ name: string; type: string }>;
  numericColumns: string[];
  hasSyntheticAnomalyFlag: boolean;
};

type Point = {
  time: string;
  cityId: string;
  x: number;
  y: number;
  isAnomaly: boolean;
  isSyntheticAny?: boolean;
  shiftPct: number | null;
  actualValue: number | null;
  targetColumn: string | null;
  anomalyHours: number | null;
};

type ScatterResponse = {
  dataset: DatasetKey;
  city: string;
  xColumn: string;
  yColumn: string;
  rowCount: number;
  anomalyCount: number;
  totalSyntheticCount?: number;
  anomalyRate: number;
  avgShiftPct: number | null;
  points: Point[];
};

type ChartPointDatum = {
  value: [number, number];
  point: Point;
};

const datasetLabels: Record<DatasetKey, string> = {
  raw_hourly: "Raw Hourly (S3)",
  daily_with_anomalies: "Daily With Anomalies (MotherDuck)",
  daily_without_anomalies: "Daily Without Anomalies (MotherDuck)",
};

const sourceModeLabels: Record<SourceMode, string> = {
  raw_hourly: "Raw Hourly",
  daily: "Daily Aggregate",
};

const dailyVariantLabels: Record<DailyVariant, string> = {
  with_anomalies: "With Anomalies",
  without_anomalies: "Without Anomalies",
};

function formatDateTick(value: number): string {
  return new Date(value).toLocaleDateString(undefined, {
    year: "numeric",
    month: "short",
    day: "numeric",
  });
}

function formatDateTime(value: number): string {
  return new Date(value).toLocaleString(undefined, {
    year: "numeric",
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
}

export default function Home() {
  const [sourceMode, setSourceMode] = useState<SourceMode>("raw_hourly");
  const [dailyVariant, setDailyVariant] = useState<DailyVariant>("with_anomalies");
  const [meta, setMeta] = useState<MetaResponse | null>(null);
  const [city, setCity] = useState<string>("");
  const [yColumn, setYColumn] = useState<string>("");
  const [scatterData, setScatterData] = useState<ScatterResponse | null>(null);
  const [isLoadingMeta, setIsLoadingMeta] = useState<boolean>(false);
  const [isLoadingData, setIsLoadingData] = useState<boolean>(false);
  const [errorMessage, setErrorMessage] = useState<string>("");

  const dataset: DatasetKey =
    sourceMode === "raw_hourly"
      ? "raw_hourly"
      : dailyVariant === "with_anomalies"
        ? "daily_with_anomalies"
        : "daily_without_anomalies";

  useEffect(() => {
    let isCancelled = false;

    async function loadMeta() {
      setIsLoadingMeta(true);
      setErrorMessage("");

      try {
        const res = await fetch(`/api/weather/metadata?dataset=${dataset}`);
        const payload = (await res.json()) as MetaResponse | { error: string };

        if (!res.ok || "error" in payload) {
          throw new Error("error" in payload ? payload.error : "Failed to load dataset metadata");
        }

        if (isCancelled) {
          return;
        }

        setMeta(payload);
        const defaultCity = payload.cities[0] ?? "";

        const defaultY = payload.numericColumns.includes("precipitation")
          ? "precipitation"
          : payload.numericColumns[1] ?? payload.numericColumns[0] ?? "";

        setCity(defaultCity);
        setYColumn(defaultY);
      } catch (error) {
        if (!isCancelled) {
          setErrorMessage(error instanceof Error ? error.message : "Unknown metadata error");
          setMeta(null);
        }
      } finally {
        if (!isCancelled) {
          setIsLoadingMeta(false);
        }
      }
    }

    loadMeta();

    return () => {
      isCancelled = true;
    };
  }, [dataset]);

  useEffect(() => {
    if (!meta || !yColumn) {
      return;
    }
    if (dataset.startsWith("daily_") && !city) {
      return;
    }

    let isCancelled = false;

    async function loadScatter() {
      setIsLoadingData(true);
      setErrorMessage("");

      const query = new URLSearchParams({
        dataset,
        city,
        yColumn,
      });

      try {
        const res = await fetch(`/api/weather/scatter?${query.toString()}`);
        const payload = (await res.json()) as ScatterResponse | { error: string };

        if (!res.ok || "error" in payload) {
          throw new Error("error" in payload ? payload.error : "Failed to load scatter data");
        }

        if (!isCancelled) {
          setScatterData(payload);
        }
      } catch (error) {
        if (!isCancelled) {
          setErrorMessage(error instanceof Error ? error.message : "Unknown data error");
          setScatterData(null);
        }
      } finally {
        if (!isCancelled) {
          setIsLoadingData(false);
        }
      }
    }

    loadScatter();

    return () => {
      isCancelled = true;
    };
  }, [meta, dataset, city, yColumn]);

  const normalPoints = useMemo(
    () => scatterData?.points.filter((point) => !point.isAnomaly) ?? [],
    [scatterData],
  );

  const anomalyPoints = useMemo(
    () => scatterData?.points.filter((point) => point.isAnomaly) ?? [],
    [scatterData],
  );

  const timeRangeLabel = useMemo(() => {
    if (!scatterData || scatterData.points.length === 0) {
      return "";
    }

    const timestamps = scatterData.points.map((point) => point.x);
    const minTs = Math.min(...timestamps);
    const maxTs = Math.max(...timestamps);

    return `${formatDateTick(minTs)} to ${formatDateTick(maxTs)}`;
  }, [scatterData]);

  const timeRangeButtons = useMemo(() => {
    if (!scatterData || scatterData.points.length === 0) {
      return [] as Array<{ label: string; from: number; to: number }>;
    }

    const timestamps = scatterData.points.map((point) => point.x);
    const minTs = Math.min(...timestamps);
    const maxTs = Math.max(...timestamps);
    const dayMs = 24 * 60 * 60 * 1000;

    return [
      { label: "7D", from: Math.max(minTs, maxTs - 7 * dayMs), to: maxTs },
      { label: "30D", from: Math.max(minTs, maxTs - 30 * dayMs), to: maxTs },
      { label: "90D", from: Math.max(minTs, maxTs - 90 * dayMs), to: maxTs },
      { label: "ALL", from: minTs, to: maxTs },
    ];
  }, [scatterData]);

  const [selectedRange, setSelectedRange] = useState<{ from: number; to: number } | null>(null);

  useEffect(() => {
    if (!scatterData || scatterData.points.length === 0) {
      setSelectedRange(null);
      return;
    }

    const timestamps = scatterData.points.map((point) => point.x);
    const minTs = Math.min(...timestamps);
    const maxTs = Math.max(...timestamps);
    setSelectedRange({ from: minTs, to: maxTs });
  }, [scatterData]);

  const chartOption = useMemo(() => {
    const isDailyDataset = dataset.startsWith("daily_");
    const normalSeriesData: ChartPointDatum[] = normalPoints.map((point) => ({
      value: [point.x, point.y],
      point,
    }));
    const anomalySeriesData: ChartPointDatum[] = anomalyPoints.map((point) => ({
      value: [point.x, point.y],
      point,
    }));

    const minTs = selectedRange?.from ?? null;
    const maxTs = selectedRange?.to ?? null;

    return {
      animation: false,
      grid: { left: 58, right: 24, top: 40, bottom: 70 },
      legend: {
        top: 4,
        textStyle: { color: "#34556a" },
      },
      tooltip: {
        trigger: "item",
        borderColor: "#a5b4c7",
        backgroundColor: "#ffffff",
        textStyle: { color: "#20394a" },
        formatter: (params: unknown) => {
          const param = params as { data?: ChartPointDatum };
          const rawPoint = param.data?.point;
          if (!rawPoint) {
            return "";
          }
          if (isDailyDataset) {
            const anomalyHoursLabel = rawPoint.anomalyHours != null ? String(rawPoint.anomalyHours) : "-";
            return [
              `<b>${rawPoint.cityId}</b>`,
              `date: ${rawPoint.time}`,
              `daily value: ${Number(rawPoint.y).toFixed(3)}`,
              `anomaly hours: ${anomalyHoursLabel}`,
              `anomaly: ${rawPoint.isAnomaly ? "yes" : "no"}`,
            ].join("<br/>");
          }
          const shiftLabel = rawPoint.shiftPct != null ? `${(rawPoint.shiftPct * 100).toFixed(2)}%` : "-";
          const actualLabel = rawPoint.actualValue != null ? Number(rawPoint.actualValue).toFixed(3) : "-";
          return [
            `<b>${rawPoint.cityId}</b>`,
            `time: ${rawPoint.time}`,
            `actual: ${actualLabel}`,
            `current: ${Number(rawPoint.y).toFixed(3)}`,
            `anomaly: ${rawPoint.isAnomaly ? "yes" : "no"}`,
            `shift: ${shiftLabel}`,
          ].join("<br/>");
        },
      },
      xAxis: {
        type: "time",
        name: "time",
        nameLocation: "middle",
        nameGap: 34,
        axisLabel: {
          color: "#325163",
          formatter: (value: number) => formatDateTick(value),
        },
        min: minTs ?? undefined,
        max: maxTs ?? undefined,
      },
      yAxis: {
        type: "value",
        name: yColumn,
        nameLocation: "middle",
        nameGap: 46,
        axisLabel: { color: "#325163" },
      },
      dataZoom: [
        {
          type: "inside",
          xAxisIndex: 0,
          filterMode: "none",
          zoomOnMouseWheel: true,
          moveOnMouseMove: true,
          moveOnMouseWheel: true,
        },
        {
          type: "slider",
          xAxisIndex: 0,
          filterMode: "none",
          height: 28,
          bottom: 14,
          borderColor: "#c9d7e4",
          handleSize: "95%",
          moveHandleSize: 4,
        },
      ],
      series: [
        {
          name: "Normal",
          type: "scatter",
          symbolSize: 8,
          itemStyle: { color: "#1f8eb5" },
          data: normalSeriesData,
          large: true,
        },
        {
          name: "Synthetic Anomaly",
          type: "scatter",
          symbolSize: 9,
          itemStyle: { color: "#d7265a" },
          data: anomalySeriesData,
          large: true,
        },
      ],
    } as EChartsOption;
  }, [normalPoints, anomalyPoints, yColumn, selectedRange, dataset]);

  return (
    <div className="min-h-screen bg-grid">
      <main className="mx-auto flex w-full max-w-7xl flex-col gap-6 px-5 py-8 md:px-8 lg:px-10">
        <header className="rounded-2xl border border-white/25 bg-[linear-gradient(125deg,#00344a_0%,#0b4f6c_45%,#14919b_100%)] p-6 text-white shadow-[0_18px_40px_rgba(0,0,0,0.25)] md:p-8">
          <p className="text-xs uppercase tracking-[0.24em] text-cyan-100/90">Adaptive Data Profiling</p>
          <h1 className="mt-2 text-2xl font-semibold tracking-tight md:text-4xl">S3 Weather Observatory</h1>
          <p className="mt-3 max-w-3xl text-sm text-cyan-50/90 md:text-base">
            Explore raw hourly weather data from S3 and daily aggregates from MotherDuck city views.
            Synthetic anomalies are highlighted in crimson so you can compare normal behavior versus injected outliers.
          </p>
        </header>

        <section className="rounded-2xl border border-slate-200/80 bg-white/90 p-4 shadow-sm backdrop-blur md:p-6">
          <div className="flex items-end justify-between gap-4">
          <div className="grid grid-cols-1 gap-3 md:grid-cols-2 xl:grid-cols-4 flex-1 min-w-0">
            <label className="control">
              <span>Dataset</span>
              <select value={sourceMode} onChange={(e) => setSourceMode(e.target.value as SourceMode)}>
                {Object.entries(sourceModeLabels).map(([key, label]) => (
                  <option key={key} value={key}>
                    {label}
                  </option>
                ))}
              </select>
            </label>

            {sourceMode === "daily" ? (
              <label className="control">
                <span>Daily View</span>
                <select value={dailyVariant} onChange={(e) => setDailyVariant(e.target.value as DailyVariant)}>
                  {Object.entries(dailyVariantLabels).map(([key, label]) => (
                    <option key={key} value={key}>
                      {label}
                    </option>
                  ))}
                </select>
              </label>
            ) : null}

            <label className="control">
              <span>City</span>
              <select value={city} onChange={(e) => setCity(e.target.value)} disabled={!meta}>
                {(meta?.cities ?? []).map((cityOption) => (
                  <option key={cityOption} value={cityOption}>
                    {cityOption}
                  </option>
                ))}
              </select>
            </label>

            <label className="control">
              <span>Y Axis</span>
              <select value={yColumn} onChange={(e) => setYColumn(e.target.value)} disabled={!meta}>
                {(meta?.numericColumns ?? []).map((column) => (
                  <option key={column} value={column}>
                    {column}
                  </option>
                ))}
              </select>
            </label>

          </div>
            <a
              href="/experiments"
              className="shrink-0 self-end rounded-xl border border-indigo-200 bg-indigo-50 px-4 py-2.5 text-sm font-medium text-indigo-800 hover:bg-indigo-100 whitespace-nowrap"
            >
              Experiment Mode →
            </a>
          </div>
        </section>

        {errorMessage ? (
          <section className="rounded-2xl border border-rose-200 bg-rose-50 px-4 py-3 text-sm text-rose-800">
            {errorMessage}
          </section>
        ) : null}

        <section className="grid grid-cols-1 gap-4 md:grid-cols-2 xl:grid-cols-4">
          <StatCard label="Rows in View" value={scatterData?.rowCount.toLocaleString() ?? "-"} tone="ocean" />
          <StatCard
            label="Anomalies (Selected Y)"
            value={scatterData?.anomalyCount.toLocaleString() ?? "-"}
            tone="rose"
          />
          <StatCard
            label="Anomaly Rate"
            value={scatterData ? `${(scatterData.anomalyRate * 100).toFixed(2)}%` : "-"}
            tone="indigo"
          />
          <StatCard
            label="Avg Shift (Selected Y)"
            value={scatterData?.avgShiftPct != null ? `${(scatterData.avgShiftPct * 100).toFixed(2)}%` : "-"}
            tone="amber"
          />
        </section>

        <section className="rounded-2xl border border-slate-200/80 bg-white/95 p-4 shadow-sm md:p-6">
          <div className="mb-3 flex items-center justify-between">
            <h2 className="text-lg font-semibold text-slate-800">Scatter Plot</h2>
            <p className="text-xs text-slate-500">
              {datasetLabels[dataset]} · {city || "All Cities"} · X=time · Y={yColumn}
            </p>
          </div>
          {timeRangeLabel ? (
            <p className="mb-2 text-xs text-slate-500">Range: {timeRangeLabel}</p>
          ) : null}
          {timeRangeButtons.length > 0 ? (
            <div className="mb-3 flex flex-wrap gap-2">
              {timeRangeButtons.map((button) => (
                <button
                  key={button.label}
                  type="button"
                  className="rounded-md border border-slate-300 bg-slate-50 px-3 py-1 text-xs font-medium text-slate-700 hover:bg-slate-100"
                  onClick={() => setSelectedRange({ from: button.from, to: button.to })}
                >
                  {button.label}
                </button>
              ))}
            </div>
          ) : null}

          <div className="h-[420px] w-full">
            {isLoadingMeta || isLoadingData ? (
              <div className="chart-loading">Loading weather data...</div>
            ) : (
              <ReactECharts
                option={chartOption}
                style={{ height: "100%", width: "100%" }}
                notMerge={true}
                opts={{ renderer: "svg" }}
              />
            )}
          </div>
        </section>

        <section className="rounded-2xl border border-slate-200/80 bg-white/95 p-4 shadow-sm md:p-6">
          <h2 className="mb-3 text-lg font-semibold text-slate-800">Recent Anomaly Samples</h2>
          <div className="overflow-auto">
            <table className="w-full min-w-[680px] border-collapse text-sm">
              <thead>
                <tr className="border-b border-slate-200 text-left text-slate-600">
                  <th className="py-2 pr-4">Time</th>
                  <th className="py-2 pr-4">City</th>
                  <th className="py-2 pr-4">{dataset.startsWith("daily_") ? "Anomaly Hours" : "Target Column"}</th>
                  <th className="py-2 pr-4">Shift %</th>
                  <th className="py-2 pr-4">X</th>
                  <th className="py-2">Y</th>
                </tr>
              </thead>
              <tbody>
                {anomalyPoints.slice(0, 12).map((point, index) => (
                  <tr key={`${point.time}-${index}`} className="border-b border-slate-100 text-slate-700">
                    <td className="py-2 pr-4">{point.time}</td>
                    <td className="py-2 pr-4">{point.cityId}</td>
                    <td className="py-2 pr-4">
                      {dataset.startsWith("daily_") ? point.anomalyHours ?? "-" : point.targetColumn || "-"}
                    </td>
                    <td className="py-2 pr-4">{point.shiftPct != null ? `${(point.shiftPct * 100).toFixed(2)}%` : "-"}</td>
                    <td className="py-2 pr-4">{formatDateTime(point.x)}</td>
                    <td className="py-2">{point.y.toFixed(3)}</td>
                  </tr>
                ))}
                {anomalyPoints.length === 0 ? (
                  <tr>
                    <td colSpan={6} className="py-4 text-center text-slate-500">
                      No anomaly points in the current selection.
                    </td>
                  </tr>
                ) : null}
              </tbody>
            </table>
          </div>
        </section>
      </main>
    </div>
  );
}

function StatCard({
  label,
  value,
  tone,
}: {
  label: string;
  value: string;
  tone: "ocean" | "rose" | "indigo" | "amber";
}) {
  const toneClass: Record<typeof tone, string> = {
    ocean: "from-cyan-50 to-sky-100 text-sky-950 border-sky-200",
    rose: "from-rose-50 to-pink-100 text-rose-950 border-rose-200",
    indigo: "from-indigo-50 to-violet-100 text-indigo-950 border-indigo-200",
    amber: "from-amber-50 to-orange-100 text-amber-950 border-amber-200",
  };

  return (
    <article className={`rounded-2xl border bg-gradient-to-br p-4 shadow-sm ${toneClass[tone]}`}>
      <p className="text-xs uppercase tracking-[0.16em] opacity-80">{label}</p>
      <p className="mt-2 text-2xl font-semibold tracking-tight">{value}</p>
    </article>
  );
}
