import { NextResponse } from "next/server";
import fs from "node:fs";
import path from "node:path";

const ARTIFACTS_DIR = path.resolve(process.cwd(), "../experiments/automl/artifacts");
const FORMULA_PARAMS_PATH = path.resolve(process.cwd(), "../projection/formula_params.json");

function parseCSV(content: string): Record<string, string>[] {
  const lines = content.trim().split("\n").filter(Boolean);
  if (lines.length < 2) return [];
  const headers = lines[0].split(",").map((h) => h.trim());
  return lines.slice(1).map((line) => {
    const values = line.split(",");
    return Object.fromEntries(headers.map((h, i) => [h, (values[i] ?? "").trim()]));
  });
}

/** Parse a pandas timedelta string like "0 days 00:00:00.022380" → milliseconds */
function parseTimedeltaMs(s: string): number {
  if (!s) return 0;
  const dayMatch = s.match(/(\d+)\s+days?\s+([\d:]+(?:\.\d+)?)/);
  if (dayMatch) {
    const days = parseInt(dayMatch[1], 10);
    const [h, m, sec] = dayMatch[2].split(":").map(Number);
    return (days * 86400 + h * 3600 + m * 60 + sec) * 1000;
  }
  const timeMatch = s.match(/([\d:]+(?:\.\d+)?)/);
  if (timeMatch) {
    const [h, m, sec] = timeMatch[1].split(":").map(Number);
    return (h * 3600 + m * 60 + sec) * 1000;
  }
  return 0;
}

const KNOWN_CITIES = ["amsterdam", "london", "new_york", "paris", "tokyo"];

function cityFromTrialFile(stem: string): string {
  const rest = stem.startsWith("trials_") ? stem.slice(7) : stem;
  return KNOWN_CITIES.find((c) => rest.startsWith(c + "_")) ?? rest.split("_")[0];
}

export type FormulaParams = {
  alpha: number; beta: number; delta: number; gamma: number; epsilon: number;
  r2: number; beta_ci95: [number, number]; delta_ci95: [number, number]; gamma_ci95: [number, number];
  n_obs: number;
};

export type CityTiming = {
  city: string;
  trainTimeSec: number;
  avgMsPerModel: number;
  n_rows: number;
  msPerKRows: number;
};

export type DashboardStats = {
  latestRun: string;
  totalRuns: number;
  totalModels: number;
  avgF2: number;
  avgRecall: number;
  avgPrecision: number;
  modelDistribution: Record<string, number>;
  cityStats: { city: string; avgF2: number; avgRecall: number; n_rows: number; models: number }[];
  details: {
    city: string;
    scope: string;
    column: string;
    model: string;
    f2: number;
    precision: number;
    recall: number;
    n_rows: number;
    n_positive_true: number;
    n_positive_pred: number;
    trainTimeSec?: number;
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
  formulaParams: FormulaParams | null;
};

export async function GET() {
  try {
    if (!fs.existsSync(ARTIFACTS_DIR)) {
      return NextResponse.json({ noRuns: true });
    }

    const entries = fs.readdirSync(ARTIFACTS_DIR, { withFileTypes: true });
    const runDirs = entries
      .filter((e) => e.isDirectory() && e.name.startsWith("run_"))
      .map((e) => e.name)
      .sort((a, b) => b.localeCompare(a));

    if (runDirs.length === 0) {
      return NextResponse.json({ noRuns: true });
    }

    const latestRun = runDirs[0];
    const runPath = path.join(ARTIFACTS_DIR, latestRun);
    const summaryPath = path.join(runPath, "summary_metrics.csv");

    if (!fs.existsSync(summaryPath)) {
      return NextResponse.json({ noRuns: true });
    }

    const rawRows = parseCSV(fs.readFileSync(summaryPath, "utf8"));

    // ── Model distribution ────────────────────────────────────────────────
    const modelDist: Record<string, number> = {};
    for (const row of rawRows) {
      if (row.model_name) modelDist[row.model_name] = (modelDist[row.model_name] ?? 0) + 1;
    }

    // ── City-level performance stats ──────────────────────────────────────
    const cityMap: Record<string, { f2: number[]; recall: number[]; n_rows: number; count: number }> = {};
    for (const row of rawRows) {
      if (!cityMap[row.city]) cityMap[row.city] = { f2: [], recall: [], n_rows: 0, count: 0 };
      const f2 = parseFloat(row.f2 ?? "0");
      const recall = parseFloat(row.recall ?? "0");
      if (!isNaN(f2)) cityMap[row.city].f2.push(f2);
      if (!isNaN(recall)) cityMap[row.city].recall.push(recall);
      cityMap[row.city].n_rows = Math.max(cityMap[row.city].n_rows, parseInt(row.n_rows ?? "0", 10));
      cityMap[row.city].count += 1;
    }

    const avg = (arr: number[]) => arr.length > 0 ? arr.reduce((a, b) => a + b, 0) / arr.length : 0;

    const cityStats = Object.entries(cityMap)
      .map(([city, d]) => ({
        city,
        avgF2: avg(d.f2),
        avgRecall: avg(d.recall),
        n_rows: d.n_rows,
        models: d.count,
      }))
      .sort((a, b) => b.avgF2 - a.avgF2);

    // ── Real timing from trials CSVs ──────────────────────────────────────
    const cityTimingMap: Record<string, { totalMs: number; models: number; n_rows: number }> = {};
    // map city→key→trainTimeSec for detail rows
    const modelTimingMap: Record<string, number> = {};
    let totalTrialsActual = 0;

    const trialFiles = fs.readdirSync(runPath).filter((f) => f.startsWith("trials_") && f.endsWith(".csv"));
    for (const fname of trialFiles) {
      const stem = fname.replace(".csv", "");
      const city = cityFromTrialFile(stem);
      const rows = parseCSV(fs.readFileSync(path.join(runPath, fname), "utf8"));
      const completed = rows.filter((r) => r.state === "COMPLETE");
      totalTrialsActual += completed.length;
      const totalMs = completed.reduce((s, r) => s + parseTimedeltaMs(r.duration), 0);

      const cityN = cityMap[city]?.n_rows ?? 0;
      if (!cityTimingMap[city]) cityTimingMap[city] = { totalMs: 0, models: 0, n_rows: cityN };
      cityTimingMap[city].totalMs += totalMs;
      cityTimingMap[city].models += 1;

      // map for detail rows: key = city:scope:column
      const rest = stem.startsWith("trials_") ? stem.slice(7 + city.length + 1) : "";
      const [scope, ...colParts] = rest.split("_");
      const col = colParts.join("_");
      modelTimingMap[`${city}:${scope}:${col}`] = totalMs / 1000;
    }

    const cityTiming: CityTiming[] = Object.entries(cityTimingMap).map(([city, d]) => ({
      city,
      trainTimeSec: d.totalMs / 1000,
      avgMsPerModel: d.models > 0 ? d.totalMs / d.models : 0,
      n_rows: d.n_rows,
      msPerKRows: d.n_rows > 0 ? d.totalMs / (d.n_rows / 1000) : 0,
    })).sort((a, b) => a.city.localeCompare(b.city));

    const totalTrainTimeSec = cityTiming.reduce((s, c) => s + c.trainTimeSec, 0);
    const totalTrainingRows = cityTiming.reduce((s, c) => s + c.n_rows, 0);

    // ── Detail rows (with timing) ─────────────────────────────────────────
    const details = rawRows.map((row) => {
      const scope = row.scope ?? "";
      const col = row.target_column ?? "";
      const city = row.city ?? "";
      const timingKey = `${city}:${scope}:${col}`;
      return {
        city,
        scope,
        column: col,
        model: row.model_name ?? "",
        f2: parseFloat(row.f2 ?? "0"),
        precision: parseFloat(row.precision ?? "0"),
        recall: parseFloat(row.recall ?? "0"),
        n_rows: parseInt(row.n_rows ?? "0", 10),
        n_positive_true: parseInt(row.n_positive_true ?? "0", 10),
        n_positive_pred: parseInt(row.n_positive_pred ?? "0", 10),
        trainTimeSec: modelTimingMap[timingKey] ?? null,
      };
    });

    const allF2 = details.map((d) => d.f2).filter((v) => !isNaN(v));
    const allRecall = details.map((d) => d.recall).filter((v) => !isNaN(v));
    const allPrec = details.map((d) => d.precision).filter((v) => !isNaN(v));

    const cities = new Set(details.map((d) => d.city));
    const columns = new Set(details.filter((d) => d.scope === "univariate").map((d) => d.column));

    const avgMsPerModel = rawRows.length > 0 ? (totalTrainTimeSec * 1000) / rawRows.length : 0;
    const throughputRowsPerSec = totalTrainTimeSec > 0 ? totalTrainingRows / totalTrainTimeSec : 0;
    const msPerKRows = totalTrainingRows > 0 ? (totalTrainTimeSec * 1000) / (totalTrainingRows / 1000) : 0;

    // ── Formula params from projection/ ──────────────────────────────────────
    let formulaParams: FormulaParams | null = null;
    if (fs.existsSync(FORMULA_PARAMS_PATH)) {
      formulaParams = JSON.parse(fs.readFileSync(FORMULA_PARAMS_PATH, "utf8")) as FormulaParams;
    }

    const stats: DashboardStats = {
      latestRun,
      totalRuns: runDirs.length,
      totalModels: rawRows.length,
      avgF2: avg(allF2),
      avgRecall: avg(allRecall),
      avgPrecision: avg(allPrec),
      modelDistribution: modelDist,
      cityStats,
      details,
      overhead: {
        totalTrainingRows,
        totalTrialsActual,
        citiesCount: cities.size,
        columnsCount: columns.size,
        totalTrainTimeSec,
        avgTrainTimeMsPerModel: avgMsPerModel,
        throughputRowsPerSec,
        msPerKRows,
        cityTiming,
      },
      formulaParams,
    };

    return NextResponse.json(stats);
  } catch (error) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "Unknown error" },
      { status: 500 },
    );
  }
}
