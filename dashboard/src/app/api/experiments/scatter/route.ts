import { NextRequest, NextResponse } from "next/server";
import fs from "node:fs";
import path from "node:path";
import { artifactsDir } from "../domain";

export type ExpScatterPoint = {
  time_ms: number;
  y_value: number | null;
  original_value: number | null;
  y_true: number;
  y_pred: number;
  shift_pct: number;
};

function parseCSV(content: string): Record<string, string>[] {
  const lines = content.trim().split("\n").filter(Boolean);
  if (lines.length < 2) return [];
  const headers = lines[0].split(",").map((h) => h.trim());
  return lines.slice(1).map((line) => {
    const values = line.split(",");
    return Object.fromEntries(headers.map((h, i) => [h, (values[i] ?? "").trim()]));
  });
}

export async function GET(request: NextRequest) {
  try {
    const params = request.nextUrl.searchParams;
    const runId = params.get("run");
    const city = params.get("city");
    const scope = params.get("scope") ?? "univariate";
    const column = params.get("column");

    if (!runId || !city || !column) {
      return NextResponse.json({ error: "run, city, and column are required" }, { status: 400 });
    }

    if ([runId, city, scope, column].some((v) => v?.includes("..") || v?.includes("/"))) {
      return NextResponse.json({ error: "Invalid parameter" }, { status: 400 });
    }

    const ARTIFACTS_DIR = artifactsDir(params.get("domain"));
    const filename = `predictions_${city}_${scope}_${column}.csv`;
    const filePath = path.join(ARTIFACTS_DIR, runId, filename);

    if (!fs.existsSync(filePath)) {
      return NextResponse.json({ points: [] });
    }

    const content = fs.readFileSync(filePath, "utf8");
    const rawRows = parseCSV(content);

    const hasYValue = rawRows.length > 0 && "y_value" in rawRows[0];
    const hasShiftPct = rawRows.length > 0 && "shift_pct" in rawRows[0];
    const hasOriginal = rawRows.length > 0 && "original_value" in rawRows[0];

    // Deduplicate on time_ms — overlapping parquet files from older runs may
    // have produced multiple identical rows. Last writer wins (same values anyway).
    const seen = new Map<number, ExpScatterPoint>();
    for (const r of rawRows) {
      const time_ms = parseInt(r.time_ms, 10);
      if (isNaN(time_ms)) continue;
      const origRaw = hasOriginal ? r.original_value : undefined;
      seen.set(time_ms, {
        time_ms,
        y_value: hasYValue && r.y_value ? parseFloat(r.y_value) : null,
        original_value: origRaw && origRaw !== "" && origRaw !== "nan" && origRaw !== "NaN"
          ? parseFloat(origRaw) : null,
        y_true: parseInt(r.y_true ?? "0", 10),
        y_pred: parseInt(r.y_pred ?? "0", 10),
        shift_pct: hasShiftPct && r.shift_pct ? parseFloat(r.shift_pct) : 0,
      });
    }

    const points = Array.from(seen.values()).sort((a, b) => a.time_ms - b.time_ms);
    return NextResponse.json({ points, hasYValue });
  } catch (error) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "Unknown error" },
      { status: 500 },
    );
  }
}
