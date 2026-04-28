import { NextRequest, NextResponse } from "next/server";
import fs from "node:fs";
import path from "node:path";

const ARTIFACTS_DIR = path.resolve(process.cwd(), "../experiments/automl/artifacts");

export type PredictionPoint = {
  time_ms: number | null;
  y_true: number;
  y_pred: number;
  is_correct: number;
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

    // Prevent path traversal
    if ([runId, city, scope, column].some((v) => v?.includes("..") || v?.includes("/"))) {
      return NextResponse.json({ error: "Invalid parameter" }, { status: 400 });
    }

    const filename = `predictions_${city}_${scope}_${column}.csv`;
    const filePath = path.join(ARTIFACTS_DIR, runId, filename);

    if (!fs.existsSync(filePath)) {
      return NextResponse.json({ predictions: [], hasTimestamps: false });
    }

    const content = fs.readFileSync(filePath, "utf8");
    const rawRows = parseCSV(content);

    const hasTimestamps = rawRows.length > 0 && "time_ms" in rawRows[0];

    const predictions: PredictionPoint[] = rawRows.map((r) => ({
      time_ms: hasTimestamps && r.time_ms ? parseInt(r.time_ms, 10) : null,
      y_true: parseInt(r.y_true ?? "0", 10),
      y_pred: parseInt(r.y_pred ?? "0", 10),
      is_correct: parseInt(r.is_correct ?? "0", 10),
    }));

    return NextResponse.json({ predictions, hasTimestamps });
  } catch (error) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "Unknown error" },
      { status: 500 },
    );
  }
}
