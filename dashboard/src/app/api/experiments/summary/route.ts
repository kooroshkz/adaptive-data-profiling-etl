import { NextRequest, NextResponse } from "next/server";
import fs from "node:fs";
import path from "node:path";

const ARTIFACTS_DIR = path.resolve(process.cwd(), "../experiments/automl/artifacts");

export type SummaryRow = {
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
    const runId = request.nextUrl.searchParams.get("run");
    if (!runId) {
      return NextResponse.json({ error: "run parameter is required" }, { status: 400 });
    }

    // Prevent path traversal
    if (runId.includes("..") || runId.includes("/")) {
      return NextResponse.json({ error: "Invalid run id" }, { status: 400 });
    }

    const summaryPath = path.join(ARTIFACTS_DIR, runId, "summary_metrics.csv");
    if (!fs.existsSync(summaryPath)) {
      return NextResponse.json({ error: "Run not found" }, { status: 404 });
    }

    const content = fs.readFileSync(summaryPath, "utf8");
    const rawRows = parseCSV(content);

    const rows: SummaryRow[] = rawRows.map((r) => ({
      city: r.city ?? "",
      scope: r.scope ?? "",
      model_name: r.model_name ?? "",
      target_column: r.target_column ?? "",
      precision: parseFloat(r.precision ?? "0"),
      recall: parseFloat(r.recall ?? "0"),
      f1: parseFloat(r.f1 ?? "0"),
      f2: parseFloat(r.f2 ?? "0"),
      n_rows: parseInt(r.n_rows ?? "0", 10),
      n_positive_true: parseInt(r.n_positive_true ?? "0", 10),
      n_positive_pred: parseInt(r.n_positive_pred ?? "0", 10),
    }));

    return NextResponse.json({ rows });
  } catch (error) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "Unknown error" },
      { status: 500 },
    );
  }
}
