import { NextRequest, NextResponse } from "next/server";
import fs from "node:fs";
import path from "node:path";
import { artifactsDir } from "../domain";

/**
 * Derives the city/partition list and the numeric (univariate) column list for
 * a given experiment run directly from its summary_metrics.csv. This lets the
 * dashboard populate the City and Column selectors for any domain without a
 * domain-specific metadata endpoint.
 */

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
    if (runId.includes("..") || runId.includes("/")) {
      return NextResponse.json({ error: "Invalid run id" }, { status: 400 });
    }

    const ARTIFACTS_DIR = artifactsDir(request.nextUrl.searchParams.get("domain"));
    const summaryPath = path.join(ARTIFACTS_DIR, runId, "summary_metrics.csv");
    if (!fs.existsSync(summaryPath)) {
      return NextResponse.json({ cities: [], numericColumns: [] });
    }

    const rows = parseCSV(fs.readFileSync(summaryPath, "utf8"));
    const cities = Array.from(new Set(rows.map((r) => r.city).filter(Boolean)));
    const numericColumns = Array.from(
      new Set(
        rows
          .filter((r) => r.scope === "univariate")
          .map((r) => r.target_column)
          .filter((c) => c && c !== "ALL_FEATURES"),
      ),
    );

    return NextResponse.json({ cities, numericColumns });
  } catch (error) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "Unknown error" },
      { status: 500 },
    );
  }
}
