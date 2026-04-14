import { NextRequest, NextResponse } from "next/server";
import { runS3Query } from "@/lib/pythonRunner";

export async function GET(request: NextRequest) {
  try {
    const params = request.nextUrl.searchParams;

    const dataset = params.get("dataset") || "raw_hourly";
    const city = params.get("city") || "";
    const yColumn = params.get("yColumn") || "precipitation";
    const limit = params.get("limit") || "0";

    const output = await runS3Query([
      "--action",
      "scatter",
      "--dataset",
      dataset,
      "--city",
      city,
      "--y-column",
      yColumn,
      "--limit",
      limit,
    ]);

    const payload = JSON.parse(output);
    return NextResponse.json(payload);
  } catch (error) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "Unknown error" },
      { status: 500 },
    );
  }
}
