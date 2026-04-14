import { NextRequest, NextResponse } from "next/server";
import { runS3Query } from "@/lib/pythonRunner";

export async function GET(request: NextRequest) {
  try {
    const dataset = request.nextUrl.searchParams.get("dataset") || "raw_hourly";

    const output = await runS3Query([
      "--action",
      "metadata",
      "--dataset",
      dataset,
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
