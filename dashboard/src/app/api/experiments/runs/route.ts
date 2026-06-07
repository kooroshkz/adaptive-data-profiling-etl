import { NextRequest, NextResponse } from "next/server";
import fs from "node:fs";
import { artifactsDir } from "../domain";

export type RunInfo = {
  id: string;
  label: string;
};

export async function GET(request: NextRequest) {
  try {
    const ARTIFACTS_DIR = artifactsDir(request.nextUrl.searchParams.get("domain"));
    if (!fs.existsSync(ARTIFACTS_DIR)) {
      return NextResponse.json({ runs: [] });
    }

    const entries = fs.readdirSync(ARTIFACTS_DIR, { withFileTypes: true });
    const runs: RunInfo[] = entries
      .filter((e) => e.isDirectory() && e.name.startsWith("run_"))
      .map((e) => {
        // run_20260416_161328 → "2026-04-16 16:13:28"
        const match = e.name.match(/^run_(\d{4})(\d{2})(\d{2})_(\d{2})(\d{2})(\d{2})$/);
        const label = match
          ? `${match[1]}-${match[2]}-${match[3]} ${match[4]}:${match[5]}:${match[6]}`
          : e.name;
        return { id: e.name, label };
      })
      .sort((a, b) => b.id.localeCompare(a.id));

    return NextResponse.json({ runs });
  } catch (error) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "Unknown error" },
      { status: 500 },
    );
  }
}
