import path from "node:path";

/**
 * Domain → experiment artifacts directory.
 *
 * Both domains write the same artifact layout (summary_metrics.csv,
 * predictions_<city>_<scope>_<column>.csv, best_models.json), so the same
 * routes can serve either one based on the `domain` query parameter.
 */
export type ExperimentDomain = "weather" | "electricity";

const ARTIFACT_DIRS: Record<ExperimentDomain, string> = {
  weather: path.resolve(process.cwd(), "../experiments/automl/artifacts"),
  electricity: path.resolve(process.cwd(), "../experiments/electricity/artifacts"),
};

export function resolveDomain(value: string | null | undefined): ExperimentDomain {
  return value === "electricity" ? "electricity" : "weather";
}

export function artifactsDir(value: string | null | undefined): string {
  return ARTIFACT_DIRS[resolveDomain(value)];
}
