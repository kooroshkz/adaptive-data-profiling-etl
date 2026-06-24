#!/usr/bin/env python3
"""Anomaly rate x shift magnitude sensitivity sweep on GB electricity demand.

A controlled robustness study built ENTIRELY on the electricity dataset. It asks:
how does the thesis's AutoML detector (Optuna + PyOD, F2 objective, contextual
univariate features) respond as we vary, independently,

  * the anomaly RATE       : fraction of rows made anomalous, and
  * the shift MAGNITUDE     : how far each injected value is moved (+/-).

For every (rate, magnitude) cell we inject fresh anomalies into the RAW data and
run the *exact same* AutoML search used in the thesis (reused verbatim from
``experiments/electricity/run_electricity_automl.py`` -> ``optimize_scope``,
which itself reuses ``experiments/automl`` for the search space and metrics).
Nothing about the detector changes between cells; only the injected data does.

Design choices (kept faithful to the main experiment, isolated for a clean sweep):
  * Injection reuses ``inject_anomalies`` with shift_min == shift_max == magnitude
    (an exact magnitude, not a band) and per_column_prob = 1.0 so BOTH columns are
    injected at every anomaly row -> clean per-column ground truth.
  * The injection seed is FIXED across cells, so at a given rate the SAME rows and
    SAME signs are used for every magnitude (a within-subjects design: only |shift|
    changes). Sign is random per anomaly (covers + and -); the +/- breakdown is
    recovered post-hoc from the signed metadata.
  * Each cell runs a univariate AutoML search per column (INDO, ITSDO), 25 trials.

Outputs (artifacts/):
    sensitivity_results.csv   one row per (column, rate, magnitude)

Usage:
    python experiments/electricity_sensitivity/run_sensitivity.py
    python experiments/electricity_sensitivity/run_sensitivity.py --rates 0.02 0.20 --mags 0.10 0.50
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import optuna
import pandas as pd

_THIS = Path(__file__).resolve().parent
_ELEC = _THIS.parent / "electricity"
sys.path.insert(0, str(_ELEC))

# Reuse the thesis electricity pipeline verbatim.
from fetch_electricity_data import fetch_demand, inject_anomalies, FEATURE_COLUMNS  # noqa: E402
from run_electricity_automl import optimize_scope  # noqa: E402

optuna.logging.set_verbosity(optuna.logging.WARNING)

INDO, ITSDO = FEATURE_COLUMNS
RAW_PATH = _THIS / "data" / "elexon_raw.parquet"

DEFAULT_RATES = [0.02, 0.05, 0.10, 0.20]          # anomaly rate (fraction of rows)
DEFAULT_MAGS = [0.10, 0.20, 0.30, 0.40, 0.50]     # exact shift magnitude (|value| fraction)
INJ_SEED = 42
OPTUNA_SEED = 42


def load_raw() -> pd.DataFrame:
    if RAW_PATH.exists():
        return pd.read_parquet(RAW_PATH)
    # Fall back to fetching if the artifact is missing.
    from datetime import timedelta
    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=int(round(6 * 30.44)))
    print(f"[fetch] raw {start} -> {end}")
    df = fetch_demand(start, end)
    RAW_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(RAW_PATH, index=False)
    return df


def sign_breakdown(injected: pd.DataFrame, predictions: pd.DataFrame, column: str) -> dict:
    """Recall split by injected sign (+ vs -), recovered from signed metadata."""
    t_ms = (pd.to_datetime(injected["time"]).astype("int64") // 10**6).to_numpy()
    signed = np.zeros(len(injected))
    for i, js in enumerate(injected["synthetic_anomaly_details_json"]):
        try:
            d = json.loads(str(js)).get(column)
        except (ValueError, TypeError):
            d = None
        if isinstance(d, dict):
            signed[i] = float(d.get("shift_pct", 0.0))
    sign_map = pd.Series(signed, index=t_ms)
    pred = predictions.set_index("time_ms")["y_pred"]
    joined = pd.DataFrame({"signed": sign_map, "y_pred": pred}).dropna()
    pos = joined[joined.signed > 0]
    neg = joined[joined.signed < 0]
    return {
        "n_pos": int(len(pos)),
        "n_neg": int(len(neg)),
        "recall_pos": float((pos.y_pred == 1).mean()) if len(pos) else np.nan,
        "recall_neg": float((neg.y_pred == 1).mean()) if len(neg) else np.nan,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rates", type=float, nargs="+", default=DEFAULT_RATES)
    ap.add_argument("--mags", type=float, nargs="+", default=DEFAULT_MAGS)
    ap.add_argument("--columns", nargs="+", default=[INDO, ITSDO])
    ap.add_argument("--n-trials", type=int, default=25)
    ap.add_argument("--inj-seed", type=int, default=INJ_SEED,
                    help="Injection seed (row selection + sign). Vary to average out draw noise.")
    ap.add_argument("--out", default=str(_THIS / "artifacts" / "sensitivity_results.csv"))
    args = ap.parse_args()

    raw = load_raw()
    print(f"[data] raw rows={len(raw):,}  cols={FEATURE_COLUMNS}")
    print(f"[grid] rates={args.rates}  mags={args.mags}  columns={args.columns}  "
          f"n_trials={args.n_trials}  -> {len(args.rates)*len(args.mags)*len(args.columns)} runs\n")

    rows = []
    for rate in args.rates:
        for mag in args.mags:
            injected = inject_anomalies(
                raw, anomaly_rate=rate, shift_min=mag, shift_max=mag,
                per_column_prob=1.0, batch_id=f"sens_r{rate}_m{mag}", seed=args.inj_seed,
            )
            for col in args.columns:
                result, _trials, preds, model_info, bench = optimize_scope(
                    col, [col], injected, n_trials=args.n_trials, seed=OPTUNA_SEED,
                )
                sb = sign_breakdown(injected, preds, col)
                row = {
                    "inj_seed": args.inj_seed,
                    "column": col,
                    "anomaly_rate": rate,
                    "shift_pct": mag,
                    "model": result.model_name,
                    "precision": round(result.precision, 4),
                    "recall": round(result.recall, 4),
                    "f1": round(result.f1, 4),
                    "f2": round(result.f2, 4),
                    "n_synthetic": bench["n_synthetic"],
                    "n_caught": bench["n_caught"],
                    "n_missed": bench["n_missed"],
                    "missed_rate": round(bench["missed_rate"], 4),
                    "n_extra_flags": bench["n_extra_flags"],
                    "extra_flag_rate": round(bench["extra_flag_rate"], 4),
                    **{k: (round(v, 4) if isinstance(v, float) else v) for k, v in sb.items()},
                }
                rows.append(row)
                short = "INDO" if col == INDO else "ITSDO"
                print(f"  rate={rate:>4.0%}  shift={mag:>4.0%}  {short:<5} "
                      f"{result.model_name:<7} F2={result.f2:.3f} R={result.recall:.3f} "
                      f"P={result.precision:.3f}  FP={bench['extra_flag_rate']:.2%}  "
                      f"R+={sb['recall_pos']:.2f} R-={sb['recall_neg']:.2f}")
            # incremental save
            out = Path(args.out)
            out.parent.mkdir(parents=True, exist_ok=True)
            pd.DataFrame(rows).to_csv(out, index=False)

    print(f"\n[done] {len(rows)} rows -> {args.out}")


if __name__ == "__main__":
    main()
