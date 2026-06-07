#!/usr/bin/env python3
"""One-time fetch of GB electricity demand (INDO + ITSDO) with injected anomalies.

Mirrors the weather ingestion pattern (``airflow/scripts/weather_ingest.py``) but
for the thesis's secondary domain: **national electricity demand** from the
Elexon BMRS Insights API (no API key required).

Two demand columns are fetched, both half-hourly:

  * ``initialDemandOutturn``                     (INDO  — national demand met by
                                                   the transmission system, MW)
  * ``initialTransmissionSystemDemandOutturn``   (ITSDO — INDO plus station load
                                                   and interconnector exports, MW)

Just like the weather pipeline, synthetic anomalies are injected **at ingestion
time** so the downstream AutoML experiment has a known ground truth to score
against. Per the task brief, every injected anomaly is shifted by a random
magnitude drawn uniformly from **50 %–100 %** of the original value (direction is
random). The exact per-row, per-column shift is tracked in metadata columns that
are identical in spirit to the weather schema, so the existing experiment /
dashboard machinery works unchanged.

Output is written **locally** (not S3) as a single Parquet file:

    experiments/electricity/data/elexon_demand.parquet

Usage::

    python experiments/electricity/fetch_electricity_data.py
    python experiments/electricity/fetch_electricity_data.py --months 5 --anomaly-rate 0.04
"""

from __future__ import annotations

import argparse
import json
import random
import urllib.request
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import List, Optional

import pandas as pd

# ── Constants ────────────────────────────────────────────────────────────────
ELEXON_BASE = "https://data.elexon.co.uk/bmrs/api/v1"
PARTITION_ID = "GB"  # single national series == one "city"-like partition
FEATURE_COLUMNS = ["initialDemandOutturn", "initialTransmissionSystemDemandOutturn"]

# /demand/outturn rejects ranges >= ~28 days, so pull history in safe chunks.
CHUNK_DAYS = 14
HTTP_TIMEOUT = 60
UA = {"User-Agent": "adaptive-profiling-thesis-electricity/1.0 (research)"}

_THIS_DIR = Path(__file__).resolve().parent
_DEFAULT_OUT = _THIS_DIR / "data" / "elexon_demand.parquet"


# ── HTTP helpers ─────────────────────────────────────────────────────────────
def _get_json(url: str):
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as resp:
        return json.loads(resp.read().decode("utf-8", "replace"))


def _date_chunks(start: date, end: date, days: int):
    cur = start
    while cur < end:
        nxt = min(cur + timedelta(days=days), end)
        yield cur, nxt
        cur = nxt


# ── Fetch ────────────────────────────────────────────────────────────────────
def fetch_demand(start: date, end: date) -> pd.DataFrame:
    """Fetch half-hourly INDO + ITSDO over ``[start, end]`` from Elexon."""
    rows: List[dict] = []
    for c_start, c_end in _date_chunks(start, end, CHUNK_DAYS):
        url = (
            f"{ELEXON_BASE}/demand/outturn"
            f"?settlementDateFrom={c_start.isoformat()}"
            f"&settlementDateTo={c_end.isoformat()}&format=json"
        )
        data = _get_json(url)
        chunk = data.get("data", data if isinstance(data, list) else [])
        rows.extend(chunk)
        print(f"   {c_start} -> {c_end}: {len(chunk)} rows")

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df["settlementDate"] = pd.to_datetime(df["settlementDate"])
    # Settlement period 1..48 -> minutes offset within the day.
    df["time"] = df["settlementDate"] + pd.to_timedelta(
        (df["settlementPeriod"].astype(int) - 1) * 30, unit="m"
    )
    df["city_id"] = PARTITION_ID
    df = (
        df[["time", "city_id", *FEATURE_COLUMNS]]
        .dropna(subset=["time"])
        .drop_duplicates("time")
        .sort_values("time")
        .reset_index(drop=True)
    )
    for col in FEATURE_COLUMNS:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype(float)
    return df


# ── Synthetic anomaly injection (mirrors weather_ingest._apply_synthetic_anomalies) ──
def inject_anomalies(
    df: pd.DataFrame,
    anomaly_rate: float,
    shift_min: float,
    shift_max: float,
    per_column_prob: float,
    batch_id: str,
    seed: int,
) -> pd.DataFrame:
    """Inject point anomalies and track exact per-column shift metadata.

    Each selected row has each feature column independently shifted with
    probability ``per_column_prob``; if no column was picked, one is forced so
    the row is a genuine anomaly. The shift magnitude is drawn uniformly from
    ``[shift_min, shift_max]`` (the task's 20 %–50 % band) with random sign.
    """
    random.seed(seed)
    result = df.copy()

    # Metadata columns (same names as the weather schema for downstream reuse).
    unchanged = json.dumps({c: None for c in FEATURE_COLUMNS}, ensure_ascii=True)
    result["synthetic_anomaly_flag"] = False
    result["synthetic_shift_pct"] = 0.0
    result["synthetic_anomaly_target_column"] = ""
    result["synthetic_original_value"] = pd.NA
    result["synthetic_anomaly_batch_id"] = ""
    result["synthetic_anomaly_details_json"] = unchanged
    result["y_true"] = 0

    if result.empty or anomaly_rate <= 0:
        return result

    total = len(result)
    target_count = max(1, int(round(total * anomaly_rate)))
    anomaly_indices = set(random.sample(range(total), min(total, target_count)))

    col_loc = {c: result.columns.get_loc(c) for c in result.columns}

    for idx in anomaly_indices:
        details = {c: None for c in FEATURE_COLUMNS}
        changed: List[str] = []
        primary_shift: Optional[float] = None
        primary_original: Optional[float] = None

        for col in FEATURE_COLUMNS:
            if random.random() > per_column_prob:
                continue
            original = result.iat[idx, col_loc[col]]
            if pd.isna(original):
                continue
            signed = random.choice([-1.0, 1.0]) * random.uniform(shift_min, shift_max)
            result.iat[idx, col_loc[col]] = float(original) * (1.0 + signed)
            details[col] = {"actual": float(original), "shift_pct": signed}
            changed.append(col)
            if primary_shift is None:
                primary_shift, primary_original = signed, float(original)

        if not changed:
            forced = random.choice(FEATURE_COLUMNS)
            original = result.iat[idx, col_loc[forced]]
            if pd.isna(original):
                continue
            signed = random.choice([-1.0, 1.0]) * random.uniform(shift_min, shift_max)
            result.iat[idx, col_loc[forced]] = float(original) * (1.0 + signed)
            details[forced] = {"actual": float(original), "shift_pct": signed}
            changed.append(forced)
            primary_shift, primary_original = signed, float(original)

        result.iat[idx, col_loc["synthetic_anomaly_flag"]] = True
        result.iat[idx, col_loc["synthetic_shift_pct"]] = float(primary_shift)
        result.iat[idx, col_loc["synthetic_anomaly_target_column"]] = changed[0]
        result.iat[idx, col_loc["synthetic_original_value"]] = float(primary_original)
        result.iat[idx, col_loc["synthetic_anomaly_batch_id"]] = batch_id
        result.iat[idx, col_loc["synthetic_anomaly_details_json"]] = json.dumps(
            details, ensure_ascii=True, sort_keys=True
        )
        result.iat[idx, col_loc["y_true"]] = 1

    applied = int(result["synthetic_anomaly_flag"].sum())
    mean_shift = (
        float(result.loc[result["synthetic_anomaly_flag"], "synthetic_shift_pct"].abs().mean())
        if applied
        else 0.0
    )
    print(
        f"   Injected anomalies: {applied}/{total} rows "
        f"({applied / total * 100:.2f}%), mean |shift|={mean_shift * 100:.1f}%"
    )
    return result


# ── CLI ──────────────────────────────────────────────────────────────────────
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--months", type=int, default=5, help="History window length in months (default 5).")
    p.add_argument("--end-date", default=None, help="End date YYYY-MM-DD (default: today UTC).")
    p.add_argument("--anomaly-rate", type=float, default=0.02, help="Fraction of rows made anomalous (default 0.02).")
    p.add_argument("--shift-min", type=float, default=0.50, help="Min anomaly shift magnitude (default 0.50 = 50%%).")
    p.add_argument("--shift-max", type=float, default=1.00, help="Max anomaly shift magnitude (default 1.00 = 100%%).")
    p.add_argument("--per-column-prob", type=float, default=0.5, help="Per-column injection probability (default 0.5).")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--out", default=str(_DEFAULT_OUT), help="Output parquet path.")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    if not 0.0 <= args.anomaly_rate <= 1.0:
        raise SystemExit("--anomaly-rate must be between 0 and 1")
    if not 0.0 <= args.shift_min <= args.shift_max:
        raise SystemExit("--shift-min must be >= 0 and <= --shift-max")

    end = (
        datetime.strptime(args.end_date, "%Y-%m-%d").date()
        if args.end_date
        else datetime.now(timezone.utc).date()
    )
    start = end - timedelta(days=int(round(args.months * 30.44)))
    batch_id = datetime.now(timezone.utc).strftime("elec_%Y%m%d_%H%M%S")

    print(f"[INFO] Source     : Elexon BMRS  {ELEXON_BASE}/demand/outturn  (no key)")
    print(f"[INFO] Window     : {start} -> {end}  (~{args.months} months)")
    print(f"[INFO] Columns    : {FEATURE_COLUMNS}")
    print(f"[INFO] Anomalies  : rate={args.anomaly_rate:.0%}, shift U[{args.shift_min:.0%},{args.shift_max:.0%}]")
    print()

    print("[STEP] Fetching half-hourly demand…")
    df = fetch_demand(start, end)
    if df.empty:
        raise SystemExit("No data returned from Elexon.")
    print(f"   Total: {len(df):,} half-hourly rows")

    print("\n[STEP] Injecting synthetic anomalies…")
    df = inject_anomalies(
        df,
        anomaly_rate=args.anomaly_rate,
        shift_min=args.shift_min,
        shift_max=args.shift_max,
        per_column_prob=args.per_column_prob,
        batch_id=batch_id,
        seed=args.seed,
    )

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_path, index=False)
    print(f"\n[DONE] Wrote {len(df):,} rows -> {out_path}")
    print("       Next: python experiments/electricity/run_electricity_automl.py")


if __name__ == "__main__":
    main()
