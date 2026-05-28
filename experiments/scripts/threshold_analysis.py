#!/usr/bin/env python3
"""
Post-hoc threshold analysis on scored anomaly predictions.

For each city-column the model already produced an anomaly_prob in [0,1].
The PyOD contamination parameter implicitly sets an initial threshold in
raw score space, which maps to some effective probability threshold.
By sweeping the prob threshold we can find operating points that trade
fewer false positives for a tolerable drop in recall.

Key findings reported
----------------------
1.  F2-optimal threshold: prob cutoff that maximises F2 on this data.
2.  Recall-safe threshold: highest threshold where recall >= 80% of the
    original recall (so we don't sacrifice detection badly).
3.  FP-reduced threshold: threshold that cuts FP by >= 75% while keeping
    F2 >= 50% of the F2-optimal value.
4.  Separability flag: whether FP and TP probability distributions overlap
    too much for thresholding to be useful (FP p50 >= TP p50 → "poor" separability).

These map directly to the discussion in the thesis about practical deployment.
"""

from __future__ import annotations

import glob
import json
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import mannwhitneyu

from utils.paths import AUTOML_DIR, latest_scored_dir
from utils.scoring import f2_score

N_STEPS = 200   # threshold grid resolution


# ── helpers ──────────────────────────────────────────────────────────────────

def f2(tp, fp, fn) -> float:
    return f2_score(tp, fp, fn)


def sweep(probs: np.ndarray, labels: np.ndarray, thresholds: np.ndarray) -> pd.DataFrame:
    """Return a DataFrame with P/R/F2/FP/TP at each threshold."""
    rows = []
    for t in thresholds:
        pred = (probs >= t).astype(int)
        tp_ = int(((pred == 1) & (labels == 1)).sum())
        fp_ = int(((pred == 1) & (labels == 0)).sum())
        fn_ = int(((pred == 0) & (labels == 1)).sum())
        p_  = tp_ / (tp_ + fp_) if tp_ + fp_ else 0.0
        r_  = tp_ / (tp_ + fn_) if tp_ + fn_ else 0.0
        rows.append({
            "threshold": round(float(t), 4),
            "tp": tp_, "fp": fp_, "fn": fn_,
            "precision": round(p_, 4),
            "recall":    round(r_, 4),
            "f2":        round(f2(tp_, fp_, fn_), 4),
        })
    return pd.DataFrame(rows)


def find_recommendations(curve: pd.DataFrame, orig_fp: int, orig_recall: float) -> dict:
    """Return three recommended thresholds from the sweep curve."""
    best_f2_row = curve.loc[curve["f2"].idxmax()]

    # Recall-safe: highest threshold where recall >= 80% of original recall
    recall_safe = curve[curve["recall"] >= 0.8 * orig_recall]
    recall_safe_row = recall_safe.loc[recall_safe["threshold"].idxmax()] \
        if not recall_safe.empty else best_f2_row

    # FP-reduced: threshold that cuts FP by >= 75% while F2 >= 50% of best F2
    fp_target  = orig_fp * 0.25
    f2_floor   = best_f2_row["f2"] * 0.5
    candidates = curve[(curve["fp"] <= fp_target) & (curve["f2"] >= f2_floor)]
    fp_reduced_row = candidates.loc[candidates["f2"].idxmax()] \
        if not candidates.empty else best_f2_row

    return {
        "f2_optimal":   best_f2_row.to_dict(),
        "recall_safe":  recall_safe_row.to_dict(),
        "fp_reduced":   fp_reduced_row.to_dict(),
    }


def separability(fp_probs: np.ndarray, tp_probs: np.ndarray) -> str:
    """Rate how well FP and TP distributions are separated."""
    if len(fp_probs) == 0 or len(tp_probs) == 0:
        return "n/a"
    fp_med = float(np.median(fp_probs))
    tp_med = float(np.median(tp_probs))
    overlap = fp_med / tp_med if tp_med > 0 else float("inf")
    # Mann-Whitney test (1-sided: TP > FP)
    try:
        _, pval = mannwhitneyu(tp_probs, fp_probs, alternative="greater")
    except ValueError:
        pval = 1.0
    if pval < 0.01 and overlap < 0.7:
        return "good"      # clear separation, thresholding very useful
    elif pval < 0.05 and overlap < 1.0:
        return "moderate"  # some separation, thresholding helps moderately
    else:
        return "poor"      # FP and TP overlap — threshold cannot discriminate


# ── main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    import argparse
    ap = argparse.ArgumentParser(description="Post-hoc threshold analysis on scored predictions.")
    ap.add_argument("--scored-dir", type=Path, default=None)
    ap.add_argument("--out-dir",    type=Path, default=None)
    args = ap.parse_args()

    scored_dir = args.scored_dir or latest_scored_dir()

    out_dir = args.out_dir or (scored_dir / "threshold_analysis")
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"Scored dir : {scored_dir}")
    print(f"Output dir : {out_dir}\n")

    summary_rows: list[dict] = []
    curve_frames: dict[str, pd.DataFrame] = {}

    files = sorted(scored_dir.glob("scored_*_univariate_*.csv"))

    for fpath in files:
        df = pd.read_csv(fpath)
        if df.empty:
            continue

        city  = df["city_id"].iloc[0]
        col   = df["target_column"].iloc[0]
        model = df["model_name"].iloc[0]

        probs  = df["anomaly_prob"].values
        labels = df["y_true"].values   # 1 = synthetic anomaly, 0 = normal

        # Effective threshold = min prob among currently flagged points
        flagged_probs = probs[df["y_pred"] == 1]
        orig_thresh = float(flagged_probs.min()) if len(flagged_probs) else 0.0

        # Original metrics (at current threshold)
        orig_tp = int(((df["y_pred"]==1)&(labels==1)).sum())
        orig_fp = int(((df["y_pred"]==1)&(labels==0)).sum())
        orig_fn = int(((df["y_pred"]==0)&(labels==1)).sum())
        orig_f2  = f2(orig_tp, orig_fp, orig_fn)
        orig_rec = orig_tp / (orig_tp + orig_fn) if orig_tp + orig_fn else 0.0

        # Probability distributions per label
        fp_probs = probs[df["label"] == "FP"]
        tp_probs = probs[df["label"] == "TP"]
        sep      = separability(fp_probs, tp_probs)

        # Build threshold grid over [orig_thresh*0.5 , min(0.99, max_prob)]
        t_min  = max(0.0, orig_thresh * 0.5)
        t_max  = min(0.99, float(probs.max()))
        thresholds = np.linspace(t_min, t_max, N_STEPS)
        curve = sweep(probs, labels, thresholds)

        # Save full curve
        key = f"{city}_{col}"
        curve_frames[key] = curve
        curve.to_csv(out_dir / f"curve_{key}.csv", index=False)

        recs = find_recommendations(curve, orig_fp, orig_rec)

        row = {
            "city":        city,
            "column":      col,
            "model":       model,
            "separability": sep,
            # Original (PyOD contamination threshold)
            "orig_thresh": round(orig_thresh, 4),
            "orig_tp":     orig_tp,
            "orig_fp":     orig_fp,
            "orig_fn":     orig_fn,
            "orig_f2":     round(orig_f2, 4),
            "orig_recall": round(orig_rec, 4),
            # F2-optimal threshold
            "f2opt_thresh":  recs["f2_optimal"]["threshold"],
            "f2opt_tp":      recs["f2_optimal"]["tp"],
            "f2opt_fp":      recs["f2_optimal"]["fp"],
            "f2opt_f2":      recs["f2_optimal"]["f2"],
            "f2opt_recall":  recs["f2_optimal"]["recall"],
            # Recall-safe threshold (max thresh keeping recall >= 80% of original)
            "recs_thresh":   recs["recall_safe"]["threshold"],
            "recs_tp":       recs["recall_safe"]["tp"],
            "recs_fp":       recs["recall_safe"]["fp"],
            "recs_f2":       recs["recall_safe"]["f2"],
            "recs_recall":   recs["recall_safe"]["recall"],
            # FP-reduction threshold (cuts FP >= 75% at cost of some recall)
            "fpr_thresh":    recs["fp_reduced"]["threshold"],
            "fpr_tp":        recs["fp_reduced"]["tp"],
            "fpr_fp":        recs["fp_reduced"]["fp"],
            "fpr_f2":        recs["fp_reduced"]["f2"],
            "fpr_recall":    recs["fp_reduced"]["recall"],
        }
        summary_rows.append(row)

        fp_cut = orig_fp - recs["recall_safe"]["fp"]
        print(
            f"{city:<12} {col:<36} sep={sep:<8} "
            f"orig: FP={orig_fp:4d} F2={orig_f2:.3f}  "
            f"f2-opt: thresh={recs['f2_optimal']['threshold']:.3f} FP={int(recs['f2_optimal']['fp']):4d} F2={recs['f2_optimal']['f2']:.3f}  "
            f"recall-safe: thresh={recs['recall_safe']['threshold']:.3f} FP={int(recs['recall_safe']['fp']):4d}"
        )

    summary = pd.DataFrame(summary_rows)
    summary.to_csv(out_dir / "threshold_summary.csv", index=False)

    # ── Compact recommendation table ────────────────────────────────────────
    print("\n" + "="*100)
    print("RECOMMENDATION TABLE  (recall-safe thresholds: keep recall ≥ 80% of original)")
    print("="*100)
    cols_show = ["city","column","model","separability",
                 "orig_thresh","orig_fp","orig_f2","orig_recall",
                 "recs_thresh","recs_fp","recs_f2","recs_recall"]
    print(summary[cols_show].to_string(index=False))

    # ── Columns where thresholding helps meaningfully ────────────────────────
    print("\n" + "="*100)
    print("COLUMNS WHERE RAISING THRESHOLD MEANINGFULLY REDUCES FP (separability ≠ poor, FP drop > 10)")
    print("="*100)
    helpful = summary[
        (summary["separability"] != "poor") &
        (summary["orig_fp"] - summary["recs_fp"] > 10)
    ][["city","column","orig_fp","recs_fp","orig_f2","recs_f2","recs_thresh","separability"]]
    if helpful.empty:
        print("  None found — see 'moderate' separability cases for partial gains.")
    else:
        print(helpful.to_string(index=False))

    # ── Problematic columns ──────────────────────────────────────────────────
    print("\n" + "="*100)
    print("PROBLEMATIC COLUMNS (poor separability OR F2-optimal F2 still < 0.3)")
    print("="*100)
    poor = summary[
        (summary["separability"] == "poor") | (summary["f2opt_f2"] < 0.3)
    ][["city","column","model","separability","orig_fp","orig_f2","f2opt_f2"]]
    print(poor.to_string(index=False))

    # ── Per-column aggregated summary ────────────────────────────────────────
    print("\n" + "="*100)
    print("AVERAGE RECOMMENDED THRESHOLD PER COLUMN (across all cities)")
    print("="*100)
    agg = summary.groupby("column").agg(
        separability_modes=("separability","unique"),
        mean_orig_fp=("orig_fp","mean"),
        mean_recs_fp=("recs_fp","mean"),
        mean_orig_f2=("orig_f2","mean"),
        mean_recs_f2=("recs_f2","mean"),
        mean_recs_thresh=("recs_thresh","mean"),
        mean_f2opt_thresh=("f2opt_thresh","mean"),
    ).round(3)
    print(agg.to_string())

    print(f"\nFull threshold curves saved per city-column in: {out_dir}")
    print(f"Summary CSV: {out_dir / 'threshold_summary.csv'}")


if __name__ == "__main__":
    main()
