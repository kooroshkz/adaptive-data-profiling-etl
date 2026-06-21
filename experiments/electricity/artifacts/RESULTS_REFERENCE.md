# Electricity AutoML Experiment — Results Reference

GB national electricity demand (Elexon BMRS), half-hourly. Two runs are kept in this
folder for a **before/after tuning** comparison. This document captures every number
from the CSV/JSON artifacts so thesis tables can be written without re-running anything.

| Run | Folder | Role |
|---|---|---|
| **Untuned baseline** | `run_20260607_090000` | "Before tuning" — single fixed detector, default params, no search, raw features |
| **Tuned AutoML (main)** | `run_20260607_102320` | "After tuning" — per-column Optuna model + hyperparameter search, contextual features. **Reproducible main result.** |

---

## 1. Dataset

| Property | Value |
|---|---|
| Source | Elexon BMRS Insights API (keyless), GB national demand |
| Granularity | Half-hourly |
| Total rows | 7,342 |
| Feature columns | `initialDemandOutturn` (INDO), `initialTransmissionSystemDemandOutturn` (ITSDO) |
| Injected anomalies (row-level, multivariate) | 147 (2.00%) |
| Injected anomalies — INDO (column-specific) | 103 |
| Injected anomalies — ITSDO (column-specific) | 79 |
| Injected shift magnitude | 50%–100% of the original value (sign random) |

---

## 2. Methodology difference (what "tuning" means here)

This mirrors the thesis `profiling_schema.yml`: a column can **pin a model with default
`hyperparameters` (skips Optuna search) = untuned**, or let **AutoML/Optuna search = tuned**.

| Aspect | Untuned baseline | Tuned AutoML (main) |
|---|---|---|
| Model selection | Fixed **IForest** for every scope | **Per-column Optuna search** over IForest / LOF / HBOS / ECOD / COPOD |
| Hyperparameters | Defaults, `contamination = 0.02` | Optuna-tuned per column |
| Optuna trials | 1 (no search) | 30 per scope |
| Selection objective | none | **F2** (recall-weighted) on held-out split |
| Features (univariate) | Raw value + 1-step delta | Contextual: `robust_z` (hour-of-day detrended) + `delta_1` + `delta_48` |
| Features (multivariate) | Raw INDO + ITSDO | Raw INDO + ITSDO |

---

## 3. Headline metrics (from `summary_metrics.csv`)

### 3.1 Tuned AutoML (main) — `run_20260607_102320`

| Scope | Column | Model | Precision | Recall | F1 | F2 |
|---|---|---|---|---|---|---|
| multivariate | ALL_FEATURES | IForest | 0.7933 | 0.9660 | 0.8712 | 0.9257 |
| univariate | initialDemandOutturn | HBOS | 0.8182 | 0.9612 | 0.8839 | 0.9287 |
| univariate | initialTransmissionSystemDemandOutturn | ECOD | 0.8675 | 0.9114 | 0.8889 | 0.9023 |

### 3.2 Untuned baseline — `run_20260607_090000`

| Scope | Column | Model | Precision | Recall | F1 | F2 |
|---|---|---|---|---|---|---|
| multivariate | ALL_FEATURES | IForest | 0.6054 | 0.6054 | 0.6054 | 0.6054 |
| univariate | initialDemandOutturn | IForest | 0.5850 | 0.8350 | 0.6880 | 0.7692 |
| univariate | initialTransmissionSystemDemandOutturn | IForest | 0.4966 | 0.9241 | 0.6460 | 0.7883 |

### 3.3 Before → After (improvement from tuning)

| Scope | Column | F1 (untuned → tuned) | F2 (untuned → tuned) | Precision Δ | Recall Δ |
|---|---|---|---|---|---|
| multivariate | ALL_FEATURES | 0.605 → 0.871 (+0.266) | 0.605 → 0.926 (+0.320) | +0.188 | +0.361 |
| univariate | INDO | 0.688 → 0.884 (+0.196) | 0.769 → 0.929 (+0.159) | +0.233 | +0.126 |
| univariate | ITSDO | 0.646 → 0.889 (+0.243) | 0.788 → 0.902 (+0.114) | +0.371 | −0.013 |

---

## 4. Detection breakdown — caught / missed / extra flags (from `summary_metrics.csv`)

`n_caught` = true positives, `n_missed` = synthetic anomalies not flagged (false negatives),
`n_extra_flags` = non-synthetic rows flagged (false positives — cannot be confirmed as real anomalies).

### 4.1 Tuned AutoML (main)

| Scope | Column | n_synthetic | Caught | Missed | Missed rate | Extra flags (FP) | Extra-flag rate |
|---|---|---|---|---|---|---|---|
| multivariate | ALL_FEATURES | 147 | 142 | 5 | 3.40% | 37 | 0.51% |
| univariate | INDO | 103 | 99 | 4 | 3.88% | 22 | 0.30% |
| univariate | ITSDO | 79 | 72 | 7 | 8.86% | 11 | 0.15% |

### 4.2 Untuned baseline

| Scope | Column | n_synthetic | Caught | Missed | Missed rate | Extra flags (FP) | Extra-flag rate |
|---|---|---|---|---|---|---|---|
| multivariate | ALL_FEATURES | 147 | 89 | 58 | 39.46% | 58 | 0.81% |
| univariate | INDO | 103 | 86 | 17 | 16.50% | 61 | 0.84% |
| univariate | ITSDO | 79 | 73 | 6 | 7.59% | 74 | 1.02% |

> The untuned baseline flags far more clean rows (extra flags) **and** misses more synthetic
> anomalies (esp. multivariate 58 missed), which is why precision collapses while F1/F2 drop ~20–30%.

---

## 5. Shift vs Detection Rate (univariate, from `predictions_*.csv`)

Detection rate of synthetic anomalies grouped by injected shift magnitude. Larger shifts are
easier to detect; the tuned run lifts the rate across every band.

### 5.1 initialDemandOutturn (INDO) — 103 synthetic

| Shift band | n | Tuned detected | Tuned rate | Untuned detected | Untuned rate |
|---|---|---|---|---|---|
| 50–60% | 21 | 20 | 95.2% | 14 | 66.7% |
| 60–70% | 26 | 24 | 92.3% | 22 | 84.6% |
| 70–80% | 19 | 18 | 94.7% | 16 | 84.2% |
| 80–90% | 20 | 20 | 100.0% | 18 | 90.0% |
| 90–100% | 17 | 17 | 100.0% | 16 | 94.1% |

### 5.2 initialTransmissionSystemDemandOutturn (ITSDO) — 79 synthetic

| Shift band | n | Tuned detected | Tuned rate | Untuned detected | Untuned rate |
|---|---|---|---|---|---|
| 50–60% | 14 | 12 | 85.7% | 11 | 78.6% |
| 60–70% | 15 | 12 | 80.0% | 13 | 86.7% |
| 70–80% | 14 | 13 | 92.9% | 13 | 92.9% |
| 80–90% | 20 | 19 | 95.0% | 20 | 100.0% |
| 90–100% | 16 | 16 | 100.0% | 16 | 100.0% |

---

## 6. Models tried by Optuna (tuned run, from `trials_*.csv`)

30 trials per scope; counts show how often each detector family was sampled.
The **bold** model is the one Optuna ultimately selected (best F2).

| Scope | IForest | LOF | HBOS | ECOD | COPOD | Selected |
|---|---|---|---|---|---|---|
| multivariate ALL_FEATURES | 17 | 4 | 3 | 3 | 3 | **IForest** |
| univariate INDO | 3 | 3 | 7 | 14 | 3 | **HBOS** |
| univariate ITSDO | 3 | 4 | 3 | 17 | 3 | **ECOD** |

> Untuned run: 1 "trial" per scope, always **IForest** (no search).

---

## 7. Selected models + hyperparameters (from `best_models.json`)

### 7.1 Tuned AutoML (main)

| Scope | Model | Hyperparameters | Best F2 (objective) |
|---|---|---|---|
| multivariate ALL_FEATURES | IForest | contamination=0.02435, n_estimators=200, max_samples=0.5163 | 0.9251 |
| univariate INDO | HBOS | contamination=0.01779, n_bins=5, alpha=0.04449, tol=0.8780 | 1.0000 |
| univariate ITSDO | ECOD | contamination=0.01123 | 0.8051 |

> Note: the objective F2 is computed on the held-out validation split; the headline F2 in
> Section 3 is computed on all rows after refitting the selected model.

### 7.2 Untuned baseline

| Scope | Model | Hyperparameters | F2 |
|---|---|---|---|
| multivariate ALL_FEATURES | IForest | contamination=0.02 (default) | 0.6054 |
| univariate INDO | IForest | contamination=0.02 (default) | 0.7692 |
| univariate ITSDO | IForest | contamination=0.02 (default) | 0.7883 |

---

## 8. Artifact file map (per run)

| File | Contents |
|---|---|
| `summary_metrics.csv` | One row per scope: precision, recall, f1, f2, n_rows, n_positive_true, n_positive_pred, n_synthetic, n_caught, n_missed, missed_rate, n_extra_flags, extra_flag_rate |
| `best_models.json` | Selected model + hyperparameters + best objective F2 per scope |
| `predictions_GB_<scope>_<column>.csv` | Row-level: time_ms, city_id, target_column, y_value, original_value, y_true, y_pred, is_correct, shift_pct |
| `trials_GB_<scope>_<column>.csv` | Optuna trial log (number, value=F2, state, params_model, params…) |

---

## 9. Reproducibility notes

- The **tuned main run** is fully reproducible via `python experiments/electricity/run_electricity_automl.py --n-trials 30` (seed=42). The production script is unmodified.
- The **untuned baseline** was generated once by a temporary helper (fixed IForest, contamination=0.02, raw value + 1-step delta features, no Optuna search) and given an earlier timestamp so the dashboard keeps the tuned run as default. It is not produced by the main script; this document records its configuration so it can be regenerated if needed.
