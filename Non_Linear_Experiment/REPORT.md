# AutoML (PyOD + Optuna) vs Auto-NN: Anomaly Detection Comparison Report

This report evaluates two Optuna-driven automated anomaly detection approaches on the same unsupervised task: detecting synthetically injected anomalies in weather ETL data across five cities without the model ever seeing the anomaly labels during training.

- **AutoML**: Optuna selects among five PyOD classical algorithms — IForest, LOF, ECOD, HBOS, COPOD — and their hyperparameters (25 trials per column).
- **Auto-NN**: Optuna performs Neural Architecture Search (NAS) over Autoencoder (AE), Variational Autoencoder (VAE), and One-Class SVM (OCSVM) — their architecture, learning rate, epoch count, activation, and batch size (15 trials per column).

Both approaches share identical data, preprocessing, train/validation split (70/30), evaluation metric (F2, β=2), and the principle of fitting without any anomaly labels.

## Executive Summary

| Metric | AutoML | Auto-NN | AutoML advantage |
|--------|--------|---------|-----------------|
| Mean F2 (univariate) | **0.683** | 0.162 | +0.521 |
| Mean Recall (univariate) | **0.765** | 0.220 | +0.545 |
| Mean Precision (univariate) | **0.618** | 0.205 | +0.413 |
| Total experiment time | **145 s** | 354 s | AutoML 2.4× faster |
| F2 per second (efficiency) | **0.1652** | 0.0160 | 10.3× better |
| Univariate wins | **28/30** (93%) | 0/30 (0%) | — |

**AutoML is the clear winner.** Across 30 city×column pairs, AutoML wins 28 (93%), Auto-NN wins 0 (0%), and 2 are roughly tied. AutoML achieves this with fewer or equal compute resources.

## Detection Performance: Full Results

### Univariate Detection — F2 Score

F2-score weights recall twice as heavily as precision (β=2): missing a real anomaly costs more than a false alarm. All 76 injected anomalies per city per column were within valid rule ranges — rule-based checks detected **0%** of them.

| City | Column | AutoML model | AutoML F2 | NN model | NN F2 | Δ F2 | Winner |
|------|--------|:------------:|:---------:|:--------:|:-----:|:-----:|:------:|
| amsterdam | apparent_temperature | LOF | 0.418 | OCSVM | 0.014 | -0.404 | **AutoML** |
| amsterdam | precipitation | LOF | 0.201 | AE | 0.015 | -0.185 | **AutoML** |
| amsterdam | soil_moisture_7_to_28cm | LOF | 0.928 | OCSVM | 0.160 | -0.768 | **AutoML** |
| amsterdam | soil_temperature_7_to_28cm | LOF | 0.961 | AE | 0.039 | -0.922 | **AutoML** |
| amsterdam | surface_pressure | ECOD | 0.928 | VAE | 0.845 | -0.083 | **AutoML** |
| amsterdam | temperature_2m | LOF | 0.668 | VAE | 0.020 | -0.648 | **AutoML** |
| london | apparent_temperature | LOF | 0.855 | OCSVM | 0.010 | -0.845 | **AutoML** |
| london | precipitation | LOF | 0.115 | VAE | 0.006 | -0.108 | **AutoML** |
| london | soil_moisture_7_to_28cm | LOF | 0.985 | OCSVM | 0.081 | -0.904 | **AutoML** |
| london | soil_temperature_7_to_28cm | LOF | 0.936 | OCSVM | 0.027 | -0.909 | **AutoML** |
| london | surface_pressure | ECOD | 0.955 | OCSVM | 0.915 | -0.039 | **AutoML** |
| london | temperature_2m | LOF | 0.137 | OCSVM | 0.033 | -0.104 | **AutoML** |
| new_york | apparent_temperature | LOF | 0.768 | OCSVM | 0.010 | -0.758 | **AutoML** |
| new_york | precipitation | LOF | 0.154 | VAE | 0.015 | -0.139 | **AutoML** |
| new_york | soil_moisture_7_to_28cm | LOF | 0.997 | AE | 0.032 | -0.965 | **AutoML** |
| new_york | soil_temperature_7_to_28cm | LOF | 0.787 | VAE | 0.000 | -0.787 | **AutoML** |
| new_york | surface_pressure | ECOD | 0.934 | VAE | 0.507 | -0.427 | **AutoML** |
| new_york | temperature_2m | LOF | 0.792 | OCSVM | 0.022 | -0.770 | **AutoML** |
| paris | apparent_temperature | LOF | 0.833 | VAE | 0.016 | -0.817 | **AutoML** |
| paris | precipitation | LOF | 0.157 | AE | 0.017 | -0.140 | **AutoML** |
| paris | soil_moisture_7_to_28cm | LOF | 0.949 | OCSVM | 0.048 | -0.901 | **AutoML** |
| paris | soil_temperature_7_to_28cm | LOF | 0.941 | VAE | 0.016 | -0.924 | **AutoML** |
| paris | surface_pressure | HBOS | 0.944 | OCSVM | 0.871 | -0.073 | **AutoML** |
| paris | temperature_2m | LOF | 0.259 | AE | 0.014 | -0.244 | **AutoML** |
| tokyo | apparent_temperature | LOF | 0.898 | AE | 0.012 | -0.886 | **AutoML** |
| tokyo | precipitation | LOF | 0.130 | AE | 0.019 | -0.110 | **AutoML** |
| tokyo | soil_moisture_7_to_28cm | LOF | 0.973 | VAE | 0.032 | -0.941 | **AutoML** |
| tokyo | soil_temperature_7_to_28cm | LOF | 0.887 | AE | 0.073 | -0.814 | **AutoML** |
| tokyo | surface_pressure | ECOD | 0.968 | OCSVM | 0.960 | -0.009 | tie |
| tokyo | temperature_2m | COPOD | 0.038 | AE | 0.035 | -0.003 | tie |

### Multivariate Detection — F2 Score

| City | AutoML model | AutoML F2 | NN model | NN F2 | Δ F2 | Winner |
|------|:------------:|:---------:|:--------:|:-----:|:-----:|:------:|
| amsterdam | LOF | 0.246 | OCSVM | 0.267 | +0.020 | **NN** |
| london | LOF | 0.295 | OCSVM | 0.286 | -0.009 | tie |
| new_york | LOF | 0.403 | OCSVM | 0.292 | -0.111 | **AutoML** |
| paris | LOF | 0.378 | AE | 0.323 | -0.055 | **AutoML** |
| tokyo | LOF | 0.314 | OCSVM | 0.283 | -0.031 | **AutoML** |

### Aggregate Detection Metrics

| Scope | Metric | AutoML | Auto-NN | Δ (NN − AutoML) |
|-------|--------|:------:|:-------:|:---------------:|
| Univariate | Mean F2 | **0.683** | 0.162 | -0.521 |
| Univariate | Mean Recall | **0.765** | 0.220 | -0.545 |
| Univariate | Mean Precision | **0.618** | 0.205 | -0.413 |
| Univariate | Mean F1 | **0.632** | 0.145 | -0.487 |
| Multivariate | Mean F2 | **0.327** | 0.290 | -0.037 |
| Multivariate | Mean Recall | **0.294** | 0.270 | -0.024 |
| Multivariate | Mean Precision | **0.628** | 0.428 | -0.200 |
| Multivariate | Mean F1 | **0.397** | 0.328 | -0.069 |

### Detection by Column Type

Columns grouped by distributional character show a clear pattern:

| Column | Distribution | AutoML avg F2 | Auto-NN avg F2 | AutoML advantage | Why |
|--------|:------------:|:-------------:|:--------------:|:----------------:|-----|
| `surface_pressure` | Tight near-Gaussian | **0.946** | 0.820 | +0.126 | Most stable, narrow range — tail detectors excel |
| `soil_moisture_7_to_28cm` | Slow seasonal cycle | **0.966** | 0.071 | +0.896 | Clear density gradient, LOF finds local outliers |
| `soil_temperature_7_to_28cm` | Seasonal, moderate noise | **0.902** | 0.031 | +0.871 | Seasonal clusters give LOF strong local context |
| `apparent_temperature` | Seasonal + noisy | **0.754** | 0.012 | +0.742 | LOF adapts to local summer/winter density |
| `temperature_2m` | Highly seasonal + erratic | **0.379** | 0.025 | +0.354 | Noise blurs AE reconstruction; LOF stays local |
| `precipitation` | Heavily right-skewed, sparse | **0.151** | 0.015 | +0.137 | Hardest for both — most hours have zero rain |

## Why AutoML (PyOD) Wins: Root-Cause Analysis

The performance gap is not random — it reflects a fundamental mismatch between the data characteristics and what neural reconstruction methods assume.

### 1. LOF and Local Density: Purpose-Built for Seasonal Time-Series

Local Outlier Factor (LOF) was selected by Optuna in **83%** of AutoML runs. LOF works by comparing the density of a point to the density of its k nearest neighbours. For weather data, this is algorithmically ideal:

- **Seasonal structure creates dense clusters**: Summer days cluster in one region of feature space; winter days cluster in another. A point is only anomalous if it is sparse *relative to its own season's cluster*, not relative to the global distribution.
- **Injected anomalies are point anomalies within a season**: The shift (4–12% of range) makes a value unusual compared to its immediate neighbours — exactly what LOF measures.
- **LOF's k-NN neighbourhood acts as an implicit seasonal window**: With n_neighbors selected by Optuna in [5, 100], the algorithm naturally adapts to the density scale of each column.

An Autoencoder, by contrast, learns a *global* reconstruction mapping over all 20,000 hours of data across all seasons. A shifted summer temperature value may still reconstruct reasonably well because the AE averages over the full distribution.

### 2. ECOD and Tail Probability: Perfect for Tight Distributions

ECOD was selected for **surface pressure** across 4 of 5 cities (F2 avg = 0.946). ECOD models the empirical cumulative distribution of each feature and flags points whose tail probability falls below the contamination threshold.

Surface pressure has a very tight near-Gaussian distribution (~50 hPa total range, standard deviation ≈ 8–10 hPa). An injected 4–12% shift immediately lands in the distribution tail. ECOD finds this directly. An AE trained on this column learns to reconstruct the mean value well, but its reconstruction error is noisy and not calibrated to tail probability — it cannot match ECOD's precision on this column.

### 3. Why AE/VAE Underperform on Univariate Data

Autoencoders are powerful for high-dimensional data (images, text, audio) where compression creates a meaningful bottleneck. For **1-dimensional input**, this advantage disappears:

- **No meaningful compression**: A 1D→8→4→8→1 AE has enough capacity to memorise all 20,000 training points. The reconstruction error does not reliably distinguish anomalies from dense normal regions.
- **Reconstruction error is not calibrated to anomaly probability**: The AE minimises MSE globally. A point with slightly higher reconstruction error may simply be in a low-frequency part of the seasonal cycle, not an anomaly.
- **Epoch count drives instability**: With 20–100 epochs searched over 15 trials, many architectures either overfit (zero reconstruction error everywhere) or underfit (constant-output decoder). Both fail to set a useful threshold.
- **Evidence from the results**: Across all 5 cities, AE/VAE achieves near-zero F2 on soil moisture, soil temperature, and apparent temperature — all columns where LOF gets F2 > 0.85. The issue is structural, not a matter of more compute.

### 4. Why OCSVM's Sub-Sampling Hurts

OCSVM with RBF kernel is O(n²) at training time, so this experiment caps training to 3,000 rows. On a 20,856-row dataset this means the model sees only **14%** of the data. Rare seasonal events that only appear a few times per year may not be represented in the random sub-sample, making the learned boundary inaccurate. Surface pressure is the exception: it has a single tight mode, so 3,000 samples characterise it well. All other columns exhibit seasonal multi-modality that 3,000 random samples mis-represent.

## Computational Cost: Measured Data

All timings are measured on the same machine. AutoML total wall-clock time comes from MLflow run metadata (start_time / end_time of the parent batch run). Auto-NN total time includes full Optuna search + final refit, measured with `time.perf_counter()`. Per-algorithm AutoML benchmark was run 3x on the actual data and averaged.

### Per-Algorithm Benchmark (Single Fit + Predict, 20 k rows)

This table shows how long *one training trial* takes for each algorithm. Multiplied by Optuna's trial count gives the expected search cost.

| Algorithm | Family | Mean trial time (s) | Median (s) | × 25 trials = est. search (s) |
|-----------|:------:|:-------------------:|:----------:|:-----------------------------:|
| ECOD | PyOD classical | 0.0094 | 0.0054 | 0.235 |
| COPOD | PyOD classical | 0.0082 | 0.0052 | 0.205 |
| HBOS | PyOD classical | 0.0135 | 0.0012 | 0.338 |
| LOF | PyOD classical | 0.3111 | 0.0494 | 7.777 |
| IForest | PyOD classical | 0.3721 | 0.3954 | 9.303 |
| AE | PyTorch NN | ~0.10–1.5 (varies) | — | ~2–22 per trial |
| VAE | PyTorch NN | ~0.30–4.0 (varies) | — | ~5–60 per trial |
| OCSVM | SVM (3k sub-sample) | ~0.01–0.05 | — | ~0.2–0.8 |

### Full Experiment Wall-Clock Time

| Experiment | # Models | # Trials/model | Total time | Mean per model | Source |
|------------|:--------:|:--------------:|:----------:|:--------------:|--------|
| AutoML (PyOD+Optuna) | 35 | 25 | **144.7 s** | 4.14 s | MLflow run metadata |
| Auto-NN (AE/VAE/OCSVM) | 35 | 15 | 354.5 s | 10.13 s | `time.perf_counter()` |

**Auto-NN is 2.4× slower** than AutoML wall-to-wall, despite using fewer trials (15 vs 25). The reason: each NN trial trains a neural network for 20–100 epochs (measured: 0.63 s/trial average), while the dominant AutoML algorithm LOF takes 0.31 s/trial and ECOD/HBOS/COPOD take < 0.014 s/trial. With 35 models × 15 trials = 525 NN trials at ~0.63 s each = 331 s search total, versus 35 × 25 trials, with LOF in ~60% of trials at 0.31 s and fast models in 40%: ≈ 35 × 25 × 0.19 = 166 s search, matching the 145 s MLflow measure.

### Per-Model Timing: Auto-NN by Algorithm Type

| NN Model | Count selected | Mean search (s) | Mean refit (s) | Mean total (s) | Mean F2 | F2/s efficiency |
|----------|:--------------:|:---------------:|:--------------:|:--------------:|:-------:|:---------------:|
| AE | 10 | 10.21 | 0.92 | 11.13 | 0.058 | 0.0052 |
| OCSVM | 16 | 8.15 | 0.06 | 8.21 | 0.267 | 0.0326 |
| VAE | 9 | 10.89 | 1.53 | 12.42 | 0.162 | 0.0130 |

### Detection Efficiency: F2 per Second of Training

This metric answers: *how much detection quality do you get per unit of compute?* Higher is better.

| Approach | Mean F2 | Mean time/model (s) | F2/s | Relative efficiency |
|----------|:-------:|:-------------------:|:----:|:-------------------:|
| **AutoML** | **0.683** | **4.14** | **0.1652** | **10.3× better** |
| Auto-NN | 0.162 | 10.13 | 0.0160 | 1× (baseline) |

### MLflow Run History (All AutoML Batch Experiments)

Each row is one complete AutoML experiment run (all cities × columns). Wall time variation reflects different n_trials settings and trial timeout changes across experiment iterations. The latest run (144.7 s) used n_trials=25.

| Run name | Wall time (s) | Notes |
|----------|:-------------:|-------|
| automl_batch_20260418_142505 | 2398.1 |  |
| automl_batch_20260418_150745 | 287.0 |  |
| automl_batch_20260418_154140 | 1.1 |  |
| automl_batch_20260418_154854 | 742.7 |  |
| automl_batch_20260424_121213 | 122.1 |  |
| automl_batch_20260428_084114 | 47.3 |  |
| automl_batch_20260428_084356 | 163.6 |  |
| automl_batch_20260428_091103 | 159.1 |  |
| automl_batch_20260428_093331 | 11.2 |  |
| automl_batch_20260428_093705 | 144.7 | ← used in this comparison |

## Scaling Characteristics

An important practical question: *how does each approach scale as data grows?*

| Aspect | AutoML (LOF dominant) | Auto-NN (AE/VAE dominant) |
|--------|:---------------------:|:-------------------------:|
| Time vs rows n | n^0.395 (sub-linear) | O(n × epochs) ≈ n^1.0 |
| 2× data → time multiplier | ×1.32 | ×2.0 |
| 10× data → time multiplier | ×2.49 | ×10.0 |
| 100× data → time multiplier | ×6.2 | ×100 |
| GPU benefit | None (CPU tree/density) | ×5–50× speed-up possible |
| Labeled data benefit | None (unsupervised) | Significant (semi-supervised) |

AutoML's sub-linear scaling (β=0.395, fitted from ~90 benchmark runs in `projection/`) means it actually becomes *relatively cheaper* at larger data sizes. The NN approaches scale linearly with data (more data = more epochs needed to converge, and longer forward/backward passes). Without GPU acceleration, NN becomes the bottleneck at large scale. With a GPU, absolute NN training time drops dramatically — but AutoML still wins on quality for this task type.

### LOF Scaling Exception

LOF is O(n²) in the worst case (all n neighbours searched), which is why the thesis scaling formula reports β=1.175 for LOF alone. However, LOF with a k-d tree implementation and leaf_size hyperparameter runs much faster in practice — the benchmark measured 0.31 s/trial at 20k rows. For datasets >500k rows, IForest (β=0.307, sub-linear) becomes the recommended AutoML choice even if LOF would win on quality.

## Model Selection Patterns

### AutoML: What Optuna Chose

| Algorithm | Selected | % | Key strength |
|-----------|:--------:|:-:|-------------|
| LOF | 29 | 83% | Local density — adapts to seasonal clusters |
| ECOD | 4 | 11% | Tail probability — exact fit for tight distributions |
| HBOS | 1 | 3% | Histogram density — fast, no distance computation |
| COPOD | 1 | 3% | Copula tail probability — handles correlated features |

### Auto-NN: What Optuna Chose

| Model | Selected | % | Observation |
|-------|:--------:|:-:|-------------|
| OCSVM | 16 | 46% | Fast (sub-sampled) — Optuna prefers it when NNs fail to find good F2 in budget |
| AE | 10 | 29% | Selected on noisy/precipitation columns where any model struggles |
| VAE | 9 | 26% | Selected on surface_pressure (works) and some noisy columns (does not work) |

**Interpretation**: OCSVM dominates (46%) because in a 15-trial budget where AE/VAE need many epochs to converge, OCSVM's sub-sampled training is fast enough for Optuna to try more configurations. This shows that NN architectures require a larger trial budget than 15 to consistently outperform simpler models in Optuna search.

## When Would Auto-NN Have an Advantage?

The current results are decisive for this specific task. However, Auto-NN would be the better choice under different conditions:

| Condition | Why Auto-NN benefits |
|-----------|---------------------|
| **High-dimensional input** (>50 features) | AE bottleneck becomes meaningful; LOF's k-NN degrades in high dimensions (curse of dimensionality) |
| **Joint/interaction anomalies** | VAE latent space captures feature correlations; univariate LOF misses these entirely |
| **Large labeled dataset available** | Switch to semi-supervised AE — the NN gains massive advantage with even a few labels |
| **GPU available + large data (>500k rows)** | NN training scales linearly with GPU acceleration; LOF scales super-linearly (β=1.175) without GPU |
| **Streaming/online learning** | NNs can be updated incrementally; PyOD models require full refit |

## Conclusion

For this weather ETL anomaly detection task (20k hourly rows, 6 tabular features, 0.36% point anomaly rate, unsupervised), **AutoML with classical PyOD algorithms outperforms Auto-NN on every measured axis**:

| Axis | AutoML result | Auto-NN result |
|------|:-------------:|:--------------:|
| Detection quality (mean F2) | **0.683** | 0.162 |
| Recall | **0.765** | 0.220 |
| Total experiment time | **145 s** | 354 s |
| F2 per second | **0.1652** | 0.0160 |
| Rule-based baseline | 0.000 | 0.000 |

The result is not surprising in retrospect. LOF and ECOD are algorithms built specifically for tabular, low-dimensional anomaly detection. LOF's local density comparison is semantically aligned with the nature of weather anomalies — a point is anomalous if it is sparse *relative to its season*, not globally. ECOD's tail probability is the theoretically correct score for detecting outliers in a near-Gaussian distribution.

Auto-NN, in contrast, applies a general-purpose function approximator to a problem that does not require it. A 1D autoencoder has no meaningful bottleneck, a VAE trained for 20–100 epochs on 20k points does not converge to a stable distribution model in a 15-trial budget, and OCSVM loses accuracy from sub-sampling.

**Both approaches far exceed rule-based checks**, which caught 0% of injected anomalies. The practical recommendation: **use AutoML (PyOD + Optuna) as the default for tabular ETL anomaly detection**. Reserve Auto-NN for high-dimensional, semi-supervised, or streaming scenarios where its architectural flexibility provides a genuine advantage.
