# AutoML (PyOD + Optuna) vs Auto-NN: Anomaly Detection Comparison Report

This report evaluates two Optuna-driven automated anomaly detection approaches on the same unsupervised task: detecting synthetically injected anomalies in weather ETL data across five cities without the model ever seeing the anomaly labels during training.

- **AutoML**: Optuna selects among five PyOD classical algorithms — IForest, LOF, ECOD, HBOS, COPOD — and their hyperparameters (25 trials per column).
- **Auto-NN**: Optuna performs Neural Architecture Search (NAS) purely over neural network models — Autoencoder (AE) and Variational Autoencoder (VAE) — varying hidden layer count, width, activation, learning rate, epoch count, and batch size (25 trials per column, matching AutoML for a fair comparison).

Both approaches share identical data, preprocessing, train/validation split (70/30), evaluation metric (F2, β=2), trial budget (25), and the principle of fitting without any anomaly labels.

## Executive Summary

| Metric | AutoML | Auto-NN | AutoML advantage |
|--------|--------|---------|-----------------|
| Mean F2 (univariate) | **0.683** | 0.167 | +0.517 |
| Mean Recall (univariate) | **0.765** | 0.196 | +0.569 |
| Mean Precision (univariate) | **0.618** | 0.153 | +0.465 |
| Total experiment time | **145 s** | 988 s | AutoML 6.8× faster |
| F2 per second (efficiency) | **0.1652** | 0.0059 | 28.0× better |
| Univariate wins | **27/30** (93%) | 0/30 (0%) | — |

**AutoML is the clear winner.** Across 30 city×column pairs, AutoML wins 27 (90%), Auto-NN wins 0 (0%), and 3 are roughly tied. AutoML achieves this with fewer or equal compute resources.

## Detection Performance: Full Results

### Univariate Detection — F2 Score

F2-score weights recall twice as heavily as precision (β=2): missing a real anomaly costs more than a false alarm. All 76 injected anomalies per city per column were within valid rule ranges — rule-based checks detected **0%** of them.

| City | Column | AutoML model | AutoML F2 | NN model | NN F2 | Δ F2 | Winner |
|------|--------|:------------:|:---------:|:--------:|:-----:|:-----:|:------:|
| amsterdam | apparent_temperature | LOF | 0.418 | AE | 0.006 | -0.412 | **AutoML** |
| amsterdam | precipitation | LOF | 0.201 | VAE | 0.000 | -0.201 | **AutoML** |
| amsterdam | soil_moisture_7_to_28cm | LOF | 0.928 | VAE | 0.174 | -0.754 | **AutoML** |
| amsterdam | soil_temperature_7_to_28cm | LOF | 0.961 | AE | 0.044 | -0.917 | **AutoML** |
| amsterdam | surface_pressure | ECOD | 0.928 | VAE | 0.882 | -0.045 | **AutoML** |
| amsterdam | temperature_2m | LOF | 0.668 | VAE | 0.027 | -0.641 | **AutoML** |
| london | apparent_temperature | LOF | 0.855 | VAE | 0.024 | -0.831 | **AutoML** |
| london | precipitation | LOF | 0.115 | VAE | 0.014 | -0.100 | **AutoML** |
| london | soil_moisture_7_to_28cm | LOF | 0.985 | AE | 0.044 | -0.941 | **AutoML** |
| london | soil_temperature_7_to_28cm | LOF | 0.936 | AE | 0.021 | -0.915 | **AutoML** |
| london | surface_pressure | ECOD | 0.955 | VAE | 0.935 | -0.019 | tie |
| london | temperature_2m | LOF | 0.137 | AE | 0.010 | -0.127 | **AutoML** |
| new_york | apparent_temperature | LOF | 0.768 | VAE | 0.000 | -0.768 | **AutoML** |
| new_york | precipitation | LOF | 0.154 | VAE | 0.016 | -0.139 | **AutoML** |
| new_york | soil_moisture_7_to_28cm | LOF | 0.997 | AE | 0.029 | -0.968 | **AutoML** |
| new_york | soil_temperature_7_to_28cm | LOF | 0.787 | AE | 0.043 | -0.744 | **AutoML** |
| new_york | surface_pressure | ECOD | 0.934 | AE | 0.942 | +0.008 | tie |
| new_york | temperature_2m | LOF | 0.792 | AE | 0.022 | -0.770 | **AutoML** |
| paris | apparent_temperature | LOF | 0.833 | AE | 0.012 | -0.821 | **AutoML** |
| paris | precipitation | LOF | 0.157 | VAE | 0.022 | -0.135 | **AutoML** |
| paris | soil_moisture_7_to_28cm | LOF | 0.949 | VAE | 0.030 | -0.919 | **AutoML** |
| paris | soil_temperature_7_to_28cm | LOF | 0.941 | VAE | 0.041 | -0.900 | **AutoML** |
| paris | surface_pressure | HBOS | 0.944 | AE | 0.676 | -0.268 | **AutoML** |
| paris | temperature_2m | LOF | 0.259 | AE | 0.012 | -0.246 | **AutoML** |
| tokyo | apparent_temperature | LOF | 0.898 | AE | 0.024 | -0.874 | **AutoML** |
| tokyo | precipitation | LOF | 0.130 | AE | 0.013 | -0.117 | **AutoML** |
| tokyo | soil_moisture_7_to_28cm | LOF | 0.973 | AE | 0.042 | -0.931 | **AutoML** |
| tokyo | soil_temperature_7_to_28cm | LOF | 0.887 | VAE | 0.076 | -0.811 | **AutoML** |
| tokyo | surface_pressure | ECOD | 0.968 | VAE | 0.789 | -0.180 | **AutoML** |
| tokyo | temperature_2m | COPOD | 0.038 | AE | 0.027 | -0.011 | tie |

### Multivariate Detection — F2 Score

| City | AutoML model | AutoML F2 | NN model | NN F2 | Δ F2 | Winner |
|------|:------------:|:---------:|:--------:|:-----:|:-----:|:------:|
| amsterdam | LOF | 0.246 | AE | 0.223 | -0.023 | **AutoML** |
| london | LOF | 0.295 | AE | 0.364 | +0.068 | **NN** |
| new_york | LOF | 0.403 | AE | 0.298 | -0.104 | **AutoML** |
| paris | LOF | 0.378 | AE | 0.211 | -0.167 | **AutoML** |
| tokyo | LOF | 0.314 | AE | 0.267 | -0.047 | **AutoML** |

### Aggregate Detection Metrics

| Scope | Metric | AutoML | Auto-NN | Δ (NN − AutoML) |
|-------|--------|:------:|:-------:|:---------------:|
| Univariate | Mean F2 | **0.683** | 0.167 | -0.517 |
| Univariate | Mean Recall | **0.765** | 0.196 | -0.569 |
| Univariate | Mean Precision | **0.618** | 0.153 | -0.465 |
| Univariate | Mean F1 | **0.632** | 0.155 | -0.477 |
| Multivariate | Mean F2 | **0.327** | 0.272 | -0.055 |
| Multivariate | Mean Recall | **0.294** | 0.277 | -0.017 |
| Multivariate | Mean Precision | **0.628** | 0.304 | -0.324 |
| Multivariate | Mean F1 | **0.397** | 0.275 | -0.121 |

### Detection by Column Type

Columns grouped by distributional character show a clear pattern:

| Column | Distribution | AutoML avg F2 | Auto-NN avg F2 | AutoML advantage | Why |
|--------|:------------:|:-------------:|:--------------:|:----------------:|-----|
| `surface_pressure` | Tight near-Gaussian | **0.946** | 0.845 | +0.101 | Most stable, narrow range — tail detectors excel |
| `soil_moisture_7_to_28cm` | Slow seasonal cycle | **0.966** | 0.064 | +0.903 | Clear density gradient, LOF finds local outliers |
| `soil_temperature_7_to_28cm` | Seasonal, moderate noise | **0.902** | 0.045 | +0.857 | Seasonal clusters give LOF strong local context |
| `apparent_temperature` | Seasonal + noisy | **0.754** | 0.013 | +0.741 | LOF adapts to local summer/winter density |
| `temperature_2m` | Highly seasonal + erratic | **0.379** | 0.020 | +0.359 | Noise blurs AE reconstruction; LOF stays local |
| `precipitation` | Heavily right-skewed, sparse | **0.151** | 0.013 | +0.138 | Hardest for both — most hours have zero rain |

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

### 4. Why More Trials Don't Close the Gap

Even with 25 trials (matching AutoML), AE and VAE do not converge to competitive thresholds on low-dimensional tabular data within a typical Optuna budget. The core issue is that the search space interaction between epochs, width, and contamination is large relative to the information content of 1-D input. Optuna's TPE sampler needs many evaluations to locate regions of the architecture space where reconstruction error is well-calibrated as an anomaly score — a property that density- and distance-based models achieve analytically.

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
| AE | PyTorch NN | ~0.10–1.5 (varies) | — | ~2.5–37 per trial |
| VAE | PyTorch NN | ~0.30–4.0 (varies) | — | ~7.5–100 per trial |

### Full Experiment Wall-Clock Time

| Experiment | # Models | # Trials/model | Total time | Mean per model | Source |
|------------|:--------:|:--------------:|:----------:|:--------------:|--------|
| AutoML (PyOD+Optuna) | 35 | 25 | **144.7 s** | 4.14 s | MLflow run metadata |
| Auto-NN (AE/VAE) | 35 | 25 | 988.4 s | 28.24 s | `time.perf_counter()` |

**Auto-NN is 6.8× slower** than AutoML wall-to-wall, using the same 25 trials. The reason: each NN trial trains a neural network for 20–100 epochs (averaging ~0.6–1.5 s/trial), while the dominant AutoML algorithm LOF takes 0.31 s/trial and ECOD/HBOS/COPOD take < 0.014 s/trial. With 35 models × 25 NN trials at ~1 s each = ~875 s search total, versus 35 × 25 AutoML trials at ~0.19 s average: ≈ 166 s search, matching the 145 s MLflow measure.

### Per-Model Timing: Auto-NN by Algorithm Type

| NN Model | Count selected | Mean search (s) | Mean refit (s) | Mean total (s) | Mean F2 | F2/s efficiency |
|----------|:--------------:|:---------------:|:--------------:|:--------------:|:-------:|:---------------:|
| AE | 21 | 26.87 | 1.38 | 28.25 | 0.159 | 0.0056 |
| VAE | 14 | 26.47 | 1.76 | 28.22 | 0.216 | 0.0077 |

### Detection Efficiency: F2 per Second of Training

This metric answers: *how much detection quality do you get per unit of compute?* Higher is better.

| Approach | Mean F2 | Mean time/model (s) | F2/s | Relative efficiency |
|----------|:-------:|:-------------------:|:----:|:-------------------:|
| **AutoML** | **0.683** | **4.14** | **0.1652** | **28.0× better** |
| Auto-NN | 0.167 | 28.24 | 0.0059 | 1× (baseline) |

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
| AE | 21 | 60% | Reconstruction-based — selected when VAE latent regularisation hurts convergence |
| VAE | 14 | 40% | ELBO-trained with KL regularisation — selected on columns with tight distributions |

With a pure neural-network search space and a matching 25-trial budget, both AE and VAE are explored equally by Optuna's TPE sampler. The selection split reflects which architecture achieves higher F2 on the validation fold for each (city, column) pair.

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

For this weather ETL anomaly detection task (20k hourly rows, 6 tabular features, 0.36% point anomaly rate, unsupervised, equal 25-trial budget), **AutoML with classical PyOD algorithms outperforms Auto-NN (pure AE/VAE) on every measured axis**:

| Axis | AutoML result | Auto-NN result |
|------|:-------------:|:--------------:|
| Detection quality (mean F2) | **0.683** | 0.167 |
| Recall | **0.765** | 0.196 |
| Total experiment time | **145 s** | 988 s |
| F2 per second | **0.1652** | 0.0059 |
| Rule-based baseline | 0.000 | 0.000 |

The result is not surprising in retrospect. LOF and ECOD are algorithms built specifically for tabular, low-dimensional anomaly detection. LOF's local density comparison is semantically aligned with the nature of weather anomalies — a point is anomalous if it is sparse *relative to its season*, not globally. ECOD's tail probability is the theoretically correct score for detecting outliers in a near-Gaussian distribution.

Auto-NN (AE and VAE), in contrast, applies general-purpose function approximators to a problem that does not require them. A 1D autoencoder has no meaningful bottleneck — even shallow architectures can memorise 20k training points, so reconstruction error does not reliably separate anomalies from normal values in low-density seasonal regions. Increasing the trial budget to match AutoML (25 trials) does not close the gap because the underlying limitation is structural, not a matter of search time.

**Both approaches far exceed rule-based checks**, which caught 0% of injected anomalies. The practical recommendation: **use AutoML (PyOD + Optuna) as the default for tabular ETL anomaly detection**. Reserve Auto-NN for high-dimensional, semi-supervised, or streaming scenarios where its architectural flexibility provides a genuine advantage.
