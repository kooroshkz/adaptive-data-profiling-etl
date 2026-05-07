# AutoML Scalability Formula

A cost model for predicting how long the anomaly detection pipeline takes to run,
based on dataset size, hardware, and search configuration.

---

## The Formula

$$\boxed{
T(n,\, m,\, k,\, p) \;=\; \alpha \cdot n^{\,\beta} \cdot m^{\,\delta} \cdot k^{\,\gamma} \cdot \left(\frac{p}{p_0}\right)^{\varepsilon}
}$$

| Symbol | Meaning |
|--------|---------|
| $T$ | Training time (seconds) |
| $n$ | Number of rows |
| $m$ | Number of feature columns |
| $k$ | Number of HPO trials (Optuna) |
| $p$ | Compute speed of the machine (GFLOP/s) |
| $p_0$ | Reference machine speed (730 GFLOP/s — Apple M3 Max) |
| $\alpha$ | Scale constant (fitted from data) |

---

## Fitted Parameters

Estimated by log-linear regression across 90 benchmark experiments on real weather data:

| Parameter | Value | 95% Confidence Interval | Meaning |
|-----------|-------|--------------------------|---------|
| $\alpha$ | $0.869$ | — | baseline cost in seconds |
| $\beta$ | $0.395$ | $[0.33,\ 0.44]$ | how time grows with rows |
| $\delta$ | $-0.017$ | $[-0.15,\ 0.08]$ | effect of feature count |
| $\gamma$ | $0.869$ | $[0.71,\ 0.99]$ | effect of trial budget |
| $\varepsilon$ | $-0.924$ | — | hardware sensitivity |
| $R^2$ | $0.864$ | — | model fit quality |

---

## Key Finding: Sub-Linear Scaling

Because $\beta = 0.395 < 1$, the pipeline scales **sub-linearly with data size**.
Doubling the number of rows increases training time by only:

$$2^{\,0.395} \approx 1.32\times$$

| Data grows by | Time grows by |
|---------------|---------------|
| $2\times$ | $1.32\times$ |
| $10\times$ | $2.49\times$ |
| $100\times$ | $6.21\times$ |

This is a strong argument for the viability of AutoML on larger datasets.

---

## Per-Detector Scaling

Each PyOD model has its own scaling behaviour, measured separately across 150 experiments:

$$T_i(n,\, m) \;=\; \alpha_i \cdot n^{\,\beta_i} \cdot m^{\,\delta_i}$$

| Detector | $\beta_i$ (row scaling) | 2× data → |
|----------|------------------------|-----------|
| IForest | 0.307 | $1.24\times$ |
| HBOS | 0.645 | $1.56\times$ |
| COPOD | 0.947 | $1.93\times$ |
| ECOD | 0.945 | $1.93\times$ |
| LOF | 1.175 | $2.26\times$ super-linear |

IForest is the most BigData-friendly detector; LOF degrades faster than linear and should be
avoided at very large $n$.

---

## Hardware Sensitivity

The exponent $\varepsilon \approx -0.924$ means the formula scales nearly inversely with
compute speed. On a machine twice as fast:

$$T_{\text{new}} \approx T_{\text{old}} \cdot \left(\frac{2p_0}{p_0}\right)^{-0.924} \approx 0.53 \times T_{\text{old}}$$

A $2\times$ faster machine delivers roughly $1.9\times$ speedup — close to ideal.
