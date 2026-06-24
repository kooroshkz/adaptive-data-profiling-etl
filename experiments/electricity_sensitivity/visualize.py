#!/usr/bin/env python3
"""Visualize the electricity sensitivity sweep.

Focus: CAPTURE RATE of the injected anomalies only (recall = caught / injected).
False positives are intentionally ignored here. Results are reported per data
column separately (two sets: INDO and ITSDO).

Produces:
  artifacts/capture_heatmaps.png   colored annotated table (rate x shift), per column
  artifacts/capture_surface_3d.png 3D surface of capture rate, per column
and prints the two capture-rate tables.
"""

from __future__ import annotations

import glob
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib import cm

_THIS = Path(__file__).resolve().parent
ART = _THIS / "artifacts"
COLS = {
    "initialDemandOutturn": "INDO  (initial demand outturn)",
    "initialTransmissionSystemDemandOutturn": "ITSDO  (transmission system demand)",
}
RATES = [0.01, 0.02, 0.05, 0.10, 0.20]
SHIFTS = [0.10, 0.20, 0.30, 0.40, 0.50]


def load() -> pd.DataFrame:
    files = sorted(glob.glob(str(ART / "sensitivity_seed*.csv")))
    df = pd.concat([pd.read_csv(f) for f in files], ignore_index=True)
    # capture rate = recall of injected anomalies, mean over seeds
    agg = df.groupby(["column", "anomaly_rate", "shift_pct"]).recall.mean().reset_index()
    return agg


def matrix(agg: pd.DataFrame, column: str) -> np.ndarray:
    """rows = anomaly rate, cols = shift magnitude; values = capture rate."""
    m = np.full((len(RATES), len(SHIFTS)), np.nan)
    sub = agg[agg.column == column]
    for i, r in enumerate(RATES):
        for j, s in enumerate(SHIFTS):
            v = sub[(np.isclose(sub.anomaly_rate, r)) & (np.isclose(sub.shift_pct, s))].recall
            if len(v):
                m[i, j] = v.values[0]
    return m


def print_tables(agg: pd.DataFrame) -> None:
    for col, label in COLS.items():
        m = matrix(agg, col) * 100
        print(f"\nCapture rate (%) — {label}   rows=anomaly rate, cols=shift")
        hdr = "        " + "".join(f"{int(s*100):>6}%" for s in SHIFTS)
        print(hdr)
        for i, r in enumerate(RATES):
            print(f"  {int(r*100):>3}%  " + "".join(f"{m[i,j]:>6.0f} " for j in range(len(SHIFTS))))


def heatmaps(agg: pd.DataFrame) -> None:
    fig, axes = plt.subplots(1, 2, figsize=(13, 5))
    cmap = plt.get_cmap("RdYlGn")
    for ax, (col, label) in zip(axes, COLS.items()):
        m = matrix(agg, col)
        im = ax.imshow(m, cmap=cmap, vmin=0.0, vmax=1.0, aspect="auto", origin="lower")
        ax.set_xticks(range(len(SHIFTS)), [f"{int(s*100)}%" for s in SHIFTS])
        ax.set_yticks(range(len(RATES)), [f"{int(r*100)}%" for r in RATES])
        ax.set_xlabel("Shift magnitude")
        ax.set_ylabel("Anomaly rate")
        ax.set_title(label, fontsize=11)
        for i in range(len(RATES)):
            for j in range(len(SHIFTS)):
                val = m[i, j]
                ax.text(j, i, f"{val*100:.0f}", ha="center", va="center",
                        color="black" if 0.30 < val < 0.85 else "white", fontsize=10, fontweight="bold")
    cbar = fig.colorbar(im, ax=axes, fraction=0.025, pad=0.02)
    cbar.set_label("Capture rate (recall of injected anomalies)")
    fig.suptitle("Injected-anomaly capture rate — electricity demand (mean of 3 injection seeds)",
                 fontsize=13, fontweight="bold")
    out = ART / "capture_heatmaps.png"
    fig.savefig(out, dpi=150, bbox_inches="tight")
    print(f"\nsaved {out}")


def surfaces(agg: pd.DataFrame) -> None:
    fig = plt.figure(figsize=(14, 5.5))
    Xi, Yi = np.meshgrid(np.arange(len(SHIFTS)), np.arange(len(RATES)))
    for k, (col, label) in enumerate(COLS.items(), 1):
        ax = fig.add_subplot(1, 2, k, projection="3d")
        Z = matrix(agg, col)
        ax.plot_surface(Xi, Yi, Z, cmap="viridis", vmin=0, vmax=1,
                        edgecolor="0.3", linewidth=0.3, antialiased=True, alpha=0.95)
        ax.set_xticks(range(len(SHIFTS)), [f"{int(s*100)}%" for s in SHIFTS])
        ax.set_yticks(range(len(RATES)), [f"{int(r*100)}%" for r in RATES])
        ax.set_xlabel("Shift magnitude", labelpad=8)
        ax.set_ylabel("Anomaly rate", labelpad=8)
        ax.set_zlabel("Capture rate", labelpad=4)
        ax.set_zlim(0, 1)
        ax.set_title(label, fontsize=11)
        ax.view_init(elev=22, azim=-60)
    fig.suptitle("Capture-rate surface over anomaly rate x shift magnitude (electricity)",
                 fontsize=13, fontweight="bold")
    out = ART / "capture_surface_3d.png"
    fig.savefig(out, dpi=150, bbox_inches="tight")
    print(f"saved {out}")


def main() -> None:
    agg = load()
    print_tables(agg)
    heatmaps(agg)
    surfaces(agg)


if __name__ == "__main__":
    main()
