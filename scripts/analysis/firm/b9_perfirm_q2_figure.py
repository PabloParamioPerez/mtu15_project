# STATUS: ALIVE
# LAST-AUDIT: 2026-04-29
# FEEDS: B9 — per-firm q₂ trajectory figure for thesis
# CLAIM: Per-Big-4-firm q₂ trajectory at firm-ISP-replicated grain visualizes
#        the U-shape: progressive collapse during asymmetric-granularity (Jun
#        2024 - Oct 2025), partial-to-full recovery once symmetric MTU15.
"""B9 per-firm q₂ trajectory — thesis figure.

Reads `b9_replicated_isp_grain_perfirm.csv` (per-firm × regime mean q₂ at
firm-ISP-replicated grain, MWh per ISP) and produces a clean line plot.

Output: figures/thesis/fig09_B9_perfirm_q2_trajectory.{pdf,png}
"""
from __future__ import annotations
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt

PROJECT  = Path(__file__).resolve().parents[3]
SRC      = PROJECT / "results" / "regressions" / "b9_replicated_isp_grain_perfirm.csv"
OUT_PDF  = PROJECT / "figures" / "thesis" / "fig09_B9_perfirm_q2_trajectory.pdf"
OUT_PNG  = PROJECT / "figures" / "thesis" / "fig09_B9_perfirm_q2_trajectory.png"

REGIMES = ["pre-IDA", "3-sess", "ISP15-win", "DA60/ID15", "DA15/ID15"]
BIG4 = ["GE", "IB", "GN", "HC"]
COLORS = {"GE": "#d62728", "IB": "#1f77b4", "GN": "#2ca02c", "HC": "#9467bd"}


def main() -> None:
    df = pd.read_csv(SRC, index_col=0)
    df = df.reindex(BIG4)[REGIMES]
    big4_mean = df.mean(axis=0)

    # Figsize × dpi controlled to stay under 2000px on any side
    fig, ax = plt.subplots(figsize=(7, 4.2), dpi=200)
    x = list(range(len(REGIMES)))

    for firm in BIG4:
        ax.plot(x, df.loc[firm].values,
                marker="o", linewidth=2.0, markersize=6,
                color=COLORS[firm], label=firm)

    ax.plot(x, big4_mean.values, color="black", linewidth=2.5,
            linestyle="--", marker="s", markersize=7,
            label="Big-4 mean", zorder=5)

    # Reform-date markers as annotations
    ax.axvspan(0.5, 3.5, alpha=0.08, color="grey",
               label="Asymmetric-granularity window")

    ax.set_xticks(x)
    ax.set_xticklabels(REGIMES, fontsize=10)
    ax.set_ylabel("q₂ — voluntary IDA MWh per firm-ISP", fontsize=11)
    ax.set_xlabel("Regime", fontsize=11)
    ax.set_title("Big-4 q₂ trajectory at firm-ISP-replicated grain\n"
                 "(IR-cleanest strategic-spot measure, max disaggregation)",
                 fontsize=11)
    ax.axhline(0, color="grey", linewidth=0.5, alpha=0.5)
    ax.grid(True, alpha=0.3)
    ax.legend(loc="upper right", fontsize=9, frameon=True, ncol=2)

    # Annotate the asymmetric-window region briefly
    y_mid = ax.get_ylim()[1] * 0.93
    ax.text(2.0, y_mid, "Jun 2024 – Oct 2025", ha="center",
            fontsize=8, color="grey", alpha=0.8)

    plt.tight_layout()
    OUT_PDF.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(OUT_PDF, bbox_inches="tight")
    plt.savefig(OUT_PNG, bbox_inches="tight", dpi=200)
    print(f"wrote {OUT_PDF}")
    print(f"wrote {OUT_PNG}")
    print()
    print("Per-firm q₂ trajectory (MWh per firm-ISP):")
    print(df.round(1).to_string())
    print()
    print(f"Big-4 mean: " + " → ".join(f"{r}: {big4_mean[r]:.1f}" for r in REGIMES))


if __name__ == "__main__":
    main()
