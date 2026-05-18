# STATUS: ALIVE
# LAST-AUDIT: 2026-05-16
# FEEDS: provisional.tex §10 (D_w decomposition channel histograms)
# CLAIM: Per-firm CCGT histograms of Δp (EUR/MWh) and Δq (MWh) channels
#        with critical / midday / flat overlay. Cleaner economic
#        interpretation than D_w magnitude.

from __future__ import annotations

from pathlib import Path
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

REPO = Path(__file__).resolve().parents[3]
CELLS = REPO / "results" / "regressions" / "bid" / "w1_decomposition" / "cells_decomposed.csv"
FIGDIR = REPO / "figures" / "working"
FIGDIR.mkdir(parents=True, exist_ok=True)

FIRMS = ("IB", "GE", "GN", "HC")
PRETTY = {"IB": "Iberdrola", "GE": "Endesa", "GN": "Naturgy", "HC": "EDP-Sp"}
HOUR_CLASSES = (("critical", "tab:red"), ("midday", "tab:green"), ("flat", "tab:blue"))


def _hist_panel(ax, sub: pd.DataFrame, col: str, bins, xlabel: str):
    any_data = False
    for hc, color in HOUR_CLASSES:
        vals_all = sub[sub["hour_class"] == hc][col].dropna().values
        if len(vals_all) == 0:
            continue
        any_data = True
        zmass = (vals_all <= 1e-6).mean() * 100
        pos = vals_all[vals_all > 1e-6]
        if len(pos):
            ax.hist(pos, bins=bins, density=False,
                     weights=np.full_like(pos, 1.0 / len(vals_all)),
                     color=color, alpha=0.45,
                     label=f"{hc[:4]} (n={len(vals_all):,}, 0={zmass:.0f}%)")
    ax.set_xlabel(xlabel, fontsize=8)
    ax.tick_params(labelsize=7)
    ax.grid(alpha=0.3, axis="y")
    ax.legend(loc="upper right", fontsize=6.5, frameon=False)
    return any_data


def main():
    df = pd.read_csv(CELLS)
    df = df[df["hour_class"].isin(("critical", "midday", "flat"))]
    print("loaded", len(df), "CCGT cells")

    # Bins: Δp in [0, 30] EUR/MWh, 30 bins of 1 EUR/MWh. Δq in [0, 500] MWh, 50 bins of 10 MWh.
    bins_dp = np.linspace(0, 30, 31)
    bins_dq = np.linspace(0, 500, 51)

    fig, axes = plt.subplots(len(FIRMS), 2,
                              figsize=(10, 2.5 * len(FIRMS)),
                              sharex="col", sharey=False)
    for i, firm in enumerate(FIRMS):
        sub = df[df["firm"] == firm]
        ok1 = _hist_panel(axes[i, 0], sub, "dp_max_eur_mwh", bins_dp,
                           r"$\Delta p$ (EUR/MWh)")
        ok2 = _hist_panel(axes[i, 1], sub, "dq_max_mwh", bins_dq,
                           r"$\Delta q$ (MWh)")
        axes[i, 0].set_ylabel(PRETTY[firm], fontsize=10)
        if i == 0:
            axes[i, 0].set_title(r"$\Delta p$: price-axis shape shift on common mass", fontsize=10)
            axes[i, 1].set_title(r"$\Delta q$: band-mass mismatch across quarters", fontsize=10)
    fig.suptitle(r"CCGT DA Oct--Dec 2025: per-firm $\Delta p$ and $\Delta q$ histograms (channel-level), critical / midday / flat overlay.",
                 fontsize=11, y=1.00)
    fig.tight_layout(rect=[0, 0, 1, 0.97])
    out = FIGDIR / "fig_dp_dq_hist_ccgt"
    plt.savefig(f"{out}.pdf", bbox_inches="tight")
    plt.savefig(f"{out}.png", bbox_inches="tight", dpi=130)
    plt.close(fig)
    print(f"saved {out}.pdf")


if __name__ == "__main__":
    main()
