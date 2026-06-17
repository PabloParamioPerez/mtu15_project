# STATUS: ALIVE
# LAST-AUDIT: 2026-05-16
# FEEDS: provisional.tex §7 (D_w distributions, replaces boxplots)
# CLAIM: Per (firm × tech) overlaid log-histogram of D_w, critical (red)
#        vs flat (blue). x-axis = log10(D_w + 1), fine bins (0.1 wide so
#        each step = factor of 1.26 in D_w). Mass-at-zero piles into the
#        leftmost bin near x=0.

from __future__ import annotations

from pathlib import Path
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

REPO = Path(__file__).resolve().parents[3]
DA_CELLS = REPO / "results" / "regressions" / "bid" / "quarter_dissimilarity" / "quarter_dissimilarity_cells_2025Q4.csv"
IDA_DIR  = REPO / "results" / "regressions" / "bid" / "quarter_dissimilarity_ida"
FIGDIR   = REPO / "figures" / "working"
FIGDIR.mkdir(parents=True, exist_ok=True)

FIRMS = ("IB", "GE", "GN", "HC")
TECHS = ("CCGT", "Hydro", "Wind", "Solar PV")
PRETTY = {"IB": "Iberdrola", "GE": "Endesa", "GN": "Naturgy", "HC": "EDP-Sp"}

# Bin grid: log10(D_w) for D_w > 0 only. Range tuned to data (D_w spans
# 10^{-1} to ~10^5). 60 bins → 0.1-wide in log10, each bin = factor of 1.26.
LOG_MIN, LOG_MAX, N_BINS = -1.0, 5.0, 60
BINS = np.linspace(LOG_MIN, LOG_MAX, N_BINS + 1)


HOUR_CLASSES = (("critical", "tab:red"), ("midday", "tab:green"), ("flat", "tab:blue"))


def plot_grid(cells: pd.DataFrame, market_label: str, fname: str):
    fig, axes = plt.subplots(len(FIRMS), len(TECHS),
                              figsize=(3.2 * len(TECHS), 2.6 * len(FIRMS)),
                              sharex=True, sharey=False)
    for i, firm in enumerate(FIRMS):
        for j, tech in enumerate(TECHS):
            ax = axes[i, j]
            sub = cells[(cells["firm"] == firm) & (cells["tech_group"] == tech)]
            any_data = False
            for hc_name, color in HOUR_CLASSES:
                vals_all = sub[sub["hour_class"] == hc_name]["d_max_w"].dropna().values
                if len(vals_all) == 0:
                    continue
                any_data = True
                zmass = (vals_all <= 1e-6).mean() * 100
                pos = vals_all[vals_all > 1e-6]
                if len(pos):
                    ax.hist(np.log10(pos), bins=BINS, density=False,
                             weights=np.full_like(pos, 1.0 / len(vals_all)),
                             color=color, alpha=0.45,
                             label=f"{hc_name[:4]} (n={len(vals_all):,}, 0={zmass:.0f}%)")
            if not any_data:
                ax.set_visible(False); continue
            if i == 0:
                ax.set_title(tech, fontsize=10)
            if j == 0:
                ax.set_ylabel(PRETTY.get(firm, firm), fontsize=10)
            if i == len(FIRMS) - 1:
                ax.set_xlabel(r"$\log_{10}(D_w)$", fontsize=8)
            ax.tick_params(labelsize=7)
            ax.grid(alpha=0.3, axis="y")
            ax.legend(loc="upper right", fontsize=6, frameon=False)
    fig.suptitle(f"$D_w$ histogram, {market_label}: positive-only $\\log_{{10}}(D_w)$, critical (red) / midday (green) / flat (blue). Mass-at-$D_w$=$0$ in legend.",
                 fontsize=11, y=1.00)
    fig.tight_layout(rect=[0, 0, 1, 0.97])
    out = FIGDIR / fname
    plt.savefig(f"{out}.pdf", bbox_inches="tight")
    plt.savefig(f"{out}.png", bbox_inches="tight", dpi=130)
    plt.close(fig)
    print(f"saved {out}.pdf")


IDA_TECHS = ("CCGT", "Hydro")


def plot_ida_merged(cells_pre: pd.DataFrame, cells_post: pd.DataFrame, fname: str):
    """Compact merged IDA figure: rows = firms, cols = (CCGT-pre, CCGT-post, Hydro-pre, Hydro-post)."""
    col_specs = [("CCGT", "pre", cells_pre), ("CCGT", "post", cells_post),
                  ("Hydro", "pre", cells_pre), ("Hydro", "post", cells_post)]
    fig, axes = plt.subplots(len(FIRMS), len(col_specs),
                              figsize=(2.6 * len(col_specs), 2.3 * len(FIRMS)),
                              sharex=True, sharey=False)
    for i, firm in enumerate(FIRMS):
        for j, (tech, regime, cells) in enumerate(col_specs):
            ax = axes[i, j]
            sub = cells[(cells["firm"] == firm) & (cells["tech_group"] == tech)]
            any_data = False
            for hc_name, color in HOUR_CLASSES:
                vals_all = sub[sub["hour_class"] == hc_name]["d_max_w"].dropna().values
                if len(vals_all) == 0: continue
                any_data = True
                zmass = (vals_all <= 1e-6).mean() * 100
                pos = vals_all[vals_all > 1e-6]
                if len(pos):
                    ax.hist(np.log10(pos), bins=BINS, density=False,
                             weights=np.full_like(pos, 1.0 / len(vals_all)),
                             color=color, alpha=0.45,
                             label=f"{hc_name[:4]} (n={len(vals_all):,}, 0={zmass:.0f}%)")
            if not any_data:
                ax.set_visible(False); continue
            if i == 0:
                ax.set_title(f"{tech} ({regime})", fontsize=9)
            if j == 0:
                ax.set_ylabel(PRETTY.get(firm, firm), fontsize=10)
            if i == len(FIRMS) - 1:
                ax.set_xlabel(r"$\log_{10}(D_w)$", fontsize=8)
            ax.tick_params(labelsize=7)
            ax.grid(alpha=0.3, axis="y")
            ax.legend(loc="upper right", fontsize=5.5, frameon=False)
    fig.suptitle(r"$D_w$ histogram, IDA: critical (red) / midday (green) / flat (blue), pre- vs post-blackout side-by-side per (firm $\times$ tech).",
                 fontsize=11, y=1.00)
    fig.tight_layout(rect=[0, 0, 1, 0.97])
    out = FIGDIR / fname
    plt.savefig(f"{out}.pdf", bbox_inches="tight")
    plt.savefig(f"{out}.png", bbox_inches="tight", dpi=130)
    plt.close(fig)
    print(f"saved {out}.pdf")


def main():
    valid_hc = ("critical", "midday", "flat")
    print("DA Oct-Dec 2025...")
    da = pd.read_csv(DA_CELLS)
    da = da[da["hour_class"].isin(valid_hc)]
    plot_grid(da, market_label="DA, Oct--Dec 2025", fname="fig_dw_loghist_da")

    print("IDA pre + post merged...")
    pre  = pd.read_csv(IDA_DIR / "cells_pre_blackout.csv")
    pre  = pre[pre["hour_class"].isin(valid_hc)]
    post = pd.read_csv(IDA_DIR / "cells_post_blackout.csv")
    post = post[post["hour_class"].isin(valid_hc)]
    plot_ida_merged(pre, post, "fig_dw_loghist_ida_merged")


if __name__ == "__main__":
    main()
