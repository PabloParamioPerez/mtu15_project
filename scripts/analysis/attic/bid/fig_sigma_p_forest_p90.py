# STATUS: ALIVE
# LAST-AUDIT: 2026-05-28
# FEEDS: advisor_memo.tex sec 4.C -- per-curve DiD on sigma_p and N_eff
#        under the window-specific p90 bandwidth (DA h=50, IDA h=62/58).
#        Replaces the old fig_sigma_p_forest.pdf which used the h=140 baseline.
#
# Layout: 2x2 panels. Rows = outcome (sigma_p, N_eff). Cols = reform (ID15, DA15).
# Each panel: 8 horizontal rows = (tech, market) pairs.
#
# IN:  results/regressions/bid/mtu15_critical_flat/spec_c_did_p90.csv
# OUT: figures/thesis/fig_sigma_p_forest.pdf

from pathlib import Path
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
SRC = REPO / "results/regressions/bid/mtu15_critical_flat/spec_c_did_p90.csv"
OUT = REPO / "figures/thesis/fig_sigma_p_forest.pdf"
OUT.parent.mkdir(parents=True, exist_ok=True)

ROWS = [
    ("CCGT, DA",         "CCGT",       "da"),
    ("CCGT, IDA",        "CCGT",       "ida"),
    ("Hydro, DA",        "Hydro",      "da"),
    ("Hydro, IDA",       "Hydro",      "ida"),
    ("Pump-storage, DA", "Hydro_pump", "da"),
    ("Pump-storage, IDA","Hydro_pump", "ida"),
    ("Wind, DA",         "Wind",       "da"),
    ("Wind, IDA",        "Wind",       "ida"),
]

COLOR_DA  = "#1f4e79"
COLOR_IDA = "#c0392b"


def panel(ax, sub, title, xlab):
    sub = sub.set_index(["tech", "market"])
    y = np.arange(len(ROWS))[::-1]
    for yi, (label, tech, market) in zip(y, ROWS):
        if (tech, market) not in sub.index:
            continue
        r = sub.loc[(tech, market)]
        lo = r["theta"] - 1.96 * r["se"]
        hi = r["theta"] + 1.96 * r["se"]
        surviving = (lo * hi) > 0
        color = COLOR_DA if market == "da" else COLOR_IDA
        alpha = 1.0 if surviving else 0.40
        lw    = 2.0 if surviving else 1.4
        ax.plot([lo, hi], [yi, yi], color=color, lw=lw,
                alpha=alpha, solid_capstyle="round", zorder=2)
        ax.plot([r["theta"]], [yi], "o", color=color, markersize=6.5,
                markeredgecolor="white", markeredgewidth=0.6,
                alpha=alpha, zorder=3)
    ax.axvline(0, color="black", lw=0.7, alpha=0.6)
    ax.set_yticks(y)
    ax.set_yticklabels([lab for lab, _, _ in ROWS], fontsize=9)
    ax.tick_params(axis="y", left=False)
    ax.set_xlabel(xlab)
    ax.set_title(title, fontsize=10.5, loc="left")
    ax.grid(axis="x", alpha=0.25, lw=0.5)
    # Thin separator after each (tech, DA+IDA) pair
    for yi in y[:-1:2]:
        ax.axhline(yi - 1.5, color="0.55", lw=1.2, zorder=0)
    for spine in ("top", "right"):
        ax.spines[spine].set_visible(False)


def main():
    df = pd.read_csv(SRC)
    fig, axes = plt.subplots(2, 2, figsize=(11.5, 7.2),
                              gridspec_kw={"wspace": 0.45, "hspace": 0.45})
    panel(axes[0, 0], df[(df["reform"]=="ID15") & (df["outcome"]=="sigma_p")],
          "ID15 -- per-curve DiD on $\\sigma_p$ (within-quarter price SD, EUR/MWh)",
          "$\\theta$ (critical $-$ flat, post $-$ pre)")
    panel(axes[0, 1], df[(df["reform"]=="DA15") & (df["outcome"]=="sigma_p")],
          "DA15 -- per-curve DiD on $\\sigma_p$ (within-quarter price SD, EUR/MWh)",
          "$\\theta$ (critical $-$ flat, post $-$ pre)")
    panel(axes[1, 0], df[(df["reform"]=="ID15") & (df["outcome"]=="n_eff")],
          "ID15 -- per-curve DiD on $N_{\\mathrm{eff}}$ (effective tranche count)",
          "$\\theta$ (critical $-$ flat, post $-$ pre)")
    panel(axes[1, 1], df[(df["reform"]=="DA15") & (df["outcome"]=="n_eff")],
          "DA15 -- per-curve DiD on $N_{\\mathrm{eff}}$ (effective tranche count)",
          "$\\theta$ (critical $-$ flat, post $-$ pre)")

    handles = [
        Line2D([0],[0], color=COLOR_DA, lw=2.0, marker="o",
                markerfacecolor=COLOR_DA, markeredgecolor="white",
                markersize=6.5, markeredgewidth=0.6, label="Day-ahead (DA)"),
        Line2D([0],[0], color=COLOR_IDA, lw=2.0, marker="o",
                markerfacecolor=COLOR_IDA, markeredgecolor="white",
                markersize=6.5, markeredgewidth=0.6, label="Intraday auctions (IDA)"),
    ]
    fig.legend(handles=handles, loc="lower center", ncol=2, fontsize=9.5,
                frameon=False, bbox_to_anchor=(0.5, 0.005))
    fig.text(0.5, -0.012,
              "Faded markers and bars indicate the 95\\% confidence interval brackets zero.",
              ha="center", va="top", fontsize=8.5, style="italic", color="0.3")
    fig.tight_layout(rect=[0, 0.04, 1, 1])
    fig.savefig(OUT, bbox_inches="tight")
    print(f"Wrote {OUT}")


if __name__ == "__main__":
    main()
