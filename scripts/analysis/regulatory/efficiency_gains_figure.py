# STATUS: ALIVE
# LAST-AUDIT: 2026-05-21
# CLAIM: Per-day ajuste-cost composition across the five reform-window
#        regimes, visualising the efficiency-gains story of section 8.7:
#        the MTU15 channels (TR up, aFRR up) shrink while the reforzada
#        channel (Fase I up) grows. Stacked bars = per-day cost composition;
#        the net is the bar height. Companion to Table tab:efficiency_gains.
#
# IN:  results/regressions/regulatory/ree_full_cascade/per_regime_costs_official.csv
# OUT: figures/working/efficiency_gains_per_regime.pdf

from pathlib import Path
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

REPO = Path(__file__).resolve().parents[3]
CSV = REPO / "results/regressions/regulatory/ree_full_cascade/per_regime_costs_official.csv"
FIG_DIR = REPO / "figures/working"
FIG_DIR.mkdir(parents=True, exist_ok=True)

# Regime label in the CSV -> (display label, window day count).
REGIMES = [
    ("3-sess (Jun-Nov 24)",     "3-sess",          170),
    ("ISP15-win (Dec24-Mar25)", "ISP15-win",       108),
    ("DA60/ID15 pre-blk",       "DA60/ID15 pre",    40),
    ("DA60/ID15 post-blk",      "DA60/ID15 post",  156),
    ("DA15/ID15 (Oct-Dec 25)",  "DA15/ID15",        92),
]

# Up-direction cost channels (the "system pays" side), stacked bottom -> top.
CHANNELS = [
    ("TR up cost (1723)",          "TR up (real-time redispatch)",  "#2c7fb8"),
    ("aFRR reserve up cost (712)", "aFRR up (reserve)",             "#7fcdbb"),
    ("Fase II up cost (1375)",     "Fase II up",                    "#fdae6b"),
    ("Fase I up cost (1373)",      "Fase I up (reforzada-driven)",  "#d7301f"),
]


def main():
    df = pd.read_csv(CSV)
    fig, ax = plt.subplots(figsize=(11, 6))

    x = np.arange(len(REGIMES))
    bottoms = np.zeros(len(REGIMES))
    perday = {}
    for csv_lab, _, ndays in REGIMES:
        sub = df[df["regime"] == csv_lab]
        perday[csv_lab] = {row["service"]: row["cost_eur_m"] / ndays
                           for _, row in sub.iterrows()}

    for svc, disp, col in CHANNELS:
        vals = np.array([perday[csv_lab].get(svc, 0.0) for csv_lab, _, _ in REGIMES])
        ax.bar(x, vals, bottom=bottoms, width=0.62, color=col, label=disp,
               edgecolor="white", linewidth=0.6)
        bottoms += vals

    # Net total annotation on top of each bar
    for xi, tot in zip(x, bottoms):
        ax.text(xi, tot + 0.15, f"{tot:.1f}", ha="center", va="bottom",
                fontsize=9, fontweight="bold")

    ax.set_xticks(x)
    ax.set_xticklabels([disp for _, disp, _ in REGIMES], fontsize=10)
    ax.set_ylabel("Up-direction ajuste cost (EUR million / day)", fontsize=10)
    ax.set_title("Efficiency gains across the reform stages: per-day ajuste-cost "
                 "composition.\nMTU15 channels (TR, aFRR) shrink; the "
                 "reforzada channel (Fase~I) grows.", fontsize=11)
    ax.legend(loc="upper left", fontsize=8, framealpha=0.9)
    ax.grid(axis="y", alpha=0.3, lw=0.5)
    ax.set_axisbelow(True)

    # Reform transition markers between bars (text near the bottom so it
    # never collides with the bars or the legend)
    y_lab = ax.get_ylim()[1] * 0.045
    for xi, txt in [(0.5, "ISP15"), (1.5, "MTU15-IDA"), (2.5, "blackout /\nreforzada"),
                    (3.5, "MTU15-DA")]:
        ax.axvline(xi, color="gray", ls=":", lw=0.8)
        ax.text(xi, y_lab, txt, ha="center", va="bottom",
                fontsize=7, color="gray", style="italic",
                bbox=dict(boxstyle="round,pad=0.15", fc="white", ec="none", alpha=0.8))

    fig.tight_layout()
    out = FIG_DIR / "efficiency_gains_per_regime.pdf"
    fig.savefig(out, bbox_inches="tight", dpi=130)
    plt.close(fig)
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
