# STATUS: ALIVE
# LAST-AUDIT: 2026-05-30
# CLAIM: Per-day ajuste-cost composition across the five reform-window regimes,
#        UP and DN stacked together (all are costs that REE pays providers).
#        UP channels at the bottom, DN channels (hatched) on top. Total bar
#        height = total ajuste cost per day. NOT seasonally adjusted.
#
#        TR dn is reported in the source data as a net-of-refund value
#        (sometimes negative, small magnitudes); we take its absolute value
#        for visualisation so the stack reads as gross system outflows.
#
# IN:  results/regressions/regulatory/ree_full_cascade/per_regime_costs_official.csv
# OUT: figures/working/efficiency_gains_per_regime_updn.pdf

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

REGIMES = [
    ("Pre-IDA (Jan-Jun 24)",    "Pre-IDA",         165),
    ("3-sess (Jun-Nov 24)",     "3-sess",          170),
    ("ISP15-win (Dec24-Mar25)", "ISP15-win",       108),
    ("DA60/ID15 pre-blk",       "DA60/ID15 pre",    40),
    ("DA60/ID15 post-blk",      "DA60/ID15 post",  156),
    ("DA15/ID15 (Oct-Dec 25)",  "DA15/ID15",        92),
]

# Stack order (bottom to top): UP first, then DN. Each layer has color + hatch.
UP = [
    ("TR up cost (1723)",          "TR up (real-time redispatch)",  "#2c7fb8"),
    ("aFRR reserve up cost (712)", "aFRR up (reserve)",             "#7fcdbb"),
    ("Fase II up cost (1375)",     "Fase II up",                    "#fdae6b"),
    ("Fase I up cost (1373)",      "Fase I up (reforzada-driven)",  "#d7301f"),
]

DN = [
    ("TR dn cost (1724)",            "TR dn",                                "#2c7fb8"),
    ("aFRR reserve dn cost (2127)",  "aFRR dn",                              "#7fcdbb"),
    ("Fase II dn cost (1376)",       "Fase II dn",                           "#fdae6b"),
    ("Fase I dn cost (1374)",        "Fase I dn (renewable curtailment)",    "#d7301f"),
]


def main():
    df = pd.read_csv(CSV)
    fig, ax = plt.subplots(figsize=(11.5, 6.5))

    x = np.arange(len(REGIMES))
    perday = {}
    for csv_lab, _, ndays in REGIMES:
        sub = df[df["regime"] == csv_lab]
        perday[csv_lab] = {row["service"]: row["cost_eur_m"] / ndays
                           for _, row in sub.iterrows()}

    bottoms = np.zeros(len(REGIMES))

    # UP stack (solid)
    for svc, disp, col in UP:
        vals = np.array([perday[csv_lab].get(svc, 0.0) for csv_lab, _, _ in REGIMES])
        ax.bar(x, vals, bottom=bottoms, width=0.62, color=col, label=disp,
               edgecolor="white", linewidth=0.6)
        bottoms += vals

    # Visual separator between UP and DN portions
    ups_height = bottoms.copy()

    # DN stack (hatched, on top of UP). Take absolute value to treat all as
    # gross system outflows (TR dn is reported net of a small refund).
    for svc, disp, col in DN:
        raw = np.array([perday[csv_lab].get(svc, 0.0) for csv_lab, _, _ in REGIMES])
        vals = np.abs(raw)
        ax.bar(x, vals, bottom=bottoms, width=0.62, color=col, label=disp,
               edgecolor="white", linewidth=0.6, hatch="//", alpha=0.85)
        bottoms += vals

    # Annotate UP subtotal (mid-stack) and total above each bar
    for xi, up_h, tot in zip(x, ups_height, bottoms):
        ax.text(xi, tot + 0.25, f"{tot:.1f}", ha="center", va="bottom",
                fontsize=10, fontweight="bold", color="#222")
        ax.text(xi - 0.35, up_h / 2, f"UP\n{up_h:.1f}", ha="center", va="center",
                fontsize=7, color="white", fontweight="bold")
        dn_h = tot - up_h
        ax.text(xi - 0.35, up_h + dn_h / 2, f"DN\n{dn_h:.1f}", ha="center", va="center",
                fontsize=7, color="#222", fontweight="bold")

    ax.set_xticks(x)
    ax.set_xticklabels([disp for _, disp, _ in REGIMES], fontsize=10)
    ax.set_ylabel("Per-day ajuste cost (EUR million)", fontsize=10)
    ax.set_title("Per-day ajuste-cost composition: UP and DN, by regime.",
                 fontsize=11)
    ax.legend(loc="upper left", fontsize=8, framealpha=0.9, ncol=2)
    ax.grid(axis="y", alpha=0.3, lw=0.5)
    ax.set_axisbelow(True)

    # Pre-blackout / post-blackout shaded backgrounds.
    # Regimes 0-3 are pre-blackout (Pre-IDA, 3-sess, ISP15-win, DA60/ID15 pre);
    # regimes 4-5 are post-blackout (DA60/ID15 post, DA15/ID15).
    ylim = ax.get_ylim()
    ax.axvspan(-0.5, 3.5, color="#e8f4f8", alpha=0.45, zorder=0)
    ax.axvspan(3.5, 5.5, color="#fdecea", alpha=0.45, zorder=0)
    ax.text(1.5, ylim[1] * 0.965, "PRE-blackout", ha="center", va="top",
            fontsize=9, color="#2c5777", fontweight="bold", style="italic")
    ax.text(4.5, ylim[1] * 0.965, "POST-blackout (reforzada)", ha="center", va="top",
            fontsize=9, color="#9a2a1f", fontweight="bold", style="italic")

    # Reform transition markers between bars
    y_lab = ax.get_ylim()[1] * 0.045
    for xi, txt in [(0.5, "IDA reform\n6$\\to$3"), (1.5, "ISP15"), (2.5, "MTU15-IDA"),
                    (3.5, "blackout /\nreforzada"), (4.5, "MTU15-DA")]:
        lw = 1.6 if abs(xi - 3.5) < 0.01 else 0.8
        col = "#9a2a1f" if abs(xi - 3.5) < 0.01 else "gray"
        ax.axvline(xi, color=col, ls=":", lw=lw)
        ax.text(xi, y_lab, txt, ha="center", va="bottom",
                fontsize=7, color="gray", style="italic",
                bbox=dict(boxstyle="round,pad=0.15", fc="white", ec="none", alpha=0.8))

    fig.tight_layout()
    out = FIG_DIR / "efficiency_gains_per_regime_updn.pdf"
    fig.savefig(out, bbox_inches="tight", dpi=130)
    plt.close(fig)
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
