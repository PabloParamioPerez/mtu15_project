# STATUS: ALIVE
# LAST-AUDIT: 2026-05-28
# FEEDS: advisor_memo.tex sec 4.B (Spec B per-hour-class BSTS). Shows the
#        PLACEBO-NET LEVEL of the BSTS effect for each (tech, market,
#        hour_class) cell -- one row per estimate (standard forest-plot
#        convention), grouped visually by (tech, market). Hour-class is
#        colour-coded. Solid markers = surviving 95%, hollow markers = CI
#        brackets zero.
#
#        IMPORTANT: these are BID-LEVEL outcomes (in-band offered MW and
#        MW-weighted mean in-band bid price), not cleared/dispatched MW.
#        The in-band test is MCP-relative, so MCP movements mechanically
#        shift the in-band region.
#
# IN:  results/regressions/bid/mtu15_critical_flat/bsts_hour_class.csv
# OUT: figures/thesis/fig_bsts_hour_class_forest.pdf

from pathlib import Path
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
SRC = REPO / "results/regressions/bid/mtu15_critical_flat/bsts_hour_class_p90_net.csv"
OUT = REPO / "figures/thesis/fig_bsts_hour_class_forest.pdf"
OUT.parent.mkdir(parents=True, exist_ok=True)

# (tech, market) groups -- top-to-bottom. Three rows each (critical / midday / flat).
TM_GROUPS = [
    ("CCGT, DA",          "ccgt",       "da"),
    ("CCGT, IDA",         "ccgt",       "ida"),
    ("Hydro, DA",         "hydro",      "da"),
    ("Hydro, IDA",        "hydro",      "ida"),
    ("Pump-storage, DA",  "hydro_pump", "da"),
    ("Pump-storage, IDA", "hydro_pump", "ida"),
]
HC_LIST = [
    ("critical", "Critical", "#c0392b"),
    ("midday",   "Midday",   "#e08e1c"),
    ("flat",     "Flat",     "#1f4e79"),
]
HC_COLOR = {hc: c for hc, _, c in HC_LIST}

# Quantity x-axis: convert raw MW sums to thousands of MW.
Q_SCALE = 1000.0
Q_XLAB  = "Thousands of MW"


def placebo_net(df, reform):
    """Take precomputed placebo-net effects per (outcome, tech, market,
    hour_class) for one reform. CSV already has eff, lo, hi, surv columns."""
    return df[df["reform"] == reform][
        ["outcome", "tech", "market", "hour_class",
         "eff", "lo", "hi", "surv"]].copy()


def build_rows():
    """Linear list of (y-position, group_label, hour_class_label, tech, market, hour_class).
    y-position descends from top; one full empty row of whitespace between
    (tech, market) groups."""
    rows = []
    y = 0.0
    ROW_STEP  = 1.0
    GROUP_GAP = 2.0   # one extra row of empty space between groups
    for (label, tech, market) in TM_GROUPS:
        for (hc, hc_label, _color) in HC_LIST:
            rows.append((y, label, hc_label, tech, market, hc))
            y -= ROW_STEP
        y -= (GROUP_GAP - ROW_STEP)
    return rows


def panel(ax, sub, title, xlab, rows, scale=1.0):
    sub = sub.set_index(["tech", "market", "hour_class"])
    y_ticks, y_labels = [], []
    for y, group_label, hc_label, tech, market, hc in rows:
        key = (tech, market, hc)
        if key not in sub.index:
            continue
        r = sub.loc[key]
        color = HC_COLOR[hc]
        surviving = bool(r["surv"])
        alpha = 1.0 if surviving else 0.40
        lw = 2.0 if surviving else 1.6
        ax.plot([r["lo"]/scale, r["hi"]/scale], [y, y],
                color=color, lw=lw, alpha=alpha,
                solid_capstyle="round", zorder=2)
        ax.plot([r["eff"]/scale], [y], marker="o",
                markerfacecolor=color, markeredgecolor="white",
                markersize=6.5, markeredgewidth=0.6,
                alpha=alpha, zorder=3)
        y_ticks.append(y)
        y_labels.append(hc_label)

    # Group labels on the right, at the centre of each 3-row block
    centre_ys = []
    for g_idx, (label, _, _) in enumerate(TM_GROUPS):
        if g_idx * 3 + 2 < len(y_ticks):
            centre = (y_ticks[g_idx * 3] + y_ticks[g_idx * 3 + 2]) / 2.0
            centre_ys.append((centre, label))

    ax.set_yticks(y_ticks)
    ax.set_yticklabels(y_labels, fontsize=8)
    ax.tick_params(axis="y", left=False)

    # (tech, market) labels on the right
    ax2 = ax.twinx()
    ax2.set_ylim(ax.get_ylim())
    ax2.set_yticks([c for c, _ in centre_ys])
    ax2.set_yticklabels([lab for _, lab in centre_ys],
                         fontsize=9, fontweight="bold")
    ax2.tick_params(axis="y", right=False, pad=2)
    ax2.spines["right"].set_visible(False)
    ax2.spines["top"].set_visible(False)

    ax.axvline(0, color="black", lw=0.7, alpha=0.6)
    ax.set_xlabel(xlab)
    ax.set_title(title, fontsize=10.5, loc="left")
    ax.grid(axis="x", alpha=0.25, lw=0.5)
    # Thin horizontal lines drawn in the middle of the whitespace gap that
    # already separates each (tech, market) group from the next.
    for g_idx in range(len(TM_GROUPS) - 1):
        if (g_idx * 3 + 2) < len(y_ticks):
            sep_y = y_ticks[g_idx * 3 + 2] - 1.0   # mid-point of the 2-unit gap
            ax.axhline(sep_y, color="0.55", lw=1.2, zorder=0)
    ymin, ymax = ax.get_ylim()
    ax.set_ylim(ymin - 0.3, ymax + 0.3)
    for spine in ("top", "right"):
        ax.spines[spine].set_visible(False)


def main():
    df = pd.read_csv(SRC)
    id_net = placebo_net(df, "ID15")
    da_net = placebo_net(df, "DA15")
    rows = build_rows()

    fig, axes = plt.subplots(2, 2, figsize=(13.0, 13.0),
                              gridspec_kw={"wspace": 0.55, "hspace": 0.40})
    panel(axes[0, 0], id_net[id_net["outcome"] == "p"],
          "ID15 -- placebo-net level on mean in-band bid price",
          "EUR / MWh", rows)
    panel(axes[0, 1], da_net[da_net["outcome"] == "p"],
          "DA15 -- placebo-net level on mean in-band bid price",
          "EUR / MWh", rows)
    panel(axes[1, 0], id_net[id_net["outcome"] == "q"],
          "ID15 -- placebo-net level on total in-band quantity",
          Q_XLAB, rows, scale=Q_SCALE)
    panel(axes[1, 1], da_net[da_net["outcome"] == "q"],
          "DA15 -- placebo-net level on total in-band quantity",
          Q_XLAB, rows, scale=Q_SCALE)

    handles = [
        Line2D([0], [0], color=HC_COLOR["critical"], lw=2.0, marker="o",
               markerfacecolor=HC_COLOR["critical"], markeredgecolor="white",
               markersize=6.5, markeredgewidth=0.6, label="Critical hours"),
        Line2D([0], [0], color=HC_COLOR["midday"], lw=2.0, marker="o",
               markerfacecolor=HC_COLOR["midday"], markeredgecolor="white",
               markersize=6.5, markeredgewidth=0.6, label="Midday hours"),
        Line2D([0], [0], color=HC_COLOR["flat"], lw=2.0, marker="o",
               markerfacecolor=HC_COLOR["flat"], markeredgecolor="white",
               markersize=6.5, markeredgewidth=0.6, label="Flat hours"),
    ]
    fig.legend(handles=handles, loc="lower center", ncol=3, fontsize=9.5,
                frameon=False, bbox_to_anchor=(0.5, 0.005))
    fig.text(0.5, -0.012,
              "Faded markers and bars indicate the 95\\% credible interval brackets zero.",
              ha="center", va="top", fontsize=8.5, style="italic", color="0.3")
    fig.tight_layout(rect=[0, 0.04, 1, 1])
    fig.savefig(OUT, bbox_inches="tight")
    print(f"Wrote {OUT}")


if __name__ == "__main__":
    main()
