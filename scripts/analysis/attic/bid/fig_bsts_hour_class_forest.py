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
SRC = REPO / "results/regressions/bid/mtu15_critical_flat/bsts_hour_class_p90.csv"
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
    ("morning_ramp", "Morning ramp", "#c0392b"),
    ("midday",       "Midday",       "#e08e1c"),
    ("evening_ramp", "Evening ramp", "#7d2d8a"),
    ("flat",         "Flat",         "#1f4e79"),
]
HC_COLOR = {hc: c for hc, _, c in HC_LIST}
HC_HOURS = {"morning_ramp": 4, "midday": 4, "evening_ramp": 7, "flat": 3}

# Quantity x-axis: convert raw MWh-per-day sums to GWh per class-hour.
#   raw value [MWh per day in class] / HC_HOURS[class] = MWh per class-hour
#   then divide by 1000 -> GWh per class-hour. Same denominator for real and
#   placebo so the placebo-net is invariant to the choice.
Q_SCALE = 1000.0
Q_XLAB  = "GWh per class-hour (in-band offered energy)"


def placebo_net(df, reform):
    """Compute placebo-net per (outcome, tech, market, hour_class) for one
    reform from the raw per-side CSV (real and placebo rows). Uses
    independent-Gaussian-posterior approximation: se from (hi-lo)/(2*1.96)."""
    sub = df[df["reform"] == reform].copy()
    sub["se"] = (sub["hi"] - sub["lo"]) / (2 * 1.96)
    real = sub[sub["side"] == "real"].set_index(
        ["outcome", "tech", "market", "hour_class"])[["eff", "se"]]
    plb  = sub[sub["side"] == "placebo"].set_index(
        ["outcome", "tech", "market", "hour_class"])[["eff", "se"]]
    j = real.join(plb, lsuffix="_r", rsuffix="_p", how="inner").reset_index()
    j["eff"] = j["eff_r"] - j["eff_p"]
    j["se"]  = np.sqrt(j["se_r"]**2 + j["se_p"]**2)
    j["lo"]  = j["eff"] - 1.96 * j["se"]
    j["hi"]  = j["eff"] + 1.96 * j["se"]
    j["surv"] = (j["lo"] > 0) | (j["hi"] < 0)
    return j[["outcome", "tech", "market", "hour_class",
              "eff", "lo", "hi", "surv"]]


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
        # Per-class-hour normalisation: divide by number of hours in the class
        # so morning ramp (4 h), midday (4 h), evening ramp (7 h) and flat (3 h)
        # are directly comparable. Same constant applied to eff/lo/hi.
        hr_scale = scale * HC_HOURS.get(hc, 1)
        ax.plot([r["lo"]/hr_scale, r["hi"]/hr_scale], [y, y],
                color=color, lw=lw, alpha=alpha,
                solid_capstyle="round", zorder=2)
        ax.plot([r["eff"]/hr_scale], [y], marker="o",
                markerfacecolor=color, markeredgecolor="white",
                markersize=6.5, markeredgewidth=0.6,
                alpha=alpha, zorder=3)
        y_ticks.append(y)
        y_labels.append(hc_label)

    # Group labels on the right, at the centre of each N-row block (N = #hour-classes)
    n_hc = len(HC_LIST)
    centre_ys = []
    for g_idx, (label, _, _) in enumerate(TM_GROUPS):
        last_in_group = g_idx * n_hc + n_hc - 1
        if last_in_group < len(y_ticks):
            centre = (y_ticks[g_idx * n_hc] + y_ticks[last_in_group]) / 2.0
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
        last_in_group = g_idx * n_hc + n_hc - 1
        if last_in_group < len(y_ticks):
            sep_y = y_ticks[last_in_group] - 1.0   # mid-point of the 2-unit gap
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

    fig, axes = plt.subplots(1, 2, figsize=(13.0, 6.8),
                              gridspec_kw={"wspace": 0.55})
    panel(axes[0], id_net[id_net["outcome"] == "mwh"],
          "ID15 -- placebo-net level on in-band offered energy",
          Q_XLAB, rows, scale=Q_SCALE)
    panel(axes[1], da_net[da_net["outcome"] == "mwh"],
          "DA15 -- placebo-net level on in-band offered energy",
          Q_XLAB, rows, scale=Q_SCALE)

    handles = [
        Line2D([0], [0], color=HC_COLOR[hc], lw=2.0, marker="o",
               markerfacecolor=HC_COLOR[hc], markeredgecolor="white",
               markersize=6.5, markeredgewidth=0.6, label=hc_label)
        for hc, hc_label, _ in HC_LIST
    ]
    fig.legend(handles=handles, loc="lower center", ncol=4, fontsize=9.5,
                frameon=False, bbox_to_anchor=(0.5, 0.005))
    fig.text(0.5, -0.012,
              "Faded markers and bars indicate the 95\\% credible interval brackets zero.",
              ha="center", va="top", fontsize=8.5, style="italic", color="0.3")
    fig.tight_layout(rect=[0, 0.04, 1, 1])
    fig.savefig(OUT, bbox_inches="tight")
    print(f"Wrote {OUT}")


if __name__ == "__main__":
    main()
