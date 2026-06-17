# STATUS: ALIVE
# LAST-AUDIT: 2026-06-09
# FEEDS: appendix bid-shape parallel-trends figures of the June 2026 deck.
#
# Reads the daily-tech-hour_class panel produced by bid_shape_did_extended.py
# and draws a 4-row x 5-col figure (4 outcomes x 5 techs) with one critical
# line and one flat line (14-day rolling) per panel, plus a vertical cut at
# the reform date.
#
# Per-panel background color (data-driven):
#   GREEN  -- pre-period (critical - flat) differential is flat AND the
#             critical-vs-flat DiD coefficient is significant (|t| >= 2.58).
#   RED    -- pre-period differential is clearly trending (|t-slope| >= 2)
#             OR the post jump is in the same direction as the pre trend.
#   YELLOW -- the pre and/or post window has too little data (incomplete
#             series for either critical or flat).
#   (no fill) -- ambiguous; reader judges.
#
# One figure per (reform, market). OUT: figures/thesis/fig_bid_shape_pt_<reform>_<market>.{pdf,png}
#
# Additionally emits metric-split variants for the thesis appendix (clean-PT
# metrics vs drifting-PT metrics):
#   fig_bid_shape_pt_<reform>_<market>_sigma_hhi.{pdf,png}   (sigma_p, hhi)
#   fig_bid_shape_pt_<reform>_<market>_beta_gamma.{pdf,png}  (beta, gamma)

from pathlib import Path
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
import pandas as pd
from scipy import stats

REPO = Path(__file__).resolve().parents[3]
PANEL = REPO / "data/derived/panels/bid_shape_daily_means_extended.parquet"
DIDCSV = REPO / "results/regressions/bid/mtu15_critical_flat/bid_shape_did_extended.csv"
OUTDIR = REPO / "figures/thesis"

TECHS_PLOT = ["CCGT", "Cogen", "Hydro_pump", "Hybrid", "Biomass"]
TECH_LABEL = {"CCGT": "CCGT", "Cogen": "Cogen (CHP)",
              "Hydro_pump": "Hydro-pump", "Hybrid": "Hybrid",
              "Biomass": "Biomass"}
OUTCOMES = ["sigma_p", "beta", "gamma", "hhi"]
OUTCOME_LABEL = {"sigma_p": r"$\sigma_p$ (EUR/MWh)",
                  "beta":   r"$\beta$ (slope)",
                  "gamma":  r"$\phi$ (curvature)",
                  "hhi":    r"HHI"}
REFORM_DATE = {"ID15": pd.Timestamp("2025-03-19"),
                "DA15": pd.Timestamp("2025-10-01")}
# Focused display window per reform (crop the noisy far-history; enough pre-trend
# to judge parallelism, post region shaded).
XLIM = {"ID15": (pd.Timestamp("2024-09-20"), pd.Timestamp("2025-05-03")),
        "DA15": (pd.Timestamp("2025-04-04"), pd.Timestamp("2026-01-29"))}

GREEN  = "#d9f2dc"
RED    = "#fadbd8"
YELLOW = "#fef5c4"

MIN_PRE_DAYS  = 60
MIN_POST_DAYS = 14
PRE_TREND_T   = 2.0
DID_SIG_T     = 2.58  # *** threshold


def classify(panel: pd.DataFrame, did_t: float) -> str:
    """Heuristic for subplot background color."""
    if panel.empty:
        return YELLOW
    pre = panel[panel["d"] < panel["cutover"].iloc[0]]
    post = panel[panel["d"] >= panel["cutover"].iloc[0]]
    crit_pre = pre.dropna(subset=["crit"])
    flat_pre = pre.dropna(subset=["flat"])
    if (len(crit_pre) < MIN_PRE_DAYS or len(flat_pre) < MIN_PRE_DAYS
            or len(post.dropna(subset=["crit", "flat"])) < MIN_POST_DAYS):
        return YELLOW
    # Pre-trend on (crit - flat) differential
    merged = pre.dropna(subset=["crit", "flat"]).copy()
    if len(merged) < MIN_PRE_DAYS:
        return YELLOW
    diff = (merged["crit"] - merged["flat"]).to_numpy()
    days = (merged["d"] - merged["d"].min()).dt.days.to_numpy()
    if np.std(diff) < 1e-9:
        return YELLOW
    slope, intercept, r, p, se = stats.linregress(days, diff)
    t_slope = abs(slope / se) if se > 0 else 0.0
    if t_slope >= PRE_TREND_T:
        return RED
    if abs(did_t) >= DID_SIG_T and t_slope < 1.5:
        return GREEN
    return "#ffffff"


def make_fig(panel: pd.DataFrame, did: pd.DataFrame, reform: str, market: str,
             outcomes=None, suffix: str = "", shade: bool = True):
    outcomes = outcomes or OUTCOMES
    sub = panel[(panel["reform"] == reform) & (panel["market"] == market)].copy()
    if sub.empty:
        return
    cut = REFORM_DATE[reform]
    fig, axes = plt.subplots(len(outcomes), len(TECHS_PLOT),
                              figsize=(13, 2.25 * len(outcomes) + 0.6),
                              sharex=True, squeeze=False)
    for r, outcome in enumerate(outcomes):
        for c, tech in enumerate(TECHS_PLOT):
            ax = axes[r, c]
            df_t = sub[sub["tech"] == tech].copy()
            # critical (morning + evening ramp) and flat means per day
            crit_df = (df_t[df_t["hour_class"].isin(["MorningRamp", "EveningRamp"])]
                          .groupby("d", as_index=False)[outcome].mean()
                          .rename(columns={outcome: "crit"}))
            flat_df = (df_t[df_t["hour_class"] == "Flat"][["d", outcome]]
                          .rename(columns={outcome: "flat"}))
            merged = pd.merge(crit_df, flat_df, on="d", how="outer").sort_values("d")
            merged["cutover"] = cut
            did_row = did[(did.reform == reform) & (did.market == market)
                           & (did.tech == tech) & (did.outcome == outcome)
                           & (did.comparison == "crit_vs_flat")]
            did_t = float(did_row["t"].iloc[0]) if len(did_row) else 0.0
            if shade:
                ax.set_facecolor(classify(merged, did_t))
            # plot lines
            for col, color, label in [("crit", "#c0392b", "Critical (5-8, 16-22)"),
                                        ("flat", "#2980b9", "Flat (1-3)")]:
                gg = merged[["d", col]].dropna()
                if gg.empty:
                    continue
                gg = gg.sort_values("d")
                gg["rm"] = gg[col].rolling(14, min_periods=3).mean()
                ax.plot(gg["d"], gg["rm"], color=color, lw=1.4, label=label)
            lo, hi = XLIM.get(reform, (merged["d"].min(), merged["d"].max()))
            if not shade:                       # subtle post-reform shading
                ax.axvspan(cut, hi, color="0.5", alpha=0.07, lw=0)
            ax.axvline(cut, color="black", lw=0.9, ls="--")
            ax.set_xlim(lo, hi)
            ax.grid(True, color="0.88", lw=0.5, alpha=0.9)
            ax.set_axisbelow(True)
            if r == 0:
                ax.set_title(TECH_LABEL[tech], fontsize=10)
            if c == 0:
                ax.set_ylabel(OUTCOME_LABEL[outcome], fontsize=9)
            if r == len(outcomes) - 1:          # readable date ticks, bottom row only
                ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
                ax.xaxis.set_major_formatter(mdates.DateFormatter("%b\n%Y"))
            ax.tick_params(axis="x", labelsize=7.5)
            ax.tick_params(axis="y", labelsize=8)
    handles, labels = axes[0, 0].get_legend_handles_labels()
    legend_handles = handles[:]
    legend_labels = labels[:]
    if shade:
        # add color-key entries
        legend_handles += [
            plt.Rectangle((0, 0), 1, 1, fc=GREEN,  ec="black", lw=0.4),
            plt.Rectangle((0, 0), 1, 1, fc=RED,    ec="black", lw=0.4),
            plt.Rectangle((0, 0), 1, 1, fc=YELLOW, ec="black", lw=0.4),
        ]
        legend_labels += ["clean parallel trend + DiD",
                          "pre-trend violation",
                          "incomplete series"]
    fig.legend(legend_handles, legend_labels, loc="lower center", ncol=5,
                bbox_to_anchor=(0.5, -0.005), frameon=False, fontsize=9)
    fig.suptitle(f"Parallel trends --- {reform} {market.upper()} "
                  "(14-day rolling mean; dashed line $=$ reform cutover)",
                  fontsize=11)
    fig.tight_layout(rect=[0, 0.04 * 4 / len(outcomes), 1, 1 - 0.04 * 4 / len(outcomes)])
    base = OUTDIR / f"fig_bid_shape_pt_{reform}_{market}{suffix}"
    OUTDIR.mkdir(parents=True, exist_ok=True)
    fig.savefig(base.with_suffix(".pdf"))
    fig.savefig(base.with_suffix(".png"), dpi=150)
    plt.close(fig)
    print(f"  -> {base}.pdf")


def main():
    panel = pd.read_parquet(PANEL)
    panel["d"] = pd.to_datetime(panel["d"])
    did = pd.read_csv(DIDCSV)
    for reform in ["ID15", "DA15"]:
        for market in ["da", "ida"]:
            make_fig(panel, did, reform, market)
            make_fig(panel, did, reform, market,
                     outcomes=["sigma_p", "hhi"], suffix="_sigma_hhi", shade=False)
            make_fig(panel, did, reform, market,
                     outcomes=["beta", "gamma"], suffix="_beta_gamma", shade=False)


if __name__ == "__main__":
    main()
