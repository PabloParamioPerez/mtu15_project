# STATUS: ALIVE
# LAST-AUDIT: 2026-06-15
# FEEDS: main-text \S6.4 parallel-trends figure for CCGT (the headline tech).
#
# Reads the daily-tech-hour_class bid-shape panel and draws a compact 2x2
# figure: rows = (sigma_p, tranche-HHI), cols = (ID15 day-ahead, DA15
# day-ahead) -- the two CCGT day-ahead cells the body leads with. One critical
# line (morning+evening ramp) and one flat line (overnight), 14-day rolling
# mean, with a vertical cut at the reform cutover. This is the "first thing a
# reader wants to see" before the DiD table.
#
# OUT: figures/thesis/fig_ccgt_pt_main.{pdf,png}

from pathlib import Path
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
PANEL = REPO / "data/derived/panels/bid_shape_daily_means_extended.parquet"
OUTDIR = REPO / "figures/thesis"

CUTOVER = {"ID15": pd.Timestamp("2025-03-19"), "DA15": pd.Timestamp("2025-10-01")}
# Focused display window per column: enough pre-trend to judge parallelism,
# without the noisy far-history that crowds the axis.
XLIM = {"ID15": (pd.Timestamp("2024-09-01"), pd.Timestamp("2025-04-27")),
        "DA15": (pd.Timestamp("2025-04-28"), pd.Timestamp("2026-02-28"))}
CRITICAL = {"MorningRamp", "EveningRamp"}
RED, BLUE = "#c0392b", "#2c6fbb"


def series(df, reform, market, outcome):
    sub = df[(df["tech"] == "CCGT") & (df["reform"] == reform) & (df["market"] == market)].copy()
    sub["d"] = pd.to_datetime(sub["d"])
    crit = (sub[sub["hour_class"].isin(CRITICAL)].groupby("d")[outcome].mean()
            .sort_index().rolling(14, min_periods=5).mean())
    flat = (sub[sub["hour_class"] == "Flat"].groupby("d")[outcome].mean()
            .sort_index().rolling(14, min_periods=5).mean())
    return crit, flat


def main():
    df = pd.read_parquet(PANEL)
    cols = [("ID15", "da", "ID15 day-ahead"), ("DA15", "da", "DA15 day-ahead")]
    rows = [("sigma_p", r"$\sigma_p$ (EUR/MWh)"), ("hhi", "tranche-HHI")]
    fig, ax = plt.subplots(2, 2, figsize=(8.6, 5.0), sharex="col")
    for j, (reform, market, title) in enumerate(cols):
        lo, hi = XLIM[reform]
        for i, (outcome, ylab) in enumerate(rows):
            a = ax[i, j]
            crit, flat = series(df, reform, market, outcome)
            a.plot(crit.index, crit.values, color=RED, lw=1.4, label="Critical (ramps)")
            a.plot(flat.index, flat.values, color=BLUE, lw=1.4, label="Flat (overnight)")
            # post-reform region shaded; cutover line labelled once (top row)
            a.axvspan(CUTOVER[reform], hi, color="0.5", alpha=0.07, lw=0)
            a.axvline(CUTOVER[reform], color="black", ls="--", lw=1.0)
            a.set_xlim(lo, hi)
            a.grid(True, axis="both", color="0.85", lw=0.6, alpha=0.8)
            a.set_axisbelow(True)
            if i == 0:
                a.set_title(title, fontsize=11)
            if j == 0:
                a.set_ylabel(ylab, fontsize=10)
            a.tick_params(labelsize=8)
    # readable date ticks on the shared bottom row: one tick every 2 months
    for j, (reform, _, _) in enumerate(cols):
        a = ax[1, j]
        a.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
        a.xaxis.set_major_formatter(mdates.DateFormatter("%b\n%Y"))
        for lbl in a.get_xticklabels():
            lbl.set_fontsize(8)
            lbl.set_ha("center")
    ax[0, 0].legend(fontsize=8, loc="upper right", framealpha=0.9)
    fig.tight_layout(h_pad=0.6, w_pad=1.2)
    OUTDIR.mkdir(parents=True, exist_ok=True)
    fig.savefig(OUTDIR / "fig_ccgt_pt_main.pdf", bbox_inches="tight")
    fig.savefig(OUTDIR / "fig_ccgt_pt_main.png", dpi=130, bbox_inches="tight")
    print("wrote fig_ccgt_pt_main.{pdf,png}")


if __name__ == "__main__":
    main()
