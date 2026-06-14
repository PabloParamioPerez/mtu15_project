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
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
PANEL = REPO / "data/derived/panels/bid_shape_daily_means_extended.parquet"
OUTDIR = REPO / "figures/thesis"

CUTOVER = {"ID15": pd.Timestamp("2025-03-19"), "DA15": pd.Timestamp("2025-10-01")}
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
    fig, ax = plt.subplots(2, 2, figsize=(8.4, 4.3), sharex="col")
    for j, (reform, market, title) in enumerate(cols):
        for i, (outcome, ylab) in enumerate(rows):
            a = ax[i, j]
            crit, flat = series(df, reform, market, outcome)
            a.plot(crit.index, crit.values, color=RED, lw=1.3, label="Critical (ramps)")
            a.plot(flat.index, flat.values, color=BLUE, lw=1.3, label="Flat (overnight)")
            a.axvline(CUTOVER[reform], color="black", ls="--", lw=1.0)
            if i == 0:
                a.set_title(title, fontsize=11)
            if j == 0:
                a.set_ylabel(ylab, fontsize=10)
            a.tick_params(labelsize=8)
            a.margins(x=0.01)
    ax[0, 0].legend(fontsize=8, loc="upper right", framealpha=0.9)
    for a in ax[1, :]:
        for lbl in a.get_xticklabels():
            lbl.set_rotation(0)
            lbl.set_fontsize(7)
    fig.tight_layout()
    OUTDIR.mkdir(parents=True, exist_ok=True)
    fig.savefig(OUTDIR / "fig_ccgt_pt_main.pdf", bbox_inches="tight")
    fig.savefig(OUTDIR / "fig_ccgt_pt_main.png", dpi=130, bbox_inches="tight")
    print("wrote fig_ccgt_pt_main.{pdf,png}")


if __name__ == "__main__":
    main()
