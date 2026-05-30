# STATUS: ALIVE
# LAST-AUDIT: 2026-05-30
# READS:  results/regressions/bid/ffr_supply_curves/ffr_curves_predicted_actual.parquet
#         data/derived/panels/supply_curves_panel.parquet  (for pre-window means)
# WRITES: figures/working/ffr_pre_actual_pred_*.pdf
#
# For each (reform, market, session, hour-class), shows:
#   - Mean PRE-window actual supply curve (training data average)
#   - Mean POST-window actual supply curve
#   - Mean POST-window FFR-predicted counterfactual
#   - Gap = actual_post - predicted_post  (reform-attributable shift, raw)
# Real vs placebo arms side by side.

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
CURVES = REPO / "results/regressions/bid/ffr_supply_curves/ffr_curves_predicted_actual.parquet"
PANEL = REPO / "data/derived/panels/supply_curves_panel.parquet"
FIG_DIR = REPO / "figures/working"
FIG_DIR.mkdir(parents=True, exist_ok=True)

HOURS = {
    "critical": [5, 6, 7, 8, 16, 17, 18, 19, 20, 21, 22],
    "midday":   [11, 12, 13, 14],
    "flat":     [1, 2, 3],
}
WINDOWS = {
    ("ID15", "real"):    (pd.Timestamp("2024-06-14"), pd.Timestamp("2025-03-18")),
    ("ID15", "placebo"): (pd.Timestamp("2023-06-14"), pd.Timestamp("2024-03-18")),
    ("DA15", "real"):    (pd.Timestamp("2025-04-28"), pd.Timestamp("2025-09-30")),
    ("DA15", "placebo"): (pd.Timestamp("2024-04-28"), pd.Timestamp("2024-09-30")),
}


def pre_mean_curve(panel, market, session, hour_class, pre_window):
    """Mean PRE-window actual supply curve."""
    if pd.isna(session):
        sub = panel[(panel["market"] == market) & panel["session"].isna()]
    else:
        sub = panel[(panel["market"] == market) & (panel["session"] == session)]
    sub = sub[sub["clock_hour"].isin(HOURS[hour_class])]
    sub = sub[(sub["d"] >= pre_window[0]) & (sub["d"] <= pre_window[1])]
    grid_cols = [c for c in sub.columns if c.startswith("Q_")]
    # Avg across (date, hour) cells
    return sub[grid_cols].mean()


def post_means(curves, reform, side, market, session, hour_class):
    """Mean POST-window actual and predicted curves."""
    sub = curves[(curves["reform"] == reform) & (curves["side"] == side)
                 & (curves["market"] == market) & (curves["hour_class"] == hour_class)]
    if pd.isna(session):
        sub = sub[sub["session"].isna()]
    else:
        sub = sub[sub["session"] == session]
    if sub.empty:
        return None, None
    actual = sub.groupby("price_eur")["actual_mw"].mean()
    pred   = sub.groupby("price_eur")["pred_mw"].mean()
    return actual, pred


def plot_cell(panel, curves, reform, market, session, tag):
    """3-row x 2-col panel: rows = critical/midday/flat; cols = real | placebo."""
    fig, axes = plt.subplots(3, 2, figsize=(11, 10), sharex=True)
    pre_real = WINDOWS[(reform, "real")]
    pre_plb  = WINDOWS[(reform, "placebo")]

    for r, hc in enumerate(["critical", "midday", "flat"]):
        # Pre means
        pre_curve_real = pre_mean_curve(panel, market, session, hc, pre_real)
        pre_curve_plb  = pre_mean_curve(panel, market, session, hc, pre_plb)
        # Post means
        a_real, p_real = post_means(curves, reform, "real", market, session, hc)
        a_plb,  p_plb  = post_means(curves, reform, "placebo", market, session, hc)

        for c, (side, pre_curve, actual, pred) in enumerate([
            ("real",    pre_curve_real, a_real, p_real),
            ("placebo", pre_curve_plb,  a_plb,  p_plb),
        ]):
            ax = axes[r, c]
            if actual is None or pred is None or pre_curve is None or pre_curve.empty:
                ax.text(0.5, 0.5, "no data", ha="center", va="center",
                        transform=ax.transAxes)
                continue
            grid_p = np.array([int(g.replace("Q_", "").replace("_mw", ""))
                               for g in pre_curve.index])
            ax.plot(grid_p, pre_curve.values / 1000.0, color="0.5", lw=1.4, ls=":",
                    label="pre mean (train)")
            ax.plot(actual.index, actual.values / 1000.0, color="C3", lw=1.6,
                    label=f"post mean (actual, {side})")
            ax.plot(pred.index, pred.values / 1000.0, color="C0", lw=1.6, ls="--",
                    label=f"post mean (FFR predicted, {side})")
            # Shade the gap (actual - predicted)
            ax.fill_between(actual.index, pred.values / 1000.0, actual.values / 1000.0,
                            color="C3", alpha=0.12,
                            label="gap (actual − predicted) = reform shift")
            ax.set_title(f"{hc} hours - {side}")
            ax.grid(alpha=0.3)
            if c == 0:
                ax.set_ylabel(f"{hc}\ncumulative MW offered (GW)")
            if r == 0 and c == 0:
                ax.legend(loc="lower right", fontsize=7.5)
            if r == 2:
                ax.set_xlabel("Bid price (EUR/MWh)")
    fig.suptitle(f"FFR supply curves: {tag}", fontsize=12)
    fig.tight_layout()
    out = FIG_DIR / f"ffr_pre_actual_pred_{tag}.pdf"
    fig.savefig(out, bbox_inches="tight")
    plt.close(fig)
    print(f"  wrote {out}")


def main():
    panel = pd.read_parquet(PANEL)
    panel["d"] = pd.to_datetime(panel["d"])
    curves = pd.read_parquet(CURVES)

    focal = [
        ("DA15", "DA",  None, "DA15_DA"),
        ("DA15", "IDA", 1.0,  "DA15_IDA_S1"),
        ("DA15", "IDA", 3.0,  "DA15_IDA_S3"),
        ("ID15", "DA",  None, "ID15_DA"),
        ("ID15", "IDA", 1.0,  "ID15_IDA_S1"),
        ("ID15", "IDA", 3.0,  "ID15_IDA_S3"),
    ]
    for reform, market, session, tag in focal:
        plot_cell(panel, curves, reform, market, session, tag)


if __name__ == "__main__":
    main()
