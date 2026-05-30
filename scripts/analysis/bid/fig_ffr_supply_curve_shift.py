# STATUS: ALIVE
# LAST-AUDIT: 2026-05-30
# READS:  results/regressions/bid/ffr_supply_curves/ffr_curves_predicted_actual.parquet
# WRITES: figures/working/ffr_supply_curve_shift_*.pdf
#         results/regressions/bid/ffr_supply_curves/placebo_net_shift.csv
#
# Computes the reform-attributable shift in the aggregate supply curve as
#   placebo_net_residual(p) = mean_t(actual_real - pred_real)(t, p)
#                           - mean_t(actual_placebo - pred_placebo)(t, p)
# and plots a few representative (reform, market, session, hour-class)
# cells. Predicted curves are FFR counterfactuals trained on the pre-window.
#
# Not wired into the memo.

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
CURVES = REPO / "results/regressions/bid/ffr_supply_curves/ffr_curves_predicted_actual.parquet"
NET_CSV = REPO / "results/regressions/bid/ffr_supply_curves/placebo_net_shift.csv"
FIG_DIR = REPO / "figures/working"

FIG_DIR.mkdir(parents=True, exist_ok=True)


def mean_residual(df, side):
    sub = df[df["side"] == side].copy()
    sub["resid"] = sub["actual_mw"] - sub["pred_mw"]
    g = (sub.groupby(["reform", "market", "session", "hour_class", "price_eur"], dropna=False)
            ["resid"].mean().rename(f"resid_{side}"))
    return g.reset_index()


def main():
    curves = pd.read_parquet(CURVES)
    print(f"Loaded curves: {curves.shape[0]:,} rows")

    real_res = mean_residual(curves, "real")
    plb_res = mean_residual(curves, "placebo")
    net = real_res.merge(
        plb_res, on=["reform", "market", "session", "hour_class", "price_eur"],
        how="inner")
    net["placebo_net"] = net["resid_real"] - net["resid_placebo"]
    net.to_csv(NET_CSV, index=False)
    print(f"Wrote {NET_CSV}: {len(net):,} (cell, price) rows")

    # Focal plots: DA15 DA and ID15 IDA-S1, all 3 hour-classes
    focal = [("DA15", "DA", None), ("ID15", "IDA", 1.0), ("ID15", "DA", None)]
    for reform, market, session in focal:
        if session is None:
            cell = net[(net["reform"] == reform) & (net["market"] == market)
                       & net["session"].isna()]
            title = f"{reform} {market}"
            tag = f"{reform}_{market}_SNA"
        else:
            cell = net[(net["reform"] == reform) & (net["market"] == market)
                       & (net["session"] == session)]
            title = f"{reform} {market}-S{int(session)}"
            tag = f"{reform}_{market}_S{int(session)}"
        if cell.empty:
            print(f"  no data for {tag}"); continue

        fig, axes = plt.subplots(1, 3, figsize=(13, 4), sharey=True)
        for ax, hc in zip(axes, ["critical", "midday", "flat"]):
            sub = cell[cell["hour_class"] == hc].sort_values("price_eur")
            if sub.empty:
                ax.set_title(f"{hc} (no data)"); continue
            ax.plot(sub["price_eur"], sub["resid_real"], label="real (actual − predicted)",
                    color="C3", lw=1.2)
            ax.plot(sub["price_eur"], sub["resid_placebo"], label="placebo (actual − predicted)",
                    color="C0", lw=1.2, ls="--")
            ax.plot(sub["price_eur"], sub["placebo_net"], label="placebo-net (real − placebo)",
                    color="k", lw=1.6)
            ax.axhline(0, color="0.6", lw=0.5)
            ax.set_title(f"{hc}")
            ax.set_xlabel("Price (EUR/MWh)")
            if hc == "critical":
                ax.set_ylabel("Residual MW (actual − FFR counterfactual)")
            ax.grid(alpha=0.3)
        axes[0].legend(loc="best", fontsize=8)
        fig.suptitle(f"FFR supply-curve shift: {title}", fontsize=11)
        fig.tight_layout()
        out = FIG_DIR / f"ffr_supply_curve_shift_{tag}.pdf"
        fig.savefig(out, bbox_inches="tight")
        plt.close(fig)
        print(f"  wrote {out}")


if __name__ == "__main__":
    main()
