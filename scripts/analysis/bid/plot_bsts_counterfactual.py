# STATUS: ALIVE
# LAST-AUDIT: 2026-05-27
# FEEDS: advisor_memo.tex sec 3.B / sec 4 -- BSTS counterfactual vs observed
#        time-series for the four headline outcomes (DA15 DA price, DA15 CCGT
#        cleared, ID15 IDA price, ID15 CCGT cleared). Inputs are the pointwise
#        CSVs produced by bsts_daily_longpre.R.
#
# OUT: figures/thesis/fig_bsts_counterfactual.pdf

from pathlib import Path
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
PW = REPO / "results/regressions/bid/mtu15_critical_flat/pointwise"
OUT = REPO / "figures/thesis/fig_bsts_counterfactual.pdf"

PANELS = [
    ("bsts_longpre_pointwise_ID15_IDA_price.csv",
     "ID15 -- IDA clearing price", "EUR / MWh", "2025-03-19"),
    ("bsts_longpre_pointwise_DA15_DA_price.csv",
     "DA15 -- DA clearing price", "EUR / MWh", "2025-10-01"),
    ("bsts_longpre_pointwise_ID15_q_ccgt_ida.csv",
     "ID15 -- IDA CCGT cleared", "GWh / day", "2025-03-19"),
    ("bsts_longpre_pointwise_DA15_q_ccgt_da.csv",
     "DA15 -- DA CCGT cleared", "GWh / day", "2025-10-01"),
]


WINDOW = 1  # daily, no smoothing

EVENTS = [
    (pd.Timestamp("2024-06-14"), "IDA reform"),
    (pd.Timestamp("2024-12-01"), "ISP15"),
    (pd.Timestamp("2025-03-19"), "ID15"),
    (pd.Timestamp("2025-04-28"), "Blackout"),
    (pd.Timestamp("2025-10-01"), "DA15"),
]


def main():
    fig, axes = plt.subplots(2, 2, figsize=(11, 6.5), sharex=False)
    for ax, (fn, title, ylab, cutover) in zip(axes.flat, PANELS):
        df = pd.read_csv(PW / fn)
        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values("date").reset_index(drop=True)
        for c in ["response", "point.pred", "point.pred.lower",
                   "point.pred.upper"]:
            df[c] = df[c].rolling(WINDOW, min_periods=1, center=True).mean()
        T = pd.Timestamp(cutover)
        pre = df[df["date"] < T]
        post = df[df["date"] >= T]
        ax.plot(df["date"], df["response"], color="black", lw=1.3,
                label="Observed")
        ax.plot(pre["date"], pre["point.pred"], color="C0", lw=1.0,
                alpha=0.85, label="BSTS fit (pre, in-sample)")
        ax.fill_between(pre["date"], pre["point.pred.lower"],
                        pre["point.pred.upper"], color="C0", alpha=0.15)
        ax.plot(post["date"], post["point.pred"], color="C3", lw=1.3,
                ls="--", label="BSTS counterfactual (post)")
        ax.fill_between(post["date"], post["point.pred.lower"],
                        post["point.pred.upper"], color="C3", alpha=0.2,
                        label="95\\% credible interval")
        ax.set_title(title, fontsize=10)
        ax.set_ylabel(ylab, fontsize=9)
        ax.tick_params(labelsize=8)
        ax.grid(alpha=0.25, lw=0.5)
        ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
        for lbl in ax.get_xticklabels():
            lbl.set_rotation(30); lbl.set_ha("right")
        ymin, ymax = ax.get_ylim()
        xmin, xmax = df["date"].min(), df["date"].max()
        for ts, name in EVENTS:
            if not (xmin <= ts <= xmax):
                continue
            color = "C2" if name == "Blackout" else "gray"
            lw = 1.0 if name == "Blackout" else 0.8
            ax.axvline(ts, color=color, lw=lw, ls=":")
            ax.text(ts, ymax * 0.97, name, fontsize=7,
                    color=color, ha="right", va="top", rotation=90)
    # one shared legend
    handles, labels = axes[0, 0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="lower center", ncol=3,
                fontsize=9, frameon=False, bbox_to_anchor=(0.5, -0.02))
    fig.tight_layout(rect=[0, 0.04, 1, 1])
    OUT.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(OUT, bbox_inches="tight")
    print(f"Wrote {OUT}")


if __name__ == "__main__":
    main()
