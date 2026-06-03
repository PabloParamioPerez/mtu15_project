# STATUS: ALIVE
# LAST-AUDIT: 2026-05-27
# FEEDS: advisor_memo.tex sec 4 -- long-window view of DA CCGT cleared and IDA
#        CCGT cleared (full panel 2022-2026), with the BSTS pre/post windows
#        and counterfactual + 95% credible band overlaid over the
#        bsts_daily_longpre.R post window. Lets the reader see whether the
#        narrow BSTS post-window of Oct--Nov 2025 is representative of the
#        broader 2024-26 winter pattern.
#
# OUT: figures/thesis/fig_bsts_ccgt_longview.pdf

from pathlib import Path
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
PANEL = REPO / "data/derived/panels/bsts_quantities_panel.parquet"
PW = REPO / "results/regressions/bid/mtu15_critical_flat/pointwise"
OUT = REPO / "figures/thesis/fig_bsts_ccgt_longview.pdf"

WINDOW = 7
EVENTS = [
    (pd.Timestamp("2024-06-14"), "IDA reform"),
    (pd.Timestamp("2024-12-01"), "ISP15"),
    (pd.Timestamp("2025-03-19"), "ID15"),
    (pd.Timestamp("2025-04-28"), "Blackout"),
    (pd.Timestamp("2025-10-01"), "DA15"),
]
ID15 = pd.Timestamp("2025-03-19")
DA15 = pd.Timestamp("2025-10-01")


def smooth(s):
    return s.rolling(WINDOW, min_periods=1, center=True).mean()


def panel_with_cf(panel_col, pw_file, cutover, ax, title, ylab):
    panel = pd.read_parquet(PANEL)[["d", panel_col]].copy()
    panel["d"] = pd.to_datetime(panel["d"])
    panel = panel.dropna().sort_values("d").reset_index(drop=True)
    panel[panel_col] = smooth(panel[panel_col])
    ax.plot(panel["d"], panel[panel_col], color="black", lw=1.3,
            label="Observed (14-day MA)")

    cf = pd.read_csv(PW / pw_file)
    cf["date"] = pd.to_datetime(cf["date"])
    cf = cf.sort_values("date").reset_index(drop=True)
    for c in ["point.pred", "point.pred.lower", "point.pred.upper"]:
        cf[c] = smooth(cf[c])
    pre = cf[cf["date"] < cutover]
    post = cf[cf["date"] >= cutover]
    # pre-window: in-sample fit (lighter / dotted)
    ax.plot(pre["date"], pre["point.pred"], color="C0", lw=1.0, ls="-",
            alpha=0.85, label="BSTS fit (pre, in-sample)")
    ax.fill_between(pre["date"], pre["point.pred.lower"],
                    pre["point.pred.upper"], color="C0", alpha=0.15)
    # post-window: counterfactual (red dashed)
    ax.plot(post["date"], post["point.pred"], color="C3", lw=1.3, ls="--",
            label="BSTS counterfactual (post)")
    ax.fill_between(post["date"], post["point.pred.lower"],
                    post["point.pred.upper"], color="C3", alpha=0.22,
                    label="95\\% credible interval")

    ymin, ymax = ax.get_ylim()
    for ts, name in EVENTS:
        color = "C2" if name == "Blackout" else "gray"
        lw = 1.0 if name == "Blackout" else 0.8
        ax.axvline(ts, color=color, lw=lw, ls=":")
        ax.text(ts, ymax * 0.97, name, fontsize=7,
                color=color, ha="right", va="top", rotation=90)
    ax.set_title(title, fontsize=10)
    ax.set_ylabel(ylab, fontsize=9)
    ax.tick_params(labelsize=8)
    ax.grid(alpha=0.25, lw=0.5)
    ax.xaxis.set_major_locator(mdates.YearLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    for lbl in ax.get_xticklabels():
        lbl.set_rotation(0); lbl.set_ha("center")


def main():
    fig, axes = plt.subplots(2, 2, figsize=(11, 4.8), sharex=True)
    panel_with_cf("ida_price_eur",
                   "bsts_extpost_pointwise_ID15_IDA_price.csv", ID15,
                   axes[0, 0], "ID15: IDA price",
                   "EUR / MWh")
    panel_with_cf("da_price_eur",
                   "bsts_extpost_pointwise_DA15_DA_price.csv", DA15,
                   axes[0, 1], "DA15: DA price",
                   "EUR / MWh")
    panel_with_cf("q_ccgt_gwh_ida",
                   "bsts_extpost_pointwise_ID15_q_ccgt_ida.csv", ID15,
                   axes[1, 0], "ID15: IDA CCGT cleared",
                   "GWh / day")
    panel_with_cf("q_ccgt_gwh_da",
                   "bsts_extpost_pointwise_DA15_q_ccgt_da.csv", DA15,
                   axes[1, 1], "DA15: DA CCGT cleared",
                   "GWh / day")
    for ax in axes[1, :]:
        ax.set_xlabel("Date", fontsize=9)
    handles, labels = axes[0, 0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="lower center", ncol=3, fontsize=9,
                frameon=False, bbox_to_anchor=(0.5, -0.02))
    fig.tight_layout(rect=[0, 0.04, 1, 1])
    OUT.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(OUT, bbox_inches="tight")
    print(f"Wrote {OUT}")


if __name__ == "__main__":
    main()
