# STATUS: ALIVE
# LAST-AUDIT: 2026-06-09
# Full-history parallel-trends of sigma_p and HHI per tech per market.
# Critical vs flat (14-day rolling mean), with ID15 and DA15 reform lines.
#
# OUT: figures/thesis/fig_bid_shape_full_history_{sigma_p,hhi}_{da,ida}.{pdf,png}
#      (slides-only figures; the slides graphicspath resolves figures/thesis/)

from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

REPO = Path(__file__).resolve().parents[3]
PANEL = REPO / "data/derived/panels/bid_shape_daily_means_extended.parquet"
OUT_DIR = REPO / "figures/thesis"
OUT_DIR.mkdir(parents=True, exist_ok=True)

ID15 = pd.Timestamp("2025-03-19")
DA15 = pd.Timestamp("2025-10-01")
BLACKOUT = pd.Timestamp("2025-04-28")

TECHS = ["CCGT", "Hydro", "Hydro_pump", "Wind", "Cogen", "Coal", "Hybrid", "Biomass"]
MARKETS = [("da", "DA"), ("ida", "IDA")]
METRICS = [("sigma_p", r"$\sigma_p$ (MW-weighted price SD, EUR/MWh)"),
           ("hhi", r"HHI of curve tranches")]
ROLL = 14  # days

df = pd.read_parquet(PANEL)
df["d"] = pd.to_datetime(df["d"])
# Collapse ID15 + DA15 partitions to a single continuous series
df = df.drop(columns=["reform"]).sort_values(["tech", "market", "hour_class", "d"])

# Aggregate critical = mean(Morning + Evening ramp), flat = Flat
crit_mask = df["hour_class"].isin(["MorningRamp", "EveningRamp"])
flat_mask = df["hour_class"] == "Flat"
crit = (df[crit_mask].groupby(["tech", "market", "d"], as_index=False)
        [["sigma_p", "hhi"]].mean())
flat = (df[flat_mask].groupby(["tech", "market", "d"], as_index=False)
        [["sigma_p", "hhi"]].mean())
crit["band"] = "Critical"
flat["band"] = "Flat"
long = pd.concat([crit, flat], ignore_index=True)


def plot_grid(metric_col, metric_label, market_key, market_label):
    sub = long[long["market"] == market_key].copy()
    fig, axes = plt.subplots(4, 2, figsize=(11, 12), sharex=True)
    axes = axes.ravel()
    for i, tech in enumerate(TECHS):
        ax = axes[i]
        for band, color in [("Critical", "tab:red"), ("Flat", "tab:blue")]:
            s = sub[(sub["tech"] == tech) & (sub["band"] == band)].copy()
            if s.empty:
                continue
            s = s.sort_values("d")
            s[metric_col + "_roll"] = (s[metric_col]
                                       .rolling(ROLL, min_periods=max(3, ROLL // 2))
                                       .mean())
            ax.plot(s["d"], s[metric_col + "_roll"], color=color, lw=1.5,
                    label=band, alpha=0.95)
        ax.axvline(ID15, color="black", lw=0.9, ls="--", alpha=0.7)
        ax.axvline(DA15, color="black", lw=0.9, ls="--", alpha=0.7)
        ax.axvspan(BLACKOUT, BLACKOUT + pd.Timedelta(days=1),
                   color="grey", alpha=0.15)
        ax.set_title(tech, fontsize=10)
        ax.grid(True, alpha=0.3)
        ax.xaxis.set_major_locator(mdates.MonthLocator(bymonth=[1, 7]))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
        for lbl in ax.get_xticklabels():
            lbl.set_rotation(0)
        if i == 0:
            ax.legend(fontsize=8, loc="upper right")
    # add reform-line annotation on top-left only
    axes[0].text(ID15, axes[0].get_ylim()[1], " ID15", fontsize=7, va="top")
    axes[0].text(DA15, axes[0].get_ylim()[1], " DA15", fontsize=7, va="top")
    fig.suptitle(f"{metric_label}  ---  {market_label} market   "
                 f"(14-day rolling mean, by hour-class)", fontsize=12, y=0.995)
    fig.tight_layout()
    stem = OUT_DIR / f"fig_bid_shape_full_history_{metric_col}_{market_key}"
    fig.savefig(stem.with_suffix(".pdf"), bbox_inches="tight")
    fig.savefig(stem.with_suffix(".png"), bbox_inches="tight", dpi=150)
    plt.close(fig)
    print(f"  wrote {stem.with_suffix('.png').name}")


for mcol, mlab in METRICS:
    for mk, ml in MARKETS:
        plot_grid(mcol, mlab, mk, ml)
print("done.")
