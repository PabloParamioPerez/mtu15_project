# STATUS: ALIVE
# LAST-AUDIT: 2026-05-30
# CLAIM: Daily per-day ajuste cost (EUR M / day) by service, 14-day rolling mean,
#        stacked area chart over the full reform-window timeline.
#        UP channels positive, DN channels positive (all are costs REE pays).
#        Reform dates marked. Pre/post-blackout background shaded.
#        NOT seasonally adjusted -- raw daily rates smoothed only with a moving
#        average.
#
# IN:  data/processed/esios/indicators/indicators_all.parquet
# OUT: figures/working/efficiency_gains_timeseries.pdf

from pathlib import Path
import duckdb
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

REPO = Path(__file__).resolve().parents[3]
IND  = REPO / "data" / "processed" / "esios" / "indicators" / "indicators_all.parquet"
FIG_DIR = REPO / "figures" / "working"
FIG_DIR.mkdir(parents=True, exist_ok=True)

WINDOW_START = "2024-01-01"
WINDOW_END   = "2025-12-31"

# Stack order (bottom up): UP first, then DN
UP_CHANNELS = [
    (1723, "TR up (real-time redispatch)",  "#2c7fb8"),
    (712,  "aFRR up (reserve)",             "#7fcdbb"),
    (1375, "Fase II up",                    "#fdae6b"),
    (1373, "Fase I up (reforzada-driven)",  "#d7301f"),
]
DN_CHANNELS = [
    (1724, "TR dn",                                "#2c7fb8"),
    (2127, "aFRR dn",                              "#7fcdbb"),
    (1376, "Fase II dn",                           "#fdae6b"),
    (1374, "Fase I dn (renewable curtailment)",    "#d7301f"),
]

REFORM_DATES = [
    ("2024-06-14", "IDA reform\n6$\\to$3"),
    ("2024-12-11", "ISP15"),
    ("2025-03-19", "MTU15-IDA"),
    ("2025-04-28", "blackout /\nreforzada"),
    ("2025-10-01", "MTU15-DA"),
]

ROLL = 14  # 14-day rolling mean


def load_daily_cost(con, indicator_id):
    """Return a DataFrame with [date, cost_eur_m] -- daily sum of the indicator
    in EUR million."""
    q = f"""
        SELECT date, SUM(value) AS cost_eur
        FROM '{IND}'
        WHERE indicator_id = {indicator_id}
          AND date BETWEEN '{WINDOW_START}' AND '{WINDOW_END}'
          AND value IS NOT NULL
        GROUP BY date
        ORDER BY date
    """
    df = con.execute(q).fetchdf()
    df["cost_eur_m"] = df["cost_eur"] / 1e6
    return df[["date", "cost_eur_m"]]


def main():
    con = duckdb.connect()

    # Build a daily-indexed dataframe with one column per channel.
    idx = pd.DataFrame(
        {"date": pd.date_range(WINDOW_START, WINDOW_END, freq="D")}
    )

    data = idx.copy()
    for ind, label, _ in UP_CHANNELS + DN_CHANNELS:
        d = load_daily_cost(con, ind)
        d.columns = ["date", label]
        d["date"] = pd.to_datetime(d["date"])
        data = data.merge(d, on="date", how="left")

    data = data.fillna(0.0)
    # For DN channels, take absolute value (TR dn is reported net of refund;
    # we treat all as gross costs REE pays providers).
    for ind, label, _ in DN_CHANNELS:
        data[label] = data[label].abs()

    # 14-day rolling mean to smooth daily noise
    smooth = data.copy()
    for ind, label, _ in UP_CHANNELS + DN_CHANNELS:
        smooth[label] = data[label].rolling(window=ROLL, center=True,
                                            min_periods=ROLL // 2).mean()

    # ----- plot -----
    fig, ax = plt.subplots(figsize=(13, 6.5))

    dates = smooth["date"]
    labels = [lab for _, lab, _ in UP_CHANNELS + DN_CHANNELS]
    colors = [c for _, _, c in UP_CHANNELS + DN_CHANNELS]
    series = [smooth[lab].values for lab in labels]

    # Stacked area: UP bottom, DN top
    ax.stackplot(dates, *series, labels=labels, colors=colors,
                 edgecolor="white", linewidth=0.3, alpha=0.95)

    # Visual distinction for DN portions via hatching overlay
    # (matplotlib stackplot doesn't accept hatch per layer directly, so we
    # overlay hatched fills with transparency on the DN layers)
    bottom = np.zeros(len(dates))
    for ind, lab, col in UP_CHANNELS:
        bottom += smooth[lab].fillna(0).values
    # bottom now = total UP. DN starts from here.
    cum = bottom.copy()
    for ind, lab, col in DN_CHANNELS:
        vals = smooth[lab].fillna(0).values
        top = cum + vals
        ax.fill_between(dates, cum, top, facecolor="none",
                        edgecolor="white", hatch="//", linewidth=0.0, alpha=0.55)
        cum = top

    # Background shading: pre-blackout vs post-blackout
    blk = pd.Timestamp("2025-04-28")
    ax.axvspan(pd.Timestamp(WINDOW_START), blk, color="#e8f4f8", alpha=0.25, zorder=-1)
    ax.axvspan(blk, pd.Timestamp(WINDOW_END), color="#fdecea", alpha=0.25, zorder=-1)

    # Reform vertical lines + labels
    ymax = ax.get_ylim()[1]
    for date_str, txt in REFORM_DATES:
        d = pd.Timestamp(date_str)
        is_blk = "blackout" in txt
        ax.axvline(d, color="#9a2a1f" if is_blk else "gray",
                   ls=":", lw=1.4 if is_blk else 0.8, zorder=5)
        ax.text(d, ymax * 0.98, txt, ha="center", va="top", fontsize=7,
                color="gray", style="italic",
                bbox=dict(boxstyle="round,pad=0.15", fc="white", ec="none", alpha=0.9))

    # PRE / POST blackout text labels (centered on each phase, mid-height)
    ax.text(pd.Timestamp("2024-08-15"), ymax * 0.55, "PRE-blackout",
            ha="center", va="center", fontsize=11, color="#2c5777",
            fontweight="bold", style="italic", alpha=0.55,
            bbox=dict(boxstyle="round,pad=0.3", fc="white", ec="none", alpha=0.65))
    ax.text(pd.Timestamp("2025-08-15"), ymax * 0.55, "POST-blackout (reforzada)",
            ha="center", va="center", fontsize=11, color="#9a2a1f",
            fontweight="bold", style="italic", alpha=0.55,
            bbox=dict(boxstyle="round,pad=0.3", fc="white", ec="none", alpha=0.65))

    # Formatting
    ax.set_ylabel(f"Per-day ajuste cost (EUR million, {ROLL}-day rolling mean)",
                  fontsize=10)
    ax.set_title("Evolution of daily ajuste-cost composition (UP + DN, all stacked positive).",
                 fontsize=11)
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    ax.set_xlim(pd.Timestamp(WINDOW_START), pd.Timestamp(WINDOW_END))
    ax.set_ylim(bottom=0)
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=30, ha="right")
    # Legend below the plot to avoid covering the data
    ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.15), fontsize=8,
              framealpha=0.92, ncol=4)
    ax.grid(axis="y", alpha=0.3, lw=0.5)
    ax.set_axisbelow(True)

    fig.tight_layout()
    out = FIG_DIR / "efficiency_gains_timeseries.pdf"
    fig.savefig(out, bbox_inches="tight", dpi=130)
    plt.close(fig)
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
