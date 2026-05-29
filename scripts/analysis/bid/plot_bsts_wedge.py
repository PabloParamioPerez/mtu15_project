# STATUS: ALIVE
# LAST-AUDIT: 2026-05-29
# FEEDS: advisor_memo.tex sec 4(ii) -- single BSTS on the DA - IDA daily
#        wedge over the full available history. Pre-window 2022-01-01 ->
#        2025-03-18 (all data pre-ID15); post-window 2025-03-19 ->
#        2026-04-27 (covers both reforms + the 2025-04-28 blackout). One
#        BSTS, one counterfactual, full history overlaid.
#
# OUT: figures/thesis/fig_bsts_wedge.pdf

from pathlib import Path
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
PANEL = REPO / "data/derived/panels/bsts_daily_panel.parquet"
PW = REPO / "results/regressions/bid/mtu15_critical_flat/pointwise"
OUT = REPO / "figures/thesis/fig_bsts_wedge.pdf"

WINDOW = 7  # 7-day moving average for the observed series
EVENTS = [
    (pd.Timestamp("2024-06-14"), "IDA reform"),
    (pd.Timestamp("2024-12-11"), "ISP15"),
    (pd.Timestamp("2025-03-19"), "ID15"),
    (pd.Timestamp("2025-04-28"), "Blackout"),
    (pd.Timestamp("2025-10-01"), "DA15"),
]
EVENT_COLOR = {
    "ID15": "purple", "DA15": "purple",
    "Blackout": "green", "ISP15": "gray", "IDA reform": "gray",
}

CUTOVER = pd.Timestamp("2025-03-19")
PRE_LO  = pd.Timestamp("2022-01-01")
POST_HI = pd.Timestamp("2026-04-27")


def smooth(s):
    return s.rolling(WINDOW, min_periods=1, center=True).mean()


def main():
    OUT.parent.mkdir(parents=True, exist_ok=True)

    # Observed wedge (full panel, 7-day smoothed)
    base = pd.read_parquet(PANEL)[["d", "da_price_eur", "ida_price_eur"]].copy()
    base["d"] = pd.to_datetime(base["d"])
    base["wedge"] = base["da_price_eur"] - base["ida_price_eur"]
    base["w_smooth"] = smooth(base["wedge"])

    # BSTS pointwise series (LONG-real run)
    pw = pd.read_csv(PW / "bsts_wedge_pointwise_LONG_real.csv")
    pw["date"] = pd.to_datetime(pw["date"])
    in_pre = pw["date"] < CUTOVER
    in_post = pw["date"] >= CUTOVER

    fig, ax = plt.subplots(figsize=(14, 4.8))

    # Observed
    ax.plot(base["d"], base["w_smooth"], color="black", lw=1.0, alpha=0.85,
             label="Observed wedge (7-day MA)", zorder=3)

    # In-sample fit on pre-window + 95% band
    ax.fill_between(pw.loc[in_pre, "date"],
                     pw.loc[in_pre, "point.pred.lower"],
                     pw.loc[in_pre, "point.pred.upper"],
                     color="#3a7bd5", alpha=0.18, zorder=1)
    ax.plot(pw.loc[in_pre, "date"], pw.loc[in_pre, "point.pred"],
             color="#3a7bd5", lw=1.0, alpha=0.9,
             label="BSTS in-sample fit (pre-ID15)", zorder=2)

    # Counterfactual on post-window + 95% band
    ax.fill_between(pw.loc[in_post, "date"],
                     pw.loc[in_post, "point.pred.lower"],
                     pw.loc[in_post, "point.pred.upper"],
                     color="#c0392b", alpha=0.18, zorder=1)
    ax.plot(pw.loc[in_post, "date"], pw.loc[in_post, "point.pred"],
             color="#c0392b", ls="--", lw=1.6,
             label="BSTS counterfactual (post)", zorder=4)

    # Event vlines + labels at top
    ymin, ymax = -25, 25
    for ev_date, ev_label in EVENTS:
        if ev_date < PRE_LO or ev_date > POST_HI:
            continue
        color = EVENT_COLOR.get(ev_label, "gray")
        ax.axvline(ev_date, color=color, ls=":", lw=0.8, alpha=0.7, zorder=0)
        ax.annotate(ev_label, xy=(ev_date, ymax),
                     xytext=(2, -4), textcoords="offset points",
                     fontsize=8, color=color, ha="left", va="top", rotation=0)

    # Pre/post shading
    ax.axvspan(PRE_LO, CUTOVER - pd.Timedelta(days=1),
                color="#3a7bd5", alpha=0.04, zorder=0)
    ax.axvspan(CUTOVER, POST_HI, color="#c0392b", alpha=0.04, zorder=0)

    # Zero line
    ax.axhline(0, color="gray", lw=0.5, alpha=0.5, zorder=0)

    ax.set_title("BSTS counterfactual on the daily DA $-$ IDA wedge "
                  "(single run, pre-window 2022-01-01 to 2025-03-18, "
                  "post covers both reforms)",
                  fontsize=10.5, loc="left", pad=10)
    ax.set_ylabel("DA $-$ IDA price (EUR/MWh)")
    ax.set_ylim(ymin, ymax)
    ax.xaxis.set_major_locator(mdates.MonthLocator(bymonth=[1, 7]))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    ax.grid(axis="y", alpha=0.25, lw=0.5)
    for spine in ("top", "right"):
        ax.spines[spine].set_visible(False)
    ax.legend(loc="lower left", fontsize=9, frameon=False)

    fig.tight_layout()
    fig.savefig(OUT, bbox_inches="tight")
    print(f"Wrote {OUT}")


if __name__ == "__main__":
    main()
