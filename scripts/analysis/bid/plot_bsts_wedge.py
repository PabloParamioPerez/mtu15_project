# STATUS: ALIVE
# LAST-AUDIT: 2026-05-29
# FEEDS: advisor_memo.tex sec 4(ii) -- BSTS counterfactual on the DA - IDA
#        daily wedge for both reforms, using the reforzada-constant windows
#        from sec 3.A. Shows the full 2022--2026 observed wedge in black,
#        with the BSTS in-sample fit on the pre-window in blue (with 95%
#        credible band) and the out-of-sample counterfactual on the post-
#        window in red dashed (with 95% credible band).
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

# (reform, side, cutover, pre_lo, post_hi)
PANELS = [
    ("ID15", "real",    pd.Timestamp("2025-03-19"),
                          pd.Timestamp("2024-06-14"),
                          pd.Timestamp("2025-04-27")),
    ("ID15", "placebo", pd.Timestamp("2024-03-19"),
                          pd.Timestamp("2023-06-14"),
                          pd.Timestamp("2024-04-27")),
    ("DA15", "real",    pd.Timestamp("2025-10-01"),
                          pd.Timestamp("2025-04-28"),
                          pd.Timestamp("2025-12-31")),
    ("DA15", "placebo", pd.Timestamp("2024-10-01"),
                          pd.Timestamp("2024-04-28"),
                          pd.Timestamp("2024-12-31")),
]


def smooth(s):
    return s.rolling(WINDOW, min_periods=1, center=True).mean()


def panel_plot(ax, reform, side, cutover, pre_lo, post_hi):
    # Observed wedge (full panel, 7-day smoothed)
    base = pd.read_parquet(PANEL)[["d", "da_price_eur", "ida_price_eur"]].copy()
    base["d"] = pd.to_datetime(base["d"])
    base["wedge"] = base["da_price_eur"] - base["ida_price_eur"]
    base["w_smooth"] = smooth(base["wedge"])
    ax.plot(base["d"], base["w_smooth"], color="black", lw=0.9, alpha=0.85,
             label="Observed (7-day MA)", zorder=3)

    # BSTS pointwise series
    pw = pd.read_csv(PW / f"bsts_wedge_pointwise_{reform}_{side}.csv")
    pw["date"] = pd.to_datetime(pw["date"])
    in_pre = pw["date"] < cutover
    in_post = pw["date"] >= cutover

    # In-sample fit on pre-window (blue) + 95% band
    ax.fill_between(pw.loc[in_pre, "date"],
                     pw.loc[in_pre, "point.pred.lower"],
                     pw.loc[in_pre, "point.pred.upper"],
                     color="#3a7bd5", alpha=0.18, zorder=1)
    ax.plot(pw.loc[in_pre, "date"], pw.loc[in_pre, "point.pred"],
             color="#3a7bd5", lw=1.0, alpha=0.9,
             label="BSTS in-sample fit (pre)", zorder=2)

    # Counterfactual on post-window (red dashed) + 95% band
    ax.fill_between(pw.loc[in_post, "date"],
                     pw.loc[in_post, "point.pred.lower"],
                     pw.loc[in_post, "point.pred.upper"],
                     color="#c0392b", alpha=0.18, zorder=1)
    ax.plot(pw.loc[in_post, "date"], pw.loc[in_post, "point.pred"],
             color="#c0392b", ls="--", lw=1.4,
             label="BSTS counterfactual (post)", zorder=4)

    # Event vlines on the full panel range
    for ev_date, ev_label in EVENTS:
        if ev_date < base["d"].min() or ev_date > base["d"].max():
            continue
        ax.axvline(ev_date, color=EVENT_COLOR.get(ev_label, "gray"),
                    ls=":", lw=0.7, alpha=0.55, zorder=0)

    # Cutover and pre-window markers
    ax.axvline(cutover, color="black", lw=0.6, alpha=0.4, zorder=0)
    ax.axvspan(pre_lo, cutover - pd.Timedelta(days=1),
                color="#3a7bd5", alpha=0.04, zorder=0)
    ax.axvspan(cutover, post_hi, color="#c0392b", alpha=0.04, zorder=0)

    # Zero line
    ax.axhline(0, color="gray", lw=0.5, alpha=0.5, zorder=0)

    title = f"{reform} {side.upper()}: DA $-$ IDA daily wedge, BSTS"
    ax.set_title(title, fontsize=10.5, loc="left")
    ax.set_ylabel("EUR/MWh")
    ax.set_ylim(-25, 25)  # constrain to readable range; observed has outliers
    ax.xaxis.set_major_locator(mdates.MonthLocator(bymonth=[1, 4, 7, 10]))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    for lbl in ax.get_xticklabels():
        lbl.set_rotation(30); lbl.set_ha("right")
    ax.grid(axis="y", alpha=0.25, lw=0.5)
    for spine in ("top", "right"):
        ax.spines[spine].set_visible(False)


def main():
    OUT.parent.mkdir(parents=True, exist_ok=True)
    fig, axes = plt.subplots(2, 2, figsize=(14, 7.5),
                              gridspec_kw={"hspace": 0.55, "wspace": 0.2})
    for ax, (reform, side, cutover, pre_lo, post_hi) in zip(axes.flat, PANELS):
        panel_plot(ax, reform, side, cutover, pre_lo, post_hi)

    # Shared legend
    handles, labels = axes[0, 0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="lower center", ncol=3, fontsize=9.5,
                frameon=False, bbox_to_anchor=(0.5, 0.01))
    fig.suptitle("BSTS counterfactual on the DA $-$ IDA daily wedge "
                  "(2022--2026 history, reforzada-constant pre-windows)",
                  fontsize=12, y=0.99)
    fig.tight_layout(rect=[0, 0.05, 1, 0.97])
    fig.savefig(OUT, bbox_inches="tight")
    print(f"Wrote {OUT}")


if __name__ == "__main__":
    main()
