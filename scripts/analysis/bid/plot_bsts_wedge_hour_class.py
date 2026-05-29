# STATUS: ALIVE
# LAST-AUDIT: 2026-05-29
# FEEDS: advisor_memo.tex sec 4(ii) -- per-hour-class BSTS on the DA - IDA
#        wedge (critical / midday / flat), one panel each. Same long pre-
#        window as the daily wedge BSTS.
#
# OUT: figures/thesis/fig_bsts_wedge_hour_class.pdf

from pathlib import Path
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
PANEL = REPO / "data/derived/panels/wedge_hour_class_panel.parquet"
PW = REPO / "results/regressions/bid/mtu15_critical_flat/pointwise"
OUT = REPO / "figures/thesis/fig_bsts_wedge_hour_class.pdf"

WINDOW = 7
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


def panel_plot(ax, observed_col, pw_name, title, show_event_labels):
    base = pd.read_parquet(PANEL)[["d", observed_col]].copy()
    base["d"] = pd.to_datetime(base["d"])
    base["smooth"] = smooth(base[observed_col])

    pw = pd.read_csv(PW / pw_name)
    pw["date"] = pd.to_datetime(pw["date"])
    in_pre = pw["date"] < CUTOVER
    in_post = pw["date"] >= CUTOVER

    ax.plot(base["d"], base["smooth"], color="black", lw=0.85, alpha=0.85,
             label="Observed (7-day MA)", zorder=3)
    ax.fill_between(pw.loc[in_pre, "date"],
                     pw.loc[in_pre, "point.pred.lower"],
                     pw.loc[in_pre, "point.pred.upper"],
                     color="#3a7bd5", alpha=0.18, zorder=1)
    ax.plot(pw.loc[in_pre, "date"], pw.loc[in_pre, "point.pred"],
             color="#3a7bd5", lw=0.9, alpha=0.9,
             label="BSTS in-sample fit (pre)", zorder=2)
    ax.fill_between(pw.loc[in_post, "date"],
                     pw.loc[in_post, "point.pred.lower"],
                     pw.loc[in_post, "point.pred.upper"],
                     color="#c0392b", alpha=0.18, zorder=1)
    ax.plot(pw.loc[in_post, "date"], pw.loc[in_post, "point.pred"],
             color="#c0392b", ls="--", lw=1.4,
             label="BSTS counterfactual (post)", zorder=4)

    ymin, ymax = -25, 25
    for ev_date, ev_label in EVENTS:
        color = EVENT_COLOR.get(ev_label, "gray")
        ax.axvline(ev_date, color=color, ls=":", lw=0.7, alpha=0.65, zorder=0)
        if show_event_labels:
            ax.annotate(ev_label, xy=(ev_date, ymax),
                         xytext=(2, -4), textcoords="offset points",
                         fontsize=7.5, color=color, ha="left", va="top")
    ax.axvspan(PRE_LO, CUTOVER - pd.Timedelta(days=1),
                color="#3a7bd5", alpha=0.04, zorder=0)
    ax.axvspan(CUTOVER, POST_HI, color="#c0392b", alpha=0.04, zorder=0)
    ax.axhline(0, color="gray", lw=0.5, alpha=0.5, zorder=0)
    ax.set_title(title, fontsize=10.5, loc="left")
    ax.set_ylabel("EUR/MWh")
    ax.set_ylim(ymin, ymax)
    ax.xaxis.set_major_locator(mdates.MonthLocator(bymonth=[1, 7]))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    ax.grid(axis="y", alpha=0.25, lw=0.5)
    for spine in ("top", "right"):
        ax.spines[spine].set_visible(False)


def main():
    OUT.parent.mkdir(parents=True, exist_ok=True)
    fig, axes = plt.subplots(3, 1, figsize=(14, 10),
                              gridspec_kw={"hspace": 0.45})
    panel_plot(axes[0], "wedge_critical",
                "bsts_wedge_hour_class_pointwise_critical.csv",
                "Critical hours wedge (BSTS effect $+0.68$, 95\\% CI "
                "$[-1.62, +2.88]$, $p{=}0.28$)", show_event_labels=True)
    panel_plot(axes[1], "wedge_midday",
                "bsts_wedge_hour_class_pointwise_midday.csv",
                "Midday hours wedge (BSTS effect $+0.44$, 95\\% CI "
                "$[-2.56, +3.34]$, $p{=}0.39$)", show_event_labels=False)
    panel_plot(axes[2], "wedge_flat",
                "bsts_wedge_hour_class_pointwise_flat.csv",
                "Flat hours wedge (BSTS effect $+0.21$, 95\\% CI "
                "$[-2.71, +2.83]$, $p{=}0.43$)", show_event_labels=False)

    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="lower center", ncol=3, fontsize=9.5,
                frameon=False, bbox_to_anchor=(0.5, 0.005))
    fig.suptitle("BSTS counterfactual on the daily DA $-$ IDA wedge, "
                  "by hour-class --- single long-history run",
                  fontsize=12, y=0.995)
    fig.tight_layout(rect=[0, 0.04, 1, 0.975])
    fig.savefig(OUT, bbox_inches="tight")
    print(f"Wrote {OUT}")


if __name__ == "__main__":
    main()
