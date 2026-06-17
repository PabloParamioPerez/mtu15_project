# STATUS: ALIVE
# LAST-AUDIT: 2026-06-06
# FEEDS: thesis/paper/thesis.tex --- tests Claim A from NeuroDATE_II
#        (3 Dec 2025): pre-DA15, IDA prices show within-hour sawtooth
#        because hourly DA blocks redistributed across 4 quarter-IDA
#        prices. Post-DA15, both sides quarter-hourly --> sawtooth
#        should disappear.
#
# TEST: weekly within-hour SD of the 4 quarter-hourly IDA prices per
#        (date, hour, session). Pre-DA15: 2025-03-19 to 2025-09-30
#        (DA60 + ID15 era). Post-DA15: 2025-10-01 to 2025-12-31.
#
# IMPORTANT: IDA3 only covers periods 49-96 (afternoon). Reported per
#        session.
#
# OUT: figures/working/fig_claim_A_within_hour_ida_sd.{pdf,png}
#      Console summary statistics.

from pathlib import Path
import duckdb
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
MCPIDA = REPO / "data/processed/omie/mercado_intradiario_subastas/precios/marginalpibc_all.parquet"
OUT_DIR = REPO / "figures/working"

DA15 = pd.to_datetime("2025-10-01")
PRE_START = "2025-03-19"
POST_END  = "2025-12-31"


def main():
    con = duckdb.connect()
    con.execute("SET threads=4; SET memory_limit='6GB'")

    # Fetch quarter-hourly IDA prices per session; map to hour-of-day.
    df = con.execute(f"""
        SELECT CAST(date AS DATE) AS d, session_number AS sess, period,
               price_es_eur_mwh AS p,
               CAST(CEIL(period / 4.0) AS INT) AS hour
        FROM '{MCPIDA}'
        WHERE date BETWEEN '{PRE_START}' AND '{POST_END}'
          AND mtu_minutes = 15
          AND price_es_eur_mwh IS NOT NULL
    """).df()
    con.close()
    print(f"Quarter-hourly IDA prices: {len(df):,} rows, "
          f"{df['d'].nunique()} dates, {df['sess'].nunique()} sessions",
          flush=True)

    # Per (date, hour, session): SD of the 4 quarter-prices.
    df["d"] = pd.to_datetime(df["d"])
    g = (df.groupby(["d", "sess", "hour"], observed=True)
           .agg(sd=("p", "std"), n=("p", "size"))
           .reset_index())
    # Need exactly 4 quarter-prices in the hour.
    g = g[g["n"] == 4]
    g["week"] = g["d"] - pd.to_timedelta(g["d"].dt.weekday, unit="D")
    g["regime"] = np.where(g["d"] < DA15, "pre_DA15", "post_DA15")

    # Per-session summary.
    print("\n=== Within-hour IDA quarter-price SD (EUR/MWh), by session × regime ===")
    print(f"{'Session':>7s} {'Regime':>10s} {'N (hour×date)':>14s}  "
          f"{'mean SD':>10s}  {'median SD':>10s}  {'p25':>7s}  {'p75':>7s}")
    for sess in (1, 2, 3):
        for reg in ("pre_DA15", "post_DA15"):
            sub = g[(g["sess"] == sess) & (g["regime"] == reg)]["sd"]
            if len(sub) == 0:
                continue
            print(f"{'IDA'+str(sess):>7s} {reg:>10s} {len(sub):>14,d}  "
                  f"{sub.mean():>10.3f}  {sub.median():>10.3f}  "
                  f"{sub.quantile(0.25):>7.3f}  {sub.quantile(0.75):>7.3f}")

    # Critical caveat: IDA3 only has periods 49+, so restrict cross-session
    # comparison to hours 13-24 where all three sessions cover.
    print("\n=== Restricted to hours 13-24 (where IDA3 also covers) ===")
    g_late = g[g["hour"] >= 13]
    print(f"{'Session':>7s} {'Regime':>10s} {'N':>14s}  {'mean SD':>10s}  "
          f"{'median SD':>10s}  {'pre→post %Δ med':>16s}")
    for sess in (1, 2, 3):
        pre  = g_late[(g_late["sess"] == sess) & (g_late["regime"] == "pre_DA15")]["sd"]
        post = g_late[(g_late["sess"] == sess) & (g_late["regime"] == "post_DA15")]["sd"]
        if len(pre) == 0 or len(post) == 0:
            continue
        delta_pct = 100.0 * (post.median() - pre.median()) / pre.median()
        print(f"{'IDA'+str(sess):>7s} {'pre_DA15':>10s} {len(pre):>14,d}  "
              f"{pre.mean():>10.3f}  {pre.median():>10.3f}")
        print(f"{'IDA'+str(sess):>7s} {'post_DA15':>10s} {len(post):>14,d}  "
              f"{post.mean():>10.3f}  {post.median():>10.3f}  "
              f"{delta_pct:>15.1f}%")

    # Weekly plot per session.
    w = g.groupby(["week", "sess"], observed=True).agg(sd=("sd", "median")).reset_index()
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    fig, axes = plt.subplots(3, 1, figsize=(11, 7), sharex=True)
    for ax, sess in zip(axes, (1, 2, 3)):
        sub = w[w["sess"] == sess].sort_values("week")
        ax.plot(sub["week"], sub["sd"], lw=1.5, color="tab:blue",
                marker="o", markersize=2.0)
        ax.axvline(DA15, color="tab:green", ls="--", lw=0.9,
                   label="DA15 (2025-10-01)")
        ax.set_title(f"IDA{sess} weekly median within-hour SD of quarter-prices",
                     fontsize=10, weight="bold")
        ax.set_ylabel("SD (EUR/MWh)", fontsize=9)
        ax.grid(alpha=0.3)
        ax.legend(loc="best", fontsize=8, frameon=False)
        ax.tick_params(labelsize=7)
        ax.xaxis.set_major_locator(mdates.MonthLocator(interval=1))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    axes[-1].set_xlabel("Week", fontsize=9)
    fig.suptitle("Claim A test: pre-DA15 sawtooth in IDA quarter-prices "
                 "should disappear post-DA15", fontsize=10)
    fig.tight_layout()
    OUT = OUT_DIR / "fig_claim_A_within_hour_ida_sd"
    plt.savefig(f"{OUT}.pdf", bbox_inches="tight")
    plt.savefig(f"{OUT}.png", bbox_inches="tight", dpi=130)
    plt.close(fig)
    print(f"\nsaved {OUT}.pdf")


if __name__ == "__main__":
    main()
