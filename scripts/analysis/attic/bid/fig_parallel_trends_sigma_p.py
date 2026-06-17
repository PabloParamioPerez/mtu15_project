# STATUS: ALIVE
# LAST-AUDIT: 2026-06-03
# FEEDS: thesis/paper/thesis.tex --- visual parallel-trends figure
#        for the Spec C bid-shape DiD. Two panels (DA market, IDA market),
#        each with weekly mean within-quarter sigma_p for CCGT, separately
#        for critical and flat hours. Reform-date verticals mark
#        ID15 (2025-03-19), blackout (2025-04-28), and DA15 (2025-10-01).
#
# OUT: figures/thesis/fig_parallel_trends_sigma_p.{pdf,png}

from pathlib import Path

import duckdb
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
CAB  = REPO / "data/processed/omie/mercado_diario/ofertas/cab_all.parquet"
DET  = REPO / "data/processed/omie/mercado_diario/ofertas/det_all.parquet"
ICAB = REPO / "data/processed/omie/mercado_intradiario_subastas/ofertas/icab_all.parquet"
IDET = REPO / "data/processed/omie/mercado_intradiario_subastas/ofertas/idet_all.parquet"
MCPDA  = REPO / "data/processed/omie/mercado_diario/precios/marginalpdbc_all.parquet"
MCPIDA = REPO / "data/processed/omie/mercado_intradiario_subastas/precios/marginalpibc_all.parquet"
UMAP = REPO / "data/derived/panels/bid_shape_critical_flat/_unit_map.parquet"
OUT  = REPO / "figures/thesis/fig_parallel_trends_sigma_p"

H_BAND     = 50.0  # uniform bandwidth for the visualization (close to both window p90)
DATE_START = "2024-06-14"
DATE_END   = "2025-12-31"
CRIT_HOURS = "(5,6,7,8,16,17,18,19,20,21,22)"
FLAT_HOURS = "(1,2,3)"
ID15 = "2025-03-19"
BLK  = "2025-04-28"
DA15 = "2025-10-01"


def compute_sigma_da(con):
    """Daily mean σ_p of CCGT in-band tranches, DA market, split by hour-class."""
    q = f"""
    WITH u AS (SELECT unit_code FROM '{UMAP}' WHERE tech_group = 'CCGT'),
         mcp AS (
           SELECT CAST(date AS DATE) AS d, period, price_es_eur_mwh AS mcp
           FROM '{MCPDA}'
           WHERE date BETWEEN '{DATE_START}' AND '{DATE_END}'
             AND price_es_eur_mwh IS NOT NULL
         ),
         offers AS (
           SELECT CAST(c.date AS DATE) AS d, c.offer_code, c.version, c.unit_code,
                  d.period, d.price_eur_mwh AS p, d.quantity_mw AS q
           FROM '{CAB}' c JOIN '{DET}' d
             ON c.date = d.date AND c.offer_code = d.offer_code AND c.version = d.version
           WHERE c.buy_sell = 'V'
             AND d.price_eur_mwh IS NOT NULL AND d.quantity_mw IS NOT NULL AND d.quantity_mw > 0
             AND c.date BETWEEN '{DATE_START}' AND '{DATE_END}'
         ),
         banded AS (
           SELECT o.d, o.offer_code, o.version, o.unit_code, o.period, o.p,
                  CASE WHEN o.period <= 24 THEN o.period            -- MTU60 hour = period
                       ELSE CAST(CEIL(o.period / 4.0) AS INT)        -- MTU15 hour = ceil(period/4)
                  END AS hour
           FROM offers o JOIN mcp m USING (d, period) JOIN u USING (unit_code)
           WHERE ABS(o.p - m.mcp) <= {H_BAND}
         ),
         per_curve AS (
           SELECT d, unit_code, period, hour,
                  CASE WHEN COUNT(*) >= 2 THEN STDDEV_SAMP(p) ELSE 0 END AS sigma_p
           FROM banded
           GROUP BY 1,2,3,4
         )
    SELECT d, hour,
           CASE WHEN hour IN {CRIT_HOURS} THEN 'critical'
                WHEN hour IN {FLAT_HOURS} THEN 'flat' END AS hour_class,
           AVG(sigma_p) AS mean_sigma_p, COUNT(*) AS n_curves
    FROM per_curve
    WHERE hour IN {CRIT_HOURS} OR hour IN {FLAT_HOURS}
    GROUP BY 1, 2, 3
    """
    return con.execute(q).df()


def compute_sigma_ida(con):
    """Daily mean σ_p of CCGT in-band tranches, IDA market (latest-session per period)."""
    q = f"""
    WITH u AS (SELECT unit_code FROM '{UMAP}' WHERE tech_group = 'CCGT'),
         mcp AS (
           SELECT CAST(date AS DATE) AS d, period, price_es_eur_mwh AS mcp,
                  ROW_NUMBER() OVER (PARTITION BY date::DATE, period
                                      ORDER BY mtu_minutes ASC) AS rn
           FROM '{MCPIDA}'
           WHERE date BETWEEN '{DATE_START}' AND '{DATE_END}'
             AND price_es_eur_mwh IS NOT NULL
         ),
         mcp1 AS (SELECT d, period, mcp FROM mcp WHERE rn = 1),
         offers AS (
           SELECT CAST(c.date AS DATE) AS d, c.offer_code, c.version, c.unit_code,
                  d.period, d.price_eur_mwh AS p, d.quantity_mw AS q
           FROM '{ICAB}' c JOIN '{IDET}' d
             ON c.date = d.date AND c.offer_code = d.offer_code AND c.version = d.version
           WHERE c.buy_sell = 'V'
             AND d.price_eur_mwh IS NOT NULL AND d.quantity_mw IS NOT NULL AND d.quantity_mw > 0
             AND c.date BETWEEN '{DATE_START}' AND '{DATE_END}'
         ),
         banded AS (
           SELECT o.d, o.offer_code, o.version, o.unit_code, o.period, o.p,
                  CASE WHEN o.period <= 24 THEN o.period
                       ELSE CAST(CEIL(o.period / 4.0) AS INT) END AS hour
           FROM offers o JOIN mcp1 m USING (d, period) JOIN u USING (unit_code)
           WHERE ABS(o.p - m.mcp) <= {H_BAND}
         ),
         per_curve AS (
           SELECT d, unit_code, period, hour,
                  CASE WHEN COUNT(*) >= 2 THEN STDDEV_SAMP(p) ELSE 0 END AS sigma_p
           FROM banded
           GROUP BY 1,2,3,4
         )
    SELECT d, hour,
           CASE WHEN hour IN {CRIT_HOURS} THEN 'critical'
                WHEN hour IN {FLAT_HOURS} THEN 'flat' END AS hour_class,
           AVG(sigma_p) AS mean_sigma_p, COUNT(*) AS n_curves
    FROM per_curve
    WHERE hour IN {CRIT_HOURS} OR hour IN {FLAT_HOURS}
    GROUP BY 1, 2, 3
    """
    return con.execute(q).df()


def weekly_aggregate(df):
    df = df.copy()
    df["week"] = pd.to_datetime(df["d"]) - pd.to_timedelta(
        pd.to_datetime(df["d"]).dt.weekday, unit="D")
    w = (df.groupby(["week", "hour_class"], observed=True)
           .agg(mean_sigma_p=("mean_sigma_p", "mean"),
                n_curves=("n_curves", "sum")).reset_index())
    return w


def plot_panel(ax, w, title):
    for cls, col in [("critical", "tab:red"), ("flat", "tab:blue")]:
        sub = w[w["hour_class"] == cls].sort_values("week")
        ax.plot(sub["week"], sub["mean_sigma_p"], lw=1.6, color=col,
                label=cls.capitalize(), marker="o", markersize=2.2)
    for date_str, label, color in [
        (ID15, "ID15", "tab:purple"),
        (BLK,  "Blackout", "tab:gray"),
        (DA15, "DA15", "tab:green"),
    ]:
        d = pd.to_datetime(date_str)
        ax.axvline(d, color=color, ls="--", lw=1.0, alpha=0.7)
        ax.text(d, ax.get_ylim()[1] * 0.97, "  " + label,
                rotation=90, va="top", ha="left", fontsize=8, color=color)
    ax.set_title(title, fontsize=11, weight="bold")
    ax.set_ylabel("Mean $\\sigma_p$ (EUR/MWh)", fontsize=9)
    ax.grid(alpha=0.3)
    ax.tick_params(labelsize=8)
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    ax.legend(loc="upper left", fontsize=8, frameon=False)


def main():
    con = duckdb.connect(); con.execute("SET threads=4; SET memory_limit='4GB'")
    print("Computing DA σ_p time series ...")
    da_daily = compute_sigma_da(con)
    print(f"  DA daily rows: {len(da_daily)}")
    print("Computing IDA σ_p time series ...")
    ida_daily = compute_sigma_ida(con)
    print(f"  IDA daily rows: {len(ida_daily)}")
    con.close()

    da_weekly  = weekly_aggregate(da_daily)
    ida_weekly = weekly_aggregate(ida_daily)

    fig, axes = plt.subplots(2, 1, figsize=(11, 6.6), sharex=True)
    plot_panel(axes[0], da_weekly,
               "CCGT day-ahead within-quarter $\\sigma_p$ "
               "(critical = ramp hours, flat = overnight)")
    plot_panel(axes[1], ida_weekly,
               "CCGT intraday-auction within-quarter $\\sigma_p$")
    axes[1].set_xlabel("Week", fontsize=9)
    fig.tight_layout()
    OUT.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(f"{OUT}.pdf", bbox_inches="tight")
    plt.savefig(f"{OUT}.png", bbox_inches="tight", dpi=130)
    plt.close(fig)
    print(f"saved {OUT}.pdf / .png")


if __name__ == "__main__":
    main()
