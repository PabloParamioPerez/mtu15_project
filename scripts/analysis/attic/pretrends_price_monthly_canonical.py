# STATUS: ALIVE
# LAST-AUDIT: 2026-05-14
# FEEDS: thesis paper.tex §A.12 (parallel-trends visual diagnostic, prices)
# CLAIM: Monthly evolution of (i) mean IDA Spanish clearing price
#        across the 3 auction sessions, (ii) DA Spanish clearing price,
#        per hour-class, Jan 2024 - Dec 2025.  Single combined figure.

from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

REPO = Path(__file__).resolve().parents[3]

MPIBC = REPO / "data" / "processed" / "omie" / "mercado_intradiario_subastas" / "precios" / "marginalpibc_all.parquet"
MPDBC = REPO / "data" / "processed" / "omie" / "mercado_diario" / "precios" / "marginalpdbc_all.parquet"

OUTDIR = REPO / "results" / "regressions" / "firm" / "parallel_trends"
OUTDIR.mkdir(parents=True, exist_ok=True)
FIGDIR = REPO / "figures" / "thesis"

START = "2024-01-01"
END   = "2026-03-01"
REFORM = pd.Timestamp("2025-10-01")
MTU15_IDA = pd.Timestamp("2025-03-19")

CRITICAL_HOURS = (5, 6, 7, 8, 16, 17, 18, 19, 20, 21, 22)
FLAT_HOURS = (1, 2, 3)


def build_ida():
    """Per (month, hour-class): mean Spanish IDA clearing price.
    Two variants: (i) mean across the 3 sessions covering each
    (date, clock-hour), (ii) final session only."""
    con = duckdb.connect()
    con.execute("PRAGMA threads = 4")

    crit_list = ",".join(map(str, CRITICAL_HOURS))
    flat_list = ",".join(map(str, FLAT_HOURS))

    df = con.execute(
        f"""
        WITH base AS (
            SELECT date::DATE AS d, session_number, period, mtu_minutes,
                   CASE WHEN mtu_minutes = 60 THEN period - 1
                        WHEN mtu_minutes = 15 THEN (period - 1) // 4
                        ELSE NULL END AS hour,
                   price_es_eur_mwh AS p
            FROM '{MPIBC}'
            WHERE date::DATE >= DATE '{START}' AND date::DATE < DATE '{END}'
              AND price_es_eur_mwh IS NOT NULL
        ),
        per_date_hour_session AS (
            SELECT d, hour, session_number, AVG(p) AS p_hr
            FROM base
            WHERE hour BETWEEN 0 AND 23
            GROUP BY 1, 2, 3
        ),
        per_date_hour AS (
            SELECT d, hour, AVG(p_hr) AS p_avg,
                   MAX(session_number) AS last_sess
            FROM per_date_hour_session
            GROUP BY 1, 2
        ),
        final_only AS (
            SELECT a.d, a.hour, a.p_hr AS p_final
            FROM per_date_hour_session a
            JOIN per_date_hour b
              ON a.d = b.d AND a.hour = b.hour AND a.session_number = b.last_sess
        ),
        merged AS (
            SELECT pdh.d, pdh.hour, pdh.p_avg, fo.p_final
            FROM per_date_hour pdh JOIN final_only fo
              ON pdh.d = fo.d AND pdh.hour = fo.hour
        )
        SELECT CASE WHEN hour IN ({crit_list}) THEN 'critical'
                    WHEN hour IN ({flat_list}) THEN 'flat'
                    ELSE 'other' END AS hour_class,
               DATE_TRUNC('month', d) AS year_month,
               AVG(p_avg)   AS p_avg_sessions,
               AVG(p_final) AS p_final_session
        FROM merged
        WHERE hour IN ({crit_list}) OR hour IN ({flat_list})
        GROUP BY 1, 2
        ORDER BY 2, 1
        """
    ).df()
    df["year_month"] = pd.to_datetime(df["year_month"])
    return df


def build_da():
    """Per (month, hour-class): mean Spanish DA clearing price."""
    con = duckdb.connect()
    con.execute("PRAGMA threads = 4")

    crit_list = ",".join(map(str, CRITICAL_HOURS))
    flat_list = ",".join(map(str, FLAT_HOURS))

    df = con.execute(
        f"""
        WITH base AS (
            SELECT date::DATE AS d, period, mtu_minutes,
                   CASE WHEN mtu_minutes = 60 THEN period - 1
                        WHEN mtu_minutes = 15 THEN (period - 1) // 4
                        ELSE NULL END AS hour,
                   price_es_eur_mwh AS p
            FROM '{MPDBC}'
            WHERE date::DATE >= DATE '{START}' AND date::DATE < DATE '{END}'
              AND price_es_eur_mwh IS NOT NULL
        ),
        per_date_hour AS (
            SELECT d, hour, AVG(p) AS p_da
            FROM base
            WHERE hour BETWEEN 0 AND 23
            GROUP BY 1, 2
        )
        SELECT CASE WHEN hour IN ({crit_list}) THEN 'critical'
                    WHEN hour IN ({flat_list}) THEN 'flat'
                    ELSE 'other' END AS hour_class,
               DATE_TRUNC('month', d) AS year_month,
               AVG(p_da) AS p_da
        FROM per_date_hour
        WHERE hour IN ({crit_list}) OR hour IN ({flat_list})
        GROUP BY 1, 2
        ORDER BY 2, 1
        """
    ).df()
    df["year_month"] = pd.to_datetime(df["year_month"])
    return df


def pivot(df, value_col):
    wide = df.pivot_table(index="year_month",
                          columns="hour_class",
                          values=value_col).reset_index()
    wide.columns.name = None
    return wide.sort_values("year_month")


def _plot_one(ax, wide, title):
    crit_base = wide["critical"].dropna().iloc[0]
    flat_base = wide["flat"].dropna().iloc[0]
    wide = wide.copy()
    wide["crit_dev"] = wide["critical"] - crit_base
    wide["flat_dev"] = wide["flat"] - flat_base
    ax.plot(wide["year_month"], wide["crit_dev"], marker="o",
            color="C3", linewidth=1.6, markersize=4.5, label="Critical")
    ax.plot(wide["year_month"], wide["flat_dev"], marker="o",
            color="C0", linewidth=1.6, markersize=4.5, label="Flat")
    ax.axvline(REFORM, color="red", linestyle="--", linewidth=1.0)
    ax.axvline(MTU15_IDA, color="gray", linestyle=":", linewidth=0.9)
    ax.axhline(0, color="black", linewidth=0.5)
    ax.set_title(title, fontsize=10)
    ax.set_ylabel("Deviation from Jan-2024 baseline (EUR/MWh)", fontsize=9)
    ax.grid(alpha=0.3)
    ax.tick_params(labelsize=8)
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    for lbl in ax.get_xticklabels():
        lbl.set_rotation(45)
        lbl.set_ha("right")
    ax.legend(loc="upper left", fontsize=8, frameon=False)


def plot_combined(df_ida, df_da):
    """1x3 grid: IDA mean-of-3, IDA final-session, DA."""
    fig, axes = plt.subplots(1, 3, figsize=(18, 4.5), sharex=True)
    _plot_one(axes[0], pivot(df_ida, "p_avg_sessions"),
              "IDA -- mean of 3 sessions")
    _plot_one(axes[1], pivot(df_ida, "p_final_session"),
              "IDA -- final session only")
    _plot_one(axes[2], pivot(df_da, "p_da"),
              "Day-ahead")
    fig.suptitle("Pre-trend diagnostic: monthly mean clearing price by hour-class -- IDA vs DA (red dashed = Oct 2025 MTU15-DA reform; grey dotted = Mar 2025 MTU15-IDA)",
                 fontsize=11, y=1.00)
    fig.tight_layout(rect=[0, 0, 1, 0.95])
    out = FIGDIR / "fig_pretrends_price_monthly"
    for ext in ("pdf", "png"):
        fig.savefig(f"{out}.{ext}", bbox_inches="tight", dpi=130 if ext == "png" else None)
    plt.close(fig)
    print(f"saved {out}.pdf")


def main():
    print("Building monthly IDA-price panel (canonical hours)...")
    df_ida = build_ida()
    print(f"  {len(df_ida):,} cells")
    print("\nBuilding monthly DA-price panel (canonical hours)...")
    df_da = build_da()
    print(f"  {len(df_da):,} cells")

    df_ida.to_csv(OUTDIR / "p_monthly_critical_flat_marketwide_ida.csv", index=False)
    df_da.to_csv(OUTDIR / "p_monthly_critical_flat_marketwide_da.csv", index=False)

    plot_combined(df_ida, df_da)


if __name__ == "__main__":
    main()
