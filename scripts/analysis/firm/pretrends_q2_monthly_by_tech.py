# STATUS: ALIVE
# LAST-AUDIT: 2026-05-14
# FEEDS: thesis paper.tex §5 (parallel-trends visual diagnostic, tech-stratified)
# CLAIM: Monthly evolution of q_2 (deviation from Jan-2024 baseline) per
#        tech_group, hour-class, Jan 2024 - Dec 2025, canonical hour set
#        ({5-9, 16-22} vs {1-3}). Tech mirror of pretrends_q2_monthly_canonical.

from __future__ import annotations

from pathlib import Path
import sys

import duckdb
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "src"))
from mtu.classification.units import (  # noqa: E402
    firm_unit_panel,
    TREATMENT_PARENTS_SHORT as PIVOTAL,
    PLACEBO_PARENTS_SHORT as NON_PIVOTAL,
)

PIBCI = REPO / "data" / "processed" / "omie" / "mercado_intradiario_subastas" / "programas" / "pibci_all.parquet"
UNITS_CSV = REPO / "data" / "external" / "omie_reference" / "lista_unidades.csv"

OUTDIR = REPO / "results" / "regressions" / "firm" / "parallel_trends"
OUTDIR.mkdir(parents=True, exist_ok=True)
FIGDIR = REPO / "thesis" / "paper" / "figures"

START = "2024-01-01"
END   = "2026-01-01"
REFORM = pd.Timestamp("2025-10-01")
MTU15_IDA = pd.Timestamp("2025-03-19")

CRITICAL_HOURS = (5, 6, 7, 8, 16, 17, 18, 19, 20, 21, 22)
FLAT_HOURS = (1, 2, 3)

TECHS_TO_PLOT = [
    "CCGT", "Hydro", "Coal", "Hybrid_RES_storage",
    "Wind", "Solar PV", "Nuclear", "Hydro_RES",
]


def build_panel():
    """Per (tech_group, month, hour-class): mean q_2 in MWh per
    (unit, date, clock-hour) cell. Same construction as the per-firm
    pretrends panel but grouped by tech instead of parent. Restricted
    to pivotal + non-pivotal parents for consistency with the firm plot."""
    units = firm_unit_panel(csv_path=str(UNITS_CSV), scheme="short", mode="primary_owner")
    keep = units[units["parent"].isin(list(PIVOTAL) + list(NON_PIVOTAL))][
        ["unit_code", "parent", "tech_group"]
    ]
    con = duckdb.connect()
    con.execute("PRAGMA threads = 4")
    con.execute("SET memory_limit = '10GB'")
    con.register("uft", keep)

    crit_list = ",".join(map(str, CRITICAL_HOURS))
    flat_list = ",".join(map(str, FLAT_HOURS))

    df = con.execute(
        f"""
        WITH base AS (
            SELECT date::DATE AS d,
                   period, mtu_minutes,
                   CASE WHEN mtu_minutes = 60 THEN period - 1
                        WHEN mtu_minutes = 15 THEN (period - 1) // 4
                        ELSE NULL END AS hour,
                   unit_code,
                   assigned_power_mw * mtu_minutes / 60.0 AS q2_mwh
            FROM '{PIBCI}'
            WHERE date::DATE >= DATE '{START}' AND date::DATE < DATE '{END}'
        ),
        unit_hour AS (
            SELECT b.d, b.unit_code, u.tech_group, b.hour,
                   SUM(b.q2_mwh) AS q2_mwh_hour,
                   CASE WHEN b.hour IN ({crit_list}) THEN 'critical'
                        WHEN b.hour IN ({flat_list}) THEN 'flat'
                        ELSE 'other' END AS hour_class
            FROM base b JOIN uft u ON b.unit_code = u.unit_code
            WHERE b.hour BETWEEN 0 AND 23
            GROUP BY 1,2,3,4
        )
        SELECT tech_group, hour_class,
               DATE_TRUNC('month', d) AS year_month,
               AVG(q2_mwh_hour) AS q2_mean,
               COUNT(*) AS n_cells
        FROM unit_hour
        WHERE hour_class IN ('critical', 'flat')
        GROUP BY 1, 2, 3
        """
    ).df()
    df["year_month"] = pd.to_datetime(df["year_month"])
    return df


def pivot_diff(df):
    wide = df.pivot_table(index=["tech_group", "year_month"],
                          columns="hour_class",
                          values="q2_mean").reset_index()
    wide.columns.name = None
    wide["diff"] = wide["critical"] - wide["flat"]
    return wide.sort_values(["tech_group", "year_month"])


def plot_pretrends(wide):
    """One panel per tech. Each panel shows the EVOLUTION of monthly
    mean q_2 in critical (red) and flat (blue) hours, normalised so
    each line starts at 0 at its first observation. Parallel-trends
    visual test: red and blue should overlap pre-reform."""
    fig, axes = plt.subplots(2, 4, figsize=(15, 6), sharex=True)
    for ax, tech in zip(axes.flatten(), TECHS_TO_PLOT):
        sub = wide[wide["tech_group"] == tech].sort_values("year_month").copy()
        if len(sub) > 0:
            crit_base = sub["critical"].iloc[0]
            flat_base = sub["flat"].iloc[0]
            sub["crit_dev"] = sub["critical"] - crit_base
            sub["flat_dev"] = sub["flat"] - flat_base
            ax.plot(sub["year_month"], sub["crit_dev"], marker="o",
                    color="C3", linewidth=1.4, markersize=3.5, label="Critical")
            ax.plot(sub["year_month"], sub["flat_dev"], marker="o",
                    color="C0", linewidth=1.4, markersize=3.5, label="Flat")
        ax.axvline(REFORM, color="red", linestyle="--", linewidth=1.0)
        ax.axvline(MTU15_IDA, color="gray", linestyle=":", linewidth=0.9)
        ax.axhline(0, color="black", linewidth=0.5)
        ax.set_title(tech, fontsize=10)
        ax.grid(alpha=0.3)
        ax.tick_params(labelsize=7)
        ax.xaxis.set_major_locator(mdates.MonthLocator(interval=3))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
        for lbl in ax.get_xticklabels():
            lbl.set_rotation(45)
            lbl.set_ha("right")
    axes[0, 0].legend(loc="upper left", fontsize=8, frameon=False)
    for ax in axes[0]:
        ax.annotate("Dispatchable", xy=(0.02, 0.92), xycoords="axes fraction",
                    fontsize=8, color="C3", fontweight="bold")
    for ax in axes[1]:
        ax.annotate("Non-dispatchable", xy=(0.02, 0.92), xycoords="axes fraction",
                    fontsize=8, color="gray", fontweight="bold")
    fig.suptitle(r"Pre-trend diagnostic: evolution of monthly $q_2$ relative to baseline (Jan 2024), by technology and hour-class (red dashed = Oct 2025 reform; dotted grey = Mar 2025 MTU15-IDA)",
                 fontsize=11, y=1.00)
    fig.tight_layout(rect=[0, 0, 1, 0.95])
    out = FIGDIR / "fig_pretrends_q2_monthly_by_tech"
    for ext in ("pdf", "png"):
        fig.savefig(f"{out}.{ext}", bbox_inches="tight", dpi=130 if ext == "png" else None)
    plt.close(fig)
    print(f"saved {out}.pdf")


def main():
    print("Building monthly q_2 panel by tech (canonical hours)...")
    df = build_panel()
    print(f"  {len(df):,} (tech, month, hour-class) cells")
    print("\nCells per tech x hour-class:")
    print(df.groupby(["tech_group", "hour_class"])["n_cells"].sum().to_string())

    wide = pivot_diff(df)
    out_csv = OUTDIR / "q2_monthly_critical_minus_flat_by_tech.csv"
    wide.to_csv(out_csv, index=False)
    print(f"\nSaved {out_csv}")

    plot_pretrends(wide)


if __name__ == "__main__":
    main()
