# STATUS: ALIVE
# LAST-AUDIT: 2026-05-16
# FEEDS: parallel-trends check on RELATIVE repositioning
# CLAIM: Pre-trend diagnostic on q_2 / q_1 (intraday upward repositioning
#        as % of DA cleared volume) — the scale-free version of the
#        Ito-Reguant outcome. Companion to pretrends_q2_monthly_canonical.py
#        which uses absolute q_2.

from __future__ import annotations

from pathlib import Path
import sys

import duckdb
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "src"))
from mtu.classification.units import (  # noqa: E402
    firm_unit_panel,
    TREATMENT_PARENTS_SHORT as PIVOTAL,
    PLACEBO_PARENTS_SHORT as NON_PIVOTAL,
)

PIBCI = REPO / "data" / "processed" / "omie" / "mercado_intradiario_subastas" / "programas" / "pibci_all.parquet"
PDBC  = REPO / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbc_all.parquet"
UNITS_CSV = REPO / "data" / "external" / "omie_reference" / "lista_unidades.csv"

OUTDIR = REPO / "results" / "regressions" / "firm" / "parallel_trends"
OUTDIR.mkdir(parents=True, exist_ok=True)
FIGDIR = REPO / "figures" / "thesis"

START = "2024-01-01"
END   = "2026-03-01"
REFORM = pd.Timestamp("2025-10-01")
MTU15_IDA = pd.Timestamp("2025-03-19")

CRITICAL_HOURS = (5, 6, 7, 8, 16, 17, 18, 19, 20, 21, 22)
FLAT_HOURS = (1, 2, 3)
Q1_FLOOR_MWH = 5.0  # exclude unit-day-hours with q_1 < 5 MWh to keep ratio defined

FIRM_DISPLAY = {
    "IB": "Iberdrola", "GE": "Endesa", "GN": "Naturgy", "HC": "EDP-Spain",
    "EDP-PT": "EDP-Portugal", "Repsol": "Repsol", "Engie": "Engie",
    "TotalEnergies": "TotalEnergies", "Moeve": "Moeve",
}


def build_panel():
    """Per (firm, month, hour-class): mean(q_2/q_1) % across (unit, date, clock-hour) cells.

    q_1 = SUM(pdbc.MW × mtu/60) within (date, unit, clock-hour) — DA cleared MWh in that hour.
    q_2 = SUM(pibci.MW × mtu/60) within (date, unit, clock-hour) — net IDA cleared MWh,
          summed across IDA sessions.
    Ratio computed per (unit, date, hour) cell, then averaged within (firm, month, hour-class).
    Cells with q_1 < Q1_FLOOR_MWH dropped (ratio undefined / unstable).
    """
    units = firm_unit_panel(csv_path=str(UNITS_CSV), scheme="short", mode="primary_owner")
    keep = units[units["parent"].isin(list(PIVOTAL) + list(NON_PIVOTAL))][["unit_code", "parent"]]
    con = duckdb.connect(); con.execute("PRAGMA threads = 4"); con.execute("SET memory_limit = '10GB'")
    con.register("uft", keep)
    crit_list = ",".join(map(str, CRITICAL_HOURS))
    flat_list = ",".join(map(str, FLAT_HOURS))
    q = f"""
    WITH q1 AS (
        SELECT date::DATE AS d, unit_code,
               CASE WHEN mtu_minutes = 60 THEN period - 1
                    WHEN mtu_minutes = 15 THEN (period - 1) // 4
                    ELSE NULL END AS hour,
               SUM(assigned_power_mw * mtu_minutes / 60.0) AS q1_mwh
        FROM '{PDBC}'
        WHERE date::DATE >= DATE '{START}' AND date::DATE < DATE '{END}'
          AND assigned_power_mw IS NOT NULL
        GROUP BY 1, 2, 3
    ),
    q2 AS (
        SELECT date::DATE AS d, unit_code,
               CASE WHEN mtu_minutes = 60 THEN period - 1
                    WHEN mtu_minutes = 15 THEN (period - 1) // 4
                    ELSE NULL END AS hour,
               SUM(assigned_power_mw * mtu_minutes / 60.0) AS q2_mwh
        FROM '{PIBCI}'
        WHERE date::DATE >= DATE '{START}' AND date::DATE < DATE '{END}'
        GROUP BY 1, 2, 3
    ),
    joined AS (
        SELECT q1.d, q1.unit_code, q1.hour,
               u.parent,
               q1.q1_mwh,
               COALESCE(q2.q2_mwh, 0) AS q2_mwh,
               CASE WHEN q1.hour IN ({crit_list}) THEN 'critical'
                    WHEN q1.hour IN ({flat_list}) THEN 'flat'
                    ELSE 'other' END AS hour_class
        FROM q1
          JOIN uft u ON q1.unit_code = u.unit_code
          LEFT JOIN q2 ON q1.d = q2.d AND q1.unit_code = q2.unit_code AND q1.hour = q2.hour
        WHERE q1.hour BETWEEN 0 AND 23
          AND q1.q1_mwh >= {Q1_FLOOR_MWH}
    )
    SELECT parent, hour_class,
           DATE_TRUNC('month', d) AS year_month,
           AVG(100.0 * q2_mwh / q1_mwh) AS q2_q1_pct_mean,
           AVG(q1_mwh) AS q1_mean,
           AVG(q2_mwh) AS q2_mean,
           COUNT(*) AS n_cells
    FROM joined
    WHERE hour_class IN ('critical', 'flat')
    GROUP BY 1, 2, 3
    """
    df = con.execute(q).df()
    df["year_month"] = pd.to_datetime(df["year_month"])
    return df


def plot_pretrends(df: pd.DataFrame):
    firms_to_plot = ["IB", "GE", "GN", "HC",
                      "Repsol", "Engie", "TotalEnergies", "Moeve"]
    wide = df.pivot_table(index=["parent", "year_month"], columns="hour_class",
                           values="q2_q1_pct_mean").reset_index()
    wide.columns.name = None

    fig, axes = plt.subplots(2, 4, figsize=(15, 6), sharex=True)
    for ax, firm in zip(axes.flatten(), firms_to_plot):
        sub = wide[wide["parent"] == firm].sort_values("year_month").copy()
        if len(sub) > 0:
            crit_base = sub["critical"].iloc[0] if "critical" in sub else np.nan
            flat_base = sub["flat"].iloc[0]     if "flat" in sub     else np.nan
            if "critical" in sub:
                ax.plot(sub["year_month"], sub["critical"] - crit_base,
                        marker="o", color="C3", lw=1.4, ms=3.5, label="Critical")
            if "flat" in sub:
                ax.plot(sub["year_month"], sub["flat"] - flat_base,
                        marker="o", color="C0", lw=1.4, ms=3.5, label="Flat")
        ax.axvline(REFORM, color="red", ls="--", lw=1.0)
        ax.axvline(MTU15_IDA, color="gray", ls=":", lw=0.9)
        ax.axhline(0, color="black", lw=0.5)
        ax.set_title(FIRM_DISPLAY.get(firm, firm), fontsize=10)
        ax.grid(alpha=0.3)
        ax.tick_params(labelsize=7)
        ax.xaxis.set_major_locator(mdates.MonthLocator(interval=3))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
        for lbl in ax.get_xticklabels():
            lbl.set_rotation(45); lbl.set_ha("right")
    axes[0, 0].legend(loc="upper left", fontsize=8, frameon=False)
    for ax in axes[0]:
        ax.annotate("Pivotal", xy=(0.02, 0.92), xycoords="axes fraction",
                     fontsize=8, color="C3", fontweight="bold")
    for ax in axes[1]:
        ax.annotate("Non-pivotal", xy=(0.02, 0.92), xycoords="axes fraction",
                     fontsize=8, color="gray", fontweight="bold")
    fig.suptitle(r"Pre-trend diagnostic on RELATIVE repositioning: monthly mean $q_2/q_1$ (\%) per unit-day-hour, deviation from Jan-2024 baseline, by firm $\times$ hour-class",
                 fontsize=11, y=1.00)
    fig.tight_layout(rect=[0, 0, 1, 0.95])
    out = FIGDIR / "fig_pretrends_q2_over_q1_monthly"
    for ext in ("pdf", "png"):
        fig.savefig(f"{out}.{ext}", bbox_inches="tight", dpi=130 if ext == "png" else None)
    plt.close(fig)
    print(f"saved {out}.pdf")
    return wide


def main():
    print("Building monthly q_2/q_1 panel...")
    df = build_panel()
    print(f"  {len(df):,} (parent, month, hour-class) cells")
    print("\nCells per parent × hour-class:")
    print(df.groupby(["parent", "hour_class"])["n_cells"].sum().to_string())

    wide = plot_pretrends(df)
    wide.to_csv(OUTDIR / "q2_over_q1_monthly_canonical.csv", index=False)
    print("\n--- per-firm Jan-Feb 2024 baseline + final 3-month average ---")
    rep = []
    for firm in ("IB", "GE", "GN", "HC", "Repsol", "Engie", "TotalEnergies", "Moeve"):
        sub = wide[wide["parent"] == firm].sort_values("year_month")
        if len(sub) < 6: continue
        early = sub.head(3)
        late  = sub.tail(3)
        rep.append({
            "firm": firm,
            "early_crit_pct": round(early["critical"].mean(), 2),
            "early_flat_pct": round(early["flat"].mean(), 2),
            "late_crit_pct":  round(late["critical"].mean(), 2),
            "late_flat_pct":  round(late["flat"].mean(), 2),
        })
    print(pd.DataFrame(rep).to_string(index=False))


if __name__ == "__main__":
    main()
