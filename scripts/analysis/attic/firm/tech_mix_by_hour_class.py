# STATUS: ALIVE
# LAST-AUDIT: 2026-05-31
# CLAIM: Cross-tab of final-program (PHF) cleared-MWh share per
#        tech_group per hour-class, post-DA15 window. Feeds the
#        "who clears in which hours" descriptive table in
#        thesis/provisional/additional_results.tex.
#
# IN:  data/processed/omie/mercado_intradiario_subastas/programas/phf_all.parquet
#      data/derived/panels/bid_shape_critical_flat/_unit_map.parquet
# OUT: results/regressions/firm/tech_mix_by_hour_class.csv

from pathlib import Path

import duckdb
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
PHF = REPO / "data/processed/omie/mercado_intradiario_subastas/programas/phf_all.parquet"
UMAP = REPO / "data/derived/panels/bid_shape_critical_flat/_unit_map.parquet"
OUT = REPO / "results/regressions/firm/tech_mix_by_hour_class.csv"
OUT.parent.mkdir(parents=True, exist_ok=True)

WIN_START = "2025-10-01"  # post-DA15
WIN_END = "2025-12-31"

HOUR_CLASS_LABELS = {
    "critical_morning": "Morning ramp (5-8)",
    "critical_evening": "Evening ramp (16-22)",
    "midday": "Midday (11-14)",
    "flat": "Flat (1-3)",
}


def main():
    con = duckdb.connect()
    q = f"""
    WITH final_program AS (
      -- Take the last-session assigned MW per (date, period, unit)
      SELECT date, period, unit_code, mtu_minutes,
             MAX_BY(assigned_power_mw, session_number) AS mw_final
      FROM '{PHF}'
      WHERE date BETWEEN '{WIN_START}' AND '{WIN_END}'
        AND period BETWEEN 1 AND 96
      GROUP BY date, period, unit_code, mtu_minutes
    ),
    joined AS (
      SELECT
        fp.date, fp.period, fp.unit_code, fp.mw_final, fp.mtu_minutes,
        COALESCE(um.tech_group, 'Unknown') AS tech_group,
        CAST(CEIL(fp.period / 4.0) AS INT) AS clock_hour
      FROM final_program fp
      LEFT JOIN '{UMAP}' um USING (unit_code)
    ),
    classified AS (
      SELECT
        tech_group,
        CASE
          WHEN clock_hour BETWEEN 5 AND 8   THEN 'critical_morning'
          WHEN clock_hour BETWEEN 16 AND 22 THEN 'critical_evening'
          WHEN clock_hour BETWEEN 11 AND 14 THEN 'midday'
          WHEN clock_hour BETWEEN 1 AND 3   THEN 'flat'
          ELSE 'other'
        END AS hour_class,
        mw_final * mtu_minutes / 60.0 AS mwh
      FROM joined
      WHERE mw_final > 0
    ),
    agg AS (
      SELECT tech_group, hour_class, SUM(mwh) AS gwh
      FROM classified
      WHERE hour_class != 'other'
      GROUP BY tech_group, hour_class
    )
    SELECT
      tech_group,
      hour_class,
      ROUND(gwh / 1000.0, 1) AS gwh,
      ROUND(100.0 * gwh / SUM(gwh) OVER (PARTITION BY hour_class), 1)
        AS pct_of_class
    FROM agg
    ORDER BY hour_class, gwh DESC
    """
    df = con.execute(q).fetchdf()
    df.to_csv(OUT, index=False)
    print(f"wrote {OUT}")

    # Quick console print of the cross-tab
    pivot_pct = df.pivot(index="tech_group", columns="hour_class",
                        values="pct_of_class").fillna(0)
    pivot_pct = pivot_pct[["critical_morning", "critical_evening",
                           "midday", "flat"]]
    pivot_pct = pivot_pct.sort_values("critical_evening", ascending=False)
    print()
    print("Cross-tab (% of cleared MWh per hour-class):")
    print(pivot_pct.to_string())
    print()
    print("Totals per hour-class (TWh):")
    print(df.groupby("hour_class")["gwh"].sum() / 1000)


if __name__ == "__main__":
    main()
