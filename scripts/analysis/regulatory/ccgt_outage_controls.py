# STATUS: ALIVE
# LAST-AUDIT: 2026-05-31
# CLAIM: CCGT outage-controlled availability per regime, to defend or
#        wound the §6 q1 drop interpretation. We compute, per ISP,
#        total CCGT nominal capacity minus capacity on outage (planned
#        + forced), then average per regime. Compares to PHF cleared.
#        If outages explain the q1 drop, available-MW falls in lockstep
#        with cleared-MW; if not, the strategic-withholding reading
#        survives.
#
# IN:  data/processed/entsoe/outages/outages_{planned,forced}_all.parquet
#      data/processed/omie/.../phf_all.parquet
#      data/derived/panels/bid_shape_critical_flat/_unit_map.parquet
# OUT: results/regressions/regulatory/ccgt_outages/ccgt_avail_per_regime.csv

from pathlib import Path

import duckdb

REPO = Path(__file__).resolve().parents[3]
OP = REPO / "data/processed/entsoe/outages/outages_planned_all.parquet"
OF = REPO / "data/processed/entsoe/outages/outages_forced_all.parquet"
PHF = REPO / "data/processed/omie/mercado_intradiario_subastas/programas/phf_all.parquet"
UMAP = REPO / "data/derived/panels/bid_shape_critical_flat/_unit_map.parquet"
OUT = REPO / "results/regressions/regulatory/ccgt_outages"
OUT.mkdir(parents=True, exist_ok=True)


def main():
    con = duckdb.connect()
    con.execute("SET memory_limit='6GB'"); con.execute("SET threads=4")

    # Spanish CCGT fleet nominal capacity (from outage records,
    # taking the modal nominal_mw per unit). Used as the "100% available"
    # denominator.
    q_fleet = f"""
    WITH all_op AS (
      SELECT unit_eic, nominal_mw FROM '{OP}' WHERE psr_type='B04'
      UNION ALL
      SELECT unit_eic, nominal_mw FROM '{OF}' WHERE psr_type='B04'
    )
    SELECT COUNT(DISTINCT unit_eic) AS n_units,
           ROUND(SUM(DISTINCT nominal_mw)) AS sum_nominal_mw_approx
    FROM (SELECT DISTINCT unit_eic, nominal_mw FROM all_op)
    """
    print("=== CCGT fleet (from outage records) ===")
    print(con.execute(q_fleet).fetchdf().to_string(index=False))

    # CCGT capacity on outage per day, averaged per regime.
    # nominal_mw = unit nameplate; min_avail_mw = available capacity during
    # the outage (often 0 = total trip, sometimes partial derate). So
    # unavailable_mw = nominal_mw - min_avail_mw, integrated over the
    # outage window.
    q = f"""
    WITH outages AS (
      SELECT unit_eic, nominal_mw, min_avail_mw, start_utc, end_utc, 'planned' AS src
      FROM '{OP}' WHERE psr_type='B04'
      UNION ALL
      SELECT unit_eic, nominal_mw, min_avail_mw, start_utc, end_utc, 'forced' AS src
      FROM '{OF}' WHERE psr_type='B04'
    ),
    days AS (
      SELECT
        CAST(d AS DATE) AS d,
        CASE WHEN d < DATE '2024-06-14' THEN '1.pre-IDA'
             WHEN d < DATE '2024-12-01' THEN '2.3-sess'
             WHEN d < DATE '2025-03-19' THEN '3.ISP15-win'
             WHEN d < DATE '2025-04-28' THEN '4.MTU15-IDA pre-blk'
             WHEN d < DATE '2025-10-01' THEN '5.MTU15-IDA post-blk'
             ELSE                          '6.DA15/ID15' END AS regime
      FROM generate_series(DATE '2024-01-01', DATE '2026-04-30',
                           INTERVAL '1 day') t(d)
    ),
    day_outages AS (
      -- For each day, sum (nominal_mw - min_avail_mw) over outages
      -- whose window covers any part of that day. Approximate: counts
      -- each outage as fully unavailable for any day it overlaps; gives
      -- an upper bound on per-day unavailable MW.
      SELECT d.d, d.regime,
             SUM(CASE WHEN COALESCE(o.min_avail_mw, 0) < o.nominal_mw
                      THEN (o.nominal_mw - COALESCE(o.min_avail_mw, 0))
                      ELSE 0 END) AS unavailable_mw,
             SUM(CASE WHEN o.src='forced' AND COALESCE(o.min_avail_mw, 0) < o.nominal_mw
                      THEN (o.nominal_mw - COALESCE(o.min_avail_mw, 0))
                      ELSE 0 END) AS unavailable_mw_forced,
             SUM(CASE WHEN o.src='planned' AND COALESCE(o.min_avail_mw, 0) < o.nominal_mw
                      THEN (o.nominal_mw - COALESCE(o.min_avail_mw, 0))
                      ELSE 0 END) AS unavailable_mw_planned
      FROM days d
      LEFT JOIN outages o
        ON o.start_utc < d.d + INTERVAL '1 day'
       AND o.end_utc   > d.d
      GROUP BY 1, 2
    )
    SELECT regime,
           COUNT(*) AS n_days,
           ROUND(AVG(unavailable_mw))         AS avg_unavail_mw_total,
           ROUND(AVG(unavailable_mw_forced))  AS avg_unavail_mw_forced,
           ROUND(AVG(unavailable_mw_planned)) AS avg_unavail_mw_planned
    FROM day_outages
    GROUP BY regime ORDER BY regime
    """
    df_out = con.execute(q).fetchdf()
    df_out.to_csv(OUT / "ccgt_outage_per_regime.csv", index=False)
    print("\n=== CCGT unavailable MW per regime (daily avg) ===")
    print(df_out.to_string(index=False))

    # PHF cleared GWh/day for CCGT, same regime split
    q_phf = f"""
    WITH phf AS (
      SELECT date::DATE AS d, period, unit_code, mtu_minutes,
             MAX_BY(assigned_power_mw, session_number) AS mw
      FROM '{PHF}' WHERE date >= '2024-01-01' AND period BETWEEN 1 AND 96
      GROUP BY date, period, unit_code, mtu_minutes
    ),
    phf_ccgt AS (
      SELECT p.d,
             SUM(GREATEST(p.mw, 0) * p.mtu_minutes / 60.0) AS mwh_ccgt
      FROM phf p
      JOIN (SELECT unit_code FROM '{UMAP}' WHERE tech_group = 'CCGT')
        USING (unit_code)
      WHERE p.mw IS NOT NULL
      GROUP BY p.d
    )
    SELECT
      CASE WHEN d < DATE '2024-06-14' THEN '1.pre-IDA'
           WHEN d < DATE '2024-12-01' THEN '2.3-sess'
           WHEN d < DATE '2025-03-19' THEN '3.ISP15-win'
           WHEN d < DATE '2025-04-28' THEN '4.MTU15-IDA pre-blk'
           WHEN d < DATE '2025-10-01' THEN '5.MTU15-IDA post-blk'
           ELSE                          '6.DA15/ID15' END AS regime,
      COUNT(*) AS n_days,
      ROUND(AVG(mwh_ccgt) / 1000.0, 2) AS gwh_phf_per_day
    FROM phf_ccgt GROUP BY regime ORDER BY regime
    """
    print("\n=== CCGT PHF cleared GWh/day per regime ===")
    df_phf = con.execute(q_phf).fetchdf()
    print(df_phf.to_string(index=False))


if __name__ == "__main__":
    main()
