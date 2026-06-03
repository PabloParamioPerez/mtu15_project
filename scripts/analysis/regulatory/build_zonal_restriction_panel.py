# STATUS: ALIVE
# LAST-AUDIT: 2026-05-31
# CLAIM: Build a daily per-zone CCGT post-clearing restriction panel
#        for BSTS at the blackout cutover (2025-04-28). Per-zone outcome
#        = sum of PHF - PDBF (CCGT only, IDA + REE post-IDA RT) per day,
#        in GWh. Joined with wind+solar+gas covariates for the BSTS.
#        Also outputs the per-zone within-hour MW volatility score from
#        the 40-day MTU15-IDA pre-blackout window (2025-03-19 to
#        2025-04-27).
#
# IN:  data/processed/omie/.../{phf,pdbf}_all.parquet
#      data/derived/panels/bsts_quantities_panel.parquet (covariates)
#      data/external/ccgt_zonal_map.csv
# OUT: data/derived/panels/zonal_restriction_panel.parquet
#      data/derived/panels/zonal_volatility_score.csv

from pathlib import Path

import duckdb

REPO = Path(__file__).resolve().parents[3]
PHF = REPO / "data/processed/omie/mercado_intradiario_subastas/programas/phf_all.parquet"
PDBF = REPO / "data/processed/omie/mercado_diario/programas/pdbf_all.parquet"
UMAP = REPO / "data/derived/panels/bid_shape_critical_flat/_unit_map.parquet"
ZONE = REPO / "data/external/ccgt_zonal_map.csv"
COV = REPO / "data/derived/panels/bsts_quantities_panel.parquet"
OUT_PANEL = REPO / "data/derived/panels/zonal_restriction_panel.parquet"
OUT_VOL = REPO / "data/derived/panels/zonal_volatility_score.csv"


def main():
    con = duckdb.connect()
    con.execute("SET memory_limit='6GB'"); con.execute("SET threads=4")

    # PHF cleared MWh per (unit, day) -- IDA + post-IDA RT inclusive
    # PDBF MW per (unit, day, period) -- DA + bilateral, set hourly pre-MTU15-DA
    q = f"""
    WITH zone AS (SELECT unit_code, zone FROM read_csv_auto('{ZONE}')),
    ccgt_zone AS (
      SELECT z.unit_code, z.zone FROM zone z
      JOIN '{UMAP}' u USING (unit_code) WHERE u.tech_group = 'CCGT'
    ),
    phf_isp AS (
      SELECT date::DATE AS d, period, unit_code, mtu_minutes,
             MAX_BY(assigned_power_mw, session_number) AS mw_phf
      FROM '{PHF}' WHERE date >= '2024-01-01' AND period BETWEEN 1 AND 96
      GROUP BY date, period, unit_code, mtu_minutes
    ),
    phf_daily AS (
      SELECT d, unit_code,
             SUM(GREATEST(mw_phf, 0) * mtu_minutes / 60.0) AS mwh_phf
      FROM phf_isp WHERE mw_phf IS NOT NULL
      GROUP BY 1, 2
    ),
    pdbf_daily AS (
      SELECT date::DATE AS d, unit_code,
             SUM(GREATEST(assigned_power_mw, 0) * mtu_minutes / 60.0) AS mwh_pdbf
      FROM '{PDBF}' WHERE date >= '2024-01-01' AND period BETWEEN 1 AND 96
        AND assigned_power_mw IS NOT NULL
      GROUP BY 1, 2
    ),
    gap_daily AS (
      SELECT p.d, p.unit_code,
             p.mwh_phf - COALESCE(b.mwh_pdbf, 0) AS gap_mwh
      FROM phf_daily p LEFT JOIN pdbf_daily b USING (d, unit_code)
    ),
    zone_daily AS (
      SELECT z.zone, g.d,
             SUM(GREATEST(g.gap_mwh, 0)) / 1000.0 AS phf_pdbf_gap_gwh
      FROM gap_daily g JOIN ccgt_zone z USING (unit_code)
      GROUP BY z.zone, g.d
    )
    SELECT * FROM zone_daily
    ORDER BY zone, d
    """
    df_gap = con.execute(q).fetchdf()
    print(f"Built zone-daily gap panel: {len(df_gap)} rows, {df_gap['zone'].nunique()} zones")

    # Join with weather + gas covariates from preliminary's BSTS quantity panel
    cov_df = con.execute(f"""
    SELECT d::DATE AS d, wind_gwh, solar_gwh, gas_eur
    FROM '{COV}' WHERE d >= DATE '2024-01-01'
    """).fetchdf()
    import pandas as pd
    df_gap['d'] = pd.to_datetime(df_gap['d'])
    cov_df['d'] = pd.to_datetime(cov_df['d'])
    df = df_gap.merge(cov_df, on='d', how='inner')
    df.to_parquet(OUT_PANEL, index=False)
    print(f"wrote: {OUT_PANEL}")
    print(f"Date range: {df['d'].min()} to {df['d'].max()}, {df['zone'].nunique()} zones")

    # Volatility score per zone (MTU15-IDA pre-blackout window)
    q_vol = f"""
    WITH zone AS (SELECT unit_code, zone FROM read_csv_auto('{ZONE}')),
    ccgt AS (SELECT unit_code FROM '{UMAP}' WHERE tech_group = 'CCGT'),
    phf AS (
      SELECT date::DATE AS d, period, mtu_minutes, unit_code,
             MAX_BY(assigned_power_mw, session_number) AS mw
      FROM '{PHF}' WHERE date BETWEEN '2025-03-19' AND '2025-04-27'
        AND period BETWEEN 1 AND 96
      GROUP BY date, period, unit_code, mtu_minutes
    ),
    phf_clock AS (
      SELECT d, unit_code,
             CAST(CEIL(period * (mtu_minutes / 60.0)) AS INT) AS clock_hour,
             stddev_pop(mw) AS sd_mw, AVG(mw) AS mean_mw, COUNT(*) AS n_q
      FROM phf JOIN ccgt USING (unit_code) WHERE mw IS NOT NULL AND mw > 0
      GROUP BY d, unit_code, clock_hour
    )
    SELECT z.zone,
           COUNT(DISTINCT p.unit_code) AS n_units,
           ROUND(AVG(p.sd_mw), 2) AS within_hour_sd_mw,
           ROUND(AVG(p.sd_mw / NULLIF(p.mean_mw, 0)) * 100, 2) AS within_hour_cv_pct
    FROM phf_clock p JOIN zone z USING (unit_code)
    WHERE p.n_q = 4
    GROUP BY z.zone ORDER BY within_hour_sd_mw DESC
    """
    vol = con.execute(q_vol).fetchdf()
    vol.to_csv(OUT_VOL, index=False)
    print(f"\nwrote: {OUT_VOL}")
    print(vol.to_string(index=False))


if __name__ == "__main__":
    main()
