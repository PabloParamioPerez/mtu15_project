# STATUS: ALIVE
# LAST-AUDIT: 2026-05-31
# CLAIM: Spain vs France DA price gap (Spain - FR) per regime, as a
#        cross-border price-level control. CRITICAL caveat: ID15
#        (2025-03-19) and DA15 (2025-10-01) were EU-wide reforms (SIDC
#        and SDAC), so FR is NOT a clean control for either of those.
#        ISP15 (2024-12-11, settlement) and reforzada (2025-04-28) ARE
#        Spain-specific --- the gap is informative for these.
#
# IN:  data/processed/omie/.../marginalpdbc_all.parquet (Spain DA)
#      data/processed/entsoe/prices/fr_da_all.parquet
# OUT: results/regressions/regulatory/cross_border/es_fr_da_gap_per_regime.csv

from pathlib import Path

import duckdb

REPO = Path(__file__).resolve().parents[3]
ES = REPO / "data/processed/omie/mercado_diario/precios/marginalpdbc_all.parquet"
FR = REPO / "data/processed/entsoe/prices/fr_da_all.parquet"
OUT = REPO / "results/regressions/regulatory/cross_border"
OUT.mkdir(parents=True, exist_ok=True)


def main():
    con = duckdb.connect()
    q = f"""
    WITH es_hourly AS (
      SELECT date::DATE AS d,
             ((period - 1) * mtu_minutes / 60)::INT AS hour,
             AVG(price_es_eur_mwh) AS es_price
      FROM '{ES}' WHERE date >= '2024-01-01'
      GROUP BY 1, 2
    ),
    fr_hourly AS (
      SELECT (isp_start_utc AT TIME ZONE 'Europe/Madrid')::DATE AS d,
             EXTRACT(HOUR FROM (isp_start_utc AT TIME ZONE 'Europe/Madrid')) AS hour,
             AVG(price_eur_per_mwh) AS fr_price
      FROM '{FR}' WHERE isp_start_utc >= '2024-01-01'
      GROUP BY 1, 2
    ),
    joined AS (
      SELECT e.d, e.hour, e.es_price, f.fr_price,
             (e.es_price - f.fr_price) AS gap
      FROM es_hourly e JOIN fr_hourly f USING (d, hour)
    )
    SELECT
      CASE WHEN d < DATE '2024-06-14' THEN '1.pre-IDA'
           WHEN d < DATE '2024-12-01' THEN '2.3-sess'
           WHEN d < DATE '2025-03-19' THEN '3.ISP15-win'
           WHEN d < DATE '2025-04-28' THEN '4.MTU15-IDA pre-blk'
           WHEN d < DATE '2025-10-01' THEN '5.MTU15-IDA post-blk'
           ELSE                          '6.DA15/ID15' END AS regime,
      COUNT(*) AS n_clock_hours,
      ROUND(AVG(es_price), 2) AS es_da,
      ROUND(AVG(fr_price), 2) AS fr_da,
      ROUND(AVG(gap), 2) AS es_minus_fr
    FROM joined GROUP BY regime ORDER BY regime
    """
    df = con.execute(q).fetchdf()
    df.to_csv(OUT / "es_fr_da_gap_per_regime.csv", index=False)
    print(df.to_string(index=False))


if __name__ == "__main__":
    main()
