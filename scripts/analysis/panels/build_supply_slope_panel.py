# STATUS: ALIVE
# LAST-AUDIT: 2026-04-25
# FEEDS: F1
# CLAIM: Per-(date, hour) supply-curve slope from curva_pbc; input to firm Lerner
"""Precompute per-(date, hour) supply slope from curva_pbc.

Two period-encoding regimes in curva_pbc:
  pre-2025-10-01: country='MI' (combined MIBEL), typology NULL, period_raw='1'..'24' hourly
  post-2025-10-01: country='ES', typology='S' for simple, period_raw='H<h>Q<q>'
     → aggregate q=1..4 into a single hourly curve
"""
from __future__ import annotations
import duckdb
import time
from pathlib import Path

OUT = Path('data/derived/panels/supply_slope_hourly.parquet')
OUT.parent.mkdir(parents=True, exist_ok=True)

con = duckdb.connect()
con.execute("SET memory_limit='12GB'")
con.execute("SET threads=6")
con.execute("SET preserve_insertion_order=false")

DELTA = 10.0
t0 = time.time()

print('1. Aggregate sell-side simple curves to (date, hour, price)...')
con.execute("""
    CREATE TEMP TABLE sell_agg AS
    SELECT date,
           CASE
               WHEN period_raw LIKE 'H%Q%'
                 THEN CAST(regexp_extract(period_raw, 'H([0-9]+)Q', 1) AS INTEGER)
               ELSE CAST(period_raw AS INTEGER)
           END AS hour,
           price_eur_mwh,
           SUM(power_mw) AS mw
    FROM 'data/processed/omie/mercado_diario/curvas/curva_pbc_all.parquet'
    WHERE curve_type = 'O'
      AND country IN ('MI', 'ES')
      AND (offer_typology IS NULL OR offer_typology = 'S')
      AND power_mw IS NOT NULL
      AND price_eur_mwh IS NOT NULL
    GROUP BY 1, 2, 3
""")
n_agg = con.sql("SELECT COUNT(*) FROM sell_agg").fetchone()[0]
n_dh = con.sql("SELECT COUNT(DISTINCT (date,hour)) FROM sell_agg").fetchone()[0]
print(f'   sell_agg rows: {n_agg:,}  distinct (date,hour): {n_dh:,}  in {time.time()-t0:.1f}s')

t1 = time.time()
print('2. Cumulative supply per (date, hour)...')
con.execute("""
    CREATE TEMP TABLE cum_supply AS
    SELECT date, hour, price_eur_mwh,
           SUM(mw) OVER (PARTITION BY date, hour
                          ORDER BY price_eur_mwh
                          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_mw
    FROM sell_agg
""")
print(f'   cum_supply in {time.time()-t1:.1f}s')

t2 = time.time()
print('3. Hourly clearing price (avg 4 MTU15 quarters post-MTU15-DA)...')
con.execute("""
    CREATE TEMP TABLE clearing AS
    SELECT date,
           CASE WHEN mtu_minutes = 15 THEN CEIL(period / 4.0)::INTEGER
                ELSE period
           END AS hour,
           AVG(price_es_eur_mwh) AS p_star
    FROM 'data/processed/omie/mercado_diario/precios/marginalpdbc_all.parquet'
    WHERE price_es_eur_mwh IS NOT NULL
    GROUP BY 1, 2
""")
print(f'   clearing: {con.sql("SELECT COUNT(*) FROM clearing").fetchone()[0]:,}')

t3 = time.time()
print(f'4. Finite-difference slope at p* ± {DELTA}...')
con.execute(f"""
    CREATE TEMP TABLE slope AS
    WITH full_cum AS (
        SELECT c.date, c.hour, c.p_star,
               MAX(CASE WHEN cs.price_eur_mwh <= c.p_star + {DELTA}
                        THEN cs.cum_mw END) AS q_plus,
               MAX(CASE WHEN cs.price_eur_mwh <= c.p_star - {DELTA}
                        THEN cs.cum_mw END) AS q_minus,
               MAX(CASE WHEN cs.price_eur_mwh <= c.p_star
                        THEN cs.cum_mw END) AS q_star,
               -- Wider fallback window for edge cases
               MAX(CASE WHEN cs.price_eur_mwh <= c.p_star + 50
                        THEN cs.cum_mw END) AS q_plus50,
               MAX(CASE WHEN cs.price_eur_mwh <= c.p_star - 50
                        THEN cs.cum_mw END) AS q_minus50
        FROM clearing c
        LEFT JOIN cum_supply cs
          ON cs.date = c.date
         AND cs.hour = c.hour
        GROUP BY c.date, c.hour, c.p_star
    )
    SELECT date, hour, p_star AS clearing_price_eur_mwh,
           q_star AS q_at_p_star,
           q_plus, q_minus, q_plus50, q_minus50,
           -- Primary slope: ±10 window
           CASE
               WHEN q_plus IS NOT NULL AND q_minus IS NOT NULL
                    AND q_plus > q_minus
               THEN (q_plus - q_minus) / (2.0 * {DELTA})
           END AS slope_pm10,
           -- Fallback slope: ±50 window if ±10 is degenerate
           CASE
               WHEN q_plus50 IS NOT NULL AND q_minus50 IS NOT NULL
                    AND q_plus50 > q_minus50
               THEN (q_plus50 - q_minus50) / (2.0 * 50.0)
           END AS slope_pm50
    FROM full_cum
""")

t4 = time.time()
con.execute(f"""
    COPY (
        SELECT date, hour, clearing_price_eur_mwh, q_at_p_star,
               COALESCE(slope_pm10, slope_pm50) AS supply_slope_mw_per_eur,
               CASE WHEN slope_pm10 IS NOT NULL THEN 'pm10' ELSE 'pm50' END AS slope_source
        FROM slope
        WHERE COALESCE(slope_pm10, slope_pm50) IS NOT NULL
          AND q_at_p_star IS NOT NULL
        ORDER BY date, hour
    ) TO '{OUT}' (FORMAT PARQUET)
""")

stats = con.execute(f"""
    SELECT COUNT(*) n,
           MIN(date) dmin, MAX(date) dmax,
           SUM(CASE WHEN slope_source='pm10' THEN 1 ELSE 0 END) n_pm10,
           SUM(CASE WHEN slope_source='pm50' THEN 1 ELSE 0 END) n_pm50,
           AVG(supply_slope_mw_per_eur) avg_slope,
           MEDIAN(supply_slope_mw_per_eur) med_slope,
           AVG(q_at_p_star) avg_q,
           AVG(clearing_price_eur_mwh) avg_p
    FROM '{OUT}'
""").fetchone()
print(f'\nWrote {OUT}')
print(f'   n={stats[0]:,} hours | {stats[1]} -> {stats[2]}')
print(f'   slope sources: pm10={stats[3]:,}  pm50={stats[4]:,}')
print(f'   supply slope: mean={stats[5]:.1f} median={stats[6]:.1f} MW/(EUR/MWh)')
print(f'   q avg: {stats[7]:.0f} MW | p avg: {stats[8]:.1f} EUR/MWh')
print(f'total: {time.time()-t0:.1f}s')
