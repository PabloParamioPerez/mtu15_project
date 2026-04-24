"""Build firm-level hourly Lerner-index panel for Big-4 (GE, IB, GN, HC).

Method: Cournot-Nash Lerner under quasi-inelastic demand:

    L_i = (p* - MC_i) / p* ≈ q_i / (p* × (1 - s_i) × |∂S/∂p|)

where
    q_i = firm i's cleared quantity (MWh/h) from pdbce offer_type=1
    s_i = q_i / Q_cleared  (firm share of aggregate supply)
    p* = DA hourly clearing price
    |∂S/∂p| = supply-curve slope at clearing (precomputed panel)

Assumes |∂D/∂p| = 0 (reasonable for DA, most demand is price-inelastic
retailer bids) and |∂S_i/∂p| = s_i × |∂S/∂p| (homogeneous cost
structure at the margin across firms).

Output: data/derived/firm_lerner_hourly.parquet
    (date, hour, firm, q_mwh, s_share, clearing_price,
     supply_slope, lerner_index)
"""
from __future__ import annotations
import duckdb
import time
from pathlib import Path

SLOPE_PANEL = 'data/derived/supply_slope_hourly.parquet'
PDBCE = 'data/processed/omie/mercado_diario/programas/pdbce_all.parquet'
OUT = Path('data/derived/firm_lerner_hourly.parquet')

con = duckdb.connect()
con.execute("SET memory_limit='8GB'")
con.execute("SET threads=6")

t0 = time.time()

# Aggregate pdbce (offer_type=1 = firm-level generation) to hourly
# Q per firm. period encoding:
#   Pre-MTU15-DA: period=1..24 is hourly
#   Post-MTU15-DA: period=1..96 is 15-min → average 4 into hour
print('1. Aggregate firm hourly generation from pdbce...')
con.execute(f"""
    CREATE TEMP TABLE firm_q AS
    SELECT date,
           CASE WHEN mtu_minutes = 15 THEN CEIL(period / 4.0)::INTEGER
                ELSE period
           END AS hour,
           grupo_empresarial AS firm,
           -- Hourly MWh: if MTU15, average 4 quarters × 1 hour; if MTU60, take value
           SUM(assigned_power_mw) / CASE WHEN mtu_minutes = 15 THEN 4.0 ELSE 1.0 END AS q_mwh
    FROM '{PDBCE}'
    WHERE offer_type = 1
      AND assigned_power_mw IS NOT NULL
      AND grupo_empresarial IN ('GE','IB','GN','HC')
    GROUP BY date, hour, firm, mtu_minutes
""")
print(f'   firm_q: {con.sql("SELECT COUNT(*) FROM firm_q").fetchone()[0]:,} rows')

print('2. Aggregate total market Q (all offer_type=1 for Spain)...')
con.execute(f"""
    CREATE TEMP TABLE total_q AS
    SELECT date,
           CASE WHEN mtu_minutes = 15 THEN CEIL(period / 4.0)::INTEGER
                ELSE period
           END AS hour,
           SUM(assigned_power_mw) / CASE WHEN mtu_minutes = 15 THEN 4.0 ELSE 1.0 END AS q_total_mwh
    FROM '{PDBCE}'
    WHERE offer_type = 1
      AND assigned_power_mw IS NOT NULL
      AND assigned_power_mw > 0
    GROUP BY date, hour, mtu_minutes
""")
print(f'   total_q: {con.sql("SELECT COUNT(*) FROM total_q").fetchone()[0]:,} rows')

print('3. Join with slope panel + compute Lerner...')
con.execute(f"""
    COPY (
        SELECT f.date, f.hour, f.firm,
               f.q_mwh,
               f.q_mwh / NULLIF(t.q_total_mwh, 0) AS s_share,
               s.clearing_price_eur_mwh,
               s.supply_slope_mw_per_eur,
               CASE WHEN s.clearing_price_eur_mwh > 0
                         AND s.supply_slope_mw_per_eur > 0
                         AND (t.q_total_mwh - f.q_mwh) > 0
                    THEN f.q_mwh
                         / (s.clearing_price_eur_mwh
                            * (1 - f.q_mwh / NULLIF(t.q_total_mwh, 0))
                            * s.supply_slope_mw_per_eur)
                    ELSE NULL
               END AS lerner_index
        FROM firm_q f
        JOIN total_q t USING (date, hour)
        JOIN '{SLOPE_PANEL}' s USING (date, hour)
        WHERE f.q_mwh > 0
        ORDER BY date, hour, firm
    ) TO '{OUT}' (FORMAT PARQUET)
""")

stats = con.execute(f"""
    SELECT firm,
           COUNT(*) n,
           AVG(lerner_index) avg_l,
           MEDIAN(lerner_index) med_l,
           PERCENTILE_CONT(0.1) WITHIN GROUP (ORDER BY lerner_index) p10,
           PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY lerner_index) p90,
           AVG(s_share) avg_share,
           AVG(q_mwh) avg_q
    FROM '{OUT}'
    WHERE lerner_index IS NOT NULL
    GROUP BY firm ORDER BY firm
""").df()
print(f'\nWrote {OUT}')
print('Per-firm Lerner summary:')
print(stats.to_string(index=False))
print(f'\ntotal time: {time.time()-t0:.1f}s')
