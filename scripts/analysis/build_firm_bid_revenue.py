"""Build per-firm per-day bid-level + revenue panels for P2 + P5.

Outputs:
  data/derived/firm_bid_panel.parquet  (DA bid-weighted avg offer price per firm×day)
  data/derived/firm_revenue_panel.parquet  (DA + IDA revenue per firm×day)
"""
from __future__ import annotations
import duckdb
import time

con = duckdb.connect()
con.execute("SET memory_limit='12GB'")
con.execute("SET threads=6")

t0 = time.time()
print('1. Unit → firm (grupo) mapping from pdbce...')
# Most-frequent firm per unit over the full sample (mode).
con.execute("""
    CREATE TEMP TABLE unit_firm AS
    WITH cnts AS (
        SELECT unit_code, grupo_empresarial, COUNT(*) n
        FROM 'data/processed/omie/mercado_diario/programas/pdbce_all.parquet'
        WHERE offer_type = 1 AND grupo_empresarial IS NOT NULL
        GROUP BY 1, 2
    ),
    ranked AS (
        SELECT unit_code, grupo_empresarial,
               ROW_NUMBER() OVER (PARTITION BY unit_code ORDER BY n DESC) rk
        FROM cnts
    )
    SELECT unit_code, grupo_empresarial FROM ranked WHERE rk = 1
""")
print(f'   unit_firm: {con.sql("SELECT COUNT(*) FROM unit_firm").fetchone()[0]:,}')

# ============================================================
# P2: DA bid-level offer price per firm
# ============================================================
print('\n2. DA bid-weighted avg offer price per firm×day (from det+cab)...')
t1 = time.time()
con.execute("""
    CREATE TEMP TABLE cab_small AS
    SELECT date, offer_code, version, unit_code, buy_sell
    FROM 'data/processed/omie/mercado_diario/ofertas/cab_all.parquet'
    WHERE buy_sell = 'V'  -- sell side only
""")
print(f'   cab_small (sell): {con.sql("SELECT COUNT(*) FROM cab_small").fetchone()[0]:,}')

con.execute("""
    CREATE TEMP TABLE det_firm AS
    SELECT d.date, d.price_eur_mwh, d.quantity_mw,
           c.unit_code,
           COALESCE(u.grupo_empresarial, 'Fringe') AS firm_group
    FROM 'data/processed/omie/mercado_diario/ofertas/det_all.parquet' d
    JOIN cab_small c
       ON c.date = d.date
      AND c.offer_code = d.offer_code
      AND c.version = d.version
    LEFT JOIN unit_firm u USING (unit_code)
    WHERE d.quantity_mw > 0 AND d.price_eur_mwh IS NOT NULL
""")
print(f'   det_firm rows: {con.sql("SELECT COUNT(*) FROM det_firm").fetchone()[0]:,} in {time.time()-t1:.1f}s')

print('\n3. Per firm×day bid-weighted avg offer price...')
t2 = time.time()
con.execute("""
    COPY (
        SELECT date,
               CASE WHEN firm_group IN ('GE','IB','GN','HC') THEN firm_group
                    ELSE 'Fringe'
               END AS firm_group,
               SUM(quantity_mw) AS total_offered_mw,
               SUM(quantity_mw * price_eur_mwh) / SUM(quantity_mw) AS wavg_offer_eur_mwh,
               MIN(price_eur_mwh) AS min_offer,
               MAX(price_eur_mwh) AS max_offer
        FROM det_firm
        WHERE price_eur_mwh BETWEEN -500 AND 4000  -- sanity bounds
        GROUP BY 1, 2
        ORDER BY 1, 2
    ) TO 'data/derived/firm_bid_panel.parquet' (FORMAT PARQUET)
""")
print(f'   firm_bid_panel written in {time.time()-t2:.1f}s')

# ============================================================
# P5: Revenue per firm per day
# ============================================================
print('\n4. DA cleared revenue per firm×day...')
t3 = time.time()
con.execute("""
    CREATE TEMP TABLE da_cleared_hourly AS
    SELECT date,
           CASE WHEN mtu_minutes = 15 THEN CEIL(period / 4.0)::INTEGER
                ELSE period
           END AS hour,
           grupo_empresarial AS firm,
           SUM(assigned_power_mw) / CASE WHEN mtu_minutes = 15 THEN 4.0 ELSE 1.0 END AS q_mwh
    FROM 'data/processed/omie/mercado_diario/programas/pdbce_all.parquet'
    WHERE offer_type = 1 AND assigned_power_mw > 0
      AND grupo_empresarial IS NOT NULL
    GROUP BY date, hour, firm, mtu_minutes
""")

con.execute("""
    CREATE TEMP TABLE da_price_hourly AS
    SELECT date,
           CASE WHEN mtu_minutes = 15 THEN CEIL(period / 4.0)::INTEGER
                ELSE period
           END AS hour,
           AVG(price_es_eur_mwh) AS da_price
    FROM 'data/processed/omie/mercado_diario/precios/marginalpdbc_all.parquet'
    WHERE price_es_eur_mwh IS NOT NULL
    GROUP BY 1, 2
""")

con.execute("""
    CREATE TEMP TABLE da_rev AS
    SELECT q.date, q.firm,
           SUM(q.q_mwh * p.da_price) AS da_rev_eur,
           SUM(q.q_mwh) AS da_q_mwh
    FROM da_cleared_hourly q
    JOIN da_price_hourly p USING (date, hour)
    GROUP BY q.date, q.firm
""")
print(f'   da_rev done in {time.time()-t3:.1f}s')

print('5. IDA cleared revenue per firm×day...')
t4 = time.time()
con.execute("""
    CREATE TEMP TABLE ida_cleared AS
    SELECT date, session_number,
           CASE WHEN mtu_minutes = 15 THEN CEIL(period / 4.0)::INTEGER
                ELSE period END AS hour,
           grupo_empresarial AS firm,
           SUM(assigned_power_mw) / CASE WHEN mtu_minutes = 15 THEN 4.0 ELSE 1.0 END AS q_mwh
    FROM 'data/processed/omie/mercado_intradiario_subastas/programas/pibcie_all.parquet'
    WHERE offer_type = 1 AND assigned_power_mw IS NOT NULL
      AND grupo_empresarial IS NOT NULL
    GROUP BY 1, 2, 3, 4, mtu_minutes
""")

con.execute("""
    CREATE TEMP TABLE ida_price AS
    SELECT date, session_number,
           CASE WHEN mtu_minutes = 15 THEN CEIL(period / 4.0)::INTEGER
                ELSE period END AS hour,
           AVG(price_es_eur_mwh) AS ida_price
    FROM 'data/processed/omie/mercado_intradiario_subastas/precios/marginalpibc_all.parquet'
    WHERE price_es_eur_mwh IS NOT NULL
    GROUP BY 1, 2, 3
""")

con.execute("""
    CREATE TEMP TABLE ida_rev AS
    SELECT q.date, q.firm,
           SUM(q.q_mwh * p.ida_price) AS ida_rev_eur,
           SUM(ABS(q.q_mwh)) AS ida_q_mwh_abs
    FROM ida_cleared q
    JOIN ida_price p USING (date, session_number, hour)
    GROUP BY q.date, q.firm
""")
print(f'   ida_rev done in {time.time()-t4:.1f}s')

print('6. Combined DA+IDA revenue panel...')
con.execute("""
    COPY (
        SELECT COALESCE(da.date, ida.date) AS date,
               CASE WHEN COALESCE(da.firm, ida.firm) IN ('GE','IB','GN','HC')
                    THEN COALESCE(da.firm, ida.firm)
                    ELSE 'Fringe' END AS firm_group,
               COALESCE(da.firm, ida.firm) AS firm,
               COALESCE(da.da_q_mwh, 0) AS da_q_mwh,
               COALESCE(da.da_rev_eur, 0) AS da_rev_eur,
               COALESCE(ida.ida_q_mwh_abs, 0) AS ida_q_mwh_abs,
               COALESCE(ida.ida_rev_eur, 0) AS ida_rev_eur
        FROM da_rev da
        FULL OUTER JOIN ida_rev ida USING (date, firm)
        ORDER BY date, firm
    ) TO 'data/derived/firm_revenue_panel.parquet' (FORMAT PARQUET)
""")

stats = con.execute("""
    SELECT firm_group,
           COUNT(*) n,
           SUM(da_q_mwh)/1e9 da_twh,
           SUM(da_rev_eur)/1e9 da_bn_eur,
           SUM(ida_q_mwh_abs)/1e9 ida_twh,
           AVG(da_rev_eur/NULLIF(da_q_mwh,0)) avg_da_price_realized
    FROM 'data/derived/firm_revenue_panel.parquet'
    WHERE firm_group IN ('GE','IB','GN','HC','Fringe')
    GROUP BY 1 ORDER BY da_bn_eur DESC
""").df()
print('\nFirm revenue totals 2018-01 → 2026-01:')
print(stats.to_string(index=False))
print(f'\ntotal time: {time.time()-t0:.1f}s')
