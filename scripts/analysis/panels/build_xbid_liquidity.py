# STATUS: ALIVE
# LAST-AUDIT: 2026-04-25
# FEEDS: B3, B4
# CLAIM: XBID liquidity panel (orders, trades, prices) by delivery_date x period
"""Build XBID liquidity panel (P4). Use `period` field (1-24 or 1-96)."""
from __future__ import annotations
import duckdb
import time

con = duckdb.connect()
con.execute("SET memory_limit='12GB'")
con.execute("SET threads=6")

t0 = time.time()
print('1. Aggregate orders by delivery_date × period...')
con.execute("""
    CREATE TEMP TABLE orders_daily AS
    SELECT delivery_date AS date,
           period,
           mtu_minutes,
           COUNT(*) n_orders,
           SUM(quantity_mw) total_offered_mw,
           AVG(price_eur_mwh) avg_offer_price
    FROM 'data/processed/omie/mercado_intradiario_continuo/ofertas/orders_all.parquet'
    WHERE quantity_mw > 0 AND delivery_date IS NOT NULL AND period IS NOT NULL
    GROUP BY 1, 2, 3
""")
n_o = con.sql("SELECT COUNT(*) FROM orders_daily").fetchone()[0]
print(f'   orders_daily: {n_o:,}  in {time.time()-t0:.1f}s')

t1 = time.time()
con.execute("""
    CREATE TEMP TABLE trades_daily AS
    SELECT delivery_date AS date,
           period,
           mtu_minutes,
           COUNT(*) n_trades,
           SUM(quantity_mw) matched_mw,
           AVG(price_eur_mwh) avg_trade_price,
           MIN(price_eur_mwh) min_trade_price,
           MAX(price_eur_mwh) max_trade_price,
           STDDEV_SAMP(price_eur_mwh) sd_trade_price
    FROM 'data/processed/omie/mercado_intradiario_continuo/transacciones/trades_all.parquet'
    WHERE quantity_mw > 0 AND delivery_date IS NOT NULL AND period IS NOT NULL
    GROUP BY 1, 2, 3
""")
print(f'   trades_daily: {con.sql("SELECT COUNT(*) FROM trades_daily").fetchone()[0]:,}  in {time.time()-t1:.1f}s')

t2 = time.time()
# Normalize period to hour 1-24 (pre-reform 1-24 is already hour; post-MTU15 need /4)
con.execute("""
    COPY (
        SELECT o.date,
               CASE WHEN o.mtu_minutes = 15 THEN CEIL(o.period / 4.0)::INTEGER
                    ELSE o.period END AS hour,
               SUM(o.n_orders) AS n_orders,
               SUM(COALESCE(t.n_trades, 0)) AS n_trades,
               SUM(o.total_offered_mw) AS total_offered_mw,
               SUM(COALESCE(t.matched_mw, 0)) AS matched_mw,
               SUM(COALESCE(t.matched_mw, 0)) / NULLIF(SUM(o.total_offered_mw), 0) AS fill_rate,
               AVG(t.avg_trade_price) AS avg_trade_price,
               AVG(CASE WHEN t.max_trade_price IS NOT NULL
                        THEN t.max_trade_price - t.min_trade_price END) AS trade_price_range,
               AVG(t.sd_trade_price) AS sd_trade_price
        FROM orders_daily o
        LEFT JOIN trades_daily t
          ON t.date = o.date AND t.period = o.period AND t.mtu_minutes = o.mtu_minutes
        GROUP BY 1, 2
        ORDER BY 1, 2
    ) TO 'data/derived/panels/xbid_liquidity_hourly.parquet' (FORMAT PARQUET)
""")
stats = con.execute("""
    SELECT COUNT(*) n, MIN(date) mn, MAX(date) mx,
           AVG(n_orders) avg_orders, AVG(n_trades) avg_trades,
           AVG(fill_rate) avg_fill, AVG(sd_trade_price) avg_sd
    FROM 'data/derived/panels/xbid_liquidity_hourly.parquet'
""").fetchone()
print('\nWrote xbid_liquidity_hourly.parquet')
print(f'   n={stats[0]:,} | {stats[1]} → {stats[2]}')
print(f'   avg orders/h: {stats[3]:.1f}  trades/h: {stats[4]:.1f}')
print(f'   avg fill rate: {stats[5]:.3f}  avg SD trade price: {stats[6]:.2f} €/MWh')
print(f'total: {time.time()-t0:.1f}s')
