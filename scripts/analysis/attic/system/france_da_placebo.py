# STATUS: ALIVE
# LAST-AUDIT: 2026-04-25
# FEEDS: B7, D1
# CLAIM: Cross-country placebo: regime contrasts on French DA prices
"""France DA price placebo (P1.3).

For each Spanish reform regime window, compute the same descriptive
statistics on French DA prices that we computed on Spanish ones. France
was NOT subject to the Spanish 15-min reform sequence, so it serves as
a control. If the same regime contrasts appear in France:
  → finding is EU-wide trend, not Spain-specific reform attributable.
If France stays flat or different:
  → Spain finding strengthens (reform-attributable).

Outcomes compared:
  (a) Mean DA price level by regime
  (b) Within-day DA price SD by regime (volatility proxy)
  (c) ES − FR price spread (already partly explored)
  (d) Within-month price SD by regime
"""
from __future__ import annotations
import duckdb
import pandas as pd
from pathlib import Path

ES = Path('data/processed/omie/mercado_diario/precios/marginalpdbc_all.parquet')
FR = Path('data/processed/entsoe/prices/fr_da_all.parquet')

con = duckdb.connect()
con.execute("SET memory_limit='6GB'")
con.execute("SET threads=4")

print('=== Build hourly ES + FR price series, aligned ===\n')
con.execute(f"""
    CREATE TEMP TABLE es_hourly AS
    SELECT date,
           CASE WHEN mtu_minutes=15 THEN CEIL(period/4.0)::INTEGER ELSE period END AS hour,
           AVG(price_es_eur_mwh) AS price
    FROM '{ES}'
    WHERE price_es_eur_mwh IS NOT NULL
    GROUP BY 1, 2
""")
con.execute(f"""
    CREATE TEMP TABLE fr_hourly AS
    SELECT CAST(isp_start_utc AS DATE) AS date,
           EXTRACT(HOUR FROM isp_start_utc)::INTEGER + 1 AS hour,
           AVG(price_eur_per_mwh) AS price
    FROM '{FR}'
    WHERE price_eur_per_mwh IS NOT NULL
    GROUP BY 1, 2
""")

print('=== Outcome (a): mean DA price level by regime ===')
df = con.sql("""
    SELECT 'ES' AS country, date, hour, price FROM es_hourly
    UNION ALL
    SELECT 'FR' AS country, date, hour, price FROM fr_hourly
""").df()
df['date'] = pd.to_datetime(df['date'])
df['ym'] = df['date'].dt.to_period('M').dt.to_timestamp()

def assign_regime(d):
    d = pd.Timestamp(d)
    if d < pd.Timestamp('2024-06-14'):
        return 'pre-IDA'
    if d < pd.Timestamp('2024-12-01'):
        return '3-sess'
    if d < pd.Timestamp('2025-03-19'):
        return 'ISP15 window'
    if d < pd.Timestamp('2025-10-01'):
        return 'DA60/ID15'
    return 'DA15/ID15'
df['regime'] = df['date'].apply(assign_regime)
REGIME_ORDER = ['pre-IDA', '3-sess', 'ISP15 window', 'DA60/ID15', 'DA15/ID15']

# (a) Mean price by regime
print()
agg = df.groupby(['country', 'regime'])['price'].agg(['mean', 'std', 'count']).round(2)
agg = agg.reset_index()
agg['regime'] = pd.Categorical(agg['regime'], categories=REGIME_ORDER, ordered=True)
agg = agg.sort_values(['country', 'regime']).set_index(['country', 'regime'])
print(agg.to_string())

# Δ vs pre-IDA
print('\nRegime mean − pre-IDA mean (€/MWh):')
print('Country  3-sess  ISP15    DA60/ID15  DA15/ID15')
for c in ['ES', 'FR']:
    sub = agg.loc[c]
    pre = sub.loc['pre-IDA', 'mean']
    deltas = [sub.loc[r, 'mean'] - pre for r in REGIME_ORDER[1:]]
    print(f'{c:7s}  ' + '  '.join(f'{d:+8.1f}' for d in deltas))

print('\n=== Outcome (b): within-day price SD by regime ===')
daily_sd = df.groupby(['country', 'date'])['price'].std().reset_index()
daily_sd['regime'] = daily_sd['date'].apply(assign_regime)
agg2 = daily_sd.groupby(['country', 'regime'])['price'].agg(['mean', 'count']).round(2)
agg2 = agg2.reset_index()
agg2['regime'] = pd.Categorical(agg2['regime'], categories=REGIME_ORDER, ordered=True)
agg2 = agg2.sort_values(['country', 'regime']).set_index(['country', 'regime'])
agg2.columns = ['avg within-day SD', 'n_days']
print(agg2.to_string())

print('\nΔ vs pre-IDA (within-day SD, €/MWh):')
print('Country  3-sess   ISP15    DA60/ID15  DA15/ID15')
for c in ['ES', 'FR']:
    sub = agg2.loc[c]
    pre = sub.loc['pre-IDA', 'avg within-day SD']
    deltas = [sub.loc[r, 'avg within-day SD'] - pre for r in REGIME_ORDER[1:]]
    print(f'{c:7s}  ' + '  '.join(f'{d:+8.2f}' for d in deltas))

print('\n=== Outcome (c): ES − FR spread by regime ===')
con.execute("""
    CREATE TEMP TABLE spread AS
    SELECT es.date, es.hour,
           es.price - fr.price AS signed_spread,
           ABS(es.price - fr.price) AS abs_spread
    FROM es_hourly es
    JOIN fr_hourly fr USING (date, hour)
""")
sp = con.sql("SELECT * FROM spread").df()
sp['date'] = pd.to_datetime(sp['date'])
sp['regime'] = sp['date'].apply(assign_regime)
agg3 = sp.groupby('regime').agg(
    n=('signed_spread', 'count'),
    mean_signed=('signed_spread', 'mean'),
    mean_abs=('abs_spread', 'mean'),
    sd_spread=('signed_spread', 'std'),
).round(2)
agg3 = agg3.reindex(REGIME_ORDER)
print(agg3.to_string())

print('\n=== Outcome (d): per-month within-month SD by regime ===')
month_sd = df.groupby(['country', 'ym'])['price'].std().reset_index()
month_sd['regime'] = month_sd['ym'].apply(assign_regime)
agg4 = month_sd.groupby(['country', 'regime'])['price'].agg(['mean', 'count']).round(2)
agg4 = agg4.reset_index()
agg4['regime'] = pd.Categorical(agg4['regime'], categories=REGIME_ORDER, ordered=True)
agg4 = agg4.sort_values(['country', 'regime']).set_index(['country', 'regime'])
agg4.columns = ['avg within-month SD', 'n_months']
print(agg4.to_string())
