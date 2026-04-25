"""DA-IDA wedge time-series structure by regime.

Economic model the patterns motivate:

  Two-stage market clearing model:
    DA price p^DA_h ~ E[clearing | DA info I^DA_h]
    IDA price p^IDA_h ~ E[clearing | IDA info I^IDA_h], I^IDA ⊃ I^DA

  Wedge w_h = p^IDA_h − p^DA_h represents the value of intraday info.
  Theoretical predictions:
    1. E[w_h] depends on systematic forecast bias (pre-MTU15-IDA hourly
       commitment can mis-allocate within-hour vs. quarter-hour reality)
    2. Var[w_h] reflects uncertainty resolution at IDA stage
    3. Autocorrelation ρ(w_h, w_{h-1}) reflects persistence of mispricing

  Test: regime contrasts on each of these moments.
"""
from __future__ import annotations
import duckdb
import pandas as pd
import numpy as np

con = duckdb.connect()
con.execute("SET memory_limit='6GB'")
con.execute("SET threads=4")

REGIME_ORDER = ['pre-IDA', '3-sess', 'ISP15 window', 'DA60/ID15', 'DA15/ID15']

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

print('Build DA + IDA hourly price panel and wedge series')
con.execute("""
    CREATE TEMP TABLE da AS
    SELECT date,
           CASE WHEN mtu_minutes=15 THEN CEIL(period/4.0)::INTEGER ELSE period END AS hour,
           AVG(price_es_eur_mwh) AS da_price
    FROM 'data/processed/omie/mercado_diario/precios/marginalpdbc_all.parquet'
    WHERE price_es_eur_mwh IS NOT NULL
    GROUP BY 1, 2
""")
con.execute("""
    CREATE TEMP TABLE ida AS
    SELECT date, session_number,
           CASE WHEN mtu_minutes=15 THEN CEIL(period/4.0)::INTEGER ELSE period END AS hour,
           AVG(price_es_eur_mwh) AS ida_price
    FROM 'data/processed/omie/mercado_intradiario_subastas/precios/marginalpibc_all.parquet'
    WHERE price_es_eur_mwh IS NOT NULL
    GROUP BY 1, 2, 3
""")

# Latest IDA session for each (date, hour) to get the LAST IDA reading
con.execute("""
    CREATE TEMP TABLE ida_last AS
    SELECT date, hour, AVG(ida_price) AS ida_price
    FROM ida
    WHERE session_number = (SELECT MAX(session_number) FROM ida i2
                             WHERE i2.date = ida.date AND i2.hour = ida.hour)
    GROUP BY date, hour
""")

# Simpler: avg across sessions
con.execute("""
    CREATE OR REPLACE TEMP TABLE ida_avg AS
    SELECT date, hour, AVG(ida_price) AS ida_price
    FROM ida GROUP BY date, hour
""")

panel = con.sql("""
    SELECT da.date, da.hour, da.da_price, ida.ida_price,
           ida.ida_price - da.da_price AS wedge
    FROM da JOIN ida_avg ida USING (date, hour)
    ORDER BY da.date, da.hour
""").df()
panel['date'] = pd.to_datetime(panel['date'])
panel['regime'] = panel['date'].apply(assign_regime)

# By regime: wedge moments
print('\nMoments of DA-IDA wedge (€/MWh) by regime:')
print(f"{'regime':<14}  {'n':>6}  {'mean':>8}  {'std':>8}  {'p10':>7}  {'p90':>7}  {'AR(1)':>8}")

# Sort by date+hour for AR(1)
panel = panel.sort_values(['date', 'hour'])

for r in REGIME_ORDER:
    sub = panel[panel['regime'] == r]
    if len(sub) < 100:
        continue
    w = sub['wedge'].values
    # AR(1) within regime
    if len(w) > 2:
        ar1 = np.corrcoef(w[:-1], w[1:])[0, 1]
    else:
        ar1 = np.nan
    print(f"{r:<14}  {len(sub):>6}  "
          f"{w.mean():>8.2f}  "
          f"{w.std():>8.2f}  "
          f"{np.percentile(w, 10):>7.2f}  "
          f"{np.percentile(w, 90):>7.2f}  "
          f"{ar1:>8.3f}")

# Wedge by regime AND hour-of-day pattern
print('\nMean wedge by (regime, hour-of-day):')
hod = panel.groupby(['regime', 'hour'])['wedge'].mean().unstack('hour')
hod = hod.reindex(REGIME_ORDER)
# Show summary stats across hours
print('  hour  ', '   '.join(f'{h:>3d}' for h in [1, 6, 9, 12, 15, 18, 21, 24]))
for r in REGIME_ORDER:
    if r in hod.index:
        vals = hod.loc[r]
        print(f'  {r:<12} ' + '  '.join(f'{vals.get(h, np.nan):>5.1f}' for h in [1, 6, 9, 12, 15, 18, 21, 24]))

# IDA price level changes by regime
print('\n=== IDA absolute price level by regime ===')
agg = panel.groupby('regime').agg(
    n=('ida_price', 'count'),
    da_mean=('da_price', 'mean'),
    ida_mean=('ida_price', 'mean'),
    wedge_mean=('wedge', 'mean'),
    abs_wedge_mean=('wedge', lambda x: x.abs().mean()),
).round(2).reindex(REGIME_ORDER)
print(agg.to_string())
