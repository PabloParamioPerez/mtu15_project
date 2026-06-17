# STATUS: ALIVE
# LAST-AUDIT: 2026-04-25
# FEEDS: B6
# CLAIM: Forecast-bias direction and curtailment timing (p<=EUR0) by regime
"""Two new exploratory analyses using existing data:

(A) Forecast-bias direction: does ISP15 change whether wind/solar
    forecasts are systematically biased high or low?

    Hypothesis: pre-ISP15, intra-hour netting absorbs forecast bias,
    so forecasters have weaker incentive to be unbiased. Post-ISP15,
    bias generates real settlement costs, so forecasters may improve.

    Test: signed mean forecast error (actual − forecast) by regime,
    same-calendar adjusted.

(B) Curtailment timing: frequency of clearing prices ≤ €0 by regime,
    same-calendar comparison.

    Hypothesis: renewable surplus + inflexible demand creates negative
    clearing. Reform might affect frequency of these episodes.
"""
from __future__ import annotations
import duckdb
import pandas as pd

con = duckdb.connect()
con.execute("SET memory_limit='8GB'")
con.execute("SET threads=4")

# ---- (A) Forecast bias by regime ----
print('=' * 80)
print('(A) Wind + solar forecast bias direction by regime (same-calendar)')
print('=' * 80)
print()

panel = pd.read_parquet('data/derived/panels/passthrough_panel.parquet')
panel['date'] = pd.to_datetime(panel['date'])

# Compute SIGNED daily forecast error (actual - forecast)
panel['wind_signed_err_GWh'] = panel['wind_err_mwh'] / 1e3   # already actual - forecast
panel['solar_signed_err_GWh'] = panel['solar_err_mwh'].fillna(0) / 1e3

print('Wind forecast bias (mean signed error in GWh/day, +ve = under-forecast):')
print(f"\n{'window':<32}  {'n':>4}  {'wind_bias':>10}  {'solar_bias':>11}")
for label, s, e in [
    ('2018 Apr-Sep', '2018-04-01', '2018-09-30'),
    ('2019 Apr-Sep', '2019-04-01', '2019-09-30'),
    ('2022 Apr-Sep (gas crisis)', '2022-04-01', '2022-09-30'),
    ('2023 Apr-Sep', '2023-04-01', '2023-09-30'),
    ('2024 Apr-Sep (3-sess included)', '2024-04-01', '2024-09-30'),
    ('2025 Apr-Sep DA60/ID15', '2025-04-01', '2025-09-30'),
    ('---', None, None),
    ('2018 Oct-Mar', '2018-10-01', '2019-03-31'),
    ('2019 Oct-Mar', '2019-10-01', '2020-03-31'),
    ('2022 Oct-Mar', '2022-10-01', '2023-03-31'),
    ('2023 Oct-Mar', '2023-10-01', '2024-03-31'),
    ('2024 Oct-Mar (3-sess+ISP15)', '2024-10-01', '2025-03-31'),
    ('2025 Oct-Apr DA15/ID15', '2025-10-01', '2026-04-30'),
]:
    if s is None:
        print('---')
        continue
    sub = panel[(panel['date'] >= s) & (panel['date'] <= e)]
    if len(sub) < 30:
        continue
    wbias = sub['wind_signed_err_GWh'].mean()
    sbias = sub['solar_signed_err_GWh'].mean()
    print(f"{label:<32}  {len(sub):>4}  {wbias:>+10.2f}  {sbias:>+11.2f}")

# ---- (B) Curtailment frequency ----
print()
print('=' * 80)
print('(B) Frequency of cleared price ≤ €0 (curtailment / surplus) by regime')
print('=' * 80)
print()
df = con.sql("""
    SELECT date,
           CASE WHEN mtu_minutes=15 THEN CEIL(period/4.0)::INTEGER ELSE period END AS hour,
           AVG(price_es_eur_mwh) AS p
    FROM 'data/processed/omie/mercado_diario/precios/marginalpdbc_all.parquet'
    WHERE price_es_eur_mwh IS NOT NULL
    GROUP BY 1, 2
""").df()
df['date'] = pd.to_datetime(df['date'])

print('Same-calendar comparison of negative-price hour frequency:\n')
print(f"{'window':<32}  {'n_h':>5}  {'avg_p':>7}  {'%p≤€0':>7}  {'%p≤€-10':>9}  {'%p≤€10':>8}")
for label, s, e in [
    ('2022 Apr-Sep (gas crisis)', '2022-04-01', '2022-09-30'),
    ('2023 Apr-Sep', '2023-04-01', '2023-09-30'),
    ('2024 Apr-Sep (3-sess included)', '2024-04-01', '2024-09-30'),
    ('2025 Apr-Sep DA60/ID15', '2025-04-01', '2025-09-30'),
    ('---', None, None),
    ('2022 Oct-Mar', '2022-10-01', '2023-03-31'),
    ('2023 Oct-Mar', '2023-10-01', '2024-03-31'),
    ('2024 Oct-Mar (3-sess+ISP15)', '2024-10-01', '2025-03-31'),
    ('2025 Oct-Apr DA15/ID15', '2025-10-01', '2026-04-30'),
]:
    if s is None:
        print('---')
        continue
    sub = df[(df['date'] >= s) & (df['date'] <= e)]
    if len(sub) < 100:
        continue
    n_h = len(sub)
    n_neg = (sub['p'] <= 0).sum()
    n_v_neg = (sub['p'] <= -10).sum()
    n_low = (sub['p'] <= 10).sum()
    print(f"{label:<32}  {n_h:>5}  "
          f"{sub['p'].mean():>7.1f}  "
          f"{n_neg/n_h*100:>7.2f}  "
          f"{n_v_neg/n_h*100:>9.2f}  "
          f"{n_low/n_h*100:>8.2f}")

# ---- (C) Bonus: cross-border net export pattern ----
print()
print('=' * 80)
print('(C) ES-FR DA price spread by season — does Spain shift to net-exporter?')
print('=' * 80)
print()
print('Signed (ES − FR) and absolute spread per same-calendar window.')
print('More positive signed spread = ES more expensive than FR (so FR exports to ES).')
print('More negative signed spread = ES cheaper than FR (so ES exports to FR).')
print()

con.execute("""
    CREATE OR REPLACE TEMP TABLE es_fr AS
    WITH es AS (
        SELECT date,
               CASE WHEN mtu_minutes=15 THEN CEIL(period/4.0)::INTEGER ELSE period END AS hour,
               AVG(price_es_eur_mwh) AS p_es
        FROM 'data/processed/omie/mercado_diario/precios/marginalpdbc_all.parquet'
        WHERE price_es_eur_mwh IS NOT NULL
        GROUP BY 1, 2
    ), fr AS (
        SELECT CAST(isp_start_utc AS DATE) AS date,
               EXTRACT(HOUR FROM isp_start_utc)::INTEGER + 1 AS hour,
               AVG(price_eur_per_mwh) AS p_fr
        FROM 'data/processed/entsoe/prices/fr_da_all.parquet'
        WHERE price_eur_per_mwh IS NOT NULL
        GROUP BY 1, 2
    )
    SELECT es.date, es.hour, es.p_es, fr.p_fr,
           es.p_es - fr.p_fr AS signed_spread,
           ABS(es.p_es - fr.p_fr) AS abs_spread
    FROM es JOIN fr USING (date, hour)
""")

cb = con.sql("SELECT * FROM es_fr").df()
cb['date'] = pd.to_datetime(cb['date'])

print(f"{'window':<32}  {'n':>5}  {'signed':>7}  {'abs':>5}  {'%ES>FR':>7}  {'%coupled<€1':>11}")
for label, s, e in [
    ('2018 Apr-Sep', '2018-04-01', '2018-09-30'),
    ('2022 Apr-Sep', '2022-04-01', '2022-09-30'),
    ('2023 Apr-Sep', '2023-04-01', '2023-09-30'),
    ('2024 Apr-Sep', '2024-04-01', '2024-09-30'),
    ('2025 Apr-Sep DA60/ID15', '2025-04-01', '2025-09-30'),
    ('---', None, None),
    ('2022 Oct-Mar', '2022-10-01', '2023-03-31'),
    ('2023 Oct-Mar', '2023-10-01', '2024-03-31'),
    ('2024 Oct-Mar (3-sess+ISP15)', '2024-10-01', '2025-03-31'),
    ('2025 Oct-Apr DA15/ID15', '2025-10-01', '2026-04-30'),
]:
    if s is None:
        print('---')
        continue
    sub = cb[(cb['date'] >= s) & (cb['date'] <= e)]
    if len(sub) < 100:
        continue
    print(f"{label:<32}  {len(sub):>5}  "
          f"{sub['signed_spread'].mean():>+7.2f}  "
          f"{sub['abs_spread'].mean():>5.1f}  "
          f"{(sub['signed_spread']>0).mean()*100:>7.1f}  "
          f"{(sub['abs_spread']<1).mean()*100:>11.1f}")
