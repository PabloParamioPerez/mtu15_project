"""(1) Within-month dispersion same-calendar with proper baselines.
   The earlier finding "ES doubles, FR flat" used regime windows.
   Here run year-by-year same-calendar to see if the doubling is
   reform-attributable or trend-driven.

(2) Post-MTU15-DA 15-min bid behavior:
   Memory note H15 says Big-4 firms replicate the same bid across the
   4 intra-hour 15-min ISPs 80-99% of the time. Re-check this with
   the new 2025-10-onward DA15/ID15 data — did the pattern persist
   into the new 15-min DA?
"""
from __future__ import annotations
import duckdb
import pandas as pd

con = duckdb.connect()
con.execute("SET memory_limit='8GB'")
con.execute("SET threads=4")

# ---- (1) Within-month dispersion year-by-year ----
print('=' * 80)
print('(1) Within-month price dispersion — year-by-year (Apr-Sep + Oct-Mar)')
print('=' * 80)

con.execute("""
    CREATE TEMP TABLE es_h AS
    SELECT date,
           CASE WHEN mtu_minutes=15 THEN CEIL(period/4.0)::INTEGER ELSE period END AS hour,
           AVG(price_es_eur_mwh) AS p_es
    FROM 'data/processed/omie/mercado_diario/precios/marginalpdbc_all.parquet'
    WHERE price_es_eur_mwh IS NOT NULL
    GROUP BY 1, 2
""")
con.execute("""
    CREATE TEMP TABLE fr_h AS
    SELECT CAST(isp_start_utc AS DATE) AS date,
           EXTRACT(HOUR FROM isp_start_utc)::INTEGER + 1 AS hour,
           AVG(price_eur_per_mwh) AS p_fr
    FROM 'data/processed/entsoe/prices/fr_da_all.parquet'
    WHERE price_eur_per_mwh IS NOT NULL
    GROUP BY 1, 2
""")

es = con.sql("SELECT * FROM es_h").df()
fr = con.sql("SELECT * FROM fr_h").df()
es['date'] = pd.to_datetime(es['date'])
fr['date'] = pd.to_datetime(fr['date'])
es['ym'] = es['date'].dt.to_period('M').dt.to_timestamp()
fr['ym'] = fr['date'].dt.to_period('M').dt.to_timestamp()

# Within-month standard deviation per (country, year-month)
es_sd = es.groupby('ym')['p_es'].std().rename('es_sd').reset_index()
fr_sd = fr.groupby('ym')['p_fr'].std().rename('fr_sd').reset_index()
sd = es_sd.merge(fr_sd, on='ym')
sd['year']  = sd['ym'].dt.year
sd['month'] = sd['ym'].dt.month

print('\nMonthly within-month SD (€/MWh) — winter months (Dec-Mar) by year:\n')
print(f"{'year':<4}  {'Dec':>5} {'Jan':>5} {'Feb':>5} {'Mar':>5}  | FR equivalents")
for y in range(2018, 2026):
    es_w = sd[(sd['year']==y) & (sd['month']==12)]['es_sd'].values
    es_j = sd[(sd['year']==y+1) & (sd['month']==1)]['es_sd'].values
    es_f = sd[(sd['year']==y+1) & (sd['month']==2)]['es_sd'].values
    es_m = sd[(sd['year']==y+1) & (sd['month']==3)]['es_sd'].values
    fr_w = sd[(sd['year']==y) & (sd['month']==12)]['fr_sd'].values
    fr_j = sd[(sd['year']==y+1) & (sd['month']==1)]['fr_sd'].values
    fr_f = sd[(sd['year']==y+1) & (sd['month']==2)]['fr_sd'].values
    fr_m = sd[(sd['year']==y+1) & (sd['month']==3)]['fr_sd'].values
    def f(v):
        return f'{v[0]:5.1f}' if len(v) else '   --'
    print(f"{y}-{y+1:<2}  ES: {f(es_w)} {f(es_j)} {f(es_f)} {f(es_m)}  |  "
          f"FR: {f(fr_w)} {f(fr_j)} {f(fr_f)} {f(fr_m)}")

print()
print('\nMonthly within-month SD — Apr-Sep by year:\n')
print(f"{'year':<6}  ES_avg     FR_avg    ES-FR diff")
for y in range(2018, 2027):
    sub_es = sd[(sd['year']==y) & (sd['month'].between(4,9))]['es_sd']
    sub_fr = sd[(sd['year']==y) & (sd['month'].between(4,9))]['fr_sd']
    if len(sub_es) > 0 and len(sub_fr) > 0:
        diff = sub_es.mean() - sub_fr.mean()
        print(f"{y:<6}    {sub_es.mean():>5.1f}      {sub_fr.mean():>5.1f}      {diff:+5.1f}")

# ---- (2) 15-min DA bid replication post-MTU15-DA ----
print()
print('=' * 80)
print('(2) DA 15-min bid replication post-MTU15-DA (2025-10 onwards)')
print('=' * 80)
print('\nFor each (date, hour), compute % of 15-min tranches priced')
print('IDENTICALLY by the same firm-unit. If 100%, firms replicate.')
print('If significantly < 100%, firms differentiate quarters.\n')

# Need DA det price/quantity in MTU15 era
df = con.sql("""
    SELECT d.date,
           CASE WHEN d.period_raw LIKE 'H%Q%'
                THEN CAST(regexp_extract(d.period_raw, 'H([0-9]+)Q', 1) AS INTEGER)
                ELSE NULL END AS hour,
           CASE WHEN d.period_raw LIKE 'H%Q%'
                THEN CAST(regexp_extract(d.period_raw, 'Q([0-9]+)', 1) AS INTEGER)
                ELSE NULL END AS quarter,
           c.unit_code,
           uf.firm,
           d.price_eur_mwh AS p,
           d.quantity_mw AS q
    FROM 'data/processed/omie/mercado_diario/ofertas/det_all.parquet' d
    JOIN 'data/processed/omie/mercado_diario/ofertas/cab_all.parquet' c
       ON c.date = d.date AND c.offer_code = d.offer_code AND c.version = d.version
       AND c.buy_sell = 'V'
    LEFT JOIN (
        SELECT unit_code, MAX(grupo_empresarial) firm
        FROM 'data/processed/omie/mercado_diario/programas/pdbce_all.parquet'
        WHERE grupo_empresarial IS NOT NULL AND offer_type = 1
        GROUP BY 1
    ) uf ON uf.unit_code = c.unit_code
    WHERE CAST(d.date AS DATE) >= '2025-10-01'
      AND uf.firm IN ('GE','IB','GN','HC')
""").df()
print(f'Rows extracted: {len(df):,}')

# For each (date, hour, unit_code, firm), check whether the prices
# across q=1..4 are identical (collapsed to one value).
print('\nBid replication: % of (date, hour, unit) cells where q1=q2=q3=q4')

if len(df) > 0:
    df = df.dropna(subset=['hour', 'quarter'])
    g = df.groupby(['date', 'hour', 'unit_code', 'firm'])
    # Per cell, check if all distinct prices/quantities are the same
    res = g.agg(
        n_quarters=('quarter', 'nunique'),
        n_distinct_p=('p', 'nunique'),
        n_distinct_q=('q', 'nunique'),
    ).reset_index()
    full = res[res['n_quarters'] == 4]  # only cells with all 4 quarters
    print(f'Cells with all 4 quarters: {len(full):,}')
    print()
    for firm in ['GE', 'IB', 'GN', 'HC']:
        sub = full[full['firm'] == firm]
        if len(sub) == 0:
            continue
        n_total = len(sub)
        n_same_p = (sub['n_distinct_p'] == 1).sum()
        n_same_q = (sub['n_distinct_q'] == 1).sum()
        n_same_both = ((sub['n_distinct_p'] == 1) & (sub['n_distinct_q'] == 1)).sum()
        print(f"  {firm}: {n_total:>6} cells | same price across 4Q: {n_same_p/n_total*100:>5.1f}% | "
              f"same q: {n_same_q/n_total*100:>5.1f}% | both same: {n_same_both/n_total*100:>5.1f}%")
