# STATUS: ALIVE
# LAST-AUDIT: 2026-04-25
# FEEDS: B2, B6
# CLAIM: Same-calendar test for forecast pass-through R-sq and reservation share
"""Skepticism checks on Pattern 2 (bid-function reservation share)
and Pattern 1 (pass-through) — the same-calendar artefact that
destroyed the original Lerner finding.

Test 1: Pattern 2 same-calendar weeks across years.
  ISP15 window = Dec 2024 - Mar 2025 → compare with Dec 2022-Mar 2023,
    Dec 2023-Mar 2024 (both pre-IDA winters).
  DA60/ID15 = Apr-Sep 2025 → compare with Apr-Sep 2023, Apr-Sep 2024
    (both pre-IDA spring/summer windows).
  DA15/ID15 = Oct-Dec 2025 → compare with Oct-Dec 2023, Oct-Dec 2024.

  If same-calendar pre-IDA reservation share is similar to post-reform
  reservation share, the finding is seasonal not reform-attributable.

Test 2: Pattern 1 pass-through with same-calendar baseline.
  DA60/ID15 R² = 0.305 (n=196 days).
  Run pass-through regression on same calendar weeks across years.
  If R² is also 0.30+ in pre-IDA same-calendar, NOT a regime effect.

Test 3: Bid-distribution shape sanity.
  Are p75 and p95 dominated by a few price-cap (€800) tranches?
  Look at the actual distribution of GE CCGT prices in 3-sess +
  ISP15 — is it bimodal (low-price + €800 only)?
"""
from __future__ import annotations
import duckdb
import pandas as pd
import numpy as np
import statsmodels.api as sm

con = duckdb.connect()
con.execute("SET memory_limit='8GB'")
con.execute("SET threads=4")

# Load tech mapping
ref = pd.read_csv('data/external/omie_reference/lista_unidades.csv', encoding='latin1')
def bucket(t):
    if pd.isna(t):
        return 'Other'
    t = str(t).lower()
    if 'ciclo combinado' in t:
        return 'CCGT'
    return 'Other'
ref['tech'] = ref['technology'].apply(bucket)
con.register('ref', ref[['unit_code', 'tech']])

con.execute("""
    CREATE TEMP TABLE unit_firm AS
    WITH cnts AS (
        SELECT unit_code, grupo_empresarial, COUNT(*) n
        FROM 'data/processed/omie/mercado_diario/programas/pdbce_all.parquet'
        WHERE offer_type=1 AND grupo_empresarial IS NOT NULL
        GROUP BY 1, 2
    ), ranked AS (
        SELECT unit_code, grupo_empresarial AS firm,
               ROW_NUMBER() OVER (PARTITION BY unit_code ORDER BY n DESC) rk
        FROM cnts
    )
    SELECT unit_code, firm FROM ranked WHERE rk=1
""")

con.execute("""
    CREATE TEMP TABLE ccgt_bids AS
    SELECT i.date, uf.firm, i.price_eur_mwh AS p, i.quantity_mw AS q
    FROM 'data/processed/omie/mercado_intradiario_subastas/ofertas/idet_all.parquet' i
    JOIN 'data/processed/omie/mercado_intradiario_subastas/ofertas/icab_all.parquet' c
       ON c.date = i.date
      AND c.session_number = i.session_number
      AND c.offer_code = i.offer_code
      AND c.version = i.version
      AND c.buy_sell = 'V'
    JOIN unit_firm uf ON uf.unit_code = i.unit_code
    JOIN ref r ON r.unit_code = i.unit_code
    WHERE r.tech = 'CCGT'
      AND uf.firm IN ('GE','IB','GN','HC')
      AND i.quantity_mw > 0
      AND i.price_eur_mwh BETWEEN -500 AND 4000
""")

# ============================================================
# Test 1: same-calendar Pattern 2 (reservation share)
# ============================================================
print('=' * 80)
print('TEST 1: Same-calendar reservation share comparison (>€100/MWh)')
print('=' * 80)
print('Question: was reservation pricing already present in pre-IDA winters/springs?')
print('If yes (similar levels in same calendar months), the finding is seasonal.')
print()

df = con.sql("SELECT date, firm, p, q FROM ccgt_bids").df()
df['date'] = pd.to_datetime(df['date'])

def compute_share(sub):
    if sub['q'].sum() == 0:
        return np.nan
    return sub.loc[sub['p'] > 100, 'q'].sum() / sub['q'].sum()

windows = {
    'WINTER (Dec-Mar)': [
        ('2022-23 pre-IDA winter (gas crisis)', '2022-12-01', '2023-03-31'),
        ('2023-24 pre-IDA winter',              '2023-12-01', '2024-03-31'),
        ('2024-25 ISP15 window',                '2024-12-01', '2025-03-18'),
    ],
    'SPRING-SUMMER (Apr-Sep)': [
        ('2023 Apr-Sep pre-IDA',                '2023-04-01', '2023-09-30'),
        ('2024 Apr-Sep pre-IDA + 3-sess',       '2024-04-01', '2024-09-30'),
        ('2025 Apr-Sep DA60/ID15',              '2025-04-01', '2025-09-30'),
    ],
    'AUTUMN-WINTER (Oct-Dec)': [
        ('2022 Oct-Dec pre-IDA',                '2022-10-01', '2022-12-31'),
        ('2023 Oct-Dec pre-IDA',                '2023-10-01', '2023-12-31'),
        ('2024 Oct-Dec 3-sess + ISP15',         '2024-10-01', '2024-12-31'),
        ('2025 Oct-Dec DA15/ID15',              '2025-10-01', '2025-12-31'),
    ],
}

for season, ws in windows.items():
    print(f'\n--- {season} ---')
    print(f"{'firm':<5} {'window':<40}  {'n':>6}  {'q_GWh':>7}  {'p_wmean':>8}  {'res>€100':>9}")
    for firm in ['GE', 'IB', 'GN', 'HC']:
        for wname, s, e in ws:
            sub = df[(df['firm']==firm) & (df['date']>=s) & (df['date']<=e)]
            if len(sub) < 50:
                continue
            res = compute_share(sub)
            wm = (sub['p']*sub['q']).sum()/sub['q'].sum() if sub['q'].sum()>0 else np.nan
            print(f"{firm:<5} {wname:<40}  {len(sub):>6}  {sub['q'].sum()/1e3:>7.0f}  "
                  f"{wm:>8.1f}  {res*100:>9.2f}")
        print()

# ============================================================
# Test 2: bid-distribution shape — is >€100 share dominated by €800 cap?
# ============================================================
print('=' * 80)
print('TEST 2: Bid-distribution shape — >€100 share vs >€500 vs at-cap (€800)')
print('=' * 80)
print('Question: are the "reservation tranches" mostly at the price cap (€800),')
print('or distributed across price levels?')
print()

print(f"{'firm':<5} {'window':<40}  {'>€100':>6}  {'>€300':>6}  {'>€500':>6}  {'≥€700':>6}  {'@cap':>6}")
target_windows = [
    ('2022-23 pre-IDA winter', '2022-12-01', '2023-03-31'),
    ('2023-24 pre-IDA winter', '2023-12-01', '2024-03-31'),
    ('ISP15 window',           '2024-12-01', '2025-03-18'),
    ('2025 Apr-Sep DA60/ID15', '2025-04-01', '2025-09-30'),
    ('2025 Oct-Dec DA15/ID15', '2025-10-01', '2025-12-31'),
]
for firm in ['GE', 'IB']:
    for wname, s, e in target_windows:
        sub = df[(df['firm']==firm) & (df['date']>=s) & (df['date']<=e)]
        if len(sub) < 50:
            continue
        q = sub['q'].sum()
        s100 = sub.loc[sub['p']>100, 'q'].sum()/q
        s300 = sub.loc[sub['p']>300, 'q'].sum()/q
        s500 = sub.loc[sub['p']>500, 'q'].sum()/q
        s700 = sub.loc[sub['p']>=700, 'q'].sum()/q
        scap = sub.loc[sub['p']>=799, 'q'].sum()/q
        print(f"{firm:<5} {wname:<40}  "
              f"{s100*100:>6.1f}  {s300*100:>6.1f}  {s500*100:>6.1f}  "
              f"{s700*100:>6.1f}  {scap*100:>6.1f}")
    print()

# ============================================================
# Test 3: Pass-through same-calendar baseline
# ============================================================
print('=' * 80)
print('TEST 3: Pass-through R² — same calendar weeks across years')
print('=' * 80)

panel = pd.read_parquet('data/derived/panels/passthrough_panel.parquet')
panel['date'] = pd.to_datetime(panel['date'])
panel['imb_GWh'] = panel['abs_imb_mwh'] / 1e3
panel['wind_GWh'] = panel['abs_wind_err'] / 1e3
panel['solar_GWh'] = panel['abs_solar_err'] / 1e3
panel = panel.dropna(subset=['imb_GWh','wind_GWh','solar_GWh'])

print('R² of |V_imb| ~ |wind_err| + |solar_err| in same Apr-Sep window across years:\n')
for label, s, e in [
    ('2018 Apr-Sep', '2018-04-01', '2018-09-30'),
    ('2019 Apr-Sep', '2019-04-01', '2019-09-30'),
    ('2020 Apr-Sep', '2020-04-01', '2020-09-30'),
    ('2021 Apr-Sep', '2021-04-01', '2021-09-30'),
    ('2022 Apr-Sep', '2022-04-01', '2022-09-30'),
    ('2023 Apr-Sep', '2023-04-01', '2023-09-30'),
    ('2024 Apr-Sep', '2024-04-01', '2024-09-30'),
    ('2025 Apr-Sep (DA60/ID15)', '2025-04-01', '2025-09-30'),
]:
    sub = panel[(panel['date']>=s) & (panel['date']<=e)].copy()
    if len(sub) < 30:
        continue
    X = sm.add_constant(sub[['wind_GWh','solar_GWh']].astype(float))
    y = sub['imb_GWh'].astype(float)
    res = sm.OLS(y, X).fit(cov_type='HC3')
    print(f"  {label:<30}  n={len(sub):>4}  R²={res.rsquared:.3f}  "
          f"β_wind={res.params['wind_GWh']:+.3f}  "
          f"β_solar={res.params['solar_GWh']:+.3f}")
