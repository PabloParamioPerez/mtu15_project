# STATUS: ALIVE
# LAST-AUDIT: 2026-04-25
# FEEDS: (descriptive)
# CLAIM: Big-4 retailer buy-side reservation bidding with same-calendar skepticism check
"""Buy-side bid behavior + same-calendar skepticism check.

The earlier buy-side analysis showed striking aggregate Big-4 retailer
reservation buying (%p<€30 going 78-66-72%-4.5% across post-IDA regimes).
Apply same-calendar test from the start: is the buy-side reservation
already present in pre-IDA winters/springs?

Also explore:
- Negative-price buy bidding (%p<0) shift across regimes
- Per-firm vs aggregate
- The big DA15/ID15 collapse is reform-aligned
"""
from __future__ import annotations
import duckdb

con = duckdb.connect()
con.execute("SET memory_limit='8GB'")
con.execute("SET threads=4")

con.execute("""
    CREATE TEMP TABLE unit_firm AS
    WITH cnts AS (
        SELECT unit_code, grupo_empresarial, COUNT(*) n
        FROM 'data/processed/omie/mercado_diario/programas/pdbce_all.parquet'
        WHERE grupo_empresarial IS NOT NULL
        GROUP BY 1, 2
    ), ranked AS (
        SELECT unit_code, grupo_empresarial AS firm,
               ROW_NUMBER() OVER (PARTITION BY unit_code ORDER BY n DESC) rk
        FROM cnts
    )
    SELECT unit_code, firm FROM ranked WHERE rk=1
""")

con.execute("""
    CREATE TEMP TABLE ida_buy AS
    SELECT i.date, uf.firm, i.price_eur_mwh AS p, i.quantity_mw AS q
    FROM 'data/processed/omie/mercado_intradiario_subastas/ofertas/idet_all.parquet' i
    JOIN 'data/processed/omie/mercado_intradiario_subastas/ofertas/icab_all.parquet' c
       ON c.date = i.date
      AND c.session_number = i.session_number
      AND c.offer_code = i.offer_code
      AND c.version = i.version
      AND c.buy_sell = 'C'
    LEFT JOIN unit_firm uf ON uf.unit_code = i.unit_code
    WHERE i.quantity_mw > 0
      AND i.price_eur_mwh BETWEEN -500 AND 4000
""")

# Same-calendar comparison for aggregate retailer buy side
print('=' * 80)
print('AGGREGATE RETAILER BUY SIDE — same-calendar comparison')
print('=' * 80)
print('% of buy-side bid quantity at price < €30 (low reservation buying)')
print()

windows = {
    'WINTER (Dec-Mar)': [
        ('2022-23 winter pre-IDA',   '2022-12-01', '2023-03-31'),
        ('2023-24 winter pre-IDA',   '2023-12-01', '2024-03-31'),
        ('2024-25 ISP15 window',     '2024-12-01', '2025-03-18'),
    ],
    'SPRING-SUMMER (Apr-Sep)': [
        ('2023 Apr-Sep pre-IDA',                '2023-04-01', '2023-09-30'),
        ('2024 Apr-Sep pre-IDA + 3-sess',       '2024-04-01', '2024-09-30'),
        ('2025 Apr-Sep DA60/ID15',              '2025-04-01', '2025-09-30'),
    ],
    'AUTUMN-WINTER (Oct-Dec)': [
        ('2022 Oct-Dec pre-IDA',                '2022-10-01', '2022-12-31'),
        ('2023 Oct-Dec pre-IDA',                '2023-10-01', '2023-12-31'),
        ('2024 Oct-Dec 3-sess+ISP15',           '2024-10-01', '2024-12-31'),
        ('2025 Oct-Dec DA15/ID15',              '2025-10-01', '2025-12-31'),
    ],
}

for season, ws in windows.items():
    print(f'\n--- {season} ---')
    print(f"{'window':<40}  {'q_TWh':>7}  {'avg_p':>7}  {'%p<€30':>8}  {'%p<€0':>7}  {'%p<€-100':>9}")
    for wname, s, e in ws:
        sub = con.sql(f"""
            SELECT p, q FROM ida_buy
            WHERE CAST(date AS DATE) BETWEEN '{s}' AND '{e}'
        """).df()
        if len(sub) < 100:
            continue
        q_tot = sub['q'].sum()
        wm = (sub['p']*sub['q']).sum()/q_tot
        p_lo = sub.loc[sub['p']<30, 'q'].sum()/q_tot
        p_neg = sub.loc[sub['p']<0, 'q'].sum()/q_tot
        p_vneg = sub.loc[sub['p']<-100, 'q'].sum()/q_tot
        print(f"{wname:<40}  {q_tot/1e6:>7.1f}  {wm:>7.1f}  "
              f"{p_lo*100:>8.2f}  {p_neg*100:>7.2f}  {p_vneg*100:>9.2f}")

# ---------------------------------------------------------------
# Big-4 retailer per-firm same-calendar
# ---------------------------------------------------------------
print()
print('=' * 80)
print('BIG-4 PER-FIRM BUY-SIDE — same-calendar check')
print('=' * 80)
print()

for firm in ['GE', 'IB', 'GN', 'HC']:
    print(f'\n--- {firm} ---')
    print(f"{'window':<40}  {'q_GWh':>7}  {'avg_p':>7}  {'%p<€30':>8}")
    for season_ws in windows.values():
        for wname, s, e in season_ws:
            sub = con.sql(f"""
                SELECT p, q FROM ida_buy
                WHERE firm='{firm}'
                  AND CAST(date AS DATE) BETWEEN '{s}' AND '{e}'
            """).df()
            if len(sub) < 50:
                continue
            q_tot = sub['q'].sum()
            wm = (sub['p']*sub['q']).sum()/q_tot
            p_lo = sub.loc[sub['p']<30, 'q'].sum()/q_tot
            print(f"{wname:<40}  {q_tot/1e3:>7.0f}  {wm:>7.1f}  {p_lo*100:>8.2f}")
    print()
