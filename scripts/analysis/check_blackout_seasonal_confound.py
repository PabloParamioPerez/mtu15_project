"""Probe: is the pre-blackout DA60/ID15 Lerner spike a seasonal artefact?

Compare GE and IB Lerner in:
  (i)  pre-blackout DA60/ID15 sub-window (Mar 19-Apr 27 2025)
  (ii) same calendar weeks one year earlier (Mar 19-Apr 27 2024) -- pre-IDA reform
  (iii) same calendar weeks two years earlier (Mar 19-Apr 27 2023)
  (iv) full DA60/ID15
  (v)  pre-blackout sub-window restricted to peak demand hours (h13-h18)

Also report supply slope levels in each sub-window.
"""
from __future__ import annotations
import duckdb
import pandas as pd
from pathlib import Path

LER = Path('data/derived/firm_lerner_hourly.parquet')
SLP = Path('data/derived/supply_slope_hourly.parquet')

con = duckdb.connect()
con.execute("SET memory_limit='6GB'")
con.execute("SET threads=4")

df = con.sql(f"""
    SELECT date, hour, firm, lerner_index, q_mwh, s_share,
           clearing_price_eur_mwh AS p, supply_slope_mw_per_eur AS slope
    FROM '{LER}'
    WHERE lerner_index BETWEEN 0 AND 1
""").df()
df['date'] = pd.to_datetime(df['date'])

slope_only = con.sql(f"""
    SELECT date, hour, clearing_price_eur_mwh AS p, supply_slope_mw_per_eur AS slope
    FROM '{SLP}'
""").df()
slope_only['date'] = pd.to_datetime(slope_only['date'])

windows = {
    '2025 pre-blackout DA60/ID15':  ('2025-03-19', '2025-04-27'),
    '2024 same calendar (pre-IDA)': ('2024-03-19', '2024-04-27'),
    '2023 same calendar (pre-IDA)': ('2023-03-19', '2023-04-27'),
    '2022 same calendar (pre-IDA)': ('2022-03-19', '2022-04-27'),
    'DA60/ID15 post-blackout':       ('2025-04-29', '2025-09-30'),
    'DA60/ID15 full':                ('2025-03-19', '2025-09-30'),
}

print('=== Slope-only panel (no firm) — supply curve characterisation ===\n')
print(f"{'window':<32}  {'n_h':>5}  {'avg_p':>6}  {'avg_slope':>10}  {'p10_slope':>10}  {'p50_slope':>10}")
for wname, (s, e) in windows.items():
    sub = slope_only[(slope_only['date'] >= s) & (slope_only['date'] <= e)]
    if len(sub) == 0:
        continue
    print(f"{wname:<32}  {len(sub):>5}  "
          f"{sub['p'].mean():>6.1f}  "
          f"{sub['slope'].mean():>10.1f}  "
          f"{sub['slope'].quantile(0.1):>10.1f}  "
          f"{sub['slope'].median():>10.1f}")

print()
print('=== Firm Lerner per window (median + share + p) ===\n')
print(f"{'firm':<5} {'window':<32}  {'n':>5}  {'med_L':>6}  {'avg_s':>6}  {'avg_p':>6}  {'avg_slope':>10}")
for firm in ['GE', 'IB']:
    for wname, (s, e) in windows.items():
        sub = df[(df['firm'] == firm) & (df['date'] >= s) & (df['date'] <= e)]
        if len(sub) == 0:
            continue
        print(f"{firm:<5} {wname:<32}  {len(sub):>5}  "
              f"{sub['lerner_index'].median():>6.3f}  "
              f"{sub['s_share'].mean():>6.3f}  "
              f"{sub['p'].mean():>6.1f}  "
              f"{sub['slope'].mean():>10.1f}")
    print()

# Peak-hour-only cut for the 2025 pre-blackout window
print('=== 2025 pre-blackout, peak-demand hours only (h13-h18) ===\n')
print(f"{'firm':<5}  {'n':>5}  {'med_L':>6}  {'avg_s':>6}  {'avg_p':>6}  {'avg_slope':>10}")
for firm in ['GE', 'IB']:
    sub = df[(df['firm'] == firm)
             & (df['date'] >= '2025-03-19') & (df['date'] <= '2025-04-27')
             & (df['hour'].between(13, 18))]
    print(f"{firm:<5}  {len(sub):>5}  "
          f"{sub['lerner_index'].median():>6.3f}  "
          f"{sub['s_share'].mean():>6.3f}  "
          f"{sub['p'].mean():>6.1f}  "
          f"{sub['slope'].mean():>10.1f}")
