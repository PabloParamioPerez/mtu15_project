# STATUS: ALIVE
# LAST-AUDIT: 2026-04-27
# FEEDS: panel-builder for B5/F11/Pigouvian analyses on ENTSO-E data
# CLAIM: ENTSO-E system supply-margin panel at MTU15 resolution
"""Build a single ENTSO-E system supply-margin panel.

Combines:
  - load actual (A65 realized, ES)
  - wind + solar gen (A75 B16 + B19)
  - cross-border physical flows (A11 FR<->ES net)
  - DA marginal price (OMIE marginalpdbc, ES side)

Output:
  data/derived/panels/entsoe_system_panel.parquet

Schema:
  isp_start_utc, mtu_minutes, load_mw, solar_mw, wind_mw, vre_mw,
  fr_to_es_mw, es_to_fr_mw, net_import_mw, residual_demand_mw, vre_share,
  price_da_eur, regime
"""
from __future__ import annotations

from pathlib import Path
import duckdb
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[3]


def assign_regime(d: pd.Timestamp) -> str:
    if d < pd.Timestamp('2024-06-14'):
        return 'pre-IDA'
    if d < pd.Timestamp('2024-12-01'):
        return '3-sess'
    if d < pd.Timestamp('2025-03-19'):
        return 'ISP15-win'
    if d < pd.Timestamp('2025-04-28'):
        return 'DA60/ID15 PRE-blackout'
    if d < pd.Timestamp('2025-10-01'):
        return 'DA60/ID15 POST-blackout'
    return 'DA15/ID15'


def main() -> None:
    proc = PROJECT_ROOT / 'data/processed/entsoe'
    out_dir = PROJECT_ROOT / 'data/derived/panels'
    out_dir.mkdir(parents=True, exist_ok=True)

    load = pd.read_parquet(proc / 'load/load_actual_all.parquet')
    load = load[['isp_start_utc', 'mtu_minutes', 'load_mw']].drop_duplicates('isp_start_utc')

    ws = pd.read_parquet(proc / 'generation/wind_solar_actual_all.parquet')
    ws_red = ws[ws['psr_type'].isin(['B16', 'B19'])]
    ws_agg = ws_red.groupby(['isp_start_utc', 'psr_type'])['quantity_mw'].sum().unstack(fill_value=0)
    ws_agg.columns = ['solar_mw' if c == 'B16' else 'wind_mw' for c in ws_agg.columns]
    ws_agg = ws_agg.reset_index()

    fres = pd.read_parquet(proc / 'transmission/flows_physical_fr_to_es_all.parquet')[['isp_start_utc', 'quantity_mw']]
    fres.columns = ['isp_start_utc', 'fr_to_es_mw']
    esfr = pd.read_parquet(proc / 'transmission/flows_physical_es_to_fr_all.parquet')[['isp_start_utc', 'quantity_mw']]
    esfr.columns = ['isp_start_utc', 'es_to_fr_mw']

    panel = load.merge(ws_agg, on='isp_start_utc', how='left')
    panel = panel.merge(fres, on='isp_start_utc', how='left')
    panel = panel.merge(esfr, on='isp_start_utc', how='left')
    panel = panel.fillna(0)
    panel['net_import_mw'] = panel['fr_to_es_mw'] - panel['es_to_fr_mw']
    panel['vre_mw'] = panel['wind_mw'] + panel['solar_mw']
    panel['residual_demand_mw'] = panel['load_mw'] - panel['vre_mw'] - panel['net_import_mw']
    panel['vre_share'] = panel['vre_mw'] / panel['load_mw']

    con = duckdb.connect()
    prices = con.execute(f"""
        SELECT date, period, price_es_eur_mwh, mtu_minutes
        FROM '{PROJECT_ROOT}/data/processed/omie/mercado_diario/precios/marginalpdbc_all.parquet'
    """).df()
    prices_h = prices[prices['mtu_minutes'] == 60].copy()
    prices_h['ts'] = pd.to_datetime(prices_h['date']) + pd.to_timedelta(prices_h['period'].astype(int) - 1, unit='h')
    prices_h = prices_h[['ts', 'price_es_eur_mwh']].drop_duplicates('ts')
    prices_h.columns = ['isp_start_utc', 'price_da_eur']

    prices_15 = prices[prices['mtu_minutes'] == 15].copy()
    prices_15['ts'] = pd.to_datetime(prices_15['date']) + pd.to_timedelta((prices_15['period'].astype(int) - 1) * 15, unit='min')
    prices_15 = prices_15[['ts', 'price_es_eur_mwh']].drop_duplicates('ts')
    prices_15.columns = ['isp_start_utc', 'price_da_eur']

    prices_all = pd.concat([prices_h, prices_15]).drop_duplicates('isp_start_utc')
    panel = panel.merge(prices_all, on='isp_start_utc', how='left')

    panel['regime'] = pd.to_datetime(panel['isp_start_utc']).apply(assign_regime)
    out = out_dir / 'entsoe_system_panel.parquet'
    panel.to_parquet(out, index=False)
    print(f'wrote {out} ({len(panel):,} rows)')


if __name__ == '__main__':
    main()
