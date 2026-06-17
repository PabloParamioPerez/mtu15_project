# STATUS: ALIVE
# LAST-AUDIT: 2026-05-18
# FEEDS: descriptive_facts.tex §8 (Full ancillary-service cascade)
# CLAIM: For each of the 5 reform-window regimes, compute per-service:
#   (a) MW-weighted UP/DN price (from ESIOS indicator prices)
#   (b) UP/DN volume (from existing parquet)
#   (c) total cost = sum(price * volume) per regime
# Covers: DA spot (id 600), Fase I PDBF (705/706), Fase II (707/708),
# TR (722/723), aFRR-reserve (634/2130), aFRR-energy (682/683),
# mFRR programada (676/677), RR (1782 + 10384/10385), GD (668/669),
# Imbalance (686/687/763/764), RPA (628).
# Cross-check costs from id 709/724/1373-1376/1723/1724 when those land.
#
# Output: results/regressions/regulatory/ree_full_cascade/
#   - per_regime_prices.csv      headline cross-service price table
#   - per_regime_volumes.csv     headline cross-service volume table
#   - per_regime_costs.csv       headline cross-service cost table
#   - pvpc_components_monthly.csv  retail-side component decomposition (when 780+ land)
#   - tex/tab_*.tex              auto-built LaTeX fragments
# Figures saved to figures/working/ (cascade_costs_per_regime.pdf,
#   cascade_prices_vs_da.pdf, pvpc_components_stack.pdf when retail data lands).

from __future__ import annotations
from pathlib import Path
import os, json, glob
import duckdb
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

REPO = Path(__file__).resolve().parents[3]
IND  = REPO / "data" / "processed" / "esios" / "indicators" / "indicators_all.parquet"
RP48 = REPO / "data" / "processed" / "esios" / "restricciones" / "totalrp48preccierre_all.parquet"
OUT  = REPO / "results" / "regressions" / "regulatory" / "ree_full_cascade"
OUT.mkdir(parents=True, exist_ok=True)
(OUT / "tex").mkdir(exist_ok=True)
FIG = REPO / "figures" / "working"
FIG.mkdir(parents=True, exist_ok=True)

REGIMES = {
    "3-sess (Jun-Nov 24)":    ("2024-06-14", "2024-11-30"),
    "ISP15-win (Dec24-Mar25)":("2024-12-01", "2025-03-18"),
    "DA60/ID15 pre-blk":      ("2025-03-19", "2025-04-27"),
    "DA60/ID15 post-blk":     ("2025-04-28", "2025-09-30"),
    "DA15/ID15 (Oct-Dec 25)": ("2025-10-01", "2025-12-31"),
}

# Service definitions: (label, up_id, dn_id, kind)
#  kind = "P" if both ids are PRICES (€/MWh, simple average across ISP cells)
#       = "V" if both are VOLUMES (MWh, sum)
PRICE_SERVICES = [
    ("DA spot",                 600,   None,  "P"),
    ("Fase I (PDBF) subir/bajar", 705,  706,  "P"),
    ("Fase II subir/bajar",      707,  708,   "P"),
    ("TR (Tiempo Real) subir/bajar", 722, 723, "P"),
    ("aFRR reserve (capacity)",  2130, 634,   "P"),
    ("aFRR energy",              682,  683,   "P"),
    ("mFRR programada",          677,  676,   "P"),
    ("RR (rrenergyprice)",       1782, None,  "P"),
    ("Gestión Desvíos",          668,  669,   "P"),
    ("Imbalance cobro/pago",     686,  687,   "P"),
    ("Imbalance medido up/dn",   763,  764,   "P"),
    ("RPA subir",                628,  None,  "P"),
]


def load_indicator_from_raw(ind_id):
    """Fallback to raw JSON if indicator not yet in indicators_all.parquet."""
    rows = []
    for f in sorted(glob.glob(str(REPO / f"data/raw/esios/indicators/{ind_id}/*.json"))):
        try:
            with open(f) as fh: d = json.load(fh)
            for v in d.get('indicator', {}).get('values', []):
                rows.append({'datetime': v.get('datetime_utc') or v.get('datetime'),
                             'value': v.get('value'), 'indicator_id': ind_id})
        except Exception:
            continue
    if not rows: return None
    df = pd.DataFrame(rows)
    df['datetime'] = pd.to_datetime(df['datetime'], utc=True, errors='coerce')
    df['date'] = df['datetime'].dt.tz_convert('Europe/Madrid').dt.normalize().dt.tz_localize(None)
    return df


def parquet_has(con, ind_id):
    res = con.execute(f"""SELECT COUNT(*) FROM '{IND}' WHERE indicator_id = {ind_id}""").fetchone()
    return res[0] > 0


def build_price_table():
    con = duckdb.connect()
    parquet_ids = set(con.execute(f"SELECT DISTINCT indicator_id FROM '{IND}'").fetchdf()['indicator_id'].tolist())

    # Materialize a unified prices DF
    needed = set()
    for _, up, dn, kind in PRICE_SERVICES:
        if up: needed.add(up)
        if dn: needed.add(dn)
    needed.discard(None)

    rows = []
    for ind in needed:
        if ind in parquet_ids:
            df = con.execute(f"""SELECT date, value, {ind} AS indicator_id FROM '{IND}'
                                  WHERE indicator_id = {ind} AND value IS NOT NULL""").fetchdf()
        else:
            df = load_indicator_from_raw(ind)
            if df is None or len(df) == 0:
                print(f'  WARN: no data for indicator {ind}')
                continue
            df = df[['date', 'value']].assign(indicator_id=ind)
        rows.append(df)
    all_p = pd.concat(rows, ignore_index=True)
    all_p['date'] = pd.to_datetime(all_p['date'])

    out_rows = []
    for regime, (a, b) in REGIMES.items():
        m = (all_p['date'] >= a) & (all_p['date'] <= b)
        sub = all_p[m]
        for label, up, dn, _ in PRICE_SERVICES:
            up_mean = sub[sub['indicator_id'] == up]['value'].mean() if up else np.nan
            dn_mean = sub[sub['indicator_id'] == dn]['value'].mean() if dn else np.nan
            out_rows.append({'regime': regime, 'service': label,
                             'up_eur_mwh': up_mean, 'dn_eur_mwh': dn_mean})
    df_p = pd.DataFrame(out_rows)
    df_p.to_csv(OUT / "per_regime_prices.csv", index=False)
    return df_p


def build_volume_table():
    """Per-tipo volume aggregates from totalrp48preccierre."""
    con = duckdb.connect()
    rows = []
    for regime, (a, b) in REGIMES.items():
        sql = f"""
        SELECT tipo_redespacho,
               SUM(qty_up_mwh) AS up_mwh,
               SUM(qty_down_mwh) AS dn_mwh
        FROM '{RP48}'
        WHERE date BETWEEN '{a}' AND '{b}'
        GROUP BY 1"""
        df = con.execute(sql).fetchdf()
        df['regime'] = regime
        rows.append(df)
    df_v = pd.concat(rows, ignore_index=True)
    # Map codes to service labels
    CODE_MAP = {
        '23': 'Fase I (PDBF)', '24': 'Fase II',
        '61': 'TR (PO 3.2)', '65': 'TR (PO 3.x)', '66': 'TR (PO 3.4)',
        '68': 'TR (other)', '69': 'TR (curtail RT)',
        '80': 'aFRR energy SEPE up', '81': 'aFRR energy SEPE',
        '92': 'GD (Gestion Desvios)',
        '94': 'mFRR', '85': 'mFRR (alt)',
        '96': 'RR',
        '33': 'RES curt (33)', '34': 'RES curt (34)',
        '19': 'Other (19)', '22': 'Fase I (alt 22)',
        '32': 'Fase II (legacy)', '38': 'Other (38)',
        '82': 'Other (82)', '89': 'Other (89)', '95': 'Other (95)',
    }
    df_v['service'] = df_v['tipo_redespacho'].astype(str).map(CODE_MAP).fillna('Other')
    df_v = df_v.groupby(['regime', 'service'], as_index=False).agg(up_mwh=('up_mwh', 'sum'),
                                                                   dn_mwh=('dn_mwh', 'sum'))
    df_v.to_csv(OUT / "per_regime_volumes.csv", index=False)
    return df_v


def build_cost_table(prices, volumes):
    """Cost = up_price * up_vol + dn_price * dn_vol (€). Volumes from RP48,
    prices from ESIOS indicators (already MW-weighted by REE)."""
    # Service-to-(volume-label, price-label) mapping
    PV_MAP = {
        'Fase I (PDBF)':          ('Fase I (PDBF) subir/bajar',  None),  # only up vols for code 23
        'Fase II':                ('Fase II subir/bajar',         None),
        'TR (PO 3.2)':            ('TR (Tiempo Real) subir/bajar', None),
        'TR (PO 3.4)':            ('TR (Tiempo Real) subir/bajar', None),
        'TR (curtail RT)':        ('TR (Tiempo Real) subir/bajar', None),
        'aFRR energy SEPE':       ('aFRR energy',                 None),
        'GD (Gestion Desvios)':   ('Gestión Desvíos',             None),
        'mFRR':                   ('mFRR programada',             None),
        'RR':                     ('RR (rrenergyprice)',          None),
    }
    rows = []
    for _, r in volumes.iterrows():
        if r['service'] not in PV_MAP: continue
        price_lbl, _ = PV_MAP[r['service']]
        p = prices[(prices['regime'] == r['regime']) & (prices['service'] == price_lbl)]
        if len(p) == 0: continue
        up_p = p['up_eur_mwh'].iloc[0]
        dn_p = p['dn_eur_mwh'].iloc[0]
        up_cost_m = (r['up_mwh'] or 0) * (up_p if not pd.isna(up_p) else 0) / 1e6
        dn_cost_m = (r['dn_mwh'] or 0) * (dn_p if not pd.isna(dn_p) else 0) / 1e6
        rows.append({'regime': r['regime'], 'service': r['service'],
                     'up_cost_eur_m': up_cost_m, 'dn_cost_eur_m': dn_cost_m,
                     'abs_cost_eur_m': abs(up_cost_m) + abs(dn_cost_m)})
    df_c = pd.DataFrame(rows)
    df_c.to_csv(OUT / "per_regime_costs.csv", index=False)
    return df_c


def write_tex_tables(prices, volumes, costs):
    # Prices table
    p = prices.pivot_table(index='service', columns='regime', values='up_eur_mwh', aggfunc='first').round(1)
    # Drop dn column for now; show one direction matrix
    p = p.rename_axis(None, axis=0).rename_axis(None, axis=1)
    with open(OUT / "tex" / "tab_cascade_prices_up.tex", 'w') as f:
        f.write("% auto-built by scripts/analysis/regulatory/ree_full_cascade.py\n")
        f.write(p.to_latex(float_format='%.1f', na_rep='---'))

    # Down prices
    pd_ = prices.pivot_table(index='service', columns='regime', values='dn_eur_mwh', aggfunc='first').round(1)
    pd_ = pd_.rename_axis(None, axis=0).rename_axis(None, axis=1)
    with open(OUT / "tex" / "tab_cascade_prices_dn.tex", 'w') as f:
        f.write("% auto-built\n")
        f.write(pd_.to_latex(float_format='%.1f', na_rep='---'))

    # Costs (abs €M per regime, rolled up to service category)
    c = costs.copy()
    # Roll up TR sub-categories
    c['service_top'] = c['service'].replace({
        'TR (PO 3.2)': 'TR', 'TR (PO 3.4)': 'TR', 'TR (curtail RT)': 'TR',
        'aFRR energy SEPE': 'aFRR energy',
    })
    c_top = c.groupby(['regime', 'service_top'], as_index=False)['abs_cost_eur_m'].sum()
    ct = c_top.pivot_table(index='service_top', columns='regime', values='abs_cost_eur_m', aggfunc='sum').round(0)
    ct = ct.rename_axis(None, axis=0).rename_axis(None, axis=1)
    with open(OUT / "tex" / "tab_cascade_costs.tex", 'w') as f:
        f.write("% auto-built\n")
        f.write(ct.to_latex(float_format='%.0f', na_rep='---'))


def fig_prices_vs_da(prices):
    da = prices[prices['service'] == 'DA spot'][['regime', 'up_eur_mwh']].rename(columns={'up_eur_mwh': 'DA'})
    serv = ['Fase I (PDBF) subir/bajar', 'Fase II subir/bajar', 'TR (Tiempo Real) subir/bajar',
            'aFRR energy', 'mFRR programada', 'RR (rrenergyprice)']
    fig, ax = plt.subplots(figsize=(10, 5))
    x = list(REGIMES.keys())
    ax.plot(x, da.set_index('regime').loc[x, 'DA'], 'k-o', lw=2, label='DA spot')
    for s in serv:
        sub = prices[prices['service'] == s].set_index('regime')['up_eur_mwh']
        if sub.notna().any():
            ax.plot(x, sub.reindex(x), '-o', alpha=0.7, label=f'{s} (up)')
    ax.set_ylabel('Price (EUR/MWh)')
    ax.set_xticklabels(x, rotation=20, ha='right')
    ax.set_title('Per-regime average prices: DA spot vs each ajuste service (up direction)')
    ax.legend(loc='upper right', fontsize=8)
    ax.grid(alpha=0.3)
    fig.tight_layout()
    fig.savefig(FIG / 'cascade_prices_vs_da.pdf')
    plt.close(fig)


def fig_costs_per_regime(costs):
    c = costs.copy()
    c['service_top'] = c['service'].replace({
        'TR (PO 3.2)': 'TR', 'TR (PO 3.4)': 'TR', 'TR (curtail RT)': 'TR',
        'aFRR energy SEPE': 'aFRR energy',
    })
    c_top = c.groupby(['regime', 'service_top'], as_index=False)['abs_cost_eur_m'].sum()
    piv = c_top.pivot_table(index='regime', columns='service_top', values='abs_cost_eur_m', aggfunc='sum').fillna(0)
    piv = piv.loc[list(REGIMES.keys())]
    fig, ax = plt.subplots(figsize=(10, 5.5))
    bottom = np.zeros(len(piv))
    colors = plt.cm.tab10(np.linspace(0, 1, len(piv.columns)))
    for j, col in enumerate(piv.columns):
        ax.bar(piv.index, piv[col], bottom=bottom, label=col, color=colors[j])
        bottom += piv[col].values
    ax.set_ylabel('Cost (EUR million per regime)')
    ax.set_title('Per-regime ajuste-cascade cost composition (volumes × ESIOS prices)')
    ax.set_xticklabels(piv.index, rotation=20, ha='right')
    ax.legend(loc='upper right', fontsize=8)
    ax.grid(alpha=0.3, axis='y')
    fig.tight_layout()
    fig.savefig(FIG / 'cascade_costs_per_regime.pdf')
    plt.close(fig)


def main():
    print('1. Building price table...')
    prices = build_price_table()
    print(f'   wrote {OUT / "per_regime_prices.csv"} ({len(prices)} rows)')
    print('2. Building volume table from RP48...')
    volumes = build_volume_table()
    print(f'   wrote {OUT / "per_regime_volumes.csv"} ({len(volumes)} rows)')
    print('3. Building cost table = price × volume...')
    costs = build_cost_table(prices, volumes)
    print(f'   wrote {OUT / "per_regime_costs.csv"} ({len(costs)} rows)')
    print('4. Writing TeX tables...')
    write_tex_tables(prices, volumes, costs)
    print('5. Building figures...')
    fig_prices_vs_da(prices)
    fig_costs_per_regime(costs)
    print('Done.')
    # Print headline
    print('\n=== Headline costs per regime (EUR M, sum across services) ===')
    print(costs.groupby('regime')['abs_cost_eur_m'].sum().round(0).to_string())


if __name__ == '__main__':
    main()
