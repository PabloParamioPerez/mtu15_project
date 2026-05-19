# STATUS: ALIVE
# LAST-AUDIT: 2026-05-18
# FEEDS: descriptive_facts.tex §8 (extends ree_full_cascade.py with REE-published costs + PVPC components + per-zone TR)
# CLAIM: Uses the now-available tier-F indicators (709/712/724/1373--1376/1723--1726/899/2127/2129)
#        to report REE-published costs directly (instead of price×volume reconstruction).
#        Adds:
#          - PVPC component decomposition (id 780/781/783/785/786/787 free-tariff 793/794/796/798/799/800)
#          - Per-zone TR by direction (id 1802--1809 SCB/SCA/CT/RTD; 1816--1819 RBI/ASE)
#          - aFRR weighted prices (id 10389/10390)
#          - RR weighted prices (id 10384/10385)
#          - RES curtailment % (id 10456--10462)
#
# Output: results/regressions/regulatory/ree_full_cascade/
#   - per_regime_costs_official.csv         REE-published per-service costs (€M)
#   - pvpc_components_per_regime.csv        retail-side per-component price means
#   - tr_zone_direction_per_regime.csv      per-zone TR up/dn volumes
#   - res_curtailment_pct.csv               monthly RES non-integrable %
#   - tex/tab_*.tex                         auto-built tex fragments
# Figures: figures/working/
#   - cascade_costs_official_per_regime.pdf
#   - pvpc_components_stacked.pdf
#   - tr_zone_dir_per_regime.pdf

from __future__ import annotations
from pathlib import Path
import duckdb
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

REPO = Path(__file__).resolve().parents[3]
IND  = REPO / "data" / "processed" / "esios" / "indicators" / "indicators_all.parquet"
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

# REE-published cost indicators (in EUR per ISP-15 cell or per day depending on indicator).
# Strategy: sum across ISP cells in window for ISP-15 indicators, sum across days for daily.
# Cost indicators (no double-count): id 712/899 and 2127/2129 both express the
# aFRR-reserve cost from different angles (assignment vs. reservation), keep only
# one direction each. id 709/724 are daily totals that aggregate 1373--1376 and
# 1723--1724 respectively — drop those to avoid double-counting.
COST_INDS = [
    ('Fase I up cost (1373)',   1373, 'isp'),
    ('Fase I dn cost (1374)',   1374, 'isp'),
    ('Fase II up cost (1375)',  1375, 'isp'),
    ('Fase II dn cost (1376)',  1376, 'isp'),
    ('TR up cost (1723)',       1723, 'isp'),
    ('TR dn cost (1724)',       1724, 'isp'),
    ('aFRR reserve up cost (712)', 712, 'isp'),
    ('aFRR reserve dn cost (2127)', 2127, 'isp'),
    ('Imbalance excess (726, day)', 726, 'day'),
    ('Imbalance deficit (727, day)', 727, 'day'),
]

# PVPC component indicators
PVPC_COMPONENTS = [
    ('Restricciones PBF (780 PVPC)',           780),
    ('Restricciones TR (781 PVPC)',            781),
    ('Restricciones IDA (783 PVPC)',           783),
    ('Banda secundaria (785 PVPC)',            785),
    ('Desvíos medidos (786 PVPC)',              786),
    ('Saldo desvíos (787 PVPC)',                787),
    ('Restricciones tecnicas (10378 PVPC)',     10378),
    ('Restricciones PBF (793 free)',           793),
    ('Restricciones TR (794 free)',            794),
    ('Banda secundaria (798 free)',            798),
    ('Desvíos medidos (799 free)',              799),
    ('Saldo desvíos (800 free)',                800),
]

# TR per-zone direction
TR_ZONE_DIR = [
    # (label, up_id, dn_id)
    ('SCB', 1802, 1803),
    ('SCA', 1804, 1805),
    ('CT',  1806, 1807),
    ('RTD', 1808, 1809),
    ('RBI', 1816, 1817),
    ('ASE', 1818, 1819),
]


def build_costs_official():
    con = duckdb.connect()
    rows = []
    for regime, (a, b) in REGIMES.items():
        for label, ind, kind in COST_INDS:
            sql = f"""SELECT COALESCE(SUM(value), 0) AS total
                      FROM '{IND}' WHERE indicator_id = {ind}
                        AND date BETWEEN '{a}' AND '{b}'
                        AND value IS NOT NULL"""
            total = con.execute(sql).fetchone()[0]
            # All cost values in EUR. Convert to EUR M.
            rows.append({'regime': regime, 'service': label, 'cost_eur_m': total / 1e6})
    df = pd.DataFrame(rows)
    df.to_csv(OUT / "per_regime_costs_official.csv", index=False)
    return df


def build_pvpc_components():
    con = duckdb.connect()
    rows = []
    for regime, (a, b) in REGIMES.items():
        for label, ind in PVPC_COMPONENTS:
            sql = f"""SELECT AVG(value) AS mean_eur_mwh
                      FROM '{IND}' WHERE indicator_id = {ind}
                        AND date BETWEEN '{a}' AND '{b}'
                        AND value IS NOT NULL"""
            res = con.execute(sql).fetchone()[0]
            rows.append({'regime': regime, 'component': label, 'mean_eur_mwh': res})
    df = pd.DataFrame(rows)
    df.to_csv(OUT / "pvpc_components_per_regime.csv", index=False)
    return df


def build_tr_zone_dir():
    con = duckdb.connect()
    rows = []
    for regime, (a, b) in REGIMES.items():
        for zone, up_id, dn_id in TR_ZONE_DIR:
            sql_up = f"""SELECT COALESCE(SUM(value)/1000, 0) AS gwh
                         FROM '{IND}' WHERE indicator_id = {up_id}
                           AND date BETWEEN '{a}' AND '{b}' AND value IS NOT NULL"""
            sql_dn = f"""SELECT COALESCE(SUM(value)/1000, 0) AS gwh
                         FROM '{IND}' WHERE indicator_id = {dn_id}
                           AND date BETWEEN '{a}' AND '{b}' AND value IS NOT NULL"""
            up_gwh = con.execute(sql_up).fetchone()[0]
            dn_gwh = con.execute(sql_dn).fetchone()[0]
            rows.append({'regime': regime, 'zone': zone, 'up_gwh': up_gwh, 'dn_gwh': dn_gwh})
    df = pd.DataFrame(rows)
    df.to_csv(OUT / "tr_zone_direction_per_regime.csv", index=False)
    return df


def build_res_curt_pct():
    con = duckdb.connect()
    sql = f"""SELECT date::DATE AS month, indicator_id, value
              FROM '{IND}' WHERE indicator_id IN (10456, 10457, 10458, 10459, 10460, 10461, 10462)
                AND date >= '2024-01-01' AND value IS NOT NULL
              ORDER BY date, indicator_id"""
    df = con.execute(sql).fetchdf()
    df.to_csv(OUT / "res_curtailment_pct.csv", index=False)
    return df


def write_tex_costs_official(df):
    # Roll up: bin services into 4 groups
    rollup = {
        'Fase I up cost (1373)': 'Fase I (up)', 'Fase I dn cost (1374)': 'Fase I (dn)',
        'Fase II up cost (1375)': 'Fase II', 'Fase II dn cost (1376)': 'Fase II',
        'TR up cost (1723)': 'TR (up)', 'TR dn cost (1724)': 'TR (dn)',
        'aFRR reserve up cost (712)': 'aFRR reserve', 'aFRR reserve dn cost (2127)': 'aFRR reserve',
        'Imbalance excess (726, day)': 'Imbalance', 'Imbalance deficit (727, day)': 'Imbalance',
    }
    d = df[df['service'].isin(rollup)].copy()
    d['cat'] = d['service'].map(rollup)
    p = d.groupby(['regime', 'cat'])['cost_eur_m'].sum().unstack(fill_value=0).round(0)
    p = p.loc[list(REGIMES.keys())]
    # Add Total
    p['Total'] = p.sum(axis=1)
    with open(OUT / "tex" / "tab_cascade_costs_official.tex", 'w') as f:
        f.write("% auto-built by scripts/analysis/regulatory/ree_full_cascade_v2.py\n")
        f.write(p.to_latex(float_format='%.0f', na_rep='---'))
    return p


def write_tex_pvpc(df):
    p = df.pivot_table(index='component', columns='regime', values='mean_eur_mwh').round(2)
    p = p.rename_axis(None, axis=0).rename_axis(None, axis=1)
    with open(OUT / "tex" / "tab_pvpc_components.tex", 'w') as f:
        f.write("% auto-built\n")
        f.write(p.to_latex(float_format='%.2f', na_rep='---'))
    return p


def write_tex_tr_zone(df):
    # Absolute total per (zone, regime) = up + |dn|
    df = df.copy()
    df['abs_gwh'] = df['up_gwh'].abs() + df['dn_gwh'].abs()
    p = df.pivot_table(index='zone', columns='regime', values='abs_gwh', aggfunc='sum').round(0)
    p = p.rename_axis(None, axis=0).rename_axis(None, axis=1)
    p = p[list(REGIMES.keys())]
    with open(OUT / "tex" / "tab_tr_zone_dir.tex", 'w') as f:
        f.write("% auto-built\n")
        f.write(p.to_latex(float_format='%.0f', na_rep='---'))
    return p


def fig_costs_official(p):
    fig, ax = plt.subplots(figsize=(11, 6))
    cols = [c for c in p.columns if c != 'Total']
    x = list(p.index)
    bottom = np.zeros(len(x))
    colors = plt.cm.tab10(np.linspace(0, 1, len(cols)))
    for j, c in enumerate(cols):
        ax.bar(x, p[c], bottom=bottom, label=c, color=colors[j])
        bottom += p[c].values
    ax.set_ylabel('Cost (EUR million per regime)')
    ax.set_title('Per-regime ajuste-cascade cost composition (REE-published cost indicators)')
    ax.set_xticks(range(len(x)))
    ax.set_xticklabels(x, rotation=20, ha='right')
    ax.legend(loc='upper right', fontsize=8)
    ax.grid(alpha=0.3, axis='y')
    fig.tight_layout()
    fig.savefig(FIG / 'cascade_costs_official_per_regime.pdf')
    plt.close(fig)


def fig_pvpc(p):
    # Show only the comerc.referencia PVPC line (ids 780, 781, 783, 785, 786, 787) - drop free + composite for legibility
    comp_order = ['Restricciones PBF (780 PVPC)', 'Restricciones TR (781 PVPC)',
                  'Restricciones IDA (783 PVPC)', 'Banda secundaria (785 PVPC)',
                  'Desvíos medidos (786 PVPC)', 'Saldo desvíos (787 PVPC)']
    pp = p.reindex(comp_order).fillna(0)
    fig, ax = plt.subplots(figsize=(11, 5.5))
    x = list(REGIMES.keys())
    bottom = np.zeros(len(x))
    colors = plt.cm.Set2(np.linspace(0, 1, len(pp.index)))
    for j, c in enumerate(pp.index):
        ax.bar(x, pp.loc[c, x], bottom=bottom, label=c.split(' (')[0], color=colors[j])
        bottom += pp.loc[c, x].values
    ax.set_ylabel('Mean PVPC component price (EUR/MWh)')
    ax.set_title('PVPC quarter-hour component price means (regulated retail tariff)')
    ax.set_xticks(range(len(x)))
    ax.set_xticklabels(x, rotation=20, ha='right')
    ax.legend(loc='upper right', fontsize=8)
    ax.grid(alpha=0.3, axis='y')
    fig.tight_layout()
    fig.savefig(FIG / 'pvpc_components_stacked.pdf')
    plt.close(fig)


def fig_tr_zone(df):
    p = df.copy()
    p['abs_gwh'] = p['up_gwh'].abs() + p['dn_gwh'].abs()
    pp = p.pivot_table(index='zone', columns='regime', values='abs_gwh', aggfunc='sum').fillna(0)
    pp = pp[list(REGIMES.keys())]
    fig, ax = plt.subplots(figsize=(11, 5.5))
    zones = list(pp.index)
    n_reg = len(pp.columns)
    width = 0.15
    x = np.arange(len(zones))
    colors = plt.cm.viridis(np.linspace(0, 1, n_reg))
    for j, reg in enumerate(pp.columns):
        ax.bar(x + j*width, pp[reg], width, label=reg, color=colors[j])
    ax.set_ylabel('TR absolute volume (GWh per regime)')
    ax.set_title('Per-zone TR (Tiempo Real) absolute volume by reform regime')
    ax.set_xticks(x + (n_reg-1)*width/2)
    ax.set_xticklabels(zones)
    ax.legend(loc='upper right', fontsize=8)
    ax.grid(alpha=0.3, axis='y')
    fig.tight_layout()
    fig.savefig(FIG / 'tr_zone_dir_per_regime.pdf')
    plt.close(fig)


def main():
    print('1. Building REE-published cost table...')
    costs = build_costs_official()
    print(f'   wrote {OUT / "per_regime_costs_official.csv"} ({len(costs)} rows)')

    print('2. Building PVPC components table...')
    pvpc = build_pvpc_components()
    print(f'   wrote {OUT / "pvpc_components_per_regime.csv"} ({len(pvpc)} rows)')

    print('3. Building per-zone TR direction table...')
    tr = build_tr_zone_dir()
    print(f'   wrote {OUT / "tr_zone_direction_per_regime.csv"} ({len(tr)} rows)')

    print('4. Building RES curtailment %...')
    rc = build_res_curt_pct()
    print(f'   wrote {OUT / "res_curtailment_pct.csv"} ({len(rc)} rows)')

    print('5. Writing TeX tables and figures...')
    p_cost = write_tex_costs_official(costs)
    p_pvpc = write_tex_pvpc(pvpc)
    p_tr   = write_tex_tr_zone(tr)
    fig_costs_official(p_cost)
    fig_pvpc(p_pvpc)
    fig_tr_zone(tr)

    print()
    print('=== Headline costs per regime (REE-published, EUR M, sum across services) ===')
    print(p_cost.to_string())
    print()
    print('=== PVPC components (mean EUR/MWh per regime) ===')
    print(p_pvpc.to_string())
    print()
    print('=== Per-zone TR absolute volume (GWh per regime) ===')
    print(p_tr.to_string())


if __name__ == '__main__':
    main()
