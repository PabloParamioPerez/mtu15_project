# STATUS: ALIVE
# LAST-AUDIT: 2026-04-27
# FEEDS: CNMC Article 64.37 hypothesis — uses A73 per-unit nuclear + A80 outages to test "unauthorized reduction"

"""Nuclear availability v3 — combine A73 actual generation with A80 outage events.

The CNMC Article 64.37 LSE allegation against Iberdrola Generación Nuclear
(Cofrentes) and C.N. Almaraz-Trillo A.I.E. is for "unauthorized reduction
of production / repeated failure to meet availability obligations."

The KEY empirical question: when nuclear plants reduce output, are those
reductions accompanied by reported outage events (planned via A53 or
forced via A54)? If reductions exceed REPORTED outage hours, the
unreported-reduction is exactly the conduct CNMC describes.

Reasoning before running:
  - Build per-(unit, year) panel of:
      * actual generation TWh (from A73)
      * scheduled-out hours (from A80 B53 planned)
      * forced-out hours (from A80 B54 forced)
      * implied reduction = nameplate × hours - generation - reported_outage_capacity_hours
  - If implied unauthorized reduction > 0 in 2024-2025 vs ~0 in 2018-2021,
    CNMC's allegation has empirical support.

Output: data/derived/results/nuclear_availability_v3.csv
"""
from __future__ import annotations

from pathlib import Path
import pandas as pd
import numpy as np

PROJECT = Path(__file__).resolve().parents[3]

NUCLEAR_NAMEPLATE = {
    'ALZ1': 1011, 'ALZ2': 1006, 'ASC1': 1032, 'ASC2': 1027,
    'COF1': 1064, 'TRL1': 1066, 'VAN2': 1087,
}
EIC_TO_OMIE = {
    '18WALZ1-12345-0E': 'ALZ1',
    '18WALZ2-12345-0K': 'ALZ2',
    '18WASC1-12345-0Z': 'ASC1',
    '18WASC2-12345-0P': 'ASC2',
    '18WCOF1-12345-0O': 'COF1',
    '18WTRL1-12345-0Y': 'TRL1',
    '18WVAN2-12345-0W': 'VAN2',
}
NAME_TO_OMIE = {
    'ALMARAZ 1': 'ALZ1', 'ALMARAZ 2': 'ALZ2',
    'ALMARAZ I': 'ALZ1', 'ALMARAZ II': 'ALZ2',
    'ASCO 1': 'ASC1', 'ASCO 2': 'ASC2',
    'COFRENTES': 'COF1',
    'TRILLO': 'TRL1', 'TRILLO 1': 'TRL1',
    'VANDELLOS 2': 'VAN2', 'VANDELLOS II': 'VAN2',
    'CN VANDELLOS II': 'VAN2',
}
IB_SHARE = {'ALZ1': 0.53, 'ALZ2': 0.53, 'ASC1': 0.0, 'ASC2': 0.15,
             'COF1': 1.0, 'TRL1': 0.48, 'VAN2': 0.28}


def map_unit(eic, name):
    if eic and eic in EIC_TO_OMIE:
        return EIC_TO_OMIE[eic]
    if name and name.upper().strip() in NAME_TO_OMIE:
        return NAME_TO_OMIE[name.upper().strip()]
    return None


def main() -> None:
    print("[1/3] Load A73 actual generation per unit...")
    gen = pd.read_parquet(PROJECT / 'data/processed/entsoe/generation/nuclear_a73_per_unit.parquet')
    gen['year'] = gen['isp_start'].dt.year
    gen['mwh'] = gen['quantity_mw'] * gen['mtu_minutes'] / 60.0
    NAME_TO_OMIE_LOCAL = {k.upper(): v for k, v in NAME_TO_OMIE.items()}
    gen['omie'] = gen['unit_name'].str.upper().str.strip().map(NAME_TO_OMIE_LOCAL).fillna(gen['unit_name'])
    gen = gen[gen['omie'].isin(NUCLEAR_NAMEPLATE.keys())]
    print(f"  generation panel: {len(gen):,} rows; units {sorted(gen.omie.unique())}")

    print()
    print("[2/3] Load A80 outages (planned + forced)...")
    plan = pd.read_parquet(PROJECT / 'data/processed/entsoe/outages/outages_planned_all.parquet')
    forc = pd.read_parquet(PROJECT / 'data/processed/entsoe/outages/outages_forced_all.parquet')

    outages = []
    for df, label in [(plan, 'planned'), (forc, 'forced')]:
        d = df[df['psr_type'] == 'B14'].copy()
        d['omie'] = d.apply(lambda r: map_unit(r.get('unit_eic'), r.get('unit_name')), axis=1)
        d['start_utc'] = pd.to_datetime(d['start_utc'], errors='coerce', utc=True).dt.tz_localize(None)
        d['end_utc'] = pd.to_datetime(d['end_utc'], errors='coerce', utc=True).dt.tz_localize(None)
        d['outage_hours'] = (d['end_utc'] - d['start_utc']).dt.total_seconds() / 3600
        d['curtailed_mw'] = d['nominal_mw'].fillna(0) - d['min_avail_mw'].fillna(d['nominal_mw'].fillna(0))
        d['curtailed_mwh'] = d['curtailed_mw'] * d['outage_hours']
        d['type'] = label
        outages.append(d)
    out = pd.concat(outages, ignore_index=True)
    out = out[out['omie'].isin(NUCLEAR_NAMEPLATE.keys())]
    print(f"  outage events: {len(out):,}")
    print(f"  by type: {out['type'].value_counts().to_dict()}")
    print(f"  by unit: {out.groupby('omie').size().to_dict()}")

    print()
    print("[3/3] Per-(unit, year) availability accounting:")
    rows = []
    for unit in NUCLEAR_NAMEPLATE:
        for yr in range(2018, 2027):
            g = gen[(gen.omie == unit) & (gen.year == yr)]
            actual_twh = g['mwh'].sum() / 1e6
            days = 366 if yr in (2020, 2024) else 365
            if yr == 2026:
                days = 114
            nameplate_twh = NUCLEAR_NAMEPLATE[unit] * 24 * days / 1e6
            cf = actual_twh / nameplate_twh * 100 if nameplate_twh > 0 else np.nan

            # Outages overlapping this year (allocate by overlap)
            yr_start = pd.Timestamp(f'{yr}-01-01')
            yr_end = pd.Timestamp(f'{yr+1}-01-01')
            o = out[(out.omie == unit)]
            o = o[(o.start_utc < yr_end) & (o.end_utc > yr_start)].copy()
            if not o.empty:
                o['ovl_start'] = o['start_utc'].clip(lower=yr_start)
                o['ovl_end'] = o['end_utc'].clip(upper=yr_end)
                o['ovl_hours'] = (o['ovl_end'] - o['ovl_start']).dt.total_seconds() / 3600
                o['ovl_curtailed_mwh'] = o['curtailed_mw'] * o['ovl_hours']
                planned_twh = o[o.type == 'planned']['ovl_curtailed_mwh'].sum() / 1e6
                forced_twh = o[o.type == 'forced']['ovl_curtailed_mwh'].sum() / 1e6
                planned_hr = o[o.type == 'planned']['ovl_hours'].sum()
                forced_hr = o[o.type == 'forced']['ovl_hours'].sum()
            else:
                planned_twh = forced_twh = planned_hr = forced_hr = 0

            # Implied unauthorized reduction = nameplate - actual - reported_outages
            unaccounted_twh = nameplate_twh - actual_twh - planned_twh - forced_twh
            rows.append({
                'unit': unit, 'year': yr, 'IB_share': IB_SHARE[unit],
                'cf_pct': cf, 'actual_twh': actual_twh,
                'nameplate_twh': nameplate_twh,
                'planned_outage_twh': planned_twh,
                'forced_outage_twh': forced_twh,
                'unaccounted_twh': unaccounted_twh,
                'planned_outage_pct_of_nameplate': planned_twh / nameplate_twh * 100 if nameplate_twh > 0 else np.nan,
                'forced_outage_pct': forced_twh / nameplate_twh * 100 if nameplate_twh > 0 else np.nan,
                'unaccounted_pct': unaccounted_twh / nameplate_twh * 100 if nameplate_twh > 0 else np.nan,
            })

    df = pd.DataFrame(rows)
    print()
    print('UNACCOUNTED REDUCTION (% of nameplate) — generation gap NOT explained by reported outages:')
    pivot = df.pivot(index='unit', columns='year', values='unaccounted_pct').round(0)
    print(pivot.to_string())
    print()
    print('PLANNED outage % of nameplate by unit × year:')
    pivot_p = df.pivot(index='unit', columns='year', values='planned_outage_pct_of_nameplate').round(0)
    print(pivot_p.to_string())
    print()
    print('FORCED outage % of nameplate by unit × year:')
    pivot_f = df.pivot(index='unit', columns='year', values='forced_outage_pct').round(0)
    print(pivot_f.to_string())

    # Saving
    out_path = PROJECT / 'data/derived/results/nuclear_availability_v3.csv'
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)
    print(f'\nwrote {out_path}')


if __name__ == "__main__":
    main()
