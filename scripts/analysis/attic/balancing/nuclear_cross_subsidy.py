# STATUS: ALIVE
# LAST-AUDIT: 2026-04-27
# FEEDS: Test 2 — moral-hazard cross-subsidy. Does IB CCGT+hydro pick up the ~21 TWh/year nuclear shortfall?

"""Cross-subsidy test for the moral-hazard hypothesis.

Test 1 established that Spanish nuclear CF collapsed from ~68% (2018-21)
to ~30% (2023-24), a ~21 TWh/year reduction. The lost generation had
to be replaced by CCGT, hydro, or imports.

The moral-hazard hypothesis predicts IB's CCGT+hydro fleet
disproportionately captures the displaced demand — same firm profits
from the gap created by alleged nuclear under-availability.

Reasoning before running:
  - System CCGT+hydro generation should rise 2022+ to offset nuclear
  - IB CCGT+hydro generation should rise BY MORE than its baseline
    share would predict if the cross-subsidy story holds
  - Confounders: solar mid-day growth displaces CCGT mid-day
    (works against finding); hydro inflows vary year-to-year (noise);
    IB capacity stable.

Output: results/regressions/nuclear_cross_subsidy.csv
"""
from __future__ import annotations

from pathlib import Path
import duckdb
import numpy as np
import pandas as pd

PROJECT = Path(__file__).resolve().parents[3]
PDBCE = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbce_all.parquet"
A75 = PROJECT / "data" / "processed" / "entsoe" / "generation" / "wind_solar_actual_all.parquet"
REF = PROJECT / "data" / "external" / "omie_reference" / "lista_unidades.csv"
OUT = PROJECT / "results" / "regressions" / "nuclear_cross_subsidy.csv"


def tech_bucket(t) -> str:
    if pd.isna(t): return "Other"
    s = str(t)
    if "Ciclo Combinado" in s: return "CCGT"
    if "Hidr" in s and "Bombeo" not in s and "Consumo" not in s: return "Hydro"
    if "Nuclear" in s: return "Nuclear"
    if "Bombeo" in s and "Consumo" not in s: return "PumpHydro"
    return "Other"


def main() -> None:
    print("[1/3] System-aggregate CCGT+Hydro+Nuclear by year (ENTSO-E A75)...")
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    sys = con.sql(f"""
        SELECT EXTRACT(YEAR FROM isp_start_utc)::INT AS year,
               psr_type,
               SUM(quantity_mw * mtu_minutes / 60.0) / 1e6 AS twh
        FROM '{A75}'
        WHERE psr_type IN ('B14','B04','B12','B11','B10')
        GROUP BY 1, 2 ORDER BY 1, 2
    """).df()
    sys_piv = sys.pivot(index='year', columns='psr_type', values='twh').round(1)
    sys_piv.columns = ['PumpHydro','RunRiver','ReservoirHydro','CCGT','Nuclear']
    sys_piv['DispatchTotal'] = sys_piv[['CCGT','ReservoirHydro','RunRiver','PumpHydro']].sum(axis=1)
    print(sys_piv.to_string())

    print()
    print("[2/3] Per-firm cleared DA volume by tech (pdbce, per (firm, tech, year))...")
    ref = pd.read_csv(REF, encoding='latin1')
    ref['tech'] = ref['technology'].apply(tech_bucket)
    con.register('ref', ref[['unit_code','tech']])
    df = con.sql(f"""
        SELECT EXTRACT(YEAR FROM CAST(p.date AS DATE))::INT AS year,
               CASE WHEN p.grupo_empresarial IN ('GE','IB','GN','HC') THEN p.grupo_empresarial ELSE 'Fringe' END AS firm,
               COALESCE(r.tech, 'Other') AS tech,
               SUM(p.assigned_power_mw * p.mtu_minutes / 60.0) / 1e6 AS twh
        FROM '{PDBCE}' p LEFT JOIN ref r ON p.unit_code = r.unit_code
        WHERE p.offer_type = 1 AND p.assigned_power_mw IS NOT NULL AND p.assigned_power_mw > 0
          AND CAST(p.date AS DATE) >= DATE '2018-01-01'
        GROUP BY 1, 2, 3
    """).df()
    print(f"   {len(df):,} firm-tech-year rows")
    print()
    print("Per-firm CCGT cleared TWh by year:")
    ccgt = df[df['tech']=='CCGT'].pivot_table(index='year', columns='firm', values='twh', aggfunc='sum').round(1)
    print(ccgt.to_string())

    print()
    print("Per-firm Hydro cleared TWh by year:")
    hydro = df[df['tech']=='Hydro'].pivot_table(index='year', columns='firm', values='twh', aggfunc='sum').round(1)
    print(hydro.to_string())

    print()
    print("[3/3] IB CCGT+hydro share of system CCGT+ReservoirHydro generation:")
    # Build IB CCGT and IB Hydro from pdbce; system CCGT and Reservoir from A75
    # NOTE: pdbce captures only DA-spot fraction (true for all firms similarly), so SHARES still informative
    ib_ccgt = ccgt['IB'].fillna(0) if 'IB' in ccgt.columns else pd.Series(0, index=ccgt.index)
    ib_hyd = hydro['IB'].fillna(0) if 'IB' in hydro.columns else pd.Series(0, index=hydro.index)
    sys_ccgt = sys_piv['CCGT']
    sys_res_hydro = sys_piv['ReservoirHydro']

    out = pd.DataFrame({
        'IB_CCGT_TWh': ib_ccgt,
        'IB_Hydro_TWh': ib_hyd,
        'SYS_CCGT_TWh': sys_ccgt,
        'SYS_Hydro_TWh': sys_res_hydro,
        'SYS_Nuclear_TWh': sys_piv['Nuclear'],
    }).fillna(0)
    out['IB_CCGT_share_pct'] = out['IB_CCGT_TWh'] / out['SYS_CCGT_TWh'] * 100
    out['IB_Hydro_share_pct'] = out['IB_Hydro_TWh'] / out['SYS_Hydro_TWh'] * 100
    out['IB_dispatchable_TWh'] = out['IB_CCGT_TWh'] + out['IB_Hydro_TWh']
    print(out.round(2).to_string())

    print()
    print("Era comparison (mean per year):")
    out['era'] = np.where(out.index <= 2021, '1.pre-2022',
                  np.where(out.index <= 2024, '2.2022-24', '3.2025+'))
    era = out.groupby('era').mean(numeric_only=True).round(2)
    print(era.to_string())

    OUT.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT)
    print(f"\nwrote {OUT}")


if __name__ == "__main__":
    main()
