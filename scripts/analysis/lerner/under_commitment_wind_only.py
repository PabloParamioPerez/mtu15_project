# STATUS: ALIVE
# LAST-AUDIT: 2026-04-27
# FEEDS: B9 wind-only placebo (Ito-Reguant style)
# CLAIM: Wind-only Big-4 ΔQ pattern across regimes — compares to all-tech to test strategic vs operational interpretation.

"""B9 wind-only placebo.

Ito-Reguant (2016) use wind producers as the "forecast-revision-only"
group: wind has minimal strategic motive (no fuel optimization, simple
forecast updates between DA and IDA). If aggregate Big-4 ΔQ compression
is strategic, wind-only Big-4 ΔQ should compress LESS. If wind-only
shows SAME pattern, the aggregate compression is partially operational.

Reasoning before running:
  - Wind producers under-forecast in DA, sell additional production in
    IDA when forecasts firm up. So wind-only ΔQ > 0 expected throughout.
  - Reform impact on wind: ISP15+MTU15-IDA give 15-min granularity, but
    wind forecast updates happen continuously regardless of granularity.
    So wind-only compression should be SMALLER than aggregate Big-4
    compression, IF strategic withholding is the main driver.
  - If wind-only compresses similarly, the aggregate pattern is partly
    operational.

Output: data/derived/results/under_commitment_wind_only.csv
"""
from __future__ import annotations

from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

PROJECT = Path(__file__).resolve().parents[3]
PIBCIE = PROJECT / "data" / "processed" / "omie" / "mercado_intradiario_subastas" / "programas" / "pibcie_all.parquet"
PDBCE = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbce_all.parquet"
REF = PROJECT / "data" / "external" / "omie_reference" / "lista_unidades.csv"


def assign_regime(d) -> str:
    if d < pd.Timestamp("2024-06-14"): return "1.pre-IDA"
    if d < pd.Timestamp("2024-12-01"): return "2.3-sess"
    if d < pd.Timestamp("2025-03-19"): return "3.ISP15-win"
    if d < pd.Timestamp("2025-10-01"): return "4.DA60/ID15"
    return "5.DA15/ID15"


def main() -> None:
    print("[1/3] Identify wind units from lista_unidades...")
    ref = pd.read_csv(REF, encoding="latin1")
    ref["tech_low"] = ref["technology"].fillna("").astype(str).str.lower()
    # The CSV is utf-8 mojibake read as latin-1; "Eólica" appears as "EÃ³lica" or similar
    is_wind = ref["technology"].fillna("").astype(str).apply(
        lambda t: ("ólic" in t.lower()) or ("Ã³lic" in t) or ("eolica" in t.lower()) or ("eólica" in t.lower())
    )
    wind_units = ref.loc[is_wind, "unit_code"].tolist()
    print(f"   wind units: {len(wind_units)}")

    print("[2/3] Per (firm-group, day) ΔQ for wind-only and ALL units...")
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    units_sql = ",".join(repr(u) for u in wind_units)

    # ALL units (for comparison) — already known from B9; just total
    con.execute(f"""
        CREATE TEMP TABLE dq_all AS
        SELECT CAST(date AS DATE) AS date,
               CASE WHEN grupo_empresarial IN ('GE','IB','GN','HC') THEN grupo_empresarial
                    ELSE 'Fringe' END AS firm_group,
               SUM(assigned_power_mw * mtu_minutes / 60.0) AS dq_mwh
        FROM '{PIBCIE}'
        WHERE assigned_power_mw IS NOT NULL
          AND CAST(date AS DATE) >= DATE '2018-01-01'
        GROUP BY 1, 2
    """)

    con.execute(f"""
        CREATE TEMP TABLE dq_wind AS
        SELECT CAST(date AS DATE) AS date,
               CASE WHEN grupo_empresarial IN ('GE','IB','GN','HC') THEN grupo_empresarial
                    ELSE 'Fringe' END AS firm_group,
               SUM(assigned_power_mw * mtu_minutes / 60.0) AS dq_mwh_wind
        FROM '{PIBCIE}'
        WHERE assigned_power_mw IS NOT NULL
          AND unit_code IN ({units_sql})
          AND CAST(date AS DATE) >= DATE '2018-01-01'
        GROUP BY 1, 2
    """)

    df_all = con.sql("SELECT * FROM dq_all").df()
    df_wind = con.sql("SELECT * FROM dq_wind").df()
    df_all["date"] = pd.to_datetime(df_all["date"])
    df_wind["date"] = pd.to_datetime(df_wind["date"])
    df_all["regime"] = df_all["date"].apply(assign_regime)
    df_wind["regime"] = df_wind["date"].apply(assign_regime)

    print(f"   wind n_firm_days: {len(df_wind):,}; all n_firm_days: {len(df_all):,}")
    print(f"   firm-groups in wind panel: {df_wind.firm_group.unique()}")

    print()
    print("[3/3] Mean ΔQ MWh/firm-day, comparing ALL units vs wind-only:")
    pivot_all = df_all.pivot_table(index="firm_group", columns="regime", values="dq_mwh", aggfunc="mean").round(0)
    pivot_wind = df_wind.pivot_table(index="firm_group", columns="regime", values="dq_mwh_wind", aggfunc="mean").round(0)

    print()
    print("ALL UNITS (B9 raw):")
    print(pivot_all.to_string())
    print()
    print("WIND-ONLY:")
    print(pivot_wind.to_string())
    print()

    # Compression ratio: ΔQ at DA60 / pre-IDA, by group & subset
    print("Compression ratio (DA60/pre-IDA — closer to 0 means more compression):")
    for fg in ["GE", "IB", "GN", "HC", "Fringe"]:
        try:
            ar = pivot_all.loc[fg, "4.DA60/ID15"] / pivot_all.loc[fg, "1.pre-IDA"]
        except (KeyError, ZeroDivisionError):
            ar = np.nan
        try:
            wr = pivot_wind.loc[fg, "4.DA60/ID15"] / pivot_wind.loc[fg, "1.pre-IDA"]
        except (KeyError, ZeroDivisionError):
            wr = np.nan
        print(f"  {fg:<8}  ALL_units={ar:>+6.2f}   wind_only={wr:>+6.2f}")

    print()
    print("Reading: if wind-only compression ≈ ALL compression, the strategic interpretation is weakened (operational driver).")
    print("         If wind-only compression < ALL compression, the strategic interpretation is supported.")

    out = PROJECT / "data" / "derived" / "results" / "under_commitment_wind_only.csv"
    pd.concat([
        pivot_all.reset_index().assign(_table="all_units"),
        pivot_wind.reset_index().assign(_table="wind_only"),
    ], ignore_index=True, sort=False).to_csv(out, index=False)
    print(f"\nwrote {out}")


if __name__ == "__main__":
    main()
