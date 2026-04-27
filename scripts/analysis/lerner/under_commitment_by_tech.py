# STATUS: ALIVE
# LAST-AUDIT: 2026-04-27
# FEEDS: B9 per-tech decomposition — adjudicates 3 specific firm-mechanism hypotheses
# CLAIM: Test (H1) GE asymmetric-window pattern is CCGT-driven; (H2) GN/HC sophistication extraction is hydro-driven; (H3) IB compression is CCGT-specific.

"""B9 per-tech decomposition.

Three a priori hypotheses to adjudicate:

  H1: GE's asymmetric-window-opportunist pattern is CCGT-driven
      Predict: GE-CCGT yield peaks sharply at DA60/ID15; GE-Hydro flat

  H2: GN/HC's sophistication extraction is hydro-driven (not CCGT)
      Predict: GN/HC-Hydro yield rises through reforms, peaks at DA15
               GN/HC-CCGT flat or declining

  H3: IB's strategic compression is CCGT-specific
      Predict: IB-CCGT yield drops sharply at DA60/ID15; IB-Hydro flatter

OVB / measurement notes:
  - per-cell N drops (firm × tech × regime = 40 cells); regime
    averages still informative but firm-tech-day means may be noisy
  - tech labels via lista_unidades; ~5% of units have NaN tech
    (treat as "Other")
  - pibcie attribution: SUM over (firm, tech, day) of signed
    assigned_power_mw × mtu/60

Output: data/derived/results/under_commitment_by_tech.csv
"""
from __future__ import annotations

from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

PROJECT = Path(__file__).resolve().parents[3]
PIBCIE = PROJECT / "data" / "processed" / "omie" / "mercado_intradiario_subastas" / "programas" / "pibcie_all.parquet"
PRICE_DA = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "precios" / "marginalpdbc_all.parquet"
PRICE_IDA = PROJECT / "data" / "processed" / "omie" / "mercado_intradiario_subastas" / "precios" / "marginalpibc_all.parquet"
REF = PROJECT / "data" / "external" / "omie_reference" / "lista_unidades.csv"
OUT = PROJECT / "data" / "derived" / "results" / "under_commitment_by_tech.csv"

BIG4 = ["GE", "IB", "GN", "HC"]


def assign_regime(d) -> str:
    if d < pd.Timestamp("2024-06-14"): return "1.pre-IDA"
    if d < pd.Timestamp("2024-12-01"): return "2.3-sess"
    if d < pd.Timestamp("2025-03-19"): return "3.ISP15-win"
    if d < pd.Timestamp("2025-10-01"): return "4.DA60/ID15"
    return "5.DA15/ID15"


def tech_bucket(t) -> str:
    """Coarse tech bucket from (latin-1-mojibake-read) lista_unidades technology."""
    if pd.isna(t): return "Other"
    s = str(t)
    if "Ciclo Combinado" in s: return "CCGT"
    if "Hidr" in s and "Bombeo" not in s and "Consumo" not in s: return "Hydro"
    if ("Ã³lic" in s) or ("ólic" in s) or ("Eolic" in s.lower()): return "Wind"
    if "Solar" in s: return "Solar"
    if "Bombeo" in s and "Consumo" not in s: return "PumpHydro"
    if "Nuclear" in s: return "Nuclear"
    return "Other"


def main() -> None:
    print("[1/4] Build unit→tech mapping...")
    ref = pd.read_csv(REF, encoding="latin1")
    ref["tech"] = ref["technology"].apply(tech_bucket)
    print("   tech distribution:", ref["tech"].value_counts().to_dict())

    print("[2/4] Build hourly DA + IDA prices...")
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    con.execute("SET threads=4")
    con.execute(f"""
        CREATE TEMP TABLE da AS
        WITH hp AS (
            SELECT date,
                   CASE WHEN mtu_minutes = 15 THEN CEIL(period / 4.0)::INTEGER ELSE period END AS hour,
                   price_es_eur_mwh AS p
            FROM '{PRICE_DA}' WHERE price_es_eur_mwh IS NOT NULL
              AND CAST(date AS DATE) >= DATE '2018-01-01'
        )
        SELECT date, hour, AVG(p) AS p_da FROM hp GROUP BY 1, 2
    """)
    con.execute(f"""
        CREATE TEMP TABLE ida AS
        WITH hp AS (
            SELECT date,
                   CASE WHEN mtu_minutes = 15 THEN CEIL(period / 4.0)::INTEGER ELSE period END AS hour,
                   price_es_eur_mwh AS p
            FROM '{PRICE_IDA}' WHERE price_es_eur_mwh IS NOT NULL
              AND CAST(date AS DATE) >= DATE '2018-01-01'
        )
        SELECT date, hour, AVG(p) AS p_ida FROM hp GROUP BY 1, 2
    """)

    print("[3/4] Build (firm, tech, day) ΔQ + arb_profit panel from pibcie...")
    con.register("ref", ref[["unit_code", "tech"]])
    con.execute(f"""
        CREATE TEMP TABLE dq AS
        WITH hf AS (
            SELECT CAST(p.date AS DATE) AS date,
                   CASE WHEN p.mtu_minutes = 15 THEN CEIL(p.period / 4.0)::INTEGER ELSE p.period END AS hour,
                   CASE WHEN p.grupo_empresarial IN ('GE','IB','GN','HC') THEN p.grupo_empresarial
                        ELSE 'Fringe' END AS firm_group,
                   COALESCE(r.tech, 'Other') AS tech,
                   p.assigned_power_mw * p.mtu_minutes / 60.0 AS dq_mwh
            FROM '{PIBCIE}' p LEFT JOIN ref r ON p.unit_code = r.unit_code
            WHERE p.assigned_power_mw IS NOT NULL
              AND CAST(p.date AS DATE) >= DATE '2018-01-01'
        )
        SELECT date, hour, firm_group, tech, SUM(dq_mwh) AS dq_mwh
        FROM hf GROUP BY 1, 2, 3, 4
    """)

    panel = con.sql("""
        SELECT q.date, q.hour, q.firm_group, q.tech, q.dq_mwh,
               d.p_da, i.p_ida,
               (d.p_da - i.p_ida) AS wedge,
               q.dq_mwh * (d.p_da - i.p_ida) AS arb_profit_eur
        FROM dq q
        JOIN da d ON q.date = d.date AND q.hour = d.hour
        JOIN ida i ON q.date = i.date AND q.hour = i.hour
    """).df()
    panel["date"] = pd.to_datetime(panel["date"])
    panel["regime"] = panel["date"].apply(assign_regime)
    print(f"   panel: {len(panel):,} firm-tech-hour rows")

    # Aggregate firm-tech-day
    daily = panel.groupby(["date", "firm_group", "tech", "regime"], as_index=False).agg(
        profit_eur=("arb_profit_eur", "sum"),
        dq_mwh=("dq_mwh", "sum"),
    )
    daily["abs_dq"] = daily["dq_mwh"].abs()

    print()
    print("[4/4] Mean yield €/MWh by (firm × tech × regime), Big-4 + main techs only:")
    main_techs = ["CCGT", "Hydro", "Nuclear", "PumpHydro", "Wind"]
    sub = daily[daily["firm_group"].isin(BIG4) & daily["tech"].isin(main_techs)].copy()
    # Yield = sum(profit) / sum(|dq|) per firm-tech-regime
    grp = sub.groupby(["firm_group", "tech", "regime"]).agg(
        sum_profit=("profit_eur", "sum"),
        sum_abs_dq=("abs_dq", "sum"),
        sum_dq=("dq_mwh", "sum"),
        n=("date", "size"),
    ).reset_index()
    grp["yield_eur_per_mwh"] = grp["sum_profit"] / grp["sum_abs_dq"]
    grp["mean_dq_per_day"] = grp["sum_dq"] / grp["n"]

    # Pivot for readability
    print()
    print("YIELD (€ per MWh repositioned) by firm-tech, across regimes:")
    pivot_yield = grp.pivot_table(index=["firm_group", "tech"], columns="regime", values="yield_eur_per_mwh").round(2)
    print(pivot_yield.to_string())

    print()
    print("Mean ΔQ per firm-tech-day (MWh) — for context on the yield base:")
    pivot_dq = grp.pivot_table(index=["firm_group", "tech"], columns="regime", values="mean_dq_per_day").round(0)
    print(pivot_dq.to_string())

    print()
    print("=" * 90)
    print("HYPOTHESIS ADJUDICATION")
    print("=" * 90)

    def yield_for(firm, tech):
        try:
            return pivot_yield.loc[(firm, tech)].to_dict()
        except KeyError:
            return None

    print()
    print("H1: GE asymmetric-window pattern is CCGT-driven")
    print("    Predict: GE-CCGT yield peaks at DA60/ID15; GE-Hydro flat")
    ge_ccgt = yield_for("GE", "CCGT")
    ge_hydro = yield_for("GE", "Hydro")
    print(f"    GE-CCGT yield: {ge_ccgt}")
    print(f"    GE-Hydro yield: {ge_hydro}")
    if ge_ccgt:
        peak_at_da60 = ge_ccgt.get("4.DA60/ID15", np.nan) > max(ge_ccgt.get("3.ISP15-win", -np.inf), ge_ccgt.get("5.DA15/ID15", -np.inf))
        print(f"    H1 verdict: GE-CCGT peaks at DA60/ID15? {peak_at_da60}")

    print()
    print("H2: GN/HC sophistication extraction is hydro-driven")
    print("    Predict: GN/HC-Hydro yield rises through reforms, peaks at DA15")
    print("             GN/HC-CCGT flat or declining")
    for f in ["GN", "HC"]:
        hy = yield_for(f, "Hydro")
        cc = yield_for(f, "CCGT")
        print(f"    {f}-Hydro yield:  {hy}")
        print(f"    {f}-CCGT  yield: {cc}")
        if hy:
            peak_at_da15 = hy.get("5.DA15/ID15", np.nan) >= max(hy.get(r, -np.inf) for r in ["1.pre-IDA","2.3-sess","3.ISP15-win","4.DA60/ID15"])
            print(f"    {f}-Hydro peaks at DA15/ID15? {peak_at_da15}")

    print()
    print("H3: IB compression is CCGT-specific")
    print("    Predict: IB-CCGT yield drops at DA60/ID15; IB-Hydro flatter")
    ib_ccgt = yield_for("IB", "CCGT")
    ib_hydro = yield_for("IB", "Hydro")
    print(f"    IB-CCGT yield:  {ib_ccgt}")
    print(f"    IB-Hydro yield: {ib_hydro}")

    OUT.parent.mkdir(parents=True, exist_ok=True)
    pivot_yield.to_csv(OUT.with_suffix(".yield.csv"))
    pivot_dq.to_csv(OUT.with_suffix(".dq.csv"))
    grp.to_csv(OUT, index=False)
    print(f"\nwrote {OUT} (and .yield.csv, .dq.csv)")


if __name__ == "__main__":
    main()
