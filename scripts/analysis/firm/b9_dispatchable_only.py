# STATUS: ALIVE
# LAST-AUDIT: 2026-04-29
# FEEDS: B9 sign-and-mechanism check — decompose ΔQ_IDA by offer_type
# CLAIM: Big-4 firms own a mix of dispatchable (CCGT/hydro/nuclear) and
#        renewable (RE Mercado) units. The aggregate ΔQ_IDA mixes
#        renewable forecast-revision repositioning with dispatchable
#        strategic conduct. This script decomposes the firm-level
#        ΔQ_IDA by offer_type and unit-tech to test whether the
#        Ito-Reguant strategic-conduct interpretation survives at the
#        dispatchable subset.
"""B9 dispatchable-only check.

The B9 finding — Big-4 net-sell in IDA, with progressive collapse —
might be operational repositioning of renewables rather than strategic
conduct of dispatchables.  Ito-Reguant 2016 found Spanish dominant
firms net-BUY in IDA (negative ΔQ), opposite of our positive aggregate.

Possible reconciliation:
   - Aggregate ΔQ = +250 because Big-4 own RE Mercado units (offer_type=10
     wind/solar) that net-sell to monetize forecast surprises
   - Big-4 dispatchable units (offer_type=1/3/8/9 simple+block sells/buys)
     might net-BUY in IDA (Ito-Reguant signature)
   - These two would cancel partially; the aggregate sign hides the
     dispatchable strategic conduct

This script:
   1. Decomposes ΔQ_IDA by offer_type per (firm, regime)
   2. Restricts to dispatchable units only via lista_unidades.csv tech
   3. Computes Big-4 dispatchable-only ΔQ trajectory
   4. Compares to aggregate
   5. Tests whether the dispatchable-only sign matches Ito-Reguant
"""
from __future__ import annotations
from pathlib import Path
import duckdb
import pandas as pd
import numpy as np

PROJECT  = Path(__file__).resolve().parents[3]
PIBCIE   = PROJECT / "data" / "processed" / "omie" / "mercado_intradiario_subastas" / "programas" / "pibcie_all.parquet"
PIBCI    = PROJECT / "data" / "processed" / "omie" / "mercado_intradiario_subastas" / "programas" / "pibci_all.parquet"
REF      = PROJECT / "data" / "external" / "omie_reference" / "lista_unidades.csv"

REGIMES_NUM = {"pre-IDA": 1, "3-sess": 2, "ISP15-win": 3, "DA60/ID15": 4, "DA15/ID15": 5}
REGIMES = list(REGIMES_NUM.keys())
BIG4 = ["GE", "IB", "GN", "HC"]


def assign_regime(d):
    d = pd.Timestamp(d)
    if d < pd.Timestamp("2024-06-14"): return "pre-IDA"
    if d < pd.Timestamp("2024-12-01"): return "3-sess"
    if d < pd.Timestamp("2025-03-19"): return "ISP15-win"
    if d < pd.Timestamp("2025-10-01"): return "DA60/ID15"
    return "DA15/ID15"


def main():
    con = duckdb.connect()
    con.execute("SET memory_limit='8GB'")
    con.execute("SET threads=4")

    # ============================================================
    # PART A: pibcie (firm-level) decomposition by offer_type
    # ============================================================
    print("=" * 80)
    print("PART A — Firm-level ΔQ_IDA by offer_type × regime")
    print("=" * 80)
    print()

    fdat = con.execute(f"""
        SELECT date,
               COALESCE(grupo_empresarial, 'NA') AS firm,
               offer_type,
               SUM(assigned_power_mw * mtu_minutes / 60.0) AS signed_mwh
        FROM '{PIBCIE}'
        WHERE assigned_power_mw IS NOT NULL
        GROUP BY 1, 2, 3
    """).df()
    fdat["date"] = pd.to_datetime(fdat["date"])
    fdat["regime"] = fdat["date"].apply(assign_regime)
    fdat["is_big4"] = fdat["firm"].isin(BIG4)

    # Map offer_type to direction
    def signed_dir(ot):
        if ot in (1, 3): return "sell_simple_block"
        if ot in (8, 9): return "buy_simple_block"
        if ot == 10:     return "RE_special_regime"
        return "other"
    fdat["dir"] = fdat["offer_type"].map(signed_dir)
    # offer_type=10 already signed natively; offer_type 1,3 positive=sell, 8,9 negative=buy in our convention
    fdat["mwh_signed_b9"] = np.where(
        fdat["offer_type"].isin([1, 3]),  fdat["signed_mwh"],
        np.where(fdat["offer_type"].isin([8, 9]), -fdat["signed_mwh"],
                 np.where(fdat["offer_type"] == 10, fdat["signed_mwh"], 0))
    )

    big4 = fdat[fdat.is_big4].copy()
    print("Big-4 ΔQ_IDA (MWh/month) by offer_type × regime — daily mean × ~30:")
    daily = big4.groupby(["regime", "dir"])["mwh_signed_b9"].mean().reset_index()
    daily["regime"] = pd.Categorical(daily["regime"], categories=REGIMES, ordered=True)
    pv = daily.pivot(index="regime", columns="dir", values="mwh_signed_b9").reindex(REGIMES)
    pv["TOTAL"] = pv.sum(axis=1)
    print(pv.round(0).to_string())
    print()
    print("Reading: this is *daily-aggregate Big-4* signed cleared MWh per offer_type.")
    print("'sell_simple_block' = offer_type 1 + 3 (sells, including blocks)")
    print("'buy_simple_block'  = offer_type 8 + 9 (buys, including blocks; sign-flipped)")
    print("'RE_special_regime' = offer_type 10 (renewables; signed natively)")
    print()
    print("If 'sell_simple_block' is NEGATIVE: dispatchables net-BUY → Ito-Reguant withholding")
    print("If 'sell_simple_block' is POSITIVE: dispatchables net-SELL → opposite sign")
    print()

    # ============================================================
    # PART B: pibci (per-unit) — tech-restricted to dispatchable
    # ============================================================
    print("=" * 80)
    print("PART B — Per-unit ΔQ_IDA, restricted to DISPATCHABLE units")
    print("=" * 80)
    print()

    ref = pd.read_csv(REF, encoding="latin1")
    def is_disp(t):
        if pd.isna(t): return False
        s = str(t).lower()
        return ("ciclo combinado" in s) or ("hidr" in s) or ("nuclear" in s)
    ref["dispatchable"] = ref["technology"].apply(is_disp)
    disp_codes = set(ref.loc[ref["dispatchable"], "unit_code"].astype(str))
    print(f"Dispatchable unit codes from lista_unidades: {len(disp_codes)}")
    con.register("disp", pd.DataFrame({"unit_code": list(disp_codes)}))

    # Map unit_code → firm from pdbce (which has BOTH unit_code AND grupo_empresarial)
    PDBCE_PATH = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbce_all.parquet"
    print("Building unit → firm map from pdbce (mode firm per unit)…")
    ufirm = con.execute(f"""
        WITH counts AS (
            SELECT unit_code, COALESCE(grupo_empresarial,'NA') AS firm, COUNT(*) AS n
            FROM '{PDBCE_PATH}'
            WHERE unit_code IS NOT NULL AND grupo_empresarial IS NOT NULL
            GROUP BY 1, 2
        ),
        ranked AS (
            SELECT unit_code, firm, ROW_NUMBER() OVER (PARTITION BY unit_code ORDER BY n DESC) rk
            FROM counts
        )
        SELECT unit_code, firm FROM ranked WHERE rk = 1
    """).df()
    print(f"unit→firm map: {len(ufirm):,} units")

    # Compute per-unit ΔQ_IDA at dispatchable units, restricted to dispatchable
    # offer types (1, 3 sells; 8, 9 buys).  Per OMIE spec §5.2.2.2, PIBCI's
    # assigned_power_mw is signed natively (range -99999.9 to 99999.9), so simple
    # SUM gives net IDA change.  Big-4 dispatchable units only have offer_type=1
    # records empirically, so this is equivalent to the legacy CASE WHEN.
    udat = con.execute(f"""
        SELECT date,
               unit_code,
               SUM(assigned_power_mw * mtu_minutes / 60.0) AS q2_disp_mwh
        FROM '{PIBCI}'
        WHERE assigned_power_mw IS NOT NULL
          AND unit_code IN (SELECT unit_code FROM disp)
          AND offer_type IN (1, 3, 8, 9)
        GROUP BY 1, 2
    """).df()
    udat["date"] = pd.to_datetime(udat["date"])
    udat = udat.merge(ufirm, on="unit_code", how="left")
    udat["regime"] = udat["date"].apply(assign_regime)
    udat["is_big4"] = udat["firm"].isin(BIG4)
    udat["regime_cat"] = pd.Categorical(udat["regime"], categories=REGIMES, ordered=True)

    # Big-4 dispatchable-only daily aggregate
    big4_disp = (udat[udat.is_big4]
                 .groupby(["regime_cat", "date"], observed=True)["q2_disp_mwh"]
                 .sum()
                 .reset_index())
    big4_disp_summary = big4_disp.groupby("regime_cat", observed=True)["q2_disp_mwh"].agg(["mean", "median", "count"]).reindex(REGIMES)
    print("Big-4 DISPATCHABLE-ONLY ΔQ_IDA (offer_type 1+3-8-9), MWh/firm-day average × Big-4 firms:")
    print(big4_disp_summary.round(1).to_string())
    print()

    # Convert per Big-4 firm-day: divide by ~4 (4 Big-4 firms)
    big4_disp_summary["mean_per_firm_day"] = big4_disp_summary["mean"] / 4.0
    print("Per-firm-day Big-4 dispatchable ΔQ_IDA:")
    print(big4_disp_summary[["mean_per_firm_day", "median", "count"]].round(1).to_string())
    print()

    print("=" * 80)
    print("INTERPRETATION CHECK")
    print("=" * 80)
    pre = big4_disp_summary.loc["pre-IDA", "mean_per_firm_day"]
    da60 = big4_disp_summary.loc["DA60/ID15", "mean_per_firm_day"] if "DA60/ID15" in big4_disp_summary.index else np.nan
    da15 = big4_disp_summary.loc["DA15/ID15", "mean_per_firm_day"] if "DA15/ID15" in big4_disp_summary.index else np.nan
    print(f"  Pre-IDA Big-4 dispatchable ΔQ per firm-day  = {pre:>+.0f} MWh")
    print(f"  DA60/ID15 dispatchable ΔQ per firm-day      = {da60:>+.0f} MWh")
    print(f"  DA15/ID15 dispatchable ΔQ per firm-day      = {da15:>+.0f} MWh")
    print()
    if pre < 0:
        print("  → Pre-IDA NEGATIVE: dispatchables net-BUY in IDA")
        print("    matches Ito-Reguant 2016 strategic-over-commitment signature")
    elif pre > 0:
        print("  → Pre-IDA POSITIVE: dispatchables net-SELL in IDA")
        print("    OPPOSITE of Ito-Reguant 2016; the original B9 framing's sign")
        print("    is preserved at the dispatchable subset.  Either modern Spain")
        print("    differs from 2008-2010 OR the strategic story needs revising.")
    print()

    # Per-firm decomposition
    by_firm = (udat[udat.is_big4]
               .groupby(["firm", "regime_cat"], observed=True)["q2_disp_mwh"]
               .sum().reset_index())
    by_firm["regime_cat"] = pd.Categorical(by_firm["regime_cat"], categories=REGIMES, ordered=True)
    pv2 = by_firm.pivot(index="regime_cat", columns="firm", values="q2_disp_mwh").reindex(REGIMES)
    print("Per-firm Big-4 dispatchable-only cumulative ΔQ_IDA by regime (MWh):")
    print(pv2.round(0).to_string())


if __name__ == "__main__":
    main()
