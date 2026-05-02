# STATUS: ALIVE
# LAST-AUDIT: 2026-05-02
# FEEDS: New empirical channel — bilateral commitments via PDBF
# CLAIM: PDBF (Programa Diario Básico de Funcionamiento) reveals the bilateral
#        commitment channel that PDBC misses. Big-4 firms execute substantial
#        bilateral contracts (offer_type=4) in parallel to DA auction bidding.
#        This script characterises the bilateral channel by firm × regime,
#        tests for the reforzada bilateral signature, and probes the Rule 28.8
#        break.
"""PDBF bilateral analysis — first substantive use of the new pdbf panel.

Tests:
  T1. Bilateral volume by firm × regime (descriptive baseline).
  T2. Bilateral share (bilateral / total) trajectory across reform regimes
      and pre/post 2025-04-28 blackout.
  T3. Reforzada bilateral signature: per-tech (CCGT, hydro, nuclear) Big-4
      bilateral share, blackout-split.
  T4. Rule 28.8 break at 2025-03-19: discontinuity in bilateral volume
      where CNMC eliminated the bilateral-contract opportunity-cost buy-back
      obligation.
  T5. B9 q₁ refinement: split firm DA forward sell into auction-cleared
      (q₁_DA) vs bilateral-cleared (q₁_bilat). Does the progressive collapse
      narrative shift?

Output:
  results/regressions/pdbf_bilateral_volume_by_firm_regime.csv
  results/regressions/pdbf_bilateral_share_blackout_split.csv
  results/regressions/pdbf_b9_q1_refinement.csv
"""
from __future__ import annotations

from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

PROJECT = Path(__file__).resolve().parents[3]
PDBF    = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbf_all.parquet"
PDBC    = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbc_all.parquet"
PDBCE   = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbce_all.parquet"
LISTA   = PROJECT / "data" / "external" / "omie_reference" / "lista_unidades.csv"
CCGT    = PROJECT / "data" / "external" / "omie_reference" / "ccgt_eic_to_omie.csv"

OUT_DIR = PROJECT / "results" / "regressions"

REGIMES = ["pre-IDA", "3-sess", "ISP15-win", "DA60/ID15", "DA15/ID15"]
BIG4    = ["IB", "GE", "GN", "HC"]


def assign_regime_sql(date_col: str = "date") -> str:
    return f"""
        CASE
          WHEN CAST({date_col} AS DATE) < DATE '2024-06-14' THEN 'pre-IDA'
          WHEN CAST({date_col} AS DATE) < DATE '2024-12-01' THEN '3-sess'
          WHEN CAST({date_col} AS DATE) < DATE '2025-03-19' THEN 'ISP15-win'
          WHEN CAST({date_col} AS DATE) < DATE '2025-10-01' THEN 'DA60/ID15'
          ELSE 'DA15/ID15'
        END
    """


def main() -> None:
    con = duckdb.connect()
    con.execute("SET memory_limit='8GB'")
    con.execute("SET threads=4")
    con.execute("SET preserve_insertion_order=false")

    # ============================================================
    # Build unit → firm + tech mapping (for blackout-split tech analysis)
    # ============================================================
    print("[setup] unit → firm + tech mapping…", flush=True)
    firms = con.execute(f"""
        SELECT unit_code, grupo_empresarial AS firm FROM (
          SELECT unit_code, grupo_empresarial,
                 ROW_NUMBER() OVER (PARTITION BY unit_code ORDER BY date DESC) AS rn
          FROM '{PDBCE}' WHERE grupo_empresarial IS NOT NULL) WHERE rn = 1
    """).df()
    lista = pd.read_csv(LISTA)[["unit_code", "technology"]]
    map_uf = firms.merge(lista, on="unit_code", how="left")

    def tech_group(t: str | None) -> str:
        if not isinstance(t, str): return "Other"
        tl = t.lower()
        if "carbón" in tl or "carbon" in tl: return "Coal"
        if "gas" in tl or "ciclo" in tl: return "CCGT"
        if "nuclear" in tl: return "Nuclear"
        if "ombeo" in tl or "idráulica" in tl: return "Hydro"
        if "eólic" in tl or "eolic" in tl: return "Wind"
        if "fotovolt" in tl or "solar" in tl: return "Solar"
        if "comerciali" in tl or "consumo" in tl or "compra" in tl: return "Demand"
        return "Other"

    map_uf["tech_group"] = map_uf["technology"].apply(tech_group)
    print(f"  {len(map_uf):,} mapped unit_codes; tech_group distribution:")
    print(map_uf.tech_group.value_counts())
    print()

    con.register("uf_map", map_uf[["unit_code", "firm", "tech_group"]])

    # ============================================================
    # T1. Bilateral volume by firm × regime
    # ============================================================
    print("[T1] Bilateral volume by firm × regime…", flush=True)
    t1 = con.execute(f"""
        SELECT m.firm,
               {assign_regime_sql('p.date')} AS regime,
               SUM(CASE WHEN p.offer_type = 4
                        THEN p.assigned_power_mw * p.mtu_minutes / 60.0
                        ELSE 0 END) AS bilateral_mwh,
               SUM(CASE WHEN p.offer_type IN (1, 3)
                        THEN p.assigned_power_mw * p.mtu_minutes / 60.0
                        ELSE 0 END) AS auction_signed_mwh,
               SUM(CASE WHEN p.offer_type IN (1, 3) AND p.assigned_power_mw > 0
                        THEN p.assigned_power_mw * p.mtu_minutes / 60.0
                        ELSE 0 END) AS auction_sell_mwh,
               COUNT(DISTINCT p.date) AS n_days
        FROM '{PDBF}' p
        JOIN uf_map m USING (unit_code)
        GROUP BY m.firm, regime
    """).df()
    t1["bilateral_share_of_sells"] = t1["bilateral_mwh"] / (t1["auction_sell_mwh"] + t1["bilateral_mwh"]).abs()
    t1["bilateral_per_day_GWh"] = t1["bilateral_mwh"] / t1["n_days"] / 1000.0
    t1["auction_sell_per_day_GWh"] = t1["auction_sell_mwh"] / t1["n_days"] / 1000.0
    t1 = t1[t1.firm.isin(BIG4 + ["OTHER"])].copy()
    t1["regime"] = pd.Categorical(t1["regime"], categories=REGIMES, ordered=True)
    t1 = t1.sort_values(["firm", "regime"])

    print()
    print("Big-4 + Fringe bilateral volume per regime (signed positive = sell-side bilaterals):")
    pv = t1.pivot(index="firm", columns="regime", values="bilateral_per_day_GWh")
    print("  Bilateral GWh/day:")
    print(pv.to_string(float_format=lambda x: f"{x:7.2f}"))
    print()
    pv = t1.pivot(index="firm", columns="regime", values="auction_sell_per_day_GWh")
    print("  Auction-cleared sell GWh/day:")
    print(pv.to_string(float_format=lambda x: f"{x:7.2f}"))
    print()
    pv = t1.pivot(index="firm", columns="regime", values="bilateral_share_of_sells")
    print("  Bilateral share of total sell-side commitment (bilateral/(auction+bilateral)):")
    print(pv.to_string(float_format=lambda x: f"{x*100:5.1f}%"))
    print()

    t1.to_csv(OUT_DIR / "pdbf_bilateral_volume_by_firm_regime.csv", index=False)

    # ============================================================
    # T3. Reforzada signature — pre/post-blackout split (DA60/ID15 only) by tech
    # ============================================================
    print("[T3] Reforzada bilateral signature: DA60/ID15 PRE-blackout vs POST-blackout × tech…", flush=True)
    t3 = con.execute(f"""
        SELECT m.firm, m.tech_group,
               CASE WHEN CAST(p.date AS DATE) < DATE '2025-04-28' THEN 'PRE-blackout'
                    ELSE 'POST-blackout' END AS phase,
               SUM(CASE WHEN p.offer_type = 4
                        THEN p.assigned_power_mw * p.mtu_minutes / 60.0
                        ELSE 0 END) AS bilateral_mwh,
               SUM(CASE WHEN p.offer_type IN (1, 3) AND p.assigned_power_mw > 0
                        THEN p.assigned_power_mw * p.mtu_minutes / 60.0
                        ELSE 0 END) AS auction_sell_mwh,
               COUNT(DISTINCT p.date) AS n_days
        FROM '{PDBF}' p
        JOIN uf_map m USING (unit_code)
        WHERE CAST(p.date AS DATE) >= DATE '2025-03-19'
          AND CAST(p.date AS DATE) <  DATE '2025-10-01'
        GROUP BY m.firm, m.tech_group, phase
    """).df()
    t3 = t3[t3.firm.isin(BIG4) & t3.tech_group.isin(["CCGT", "Nuclear", "Hydro"])].copy()
    t3["bilateral_per_day_GWh"] = t3["bilateral_mwh"] / t3["n_days"] / 1000.0
    t3["auction_per_day_GWh"]   = t3["auction_sell_mwh"] / t3["n_days"] / 1000.0
    t3["bilateral_share"] = t3["bilateral_mwh"] / (t3["auction_sell_mwh"] + t3["bilateral_mwh"]).abs()

    print()
    print("Big-4 × tech bilateral share (DA60/ID15 PRE-blackout vs POST-blackout):")
    pv = t3.pivot_table(index=["firm","tech_group"], columns="phase", values="bilateral_share")
    print(pv.to_string(float_format=lambda x: f"{x*100:5.1f}%"))
    print()

    t3.to_csv(OUT_DIR / "pdbf_bilateral_share_blackout_split.csv", index=False)

    # ============================================================
    # T4. Rule 28.8 break — monthly bilateral volume, with 2025-03-19 marker
    # ============================================================
    print("[T4] Rule 28.8 monthly bilateral volume series (Big-4 only)…", flush=True)
    t4 = con.execute(f"""
        SELECT date_trunc('month', CAST(p.date AS DATE)) AS month,
               m.firm,
               SUM(CASE WHEN p.offer_type = 4
                        THEN ABS(p.assigned_power_mw) * p.mtu_minutes / 60.0
                        ELSE 0 END) AS bilateral_abs_mwh,
               SUM(CASE WHEN p.offer_type = 4 AND p.assigned_power_mw > 0
                        THEN p.assigned_power_mw * p.mtu_minutes / 60.0
                        ELSE 0 END) AS bilateral_sell_mwh,
               SUM(CASE WHEN p.offer_type = 4 AND p.assigned_power_mw < 0
                        THEN -p.assigned_power_mw * p.mtu_minutes / 60.0
                        ELSE 0 END) AS bilateral_buy_mwh
        FROM '{PDBF}' p
        JOIN uf_map m USING (unit_code)
        WHERE m.firm IN ('IB','GE','GN','HC')
        GROUP BY 1, 2
        ORDER BY 1, 2
    """).df()
    t4["bilateral_abs_GWh"] = t4["bilateral_abs_mwh"] / 1000
    t4["bilateral_sell_GWh"] = t4["bilateral_sell_mwh"] / 1000
    t4["bilateral_buy_GWh"]  = t4["bilateral_buy_mwh"] / 1000

    # 6-month windows around the Rule 28.8 elimination (2025-03-19) and around
    # the IDA reform (2024-06-14)
    print("\nBig-4 bilateral GWh/month by 12-month window around 2025-03-19 break:")
    t4["window"] = pd.cut(
        t4["month"],
        [pd.Timestamp("2024-09-01"), pd.Timestamp("2025-03-01"),
         pd.Timestamp("2025-09-01")],
        labels=["2024-09 → 2025-02 (PRE-Rule-28.8-elim)", "2025-03 → 2025-08 (POST-elim)"],
        include_lowest=True,
    )
    pv = t4[t4.window.notna()].groupby(["firm","window"], observed=True)["bilateral_abs_GWh"].mean().unstack()
    print(pv.to_string(float_format=lambda x: f"{x:8.0f}"))
    print()

    t4.to_csv(OUT_DIR / "pdbf_bilateral_monthly.csv", index=False)

    # ============================================================
    # T5. B9 q₁ refinement: per (firm, ISP) auction-cleared vs bilateral
    # ============================================================
    print("[T5] B9 q₁ refinement (Big-4 firm-day): auction-cleared vs bilateral DA-forward sell…", flush=True)
    t5 = con.execute(f"""
        SELECT {assign_regime_sql('p.date')} AS regime,
               m.firm,
               COUNT(DISTINCT p.date) AS n_days,
               SUM(CASE WHEN p.offer_type = 1 AND p.assigned_power_mw > 0
                        THEN p.assigned_power_mw * p.mtu_minutes / 60.0
                        ELSE 0 END) AS q1_DA_sell_mwh,
               SUM(CASE WHEN p.offer_type = 4 AND p.assigned_power_mw > 0
                        THEN p.assigned_power_mw * p.mtu_minutes / 60.0
                        ELSE 0 END) AS q1_bilat_sell_mwh
        FROM '{PDBF}' p
        JOIN uf_map m USING (unit_code)
        WHERE m.firm IN ('IB','GE','GN','HC')
        GROUP BY 1, 2
    """).df()
    t5["q1_DA_per_day_GWh"]     = t5["q1_DA_sell_mwh"] / t5["n_days"] / 1000
    t5["q1_bilat_per_day_GWh"]  = t5["q1_bilat_sell_mwh"] / t5["n_days"] / 1000
    t5["q1_total_per_day_GWh"]  = t5["q1_DA_per_day_GWh"] + t5["q1_bilat_per_day_GWh"]
    t5["bilat_share_of_q1"]     = t5["q1_bilat_per_day_GWh"] / t5["q1_total_per_day_GWh"]
    t5["regime"] = pd.Categorical(t5["regime"], categories=REGIMES, ordered=True)
    t5 = t5.sort_values(["firm", "regime"])

    print()
    print("Big-4 q₁ decomposition (DA-cleared vs bilateral sell, GWh/day per firm):")
    for col, label in [("q1_DA_per_day_GWh", "q₁_DA"),
                       ("q1_bilat_per_day_GWh", "q₁_bilateral"),
                       ("q1_total_per_day_GWh", "q₁_total"),
                       ("bilat_share_of_q1", "bilat share")]:
        pv = t5.pivot(index="firm", columns="regime", values=col)
        print(f"\n  {label}:")
        if col == "bilat_share_of_q1":
            print(pv.to_string(float_format=lambda x: f"{x*100:5.1f}%"))
        else:
            print(pv.to_string(float_format=lambda x: f"{x:6.1f}"))

    t5.to_csv(OUT_DIR / "pdbf_b9_q1_refinement.csv", index=False)

    print(f"\nDone. Outputs in {OUT_DIR}/")


if __name__ == "__main__":
    main()
