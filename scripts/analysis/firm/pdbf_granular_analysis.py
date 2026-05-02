# STATUS: ALIVE
# LAST-AUDIT: 2026-05-02
# FEEDS: F23 robustness + new disaggregated-grain claims
# CLAIM: F23 (post-blackout nuclear bilateral surge) refines and survives at
#        per-unit and same-cal-month robustness checks. CCGT shows a parallel
#        bilateral signature. Bilateral commitment concentrates in peak hours,
#        consistent with strategic intent rather than passive retail-load
#        coverage.
"""PDBF granular analysis at per-unit per-period grain.

Four tests:
  T1. F23 per-nuclear-unit decomposition: bilateral share PRE vs POST blackout
      for each of the 7 Spanish nuclear plants (ALMARAZ 1/2, ASCÓ 1/2,
      COFRENTES, TRILLO, VANDELLÓS).
  T2. F23 same-cal-month robustness: post-blackout = May-Sep 2025 reforzada.
      Same-cal pre-baseline = May-Sep 2018-2024 (multi-year). Tests whether
      the bilateral surge survives controlling for spring/summer seasonality.
  T3. CCGT reforzada signature: per-Big-4-firm CCGT bilateral share
      pre/post-blackout. Does the F23 nuclear pattern extend to CCGT?
  T4. Hour-of-day bilateral share: per-firm × hour-of-day bilateral share
      restricted to post-MTU15-IDA. Strategic concentration in peak hours
      (h17-22) vs uniform distribution.

Output:
  results/regressions/pdbf_per_unit_nuclear.csv         (T1)
  results/regressions/pdbf_f23_samecal.csv              (T2)
  results/regressions/pdbf_ccgt_reforzada_signature.csv (T3)
  results/regressions/pdbf_hour_of_day_bilateral.csv    (T4)
"""
from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

PROJECT = Path(__file__).resolve().parents[3]
PDBF    = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbf_all.parquet"
PDBCE   = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbce_all.parquet"
LISTA   = PROJECT / "data" / "external" / "omie_reference" / "lista_unidades.csv"

OUT_DIR = PROJECT / "results" / "regressions"

NUCLEAR_UNITS = ["ALZ1", "ALZ2", "ASC1", "ASC2", "COF1", "TRL1", "VAN2"]


def main() -> None:
    con = duckdb.connect()
    con.execute("SET memory_limit='8GB'")
    con.execute("SET threads=4")

    # Mapping
    print("[setup] unit→firm + tech mapping…", flush=True)
    firms = con.execute(f"""
        SELECT unit_code, grupo_empresarial AS firm FROM (
          SELECT unit_code, grupo_empresarial,
                 ROW_NUMBER() OVER (PARTITION BY unit_code ORDER BY date DESC) AS rn
          FROM '{PDBCE}' WHERE grupo_empresarial IS NOT NULL) WHERE rn = 1
    """).df()
    lista = pd.read_csv(LISTA)[["unit_code", "technology", "description"]]
    map_uf = firms.merge(lista, on="unit_code", how="left")

    def tech_group(t):
        if not isinstance(t, str): return "Other"
        tl = t.lower()
        if "gas" in tl or "ciclo" in tl: return "CCGT"
        if "nuclear" in tl: return "Nuclear"
        if "ombeo" in tl or "idráulica" in tl: return "Hydro"
        return "Other"

    map_uf["tech_group"] = map_uf["technology"].apply(tech_group)
    con.register("uf", map_uf[["unit_code", "firm", "tech_group", "description"]])

    # ============================================================
    # T1. Per-nuclear-unit reforzada signature
    # ============================================================
    print("\n[T1] F23 per-nuclear-unit decomposition (PRE vs POST blackout)…", flush=True)
    nuc_units_str = ",".join(f"'{u}'" for u in NUCLEAR_UNITS)
    t1 = con.execute(f"""
        SELECT uf.unit_code, uf.firm, uf.description,
               CASE WHEN CAST(p.date AS DATE) < DATE '2025-04-28' THEN 'PRE-blackout'
                    ELSE 'POST-blackout' END AS phase,
               SUM(CASE WHEN p.offer_type = 4 AND p.assigned_power_mw > 0
                        THEN p.assigned_power_mw * p.mtu_minutes / 60.0
                        ELSE 0 END) AS bilateral_mwh,
               SUM(CASE WHEN p.offer_type = 1 AND p.assigned_power_mw > 0
                        THEN p.assigned_power_mw * p.mtu_minutes / 60.0
                        ELSE 0 END) AS auction_mwh,
               COUNT(DISTINCT p.date) AS n_days
        FROM '{PDBF}' p JOIN uf USING (unit_code)
        WHERE uf.unit_code IN ({nuc_units_str})
          AND CAST(p.date AS DATE) >= DATE '2025-03-19'
          AND CAST(p.date AS DATE) <  DATE '2025-10-01'
        GROUP BY 1, 2, 3, 4
    """).df()
    t1["bilat_share"] = t1["bilateral_mwh"] / (t1["bilateral_mwh"] + t1["auction_mwh"])
    pv = t1.pivot_table(index=["unit_code", "firm", "description"], columns="phase", values="bilat_share")
    pv["delta_pp"] = (pv.get("POST-blackout", 0) - pv.get("PRE-blackout", 0)) * 100
    pv = pv.sort_values("delta_pp", ascending=False)
    print()
    print("Per-nuclear-unit bilateral share (DA60/ID15 PRE-blackout vs POST-blackout):")
    print(pv.to_string(float_format=lambda x: f"{x*100:5.1f}%" if abs(x) <= 1 else f"{x:+5.1f}pp"))
    t1.to_csv(OUT_DIR / "pdbf_per_unit_nuclear.csv", index=False)

    # ============================================================
    # T2. F23 same-cal-month robustness (May-Sep windows, multi-year)
    # ============================================================
    print("\n[T2] F23 same-cal-month robustness (May-Sep multi-year)…", flush=True)
    t2 = con.execute(f"""
        SELECT uf.firm, uf.tech_group,
               CASE WHEN CAST(p.date AS DATE) >= DATE '2025-04-28'
                     AND CAST(p.date AS DATE) <  DATE '2025-10-01'
                    THEN 'reforzada (May-Sep 2025)'
                    ELSE 'pre-IDA same-cal (May-Sep 2018-2024)'
                END AS phase,
               EXTRACT('year' FROM CAST(p.date AS DATE)) AS yr,
               SUM(CASE WHEN p.offer_type = 4 AND p.assigned_power_mw > 0
                        THEN p.assigned_power_mw * p.mtu_minutes / 60.0
                        ELSE 0 END) AS bilateral_mwh,
               SUM(CASE WHEN p.offer_type = 1 AND p.assigned_power_mw > 0
                        THEN p.assigned_power_mw * p.mtu_minutes / 60.0
                        ELSE 0 END) AS auction_mwh
        FROM '{PDBF}' p JOIN uf USING (unit_code)
        WHERE uf.firm IN ('IB', 'GN', 'GE', 'HC')
          AND uf.tech_group IN ('Nuclear', 'CCGT', 'Hydro')
          AND EXTRACT('month' FROM CAST(p.date AS DATE)) BETWEEN 5 AND 9
          AND (CAST(p.date AS DATE) < DATE '2024-06-14'
               OR (CAST(p.date AS DATE) >= DATE '2025-04-28'
                   AND CAST(p.date AS DATE) < DATE '2025-10-01'))
        GROUP BY 1, 2, 3, 4
    """).df()
    t2_agg = t2.groupby(["firm","tech_group","phase"], as_index=False).agg(
        bilateral_mwh=("bilateral_mwh","sum"),
        auction_mwh=("auction_mwh","sum"),
    )
    t2_agg["bilat_share"] = t2_agg["bilateral_mwh"] / (t2_agg["bilateral_mwh"] + t2_agg["auction_mwh"])
    print()
    print("F23 same-cal-month restriction (May-Sep) bilateral share:")
    pv = t2_agg.pivot_table(index=["firm","tech_group"], columns="phase", values="bilat_share")
    if not pv.empty:
        cols = [c for c in pv.columns if "pre-IDA" in c] + [c for c in pv.columns if "reforzada" in c]
        pv = pv[cols] if cols else pv
        pv["delta_pp"] = (pv.iloc[:,1] - pv.iloc[:,0]) * 100 if pv.shape[1] >= 2 else float("nan")
        print(pv.to_string(float_format=lambda x: f"{x*100:5.1f}%" if abs(x) <= 1 else f"{x:+5.1f}pp"))
    t2_agg.to_csv(OUT_DIR / "pdbf_f23_samecal.csv", index=False)

    # ============================================================
    # T3. CCGT reforzada signature
    # ============================================================
    print("\n[T3] CCGT reforzada signature — pre/post blackout by Big-4 firm…", flush=True)
    t3 = con.execute(f"""
        SELECT uf.firm,
               CASE WHEN CAST(p.date AS DATE) < DATE '2025-04-28' THEN 'PRE-blackout'
                    ELSE 'POST-blackout' END AS phase,
               SUM(CASE WHEN p.offer_type = 4 AND p.assigned_power_mw > 0
                        THEN p.assigned_power_mw * p.mtu_minutes / 60.0
                        ELSE 0 END) AS bilateral_mwh,
               SUM(CASE WHEN p.offer_type = 1 AND p.assigned_power_mw > 0
                        THEN p.assigned_power_mw * p.mtu_minutes / 60.0
                        ELSE 0 END) AS auction_mwh,
               COUNT(DISTINCT uf.unit_code) AS n_units
        FROM '{PDBF}' p JOIN uf USING (unit_code)
        WHERE uf.firm IN ('IB','GE','GN','HC')
          AND uf.tech_group = 'CCGT'
          AND CAST(p.date AS DATE) >= DATE '2025-03-19'
          AND CAST(p.date AS DATE) <  DATE '2025-10-01'
        GROUP BY 1, 2
    """).df()
    t3["bilat_share"] = t3["bilateral_mwh"] / (t3["bilateral_mwh"] + t3["auction_mwh"])
    print()
    print("CCGT bilateral share by Big-4 (DA60/ID15 PRE-blackout vs POST-blackout):")
    pv = t3.pivot_table(index="firm", columns="phase", values="bilat_share")
    cols = [c for c in ["PRE-blackout", "POST-blackout"] if c in pv.columns]
    pv = pv[cols]
    pv["delta_pp"] = (pv.iloc[:, -1] - pv.iloc[:, 0]) * 100 if pv.shape[1] >= 2 else float("nan")
    print(pv.to_string(float_format=lambda x: f"{x*100:5.1f}%" if abs(x) <= 1 else f"{x:+5.1f}pp"))
    t3.to_csv(OUT_DIR / "pdbf_ccgt_reforzada_signature.csv", index=False)

    # ============================================================
    # T4. Hour-of-day bilateral share (post-MTU15-IDA, Big-4 by tech)
    # ============================================================
    print("\n[T4] Hour-of-day bilateral share post-MTU15-IDA…", flush=True)
    t4 = con.execute(f"""
        SELECT uf.firm, uf.tech_group,
               CASE WHEN p.mtu_minutes = 60 THEN p.period
                    ELSE ((p.period - 1) / 4) + 1 END AS hour_of_day,
               SUM(CASE WHEN p.offer_type = 4 AND p.assigned_power_mw > 0
                        THEN p.assigned_power_mw * p.mtu_minutes / 60.0
                        ELSE 0 END) AS bilateral_mwh,
               SUM(CASE WHEN p.offer_type = 1 AND p.assigned_power_mw > 0
                        THEN p.assigned_power_mw * p.mtu_minutes / 60.0
                        ELSE 0 END) AS auction_mwh
        FROM '{PDBF}' p JOIN uf USING (unit_code)
        WHERE uf.firm IN ('IB','GE','GN','HC')
          AND uf.tech_group IN ('Nuclear','CCGT','Hydro')
          AND CAST(p.date AS DATE) >= DATE '2025-03-19'
        GROUP BY 1, 2, 3
    """).df()
    t4["bilat_share"] = t4["bilateral_mwh"] / (t4["bilateral_mwh"] + t4["auction_mwh"])
    t4 = t4[(t4.hour_of_day >= 1) & (t4.hour_of_day <= 24)]

    print()
    print("Hour-of-day bilateral share (post-MTU15-IDA, Big-4 × tech, % of sell volume):")
    pv = t4.pivot_table(index="hour_of_day", columns=["firm","tech_group"], values="bilat_share")
    print("  Peak hours (h17-22) vs off-peak average:")
    peak  = t4[t4.hour_of_day.between(17, 22)].groupby(["firm","tech_group"])["bilat_share"].mean()
    offp  = t4[~t4.hour_of_day.between(17, 22)].groupby(["firm","tech_group"])["bilat_share"].mean()
    cmp = pd.DataFrame({"peak_h17-22": peak, "off-peak": offp})
    cmp["delta_pp"] = (cmp["peak_h17-22"] - cmp["off-peak"]) * 100
    cmp = cmp.sort_values("delta_pp", ascending=False)
    print(cmp.to_string(float_format=lambda x: f"{x*100:5.1f}%" if abs(x) <= 1 else f"{x:+5.1f}pp"))
    t4.to_csv(OUT_DIR / "pdbf_hour_of_day_bilateral.csv", index=False)

    print(f"\nDone. Outputs in {OUT_DIR}/")


if __name__ == "__main__":
    main()
