# STATUS: ALIVE
# LAST-AUDIT: 2026-05-02
# FEEDS: B9 — q₁ refinement using PDBF (auction + bilateral)
# CLAIM: B9's progressive q₂_IDA collapse story is unchanged in absolute terms
#        (q₂ comes from PIBCIE, not PDBC/PDBF). But the Ito-Reguant ratio q₂/q₁
#        depends on how q₁ is defined. With q₁ = auction-cleared (PDBC), the
#        ratio mechanically rises post-IDA because q₁_auction grew while q₂
#        fell. With q₁ = total commitment (PDBF auction + bilateral), the
#        ratio trajectory may differ — q₁_total is closer to flat.
"""B9 q₁_total refinement.

For each Big-4 firm × regime, compute:
  - q₁_DA (PDBC offer_type=1 sell, MWh)         — auction-cleared forward
  - q₁_bilateral (PDBF offer_type=4 sell, MWh)  — bilateral forward
  - q₁_total = q₁_DA + q₁_bilateral
  - q₂_IDA (PIBCIE simple-SUM)                  — voluntary IDA repositioning

Then compute the IR repositioning intensity:
  q₂ / q₁_DA      (the "auction-only" ratio used implicitly in B9 to date)
  q₂ / q₁_total   (the "true commitment" ratio with bilateral channel included)

If the latter shows a different cross-regime trajectory, B9's progressive-
collapse story may need reinterpretation through the bilateral channel.

Output:
  results/regressions/b9_q1_total_refinement.csv
"""
from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

PROJECT = Path(__file__).resolve().parents[3]
PDBF    = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbf_all.parquet"
PIBCIE  = PROJECT / "data" / "processed" / "omie" / "mercado_intradiario_subastas" / "programas" / "pibcie_all.parquet"
PDBCE   = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbce_all.parquet"

OUT = PROJECT / "results" / "regressions" / "b9_q1_total_refinement.csv"

REGIMES = ["pre-IDA", "3-sess", "ISP15-win", "DA60/ID15", "DA15/ID15"]
BIG4 = ["IB", "GE", "GN", "HC"]


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
    con.execute("SET memory_limit='6GB'")
    con.execute("SET threads=4")

    # Unit→firm mapping (same as PDBF analyses)
    print("[setup] unit → firm mapping…", flush=True)
    firms = con.execute(f"""
        SELECT unit_code, grupo_empresarial AS firm FROM (
          SELECT unit_code, grupo_empresarial,
                 ROW_NUMBER() OVER (PARTITION BY unit_code ORDER BY date DESC) AS rn
          FROM '{PDBCE}' WHERE grupo_empresarial IS NOT NULL) WHERE rn = 1
    """).df()
    con.register("uf", firms[["unit_code", "firm"]])

    # ----------------------------------------------------------------------
    # 1. q₁_DA + q₁_bilateral by (firm, date)
    # ----------------------------------------------------------------------
    print("[1/2] q₁ panel from PDBF (firm-day)…", flush=True)
    q1 = con.execute(f"""
        SELECT CAST(p.date AS DATE) AS date, uf.firm,
               {assign_regime_sql('p.date')} AS regime,
               SUM(CASE WHEN p.offer_type = 1 AND p.assigned_power_mw > 0
                        THEN p.assigned_power_mw * p.mtu_minutes / 60.0
                        ELSE 0 END) AS q1_DA_mwh,
               SUM(CASE WHEN p.offer_type = 4 AND p.assigned_power_mw > 0
                        THEN p.assigned_power_mw * p.mtu_minutes / 60.0
                        ELSE 0 END) AS q1_bilat_mwh
        FROM '{PDBF}' p JOIN uf USING (unit_code)
        WHERE uf.firm IN ('IB','GE','GN','HC')
        GROUP BY 1, 2, 3
    """).df()
    q1["q1_total_mwh"] = q1["q1_DA_mwh"] + q1["q1_bilat_mwh"]

    # ----------------------------------------------------------------------
    # 2. q₂_IDA by (firm, date)  — IR-cleanest q₂ definition
    # ----------------------------------------------------------------------
    print("[2/2] q₂_IDA panel from PIBCIE (firm-day, simple SUM signed)…", flush=True)
    q2 = con.execute(f"""
        SELECT date, COALESCE(grupo_empresarial, 'NA') AS firm,
               SUM(assigned_power_mw * mtu_minutes / 60.0) AS q2_IDA_mwh
        FROM '{PIBCIE}'
        WHERE assigned_power_mw IS NOT NULL
        GROUP BY 1, 2
    """).df()
    q2["date"] = pd.to_datetime(q2["date"])
    q2 = q2[q2.firm.isin(BIG4)]
    q1["date"] = pd.to_datetime(q1["date"])

    # ----------------------------------------------------------------------
    # Merge + compute per-firm-regime aggregates
    # ----------------------------------------------------------------------
    panel = q1.merge(q2, on=["date", "firm"], how="inner")
    print(f"   merged firm-day panel: {len(panel):,} rows", flush=True)

    # Aggregate to firm × regime — mean per firm-day (not sum), so the q₂/q₁
    # ratio is comparable across regimes of different lengths
    agg = panel.groupby(["firm", "regime"], observed=True).agg(
        n_days=("date", "nunique"),
        mean_q1_DA_GWh=("q1_DA_mwh", lambda x: x.mean() / 1000),
        mean_q1_bilat_GWh=("q1_bilat_mwh", lambda x: x.mean() / 1000),
        mean_q1_total_GWh=("q1_total_mwh", lambda x: x.mean() / 1000),
        mean_q2_IDA_MWh=("q2_IDA_mwh", "mean"),
    ).reset_index()
    agg["bilateral_share"] = agg["mean_q1_bilat_GWh"] / agg["mean_q1_total_GWh"]
    # IR repositioning intensity (per firm-day)
    agg["q2_per_q1_DA_pct"]    = agg["mean_q2_IDA_MWh"] / (agg["mean_q1_DA_GWh"] * 1000) * 100
    agg["q2_per_q1_total_pct"] = agg["mean_q2_IDA_MWh"] / (agg["mean_q1_total_GWh"] * 1000) * 100
    agg["regime"] = pd.Categorical(agg["regime"], categories=REGIMES, ordered=True)
    agg = agg.sort_values(["firm", "regime"]).reset_index(drop=True)

    print()
    print("=" * 105)
    print("B9 q₁_total REFINEMENT — Big-4 firm-day means by regime")
    print("=" * 105)
    print()
    print("q₁_DA (auction-cleared forward sell), GWh/day per firm:")
    print(agg.pivot(index="firm", columns="regime", values="mean_q1_DA_GWh").to_string(float_format=lambda x: f"{x:6.1f}"))
    print()
    print("q₁_bilateral (PDBF bilateral sell), GWh/day per firm:")
    print(agg.pivot(index="firm", columns="regime", values="mean_q1_bilat_GWh").to_string(float_format=lambda x: f"{x:6.1f}"))
    print()
    print("q₁_TOTAL = auction + bilateral, GWh/day per firm:")
    print(agg.pivot(index="firm", columns="regime", values="mean_q1_total_GWh").to_string(float_format=lambda x: f"{x:6.1f}"))
    print()
    print("Bilateral share of q₁_total:")
    print(agg.pivot(index="firm", columns="regime", values="bilateral_share").to_string(float_format=lambda x: f"{x*100:5.1f}%"))
    print()
    print("q₂_IDA (voluntary IDA repositioning, signed), MWh/day per firm:")
    print(agg.pivot(index="firm", columns="regime", values="mean_q2_IDA_MWh").to_string(float_format=lambda x: f"{x:+8.0f}"))
    print()
    print("=" * 105)
    print("Repositioning intensity — q₂ as % of q₁")
    print("=" * 105)
    print()
    print("q₂ / q₁_DA  (auction-only ratio; B9 implicit denominator), %:")
    print(agg.pivot(index="firm", columns="regime", values="q2_per_q1_DA_pct").to_string(float_format=lambda x: f"{x:+6.2f}"))
    print()
    print("q₂ / q₁_total  (auction + bilateral; the IR-correct denominator), %:")
    print(agg.pivot(index="firm", columns="regime", values="q2_per_q1_total_pct").to_string(float_format=lambda x: f"{x:+6.2f}"))
    print()

    OUT.parent.mkdir(parents=True, exist_ok=True)
    agg.to_csv(OUT, index=False)
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
