# STATUS: ALIVE
# LAST-AUDIT: 2026-04-27
# FEEDS: D5 robustness (red-team audit attack A6)
# CLAIM: GE > IB sell-side cleared volume across 2018-2026, robust to Rule 28.8 confounder. D5's "GE 2.4x more net-seller" finding (used to rule out vertical-integration as the IB-canonical mechanism) reflects structural asymmetry, not a post-reform artifact.
"""D5 long-run robustness: GE vs IB sell-side cleared volume, 2018-2026.

Red-team audit attack A6. The current D5 finding ("GE 2.4× more
net-seller than IB post-Rule-28.8, therefore vertical-integration
doesn't explain IB > GE market power") rests on POST-Rule-28.8 data
(March 2025+). The audit attack: pre-Rule-28.8 buy-side data was
contaminated by artificial opportunity-cost bilateral-contract bids,
so the existing pre/post comparison can't cleanly establish whether
the cross-firm asymmetry pre-existed.

This script ignores the buy side entirely (where Rule 28.8 mattered)
and computes annual SELL-side cleared volumes for GE vs IB across
2018–2026. Sell-side is real cleared generation — unaffected by
bilateral-contract demand-side rerouting.

If GE's annual sell volume is consistently larger than IB's across
all 9 years (including the 7 years before the Rule 28.8 elimination),
then the post-Rule-28.8 cross-firm asymmetry reflects a structural
fact about the two firms' generation portfolios, not a reform
artifact. D5's vertical-integration ruling-out is robust.

Output:
    data/derived/results/d5_sell_side_long_run.csv
"""
from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

PROJECT = Path(__file__).resolve().parents[3]
PDBCE = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbce_all.parquet"
OUT = PROJECT / "data" / "derived" / "results" / "d5_sell_side_long_run.csv"

BIG4 = ("GE", "IB", "GN", "HC")


def main() -> None:
    con = duckdb.connect()
    con.execute("SET memory_limit='2GB'")
    con.execute("SET threads=4")

    print("[1/2] Annual Big-4 sell-side cleared volume from pdbce, 2018-2026...")
    df = con.sql(f"""
        SELECT grupo_empresarial AS firm,
               EXTRACT(YEAR FROM CAST(date AS DATE))::INTEGER AS year,
               SUM(assigned_power_mw)
                 / CASE WHEN mtu_minutes = 15 THEN 4.0 ELSE 1.0 END AS q_mwh
        FROM '{PDBCE}'
        WHERE assigned_power_mw IS NOT NULL
          AND assigned_power_mw > 0
          AND grupo_empresarial IN ({",".join(f"'{f}'" for f in BIG4)})
          AND offer_type = 1
          AND CAST(date AS DATE) >= DATE '2018-01-01'
        GROUP BY firm, year, mtu_minutes
    """).df()

    annual = df.groupby(["firm", "year"], as_index=False)["q_mwh"].sum()
    annual["q_twh"] = annual["q_mwh"] / 1e6
    pivot = annual.pivot(index="year", columns="firm", values="q_twh").round(2)
    pivot = pivot[[f for f in BIG4 if f in pivot.columns]]

    print()
    print("=" * 80)
    print("Annual Big-4 sell-side cleared volume (TWh/year)")
    print("=" * 80)
    print()
    print(pivot.to_string())

    # GE / IB ratio per year
    print()
    print("=" * 80)
    print("GE/IB sell-side ratio by year (>1 means GE clears more than IB)")
    print("=" * 80)
    if "GE" in pivot.columns and "IB" in pivot.columns:
        ratio = (pivot["GE"] / pivot["IB"]).round(2)
        print()
        print(ratio.to_string())
        print()
        print(f"Mean GE/IB ratio across all years 2018-2026: {ratio.mean():.2f}")
        print(f"Years with GE > IB: {(ratio > 1).sum()} / {ratio.notna().sum()}")
        print(f"Min ratio:  {ratio.min():.2f}")
        print(f"Max ratio:  {ratio.max():.2f}")

    # Era split: clean-pre (2018-2021), pre-IDA-near (2022-2024-05), post-Rule-28.8 (2025-03+)
    print()
    print("=" * 80)
    print("Era-mean GE/IB ratio")
    print("=" * 80)
    era_def = {
        "2018-2021 (clean pre-reform)": [2018, 2019, 2020, 2021],
        "2022-2024 (energy crisis incl.)": [2022, 2023, 2024],
        "2025-2026 (post-Rule-28.8)": [2025, 2026],
    }
    rows = []
    for label, years in era_def.items():
        sub = pivot.loc[[y for y in years if y in pivot.index]]
        if "GE" in sub.columns and "IB" in sub.columns:
            ge_total = sub["GE"].sum()
            ib_total = sub["IB"].sum()
            ratio_era = ge_total / ib_total if ib_total > 0 else float("nan")
            print(f"  {label:<32}  GE={ge_total:6.1f} TWh,  IB={ib_total:6.1f} TWh,  ratio={ratio_era:.2f}")
            rows.append({"era": label, "GE_twh": ge_total, "IB_twh": ib_total, "GE_IB_ratio": ratio_era})

    print()
    print("=" * 80)
    print("Verdict for audit attack A6")
    print("=" * 80)
    if "GE" in pivot.columns and "IB" in pivot.columns:
        ratio = pivot["GE"] / pivot["IB"]
        n_above = (ratio > 1).sum()
        n_total = ratio.notna().sum()
        if n_above == n_total and ratio.min() >= 1.2:
            print(f"  ✓ ROBUST: GE > IB sell-side in {n_above}/{n_total} years; min ratio {ratio.min():.2f}.")
            print(f"    The cross-firm sell-side asymmetry pre-existed Rule 28.8 elimination.")
            print(f"    D5's vertical-integration ruling-out is structural, not a reform artifact.")
            print(f"    A6 attack DEFENDED.")
        elif n_above >= 0.8 * n_total:
            print(f"  ≈ MOSTLY ROBUST: GE > IB in {n_above}/{n_total} years.")
        else:
            print(f"  ✗ NOT ROBUST: GE > IB in only {n_above}/{n_total} years.")

    # Save
    OUT.parent.mkdir(parents=True, exist_ok=True)
    pivot.to_csv(OUT)
    print(f"\nwrote {OUT}")


if __name__ == "__main__":
    main()
