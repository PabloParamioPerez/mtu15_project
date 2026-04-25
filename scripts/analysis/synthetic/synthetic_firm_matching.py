# STATUS: ALIVE
# LAST-AUDIT: 2026-04-26
# FEEDS: F7 (synthetic-firm method, Stage 1: plant matching)
# CLAIM: Plant-pair matching table for Ciarreta-Espinosa-style synthetic-firm Lerner
"""Synthetic-firm method, Stage 1: plant-pair matching table.

Following Ciarreta & Espinosa (J Regul Econ 2010), build a Big-4 → Fringe
plant-pair matching table on (technology, observed-capacity-band).

For each Big-4 plant L, find a Fringe (or smaller-firm) plant S of the
same technology with closest observed maximum capacity. The synthetic
offer for L is then S's offer schedule scaled by K_L / K_S.

This script ONLY produces the matching table. Stages 2-4 (synthetic
supply construction, re-clearing, market-power index) follow.

Memory-conscious: streams pdbce + lista_unidades through DuckDB; only
the small per-unit summary panel is materialised in pandas.
"""
from __future__ import annotations

from pathlib import Path
import duckdb
import numpy as np
import pandas as pd

PROJECT = Path(__file__).resolve().parents[3]
PDBCE = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbce_all.parquet"
REF = PROJECT / "data" / "external" / "omie_reference" / "lista_unidades.csv"
OUT = PROJECT / "data" / "derived" / "panels" / "synthetic_plant_match.parquet"

BIG4 = ["GE", "IB", "GN", "HC"]


def bucket_tech(t) -> str:
    """Map raw OMIE technology label to coarse buckets used for matching."""
    if pd.isna(t):
        return "Other"
    t = str(t).lower()
    if "ciclo combinado" in t:
        return "CCGT"
    if "nuclear" in t:
        return "Nuclear"
    if "hidrá" in t or "hidra" in t or "hidr" in t:
        # Includes 'Hidráulica Generación' and 'RE Mercado Hidráulica'
        return "Hydro"
    if "eóli" in t or "eoli" in t:
        return "Wind"
    if "fotovolt" in t or "solar tÃ©rmica" in t or "solar termica" in t or "solar" in t:
        return "Solar"
    if "tÃ©rmica" in t or "termica" in t or "térmica" in t:
        return "Thermal"
    if "almacen" in t or "bater" in t:
        return "Storage"
    if "bombeo" in t or "bomba" in t:
        return "PumpHydro"
    return "Other"


def main() -> None:
    # ----- 1) Read reference file (unit -> tech) -----
    print("[1/4] Reading lista_unidades.csv...")
    ref = pd.read_csv(REF, encoding="latin1")
    ref["tech"] = ref["technology"].apply(bucket_tech)
    print(f"   {len(ref):,} units in registry")

    # ----- 2) Per-unit observed max capacity from pdbce (cleared Big-4 + Fringe) -----
    print("[2/4] Per-unit observed max capacity from pdbce...")
    con = duckdb.connect()
    con.execute("SET memory_limit='2GB'")
    con.execute("SET threads=4")

    # Use 99th percentile of assigned_power_mw as 'observed capacity' to be
    # robust to single-period spikes / data errors. Restrict to 2024+ to match
    # the post-IDA period of interest.
    cap_df = con.sql(f"""
        SELECT unit_code,
               grupo_empresarial AS firm,
               COUNT(*) AS n_obs,
               MAX(assigned_power_mw) AS max_mw,
               QUANTILE_CONT(assigned_power_mw, 0.99) AS p99_mw,
               QUANTILE_CONT(assigned_power_mw, 0.50) AS p50_mw
        FROM '{PDBCE}'
        WHERE offer_type = 1
          AND assigned_power_mw IS NOT NULL
          AND assigned_power_mw > 0
          AND CAST(date AS DATE) >= DATE '2024-01-01'
        GROUP BY unit_code, grupo_empresarial
    """).df()
    print(f"   {len(cap_df):,} units with positive cleared MW since 2024-01-01")

    # Use p99 as the 'observed capacity' (avoid single-spike noise)
    cap_df["capacity_mw"] = cap_df["p99_mw"]

    # ----- 3) Join tech, classify firm group -----
    print("[3/4] Join tech mapping, classify firm group...")
    cap_df = cap_df.merge(ref[["unit_code", "tech"]], on="unit_code", how="left")
    cap_df["tech"] = cap_df["tech"].fillna("Other")
    cap_df["firm_group"] = cap_df["firm"].where(cap_df["firm"].isin(BIG4), "Fringe")

    # Restrict to dispatchable / strategically-relevant techs.
    # Wind / Solar / Storage handled separately (they bid at zero or near-zero;
    # synthetic methodology doesn't apply cleanly).
    DISPATCHABLE = ["CCGT", "Nuclear", "Hydro", "PumpHydro", "Thermal"]
    cap_df = cap_df[cap_df["tech"].isin(DISPATCHABLE)].copy()
    print(f"   {len(cap_df):,} dispatchable units")
    print()
    print("Inventory by firm_group × tech:")
    inv = cap_df.groupby(["firm_group", "tech"], observed=True).agg(
        n_units=("unit_code", "count"),
        total_capacity=("capacity_mw", "sum"),
        median_capacity=("capacity_mw", "median"),
    ).round(1)
    print(inv.to_string())

    # ----- 4) Plant-pair matching: for each Big-4 plant L, find closest-capacity Fringe plant S of same tech -----
    print()
    print("[4/4] Match each Big-4 plant to closest-capacity Fringe of same tech...")
    big4 = cap_df[cap_df["firm_group"].isin(BIG4)].copy()
    fringe = cap_df[cap_df["firm_group"] == "Fringe"].copy()
    rows = []
    n_unmatched = 0
    for _, L in big4.iterrows():
        candidates = fringe[fringe["tech"] == L["tech"]].copy()
        if len(candidates) == 0:
            rows.append({
                "unit_L": L["unit_code"], "firm_L": L["firm"], "tech": L["tech"],
                "capacity_L": L["capacity_mw"],
                "unit_S": None, "firm_S": None, "capacity_S": np.nan,
                "K_ratio": np.nan, "match_distance": np.nan,
            })
            n_unmatched += 1
            continue
        # Match by absolute capacity distance
        candidates["distance"] = (candidates["capacity_mw"] - L["capacity_mw"]).abs()
        S = candidates.sort_values("distance").iloc[0]
        rows.append({
            "unit_L": L["unit_code"], "firm_L": L["firm"], "tech": L["tech"],
            "capacity_L": L["capacity_mw"],
            "unit_S": S["unit_code"], "firm_S": S["firm"],
            "capacity_S": S["capacity_mw"],
            "K_ratio": L["capacity_mw"] / S["capacity_mw"] if S["capacity_mw"] > 0 else np.nan,
            "match_distance": S["distance"],
        })

    match = pd.DataFrame(rows)
    print(f"   matched: {(match['unit_S'].notna()).sum()}/{len(match)} Big-4 plants")
    print(f"   unmatched (no Fringe of same tech): {n_unmatched}")

    # Quality summary
    print()
    print("Match quality by tech:")
    for tech, sub in match.groupby("tech"):
        matched = sub.dropna(subset=["unit_S"])
        if len(matched) == 0:
            print(f"  {tech:<10}  {len(sub):>3} Big-4 plants, NO Fringe match")
            continue
        med_K = matched["K_ratio"].median()
        med_dist = matched["match_distance"].median()
        print(
            f"  {tech:<10}  {len(matched):>3} matched / {len(sub)}  "
            f"median K_ratio={med_K:.2f}  median |Δcap|={med_dist:.1f} MW"
        )

    print()
    print("Sample matches:")
    print(match[match["unit_S"].notna()].head(20).to_string(index=False))

    # Save
    OUT.parent.mkdir(parents=True, exist_ok=True)
    match.to_parquet(OUT, index=False)
    cap_df.to_parquet(OUT.parent / "synthetic_plant_inventory.parquet", index=False)
    print(f"\nwrote {OUT}")
    print(f"wrote {OUT.parent / 'synthetic_plant_inventory.parquet'}")


if __name__ == "__main__":
    main()
