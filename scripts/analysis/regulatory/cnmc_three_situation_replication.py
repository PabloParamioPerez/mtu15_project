# STATUS: ALIVE
# LAST-AUDIT: 2026-04-28
# FEEDS: F17, F18 + new F21 candidate
# CLAIM: Replicate CNMC three-situation pivotality test (SBO3 case methodology) for Big-4 CCGTs in 2024-25
"""Replication of CNMC SNC/DE/019/22 (SBO3 2023) three-situation test.

Methodology (from docs/regulation/cnmc_resolutions/README_economic_methodology.md):

The CNMC partitions every hour by the local zone's CCGT availability:
  - Situation 1 = both CCGTs in zone available -> COMPETITIVE
  - Situation 2 = partner-plant unavailable -> SOLE-CCGT PIVOTAL
  - Situation 3 = both CCGTs needed simultaneously -> JOINT PIVOTAL

For each plant, compute:
  - Hours in each situation (from A80 outages + A73 dispatch presence)
  - Restriction-driven dispatch = A73 actual MW - PDBC cleared MW (the
    redispatch volume due to technical restrictions; "restricciones" =
    PHF - PDBC, which we approximate as A73 - PDBC since A73 ≈ PHF
    after the system operator schedules)
  - DA cleared price for the same hour (from OMIE marginalpdbc)
  - Mean per-MW restriction redispatch by situation (used as the "wedge"
    proxy since per-plant restriction bid prices are not in our
    public data — only system-aggregate prices via totalrp48preccierre)

Zones with 2+ CCGTs that map cleanly to the CNMC framework:
  Galicia       — SBO3 (Naturgy), PGR5 (Endesa)
  Cataluña      — BES3, BES5 (Endesa), BES4 (Naturgy), PVENT1+2
  Murcia/Almeria — CTGN1, CTGN2, CTGN3 (Naturgy), ESC6 (IB),
                   ESCCC1-3 (Engie), ESCCC, CTNU (Engie)
  Andalucía     — ARCOS1+2+3 (IB), PALOS1+2+3 (Naturgy), MALA1, ALG3, COL4
  Comunidad Valenciana — CTN3, CTN4 (IB), CTJON1+2+3 (TotalEnergies),
                          PVENT1+2 (GN+Alpiq), CAMGI*, CAMG20R
  Aragón        — ECT2 (Ignis), ECT3 (Repsol), ESCATRÓN

Output: results/regressions/cnmc_three_situation_replication.csv
"""
from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

PROJECT = Path(__file__).resolve().parents[3]

# Plant-zone mapping (manual, from CNMC Galicia table + OMIE plant locations).
# Conservative: include only plants where we are confident of zone assignment.
PLANT_ZONE = {
    # Galicia (the SBO3 case zone)
    "SBO3": "Galicia", "PGR5": "Galicia",
    # Cataluña — the 2019 Endesa BES3+BES5 case zone
    "BES3": "Cataluña", "BES4": "Cataluña", "BES5": "Cataluña",
    "PVENT1": "Cataluña", "PVENT2": "Cataluña",
    # Murcia/Almeria
    "CTGN1": "Murcia", "CTGN2": "Murcia", "CTGN3": "Murcia",
    "ESC6": "Murcia",
    "ESCCC1": "Murcia", "ESCCC2": "Murcia", "ESCCC3": "Murcia",
    "CTNU": "Aragón",
    # Andalucía
    "ARCOS1": "Andalucía", "ARCOS2": "Andalucía", "ARCOS3": "Andalucía",
    "PALOS1": "Andalucía", "PALOS2": "Andalucía", "PALOS3": "Andalucía",
    "MALA1": "Andalucía", "ALG3": "Andalucía", "COL4": "Andalucía",
    "CAMGI10": "Andalucía", "CAMG20R": "Andalucía",
    "SROQ1": "Andalucía", "SROQ2": "Andalucía",
    # Comunidad Valenciana
    "CTN3": "Valencia", "CTN4": "Valencia",
    "CTJON1R": "Valencia", "CTJON2": "Valencia", "CTJON3R": "Valencia",
    "SAGU1": "Valencia", "SAGU2": "Valencia", "SAGU3": "Valencia",
    # Aragón
    "ECT3": "Aragón",
    # Asturias
    "SBO3_": "Galicia",
    "SRI4R": "Asturias", "SRI5R": "Asturias",
    "STC4": "Vasconia", "BAHIAB": "Vasconia",
    # Castilla-La-Mancha
    "ACE3": "CLM", "ACE4": "CLM", "TAPOWER": "CLM",
    # Galicia coal
    "MEI1": "Galicia",  # Meirama (Naturgy coal — likely retired)
    # Arrubal, La Rioja
    "ARRU1R": "Aragón", "ARRU2R": "Aragón",
    # Amorebieta
    "AMBIETA": "Vasconia",
}


def main() -> None:
    print("Loading A73 per-unit panel + CCGT firm map...")
    df = pd.read_parquet(PROJECT / "data/processed/entsoe/generation/ccgt_per_firm_panel.parquet")
    df["ts"] = pd.to_datetime(df["isp_start_utc"])
    df["zone"] = df["omie_code"].map(PLANT_ZONE).fillna("Other")

    # Restrict to 2024-2025 + early 2026 (the post-MTU15-IDA window)
    df = df[(df.ts >= "2024-01-01") & (df.ts < "2026-04-28")]
    print(f"  {len(df):,} CCGT-MWh rows in 2024-01 → 2026-04-27")

    # Load OMIE PDBC (DA cleared per unit) — to compute restriction redispatch
    print("Loading OMIE pdbc_all (DA cleared per unit)...")
    con = duckdb.connect()
    pdbc = con.execute(f"""
        SELECT date, unit_code, period, assigned_power_mw
        FROM '{PROJECT}/data/processed/omie/mercado_diario/programas/pdbc_all.parquet'
        WHERE date >= '2024-01-01'
          AND date < '2026-04-28'
          AND offer_type = 1  -- sell offers only (not pumping/buying)
    """).df()
    print(f"  {len(pdbc):,} pdbc sell rows")
    pdbc["date"] = pdbc["date"].astype(str)

    # Hour-level: aggregate A73 (15-min) to hour for join
    df["hour_start"] = df["ts"].dt.floor("h")
    a73_hourly = df.groupby(["hour_start", "omie_code", "firm", "zone"]).agg(
        a73_mw=("quantity_mw", "mean"),
    ).reset_index()
    a73_hourly["date"] = a73_hourly["hour_start"].dt.date.astype(str)
    a73_hourly["period"] = a73_hourly["hour_start"].dt.hour + 1  # OMIE period is 1-24
    print(f"  A73 hourly: {len(a73_hourly):,} rows")

    # Join to PDBC
    mg = a73_hourly.merge(pdbc, left_on=["date", "omie_code", "period"],
                          right_on=["date", "unit_code", "period"], how="left")
    mg["pdbc_mwh"] = mg["assigned_power_mw"].fillna(0)
    mg["restriction_redisp_mw"] = mg["a73_mw"] - mg["pdbc_mwh"]
    print(f"  Joined panel: {len(mg):,} hourly plant-rows")

    # PIVOTALITY classification at zone-hour level
    # Plant-availability per hour: a73_mw > 1 MW (i.e. plant is online and producing)
    mg["available"] = (mg["a73_mw"] > 1).astype(int)
    zone_avail = mg.groupby(["hour_start", "zone"])["available"].sum().reset_index()
    zone_avail.columns = ["hour_start", "zone", "n_available_in_zone"]
    mg = mg.merge(zone_avail, on=["hour_start", "zone"], how="left")
    # Total CCGT count in each zone
    zone_size = {z: sum(1 for k, v in PLANT_ZONE.items() if v == z) for z in set(PLANT_ZONE.values())}
    mg["zone_size"] = mg["zone"].map(zone_size).fillna(1)
    # Three situations (CNMC SBO3 framework, simplified for >2 plant zones):
    #   Sit 1 = >= 50% of zone CCGTs available (competitive; multiple options)
    #   Sit 2 = the only available CCGT in zone (sole pivotal)
    #   Sit 3 = small number available, all pivotal (joint pivotal)
    def classify(row):
        if row["available"] == 0:
            return "Off"
        n_avail = row["n_available_in_zone"]
        n_total = row["zone_size"]
        if n_total <= 1:
            return "Sit_solo"  # only plant in zone — always pivotal trivially
        if n_avail == 1:
            return "Sit2_sole_pivot"
        if n_avail >= max(2, int(0.5 * n_total)):
            return "Sit1_competitive"
        return "Sit3_joint_pivot"
    mg["situation"] = mg.apply(classify, axis=1)

    # Per-plant summary in 2024-25 — mean restriction-redispatch MW by situation
    print()
    print("=" * 80)
    print("Per-Big-4-plant restriction-redispatch by situation (2024-2026 panel)")
    print(f"  restriction_redisp_mw = A73_actual - PDBC_cleared (per plant per hour)")
    print(f"  positive = plant ran more than its DA schedule (restrictions push UP)")
    print(f"  negative = plant ran less than its DA schedule (restrictions push DOWN)")
    print("=" * 80)
    big4 = mg[mg.firm.isin(["IB", "GN", "GE", "HC"])]
    summary = big4.groupby(["zone", "firm", "omie_code", "situation"]).agg(
        n_hours=("hour_start", "count"),
        mean_a73_mw=("a73_mw", "mean"),
        mean_pdbc_mw=("pdbc_mwh", "mean"),
        mean_redisp_mw=("restriction_redisp_mw", "mean"),
    ).round(1)
    summary["redisp_pct_of_a73"] = (
        summary["mean_redisp_mw"] / summary["mean_a73_mw"] * 100
    ).round(1)

    # Filter to plants with material activity (>100 hours)
    pivot = summary.reset_index()
    pivot = pivot[pivot["n_hours"] > 100]
    pivot = pivot.sort_values(["zone", "firm", "omie_code", "situation"])

    # Print zone-by-zone
    for zone in sorted(pivot["zone"].unique()):
        zd = pivot[pivot.zone == zone]
        if len(zd) == 0:
            continue
        print(f"\n--- Zone: {zone} ---")
        cols = ["firm", "omie_code", "situation", "n_hours",
                "mean_a73_mw", "mean_pdbc_mw", "mean_redisp_mw", "redisp_pct_of_a73"]
        print(zd[cols].to_string(index=False))

    # Save
    out_path = PROJECT / "results/regressions/cnmc_three_situation_replication.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    pivot.to_csv(out_path, index=False)
    print(f"\nwrote {out_path}")

    # Final summary: which plants show the strongest "pivotal pricing"?
    # That is, which plants have HIGHER restriction redispatch (Sit2/Sit3) vs Sit1?
    print()
    print("=" * 80)
    print("Plants where Sit2/Sit3 (pivotal) restriction-redispatch is HIGHEST vs Sit1")
    print("(sign of pivotality engineering / SBO3-style behaviour)")
    print("=" * 80)
    rows = []
    for (zone, firm, plant), g in pivot.groupby(["zone", "firm", "omie_code"]):
        sit1 = g[g.situation == "Sit1_competitive"]["mean_redisp_mw"]
        sit2 = g[g.situation == "Sit2_sole_pivot"]["mean_redisp_mw"]
        sit3 = g[g.situation == "Sit3_joint_pivot"]["mean_redisp_mw"]
        if len(sit1) and (len(sit2) or len(sit3)):
            sit1_v = sit1.iloc[0] if len(sit1) else None
            sit2_v = sit2.iloc[0] if len(sit2) else None
            sit3_v = sit3.iloc[0] if len(sit3) else None
            rows.append({
                "zone": zone, "firm": firm, "plant": plant,
                "sit1_competitive_redisp_MW": sit1_v,
                "sit2_sole_pivot_redisp_MW": sit2_v,
                "sit3_joint_pivot_redisp_MW": sit3_v,
                "delta_sit2_vs_sit1": (sit2_v - sit1_v) if (sit1_v is not None and sit2_v is not None) else None,
            })
    cmp = pd.DataFrame(rows)
    if len(cmp):
        cmp = cmp.sort_values("delta_sit2_vs_sit1", ascending=False, na_position="last")
        print(cmp.head(20).to_string(index=False))


if __name__ == "__main__":
    main()
