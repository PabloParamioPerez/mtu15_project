# STATUS: ALIVE
# LAST-AUDIT: 2026-05-07
# FEEDS: dominance audit (firm-class structural validation)
# CLAIM: Three exogenous structural markers that test whether each owner-agent
#        in OMIE's register has the scale + flexibility to be a strategic-
#        conduct firm. Used to validate whether the firm_class partition
#        (IB/GE/GN/HC/Fringe) survives empirically.
#
# Markers:
#   1. Critical-hour DA generation share (h{18-22}, cleared MWh, post-MTU15-DA)
#   2. Critical-hour CCGT capacity share (within flexible-thermal subset)
#   3. Fleet flexibility composition (% MW in flexible_strategic techs)
#
# Window: 2025-10-01 to 2025-12-31. Uses pdbce (cleared programs by firm) and
# OMIE register (owner_agent + technology). Aggregates owner_agent strings to
# corporate parent groups.

from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
OUTDIR = REPO / "results" / "regressions" / "firm" / "dominance_audit"
OUTDIR.mkdir(parents=True, exist_ok=True)

PDBCE = REPO / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbce_all.parquet"
PDBC = REPO / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbc_all.parquet"
CAB = REPO / "data" / "processed" / "omie" / "mercado_diario" / "ofertas" / "cab_all.parquet"
UNITS_CSV = REPO / "data" / "external" / "omie_reference" / "lista_unidades.csv"

START = "2025-10-01"
END = "2026-01-01"
CRITICAL_HOURS = (18, 19, 20, 21, 22)  # price_peak from critical_hours.py


def parent_group(owner: str | None) -> str:
    """Aggregate OMIE owner_agent strings → corporate parent for cross-firm comparison."""
    if owner is None or pd.isna(owner):
        return "Unknown"
    o = owner.upper()
    # Big-4
    if "IBERDROLA" in o:
        return "IB (Iberdrola)"
    if "ENDESA" in o:
        return "GE (Endesa)"
    if "NATURGY" in o or "GAS NATURAL" in o:
        return "GN (Naturgy)"
    if "EDP ESPAÑA" in o or "HC ENERGIA" in o:
        return "HC (EDP-Spain)"
    # Other major
    if "EDP GEM PORTUGAL" in o:
        return "EDP-Portugal"
    if "ENGIE" in o:
        return "Engie"
    if "REPSOL" in o:
        return "Repsol"
    if "MOEVE" in o or "CEPSA" in o:
        return "Moeve (Cepsa)"
    if "TOTALENERGIES" in o or "TOTAL ENERGIES" in o:
        return "TotalEnergies"
    if "ACCIONA" in o:
        return "Acciona"
    if "AXPO" in o:
        return "Axpo"
    if "ALPIQ" in o:
        return "Alpiq"
    if "SHELL" in o:
        return "Shell"
    if "GALP" in o:
        return "Galp"
    if "IGNIS" in o:
        return "Ignis"
    if "ENEL GREEN" in o:
        return "Enel-Green"
    if "GNERA" in o:
        return "GNERA"
    if "BAHIA DE BIZKAIA" in o:
        return "BBE (joint-venture)"
    return "Other"


def load_register_with_tech_role() -> pd.DataFrame:
    import sys
    sys.path.insert(0, str(REPO / "src"))
    from mtu.classification.units import classify_units

    df = classify_units(
        csv_path=str(UNITS_CSV),
        keep_columns=[
            "unit_code", "owner_agent", "technology", "zone",
            "firm_class", "tech_group", "tech_strategic_role",
        ],
    )
    df["parent_group"] = df["owner_agent"].map(parent_group)
    return df


def main() -> None:
    register = load_register_with_tech_role()
    print(f"Register units: {len(register)}")

    con = duckdb.connect()
    con.execute("PRAGMA threads = 6")
    con.register("reg", register[[
        "unit_code", "parent_group", "firm_class",
        "tech_group", "tech_strategic_role", "zone",
    ]])

    # ---- Marker 1+2: cleared energy by firm × critical-vs-flat × tech ----
    # All quantities reported as MWh (energy = MW × period_length).
    crit_list = ",".join(str(h) for h in CRITICAL_HOURS)
    print("\n--- Computing cleared-energy aggregates from pdbce ---")
    perfirm = con.execute(
        f"""
        WITH pdbce_w AS (
            SELECT date::DATE AS d, period, mtu_minutes, unit_code,
                   assigned_power_mw,
                   -- hour of day: period 1 = h0 in MTU60; period 1 = h0 in MTU15 (period 1-4 = h0)
                   CASE WHEN mtu_minutes = 60 THEN period - 1
                        WHEN mtu_minutes = 15 THEN (period - 1) // 4
                        ELSE NULL END AS hour
            FROM '{PDBCE}'
            WHERE date::DATE >= DATE '{START}'
              AND date::DATE <  DATE '{END}'
              AND assigned_power_mw > 0  -- supply only (we only want generation share)
        ),
        joined AS (
            SELECT p.*, r.parent_group, r.firm_class,
                   r.tech_group, r.tech_strategic_role, r.zone,
                   -- energy in MWh: MW × hours (mtu_minutes/60)
                   (p.assigned_power_mw * p.mtu_minutes / 60.0) AS mwh
            FROM pdbce_w p LEFT JOIN reg r USING (unit_code)
            WHERE r.zone = 'ZONA ESPAÑOLA'  -- Spanish zone only for dominance test
        )
        SELECT parent_group,
               tech_strategic_role,
               tech_group,
               CASE WHEN hour IN ({crit_list}) THEN 'critical_h18_22'
                    ELSE 'other_hours' END AS hour_class,
               SUM(mwh) AS energy_mwh
        FROM joined
        GROUP BY 1,2,3,4
        """
    ).df()

    # Total system energy by hour-class (Spain), denominator for shares.
    totals = (
        perfirm.groupby("hour_class")["energy_mwh"].sum().rename("system_total_mwh")
    )
    perfirm = perfirm.merge(totals, on="hour_class")
    perfirm["share_of_system"] = perfirm["energy_mwh"] / perfirm["system_total_mwh"]

    # Marker 1: total share by firm × hour_class
    print("\n--- Marker 1: critical-hour vs other-hour DA generation share by firm ---")
    m1 = (
        perfirm.groupby(["parent_group", "hour_class"])["share_of_system"]
        .sum()
        .unstack("hour_class")
        .fillna(0)
    )
    m1["total_share_critical_pct"] = (m1["critical_h18_22"] * 100).round(2)
    m1["total_share_other_pct"] = (m1["other_hours"] * 100).round(2)
    m1 = m1[["total_share_critical_pct", "total_share_other_pct"]].sort_values(
        "total_share_critical_pct", ascending=False
    )
    print(m1.to_string())
    m1.to_csv(OUTDIR / "marker1_generation_share_by_firm.csv")

    # Marker 2: CCGT-only share within critical hours
    ccgt = perfirm[perfirm["tech_group"] == "CCGT"].copy()
    ccgt_totals = (
        ccgt.groupby("hour_class")["energy_mwh"].sum().rename("ccgt_total_mwh")
    )
    ccgt = ccgt.merge(ccgt_totals, on="hour_class")
    ccgt["ccgt_share"] = ccgt["energy_mwh"] / ccgt["ccgt_total_mwh"]
    print("\n--- Marker 2: CCGT-only share within critical hours ---")
    m2 = (
        ccgt.groupby(["parent_group", "hour_class"])["ccgt_share"]
        .sum()
        .unstack("hour_class")
        .fillna(0)
    )
    m2["ccgt_share_critical_pct"] = (m2["critical_h18_22"] * 100).round(2)
    m2["ccgt_share_other_pct"] = (m2["other_hours"] * 100).round(2)
    m2 = m2[["ccgt_share_critical_pct", "ccgt_share_other_pct"]].sort_values(
        "ccgt_share_critical_pct", ascending=False
    )
    print(m2.to_string())
    m2.to_csv(OUTDIR / "marker2_ccgt_share_by_firm.csv")

    # Marker 2b: ALL flex-strategic share within critical hours
    # (relaxed pivotality — concentration in the segment that *can* withhold)
    flex = perfirm[perfirm["tech_strategic_role"] == "flexible_strategic"].copy()
    flex_totals = (
        flex.groupby("hour_class")["energy_mwh"].sum().rename("flex_total_mwh")
    )
    flex = flex.merge(flex_totals, on="hour_class")
    flex["flex_share"] = flex["energy_mwh"] / flex["flex_total_mwh"]
    print("\n--- Marker 2b: flex-strategic share within critical hours ---")
    m2b = (
        flex.groupby(["parent_group", "hour_class"])["flex_share"]
        .sum()
        .unstack("hour_class")
        .fillna(0)
    )
    m2b["flex_share_critical_pct"] = (m2b["critical_h18_22"] * 100).round(2)
    m2b["flex_share_other_pct"] = (m2b["other_hours"] * 100).round(2)
    m2b = m2b[["flex_share_critical_pct", "flex_share_other_pct"]].sort_values(
        "flex_share_critical_pct", ascending=False
    )
    print(m2b.to_string())
    m2b.to_csv(OUTDIR / "marker2b_flex_share_by_firm.csv")

    # HHI on flex-strategic segment, by hour_class.
    flex_shares_by_hr = (
        flex.groupby(["hour_class", "parent_group"])["flex_share"].sum()
    )
    hhi = flex_shares_by_hr.groupby(level=0).apply(lambda s: ((s * 100) ** 2).sum())
    print("\n--- Marker 2c: HHI on flex-strategic segment ---")
    print(hhi.round(0).rename("HHI").to_string())
    hhi.round(0).rename("HHI_flex_strategic").to_csv(OUTDIR / "marker2c_hhi_flex.csv")

    # Flexibility composition by firm (within Spanish zone): MWh share by tech_strategic_role
    print("\n--- Marker 3: flexibility composition by firm (cleared MWh share) ---")
    m3_raw = (
        perfirm.groupby(["parent_group", "tech_strategic_role"])["energy_mwh"]
        .sum()
        .unstack("tech_strategic_role")
        .fillna(0)
    )
    m3_raw["total_mwh"] = m3_raw.sum(axis=1)
    m3 = pd.DataFrame(index=m3_raw.index)
    for col in m3_raw.columns:
        if col == "total_mwh":
            m3["total_mwh"] = m3_raw[col]
        else:
            m3[f"{col}_pct"] = (m3_raw[col] / m3_raw["total_mwh"] * 100).round(1)
    m3 = m3.sort_values("total_mwh", ascending=False)
    print(m3.to_string())
    m3.to_csv(OUTDIR / "marker3_flexibility_composition_by_firm.csv")

    # Combined: structural-dominance scoring (relaxed: scale-in-flex segment)
    combined = (
        m1[["total_share_critical_pct"]]
        .join(m2[["ccgt_share_critical_pct"]], how="outer")
        .join(m2b[["flex_share_critical_pct"]], how="outer")
        .join(m3[["flexible_strategic_pct", "total_mwh"]], how="outer")
        .fillna(0)
    )
    # Relaxed dominance: ≥10% share of flex-strategic cleared in critical hours
    # (this measures "scale within the segment that competes strategically")
    combined["dominant_in_flex_segment"] = combined["flex_share_critical_pct"] >= 10.0
    combined = combined.sort_values("flex_share_critical_pct", ascending=False)
    print("\n=== STRUCTURAL DOMINANCE VERDICT (flex-segment-relaxed) ===")
    print(
        combined[
            [
                "total_share_critical_pct",
                "ccgt_share_critical_pct",
                "flex_share_critical_pct",
                "flexible_strategic_pct",
                "dominant_in_flex_segment",
            ]
        ].to_string()
    )
    combined.to_csv(OUTDIR / "structural_dominance_combined.csv")

    print(f"\nOutputs: {OUTDIR}")


if __name__ == "__main__":
    main()
