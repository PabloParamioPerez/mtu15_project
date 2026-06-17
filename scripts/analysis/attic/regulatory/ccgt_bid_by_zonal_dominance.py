# STATUS: ALIVE
# LAST-AUDIT: 2026-05-16
# FEEDS: provisional.tex (within-firm bid level by zonal dominance)
# CLAIM: Within each pivotal firm, classify each CCGT unit as "in a zone
#        where the firm is the dominant CCGT operator" or not (using the
#        manual zone map). Hypothesis (user): units in dominant zones bid
#        higher in DA because the RT2 pay-as-bid backstop is more reliable
#        there. Compare per-unit-day quantity-weighted DA bid distributions
#        within firm × dominance × hour-class.

from __future__ import annotations

import json
import sys
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "src"))
from mtu.classification.units import firm_unit_panel  # noqa: E402

UNIT_DAY = REPO / "results" / "regressions" / "regulatory" / "ccgt_bid_vs_rt2" / "unit_day_panel.csv"
ZONE_MAP = REPO / "data" / "external" / "ccgt_zonal_map.csv"
ESIOS_GEN = REPO / "data" / "external" / "esios_master" / "generation_units.json"
UNITS_CSV = REPO / "data" / "external" / "omie_reference" / "lista_unidades.csv"
OUTDIR = REPO / "results" / "regressions" / "regulatory" / "ccgt_bid_by_zonal_dominance"
OUTDIR.mkdir(parents=True, exist_ok=True)

PIVOTAL = ("IB", "GE", "GN", "HC")
CRIT_HOURS = (5, 6, 7, 8, 16, 17, 18, 19, 20, 21, 22)


def build_dominance_map():
    """For each (firm, zone), compute capacity share. Tag each unit as
    'dominant_in_zone' if its firm is the top by capacity in its zone."""
    zones = pd.read_csv(ZONE_MAP)
    units = firm_unit_panel(csv_path=str(UNITS_CSV), scheme="short", mode="primary_owner")
    ccgt = units[units["tech_group"] == "CCGT"][["unit_code", "parent"]].rename(columns={"parent": "firm"})
    panel = zones.merge(ccgt, on="unit_code", how="left")
    panel["firm"] = panel["firm"].fillna("OTHER")

    with open(ESIOS_GEN) as f:
        gen = json.load(f)["GenerationUnits"]
    cap = pd.DataFrame(gen)[["UP Code", "Maximum Power Capacity MW"]]
    cap.columns = ["unit_code", "capacity_mw"]
    cap["capacity_mw"] = pd.to_numeric(cap["capacity_mw"].astype(str).str.replace(",", "."), errors="coerce")
    cap = cap.groupby("unit_code", as_index=False)["capacity_mw"].sum()
    panel = panel.merge(cap, on="unit_code", how="left")
    panel["capacity_mw"] = panel["capacity_mw"].fillna(0)

    by_zone_firm = panel.groupby(["zone", "firm"], as_index=False)["capacity_mw"].sum()
    zone_total = by_zone_firm.groupby("zone")["capacity_mw"].sum().rename("zone_total_mw")
    by_zone_firm = by_zone_firm.merge(zone_total, on="zone")
    by_zone_firm["firm_share"] = by_zone_firm["capacity_mw"] / by_zone_firm["zone_total_mw"]
    # top firm in each zone
    top = by_zone_firm.sort_values(["zone", "firm_share"], ascending=[True, False]).groupby("zone").head(1)[["zone", "firm"]]
    top.columns = ["zone", "top_firm"]
    panel = panel.merge(top, on="zone")
    panel["dominant_in_zone"] = (panel["firm"] == panel["top_firm"]).astype(int)
    # also report firm-share for diagnostics
    panel = panel.merge(by_zone_firm[["zone", "firm", "firm_share"]], on=["zone", "firm"])
    return panel


def main():
    print("=== zone × firm dominance map ===")
    dom = build_dominance_map()
    print(dom[dom["firm"].isin(PIVOTAL)][["unit_code", "plant", "zone", "firm",
                                           "top_firm", "dominant_in_zone",
                                           "firm_share", "capacity_mw"]].to_string(index=False))

    print("\n=== loading unit-day panel ===")
    pdf = pd.read_csv(UNIT_DAY, parse_dates=["day"])
    pdf = pdf.merge(dom[["unit_code", "zone", "dominant_in_zone", "firm_share"]],
                     on="unit_code", how="inner")
    pdf = pdf[pdf["firm"].isin(PIVOTAL)]

    # Tag regime by date
    pdf["regime"] = np.where(pdf["day"] < pd.Timestamp("2025-04-28"), "pre", "post")

    print("\n=== Within-firm: DA qw bid price (EUR/MWh) by firm × dominant_in_zone ===")
    print("(pre-blackout: 2024-01 → 2025-04-27; post-blackout: 2025-05 → 2026-01)")
    pdf = pdf[pdf["qw_bid_eur_mwh"].notna()]
    summary = pdf.groupby(["firm", "regime", "dominant_in_zone"], observed=True).agg(
        n_days=("qw_bid_eur_mwh", "size"),
        n_units=("unit_code", "nunique"),
        median_bid=("qw_bid_eur_mwh", "median"),
        p25_bid=("qw_bid_eur_mwh", lambda v: np.quantile(v, 0.25)),
        p75_bid=("qw_bid_eur_mwh", lambda v: np.quantile(v, 0.75)),
    ).round(1).reset_index()
    print(summary.to_string(index=False))
    summary.to_csv(OUTDIR / "bid_by_firm_dominance.csv", index=False)

    print("\n=== Per-unit median (verify within-firm heterogeneity) ===")
    per_unit = pdf.groupby(["firm", "unit_code", "zone", "dominant_in_zone", "regime"],
                            observed=True).agg(
        n_days=("qw_bid_eur_mwh", "size"),
        median_bid=("qw_bid_eur_mwh", "median"),
    ).round(1).reset_index()
    per_unit = per_unit.sort_values(["firm", "regime", "dominant_in_zone", "median_bid"],
                                      ascending=[True, True, False, False])
    print(per_unit.to_string(index=False))
    per_unit.to_csv(OUTDIR / "per_unit_median.csv", index=False)


if __name__ == "__main__":
    main()
