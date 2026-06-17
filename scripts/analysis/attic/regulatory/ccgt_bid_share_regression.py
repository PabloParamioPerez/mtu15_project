# STATUS: ALIVE
# LAST-AUDIT: 2026-05-16
# FEEDS: provisional.tex (continuous zonal-dominance test)
# CLAIM: bid_{u,d} = α + β·firm_share_in_zone(u) + γ·zone_HHI(u) + FE + ε
#        Tests whether a CCGT unit's DA bid level rises with the unit's
#        firm's capacity share in its zone (zonal RT2 backstop hypothesis).
#        Within-firm + per-unit panel.

from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import statsmodels.api as sm
from statsmodels.formula.api import ols

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "src"))
from mtu.classification.units import firm_unit_panel  # noqa: E402

UNIT_DAY = REPO / "results" / "regressions" / "regulatory" / "ccgt_bid_vs_rt2" / "unit_day_panel.csv"
ZONE_MAP = REPO / "data" / "external" / "ccgt_zonal_map.csv"
ESIOS_GEN = REPO / "data" / "external" / "esios_master" / "generation_units.json"
UNITS_CSV = REPO / "data" / "external" / "omie_reference" / "lista_unidades.csv"
OUTDIR = REPO / "results" / "regressions" / "regulatory" / "ccgt_bid_share_regression"
OUTDIR.mkdir(parents=True, exist_ok=True)

PIVOTAL = ("IB", "GE", "GN", "HC")


def build_panel():
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
    panel = panel.merge(cap, on="unit_code", how="left").fillna({"capacity_mw": 0})

    by_zf = panel.groupby(["zone", "firm"], as_index=False)["capacity_mw"].sum()
    zone_total = by_zf.groupby("zone")["capacity_mw"].sum().rename("zone_total_mw")
    by_zf = by_zf.merge(zone_total, on="zone")
    by_zf["firm_share"] = by_zf["capacity_mw"] / by_zf["zone_total_mw"]

    # zone HHI
    by_zf["sq"] = (by_zf["firm_share"] ** 2)
    hhi = by_zf.groupby("zone", as_index=False)["sq"].sum().rename(columns={"sq": "zone_hhi"})
    hhi["zone_hhi"] = (hhi["zone_hhi"] * 10000).round(0)

    panel = panel.merge(by_zf[["zone", "firm", "firm_share"]], on=["zone", "firm"])
    panel = panel.merge(hhi, on="zone")
    return panel[["unit_code", "firm", "zone", "firm_share", "zone_hhi", "capacity_mw"]]


def main():
    print("=== build per-unit panel with zonal shares + HHI ===")
    p = build_panel()
    print(p[p["firm"].isin(PIVOTAL)].sort_values(["firm", "zone"]).to_string(index=False))

    print("\n=== load unit-day bid panel ===")
    bid = pd.read_csv(UNIT_DAY, parse_dates=["day"])
    bid = bid[bid["qw_bid_eur_mwh"].notna() & bid["qw_bid_eur_mwh"].gt(0)]
    bid["regime"] = np.where(bid["day"] < pd.Timestamp("2025-04-28"), "pre", "post")
    bid = bid.merge(p, on=["unit_code", "firm"], how="inner")
    bid = bid[bid["firm"].isin(PIVOTAL)]
    bid["log_bid"] = np.log(bid["qw_bid_eur_mwh"])
    bid["share_pct"] = 100 * bid["firm_share"]
    bid["month"] = bid["day"].dt.to_period("M").astype(str)

    print(f"\nN obs: {len(bid):,}")
    print(f"N units: {bid['unit_code'].nunique()}")
    print(f"Firms: {bid['firm'].value_counts().to_dict()}")

    def run_ols(formula, label, sub):
        m = ols(formula, data=sub).fit(
            cov_type="cluster", cov_kwds={"groups": sub["unit_code"]})
        print(f"\n--- {label} ---")
        print(f"  formula: {formula}")
        print(f"  N = {int(m.nobs)}, R^2 = {m.rsquared:.3f}")
        if "share_pct" in m.params:
            b = m.params["share_pct"]; se = m.bse["share_pct"]; pv = m.pvalues["share_pct"]
            print(f"  β(share_pct) = {b:+.4f}  SE = {se:.4f}  p = {pv:.4f}")
            print(f"  Interpretation: +10 pp of zonal share → {10*b:+.2f} log-EUR/MWh = {100*(np.exp(10*b)-1):+.1f}% bid level")
        if "zone_hhi" in m.params:
            b = m.params["zone_hhi"]; se = m.bse["zone_hhi"]; pv = m.pvalues["zone_hhi"]
            print(f"  β(zone_hhi)  = {b:+.5f}  SE = {se:.5f}  p = {pv:.4f}")
            print(f"  Interpretation: +1000 HHI → {1000*b:+.3f} log-EUR/MWh = {100*(np.exp(1000*b)-1):+.1f}% bid level")
        return m

    # Spec 1: pooled (regime FE only)
    sub = bid.copy()
    sub["regime_x"] = sub["regime"]
    run_ols("log_bid ~ share_pct + C(regime_x)", "Spec 1: pooled, regime FE", sub)

    # Spec 2: + firm FE (within-firm identification)
    run_ols("log_bid ~ share_pct + C(firm) + C(regime_x)", "Spec 2: + firm FE", sub)

    # Spec 3: + month FE
    sub["month"] = sub["day"].dt.to_period("M").astype(str)
    run_ols("log_bid ~ share_pct + C(firm) + C(month)", "Spec 3: + firm FE + month FE", sub)

    # Spec 4: HHI instead of share
    run_ols("log_bid ~ zone_hhi + C(firm) + C(month)", "Spec 4: HHI + firm FE + month FE", sub)

    # Spec 5: share interacted with regime
    sub["share_post"] = sub["share_pct"] * (sub["regime"] == "post").astype(int)
    run_ols("log_bid ~ share_pct + share_post + C(firm) + C(month)",
             "Spec 5: share × post interaction", sub)

    # Spec 6: pre-blackout only
    pre = bid[bid["regime"] == "pre"].copy()
    pre["month"] = pre["day"].dt.to_period("M").astype(str)
    run_ols("log_bid ~ share_pct + C(firm) + C(month)",
             "Spec 6: pre-blackout only", pre)

    # Spec 7: post-blackout only
    post = bid[bid["regime"] == "post"].copy()
    post["month"] = post["day"].dt.to_period("M").astype(str)
    run_ols("log_bid ~ share_pct + C(firm) + C(month)",
             "Spec 7: post-blackout only", post)


if __name__ == "__main__":
    main()
