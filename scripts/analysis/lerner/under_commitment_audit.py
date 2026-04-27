# STATUS: ALIVE
# LAST-AUDIT: 2026-04-27
# FEEDS: Re-audit of the DA-under-commitment / Ito-Reguant claim with first-principles OVB checks
# CLAIM: re-audit. Big-4 systematically under-commit in DA (ΔQ > 0); pattern compresses at ISP15; "stops at MTU15-DA" claim needs verification.

"""DA-under-commitment re-audit.

Reasoning before running:

DGP. For each (firm-group, day):
  DA cleared = sum_period pdbce sell-side assigned_power_mw × (mtu/60)
  IDA repositioning = sum_period_session pibcie net-sell × (mtu/60)
  ΔQ = IDA repositioning, signed positive = net-sold in IDA → under-committed in DA

If Big-4 systematically under-commit (Ito-Reguant), expect ΔQ > 0 by group.
If reform compresses behavior, expect ΔQ to shrink across regimes for Big-4
  but NOT for Fringe (placebo) and possibly NOT for wind-only (forecast-driven
  rather than strategic).

OVB candidates and predicted bias direction:

  - Spanish wind capacity grew ~22→32 GW during 2018-2026. High-wind days
    have more forecast-revision repositioning. If post-reform regimes have
    more wind on average, ΔQ would be HIGHER not lower (mechanical wind effect).
    So omitting wind-level biases regime coefficients DOWNWARD on the
    "compression" claim. Adding wind control should make the compression
    LARGER if real.

  - Blackout 2025-04-28 → operación reforzada → forced increased CCGT/nuclear
    DA commitment for ~5 months in DA60/ID15. Mechanically reduces ΔQ during
    that subwindow. Adding "post-blackout" indicator (or restricting to clean
    pre-blackout DA60/ID15) should REDUCE the apparent DA60/ID15 compression
    if blackout is the driver.

  - Gas crisis 2022-2023: pre-IDA period contains crisis years where high gas
    prices may have inflated CCGT under-commitment (operational caution).
    Compresses pre-IDA more than post-reform years. Excluding 2022-23 from
    pre-IDA baseline should make the apparent regime drop SMALLER.

Sign of compression direction: predicted to remain post all OVB checks if the
strategic-conduct interpretation holds.

Output: data/derived/results/under_commitment_audit.csv
"""
from __future__ import annotations

from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import statsmodels.api as sm

PROJECT = Path(__file__).resolve().parents[3]
PDBCE = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbce_all.parquet"
PIBCIE = PROJECT / "data" / "processed" / "omie" / "mercado_intradiario_subastas" / "programas" / "pibcie_all.parquet"
REF = PROJECT / "data" / "external" / "omie_reference" / "lista_unidades.csv"
VRE = PROJECT / "data" / "processed" / "entsoe" / "generation" / "wind_solar_actual_all.parquet"
OUT = PROJECT / "data" / "derived" / "results" / "under_commitment_audit.csv"

BIG4 = ["GE", "IB", "GN", "HC"]
BLACKOUT = "2025-04-28"


def assign_regime(d) -> str:
    if d < pd.Timestamp("2024-06-14"):
        return "1.pre-IDA"
    if d < pd.Timestamp("2024-12-01"):
        return "2.3-sess"
    if d < pd.Timestamp("2025-03-19"):
        return "3.ISP15-win"
    if d < pd.Timestamp("2025-10-01"):
        return "4.DA60/ID15"
    return "5.DA15/ID15"


def main() -> None:
    print("[1/5] Build daily firm-group ΔQ from pdbce + pibcie...")
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    con.execute("SET threads=4")

    # DA cleared per (firm-group, day) — sell-side only
    con.execute(f"""
        CREATE TEMP TABLE da_daily AS
        SELECT CAST(date AS DATE) AS date,
               CASE WHEN grupo_empresarial IN ('GE','IB','GN','HC') THEN grupo_empresarial
                    ELSE 'Fringe' END AS firm_group,
               SUM(assigned_power_mw * mtu_minutes / 60.0) AS da_mwh
        FROM '{PDBCE}'
        WHERE offer_type = 1 AND assigned_power_mw IS NOT NULL AND assigned_power_mw > 0
          AND CAST(date AS DATE) >= DATE '2018-01-01'
        GROUP BY 1, 2
    """)

    # IDA net repositioning per (firm-group, day)
    # Use signed assigned_power_mw: positive = sell (offer_type=1), negative may exist for buy (offer_type=2)
    # Per OMIE convention, assigned_power_mw is signed in pibcie.
    con.execute(f"""
        CREATE TEMP TABLE ida_daily AS
        SELECT CAST(date AS DATE) AS date,
               CASE WHEN grupo_empresarial IN ('GE','IB','GN','HC') THEN grupo_empresarial
                    ELSE 'Fringe' END AS firm_group,
               SUM(assigned_power_mw * mtu_minutes / 60.0) AS ida_net_mwh
        FROM '{PIBCIE}'
        WHERE assigned_power_mw IS NOT NULL
          AND CAST(date AS DATE) >= DATE '2018-01-01'
        GROUP BY 1, 2
    """)

    panel = con.sql("""
        SELECT d.date, d.firm_group, d.da_mwh, COALESCE(i.ida_net_mwh, 0) AS ida_net_mwh
        FROM da_daily d
        LEFT JOIN ida_daily i ON d.date = i.date AND d.firm_group = i.firm_group
    """).df()
    panel["date"] = pd.to_datetime(panel["date"])
    panel["regime"] = panel["date"].apply(assign_regime)
    panel["delta_q"] = panel["ida_net_mwh"]  # net-sold in IDA = under-commitment proxy
    panel["da_share"] = panel["delta_q"] / panel["da_mwh"]
    panel["post_blackout"] = (panel["date"] >= pd.Timestamp(BLACKOUT)).astype(int)
    print(f"   panel: {len(panel):,} firm-day rows; range {panel.date.min().date()} → {panel.date.max().date()}")

    print()
    print("[2/5] Mean ΔQ (MWh/day) and ΔQ/DA (%) by firm-group × regime:")
    print()
    agg_mwh = panel.pivot_table(index="firm_group", columns="regime", values="delta_q", aggfunc="mean").round(0)
    agg_pct = panel.pivot_table(index="firm_group", columns="regime", values="da_share", aggfunc="mean") * 100
    print("Mean ΔQ in MWh/day (positive = under-commit, IDA net-sell):")
    print(agg_mwh.to_string())
    print()
    print("Mean ΔQ as % of DA cleared (normalized for firm size):")
    print(agg_pct.round(2).to_string())

    print()
    print("[3/5] Big-4 aggregate: regime-by-regime ΔQ trajectory + OVB checks")
    big4 = panel[panel["firm_group"].isin(BIG4)].copy()
    fringe = panel[panel["firm_group"] == "Fringe"].copy()

    # Add monthly wind level as OVB control
    print("   loading monthly Spanish wind generation (B19 + B18)...")
    vre = pd.read_parquet(VRE, columns=["isp_start_utc", "psr_type", "quantity_mw", "mtu_minutes"])
    wind = vre[vre["psr_type"].isin(["B18", "B19"])].copy()
    wind["isp_start"] = pd.to_datetime(wind["isp_start_utc"]).dt.tz_localize(None)
    wind["date"] = wind["isp_start"].dt.normalize()
    wind["mwh"] = wind["quantity_mw"] * (wind["mtu_minutes"] / 60.0)
    wind_d = wind.groupby("date", as_index=False)["mwh"].sum().rename(columns={"mwh": "wind_mwh"})

    big4 = big4.merge(wind_d, on="date", how="left")
    big4["wind_mwh"] = big4["wind_mwh"].fillna(big4["wind_mwh"].mean())

    # Reasoning-led specs
    rd = pd.get_dummies(pd.Categorical(big4["regime"],
                                       categories=["1.pre-IDA","2.3-sess","3.ISP15-win","4.DA60/ID15","5.DA15/ID15"],
                                       ordered=False),
                        prefix="rg", dtype=float).drop(columns="rg_1.pre-IDA")
    big4["cal_month"] = big4["date"].dt.month
    cm = pd.get_dummies(big4["cal_month"], prefix="cm", drop_first=True, dtype=float)
    fg = pd.get_dummies(big4["firm_group"], prefix="fg", drop_first=True, dtype=float)

    y = big4["delta_q"].astype(float)

    specs = [
        ("Spec 1: regime FE only",                              [], False, False, False),
        ("Spec 2: + cal-month FE",                              [], True, False, False),
        ("Spec 3: + firm-group FE",                             [], True, True, False),
        ("Spec 4: + wind level (mech wind effect OVB)",         ["wind_mwh"], True, True, False),
        ("Spec 5: + post-blackout indicator",                   ["wind_mwh"], True, True, True),
    ]
    print()
    print(f"{'Spec':<58} {'β(3-sess)':>10} {'β(ISP15)':>10} {'β(DA60)':>10} {'β(DA15)':>10} {'β(wind)':>10} {'β(blkt)':>10}    R²")
    print("-" * 145)
    for name, controls, do_cm, do_fg, do_blkt in specs:
        X_parts = [rd]
        if do_cm: X_parts.append(cm)
        if do_fg: X_parts.append(fg)
        if controls:
            X_parts.append(big4[controls].astype(float))
        if do_blkt:
            X_parts.append(big4[["post_blackout"]].astype(float))
        X = pd.concat(X_parts, axis=1).astype(float)
        X = sm.add_constant(X)
        try:
            res = sm.OLS(y, X).fit(cov_type="cluster", cov_kwds={"groups": big4["date"].values})
        except Exception:
            res = sm.OLS(y, X).fit(cov_type="HC3")
        b3 = res.params.get("rg_2.3-sess", np.nan)
        bISP = res.params.get("rg_3.ISP15-win", np.nan)
        bDA60 = res.params.get("rg_4.DA60/ID15", np.nan)
        bDA15 = res.params.get("rg_5.DA15/ID15", np.nan)
        bw = res.params.get("wind_mwh", np.nan)
        bb = res.params.get("post_blackout", np.nan)
        print(f"{name:<58} {b3:>+10.0f} {bISP:>+10.0f} {bDA60:>+10.0f} {bDA15:>+10.0f} {bw:>+10.5f} {bb:>+10.0f}  {res.rsquared:>5.3f}")

    print()
    print("[4/5] Same regression for Fringe (placebo — should NOT compress at reforms):")
    rd_f = pd.get_dummies(pd.Categorical(fringe["regime"],
                                          categories=["1.pre-IDA","2.3-sess","3.ISP15-win","4.DA60/ID15","5.DA15/ID15"],
                                          ordered=False),
                          prefix="rg", dtype=float).drop(columns="rg_1.pre-IDA")
    fringe["cal_month"] = fringe["date"].dt.month
    cm_f = pd.get_dummies(fringe["cal_month"], prefix="cm", drop_first=True, dtype=float)
    fringe = fringe.merge(wind_d, on="date", how="left", suffixes=("", "_w"))
    fringe["wind_mwh"] = fringe["wind_mwh"].fillna(fringe["wind_mwh"].mean())
    Xf = pd.concat([rd_f.reset_index(drop=True), cm_f.reset_index(drop=True), fringe[["wind_mwh"]].astype(float).reset_index(drop=True)], axis=1)
    yf = fringe["delta_q"].astype(float).reset_index(drop=True)
    keep = (~Xf.isna().any(axis=1)) & (~yf.isna()) & np.isfinite(yf)
    Xf = Xf.loc[keep]
    yf = yf.loc[keep]
    Xf = sm.add_constant(Xf)
    res_f = sm.OLS(yf, Xf).fit(cov_type="HC3")
    print(f"  β(3-sess)  = {res_f.params.get('rg_2.3-sess', np.nan):+.0f}")
    print(f"  β(ISP15)   = {res_f.params.get('rg_3.ISP15-win', np.nan):+.0f}")
    print(f"  β(DA60)    = {res_f.params.get('rg_4.DA60/ID15', np.nan):+.0f}")
    print(f"  β(DA15)    = {res_f.params.get('rg_5.DA15/ID15', np.nan):+.0f}")
    print(f"  R² = {res_f.rsquared:.3f}, n = {len(fringe)}")

    print()
    print("[5/5] Sanity check: monthly trajectory for Big-4 sum (raw, no controls):")
    big4["month"] = big4["date"].dt.to_period("M").dt.to_timestamp()
    big4_monthly = big4.groupby(["month"])["delta_q"].sum().reset_index()
    big4_monthly["regime"] = big4_monthly["month"].apply(assign_regime)
    by_regime = big4_monthly.groupby("regime").agg(n_months=("month", "size"), mean_total_mwh=("delta_q", "mean")).round(0)
    print(by_regime.to_string())

    OUT.parent.mkdir(parents=True, exist_ok=True)
    out = pd.concat([
        agg_mwh.reset_index().assign(_table="raw_mean_mwh"),
        agg_pct.round(2).reset_index().assign(_table="raw_pct_of_da"),
        by_regime.reset_index().assign(_table="big4_monthly_by_regime"),
    ], ignore_index=True, sort=False)
    out.to_csv(OUT, index=False)
    print(f"\nwrote {OUT}")


if __name__ == "__main__":
    main()
