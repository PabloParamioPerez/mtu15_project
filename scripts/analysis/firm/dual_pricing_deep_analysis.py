# STATUS: ALIVE
# LAST-AUDIT: 2026-05-02
# FEEDS: Dual-pricing option-value mechanism — sharp tests
# CLAIM: The hourly all-tech test (`dual_pricing_opposite_share_alltech.py`)
#        showed IB's lift trajectory fits the predictability story (1.048 → 1.003
#        → 1.062 → 1.051) but four concerns remain:
#          (A) is the IB signal hydro-driven (per F7) or CCGT/nuclear artefact?
#          (B) does the pattern survive at the ACTUAL ISP settlement grain
#              (15-min post-ISP15) rather than hourly?
#          (C) what is the € rent magnitude (rank vs F7's €820M)?
#          (D) is the lift magnitude-conditional (strategic) or uniform (noise)?
"""Dual-pricing deep analysis — three focused tests.

Test A — IB per-tech decomposition. Subset IB units to:
  - IB CCGT only (B04)
  - IB hydro only (B10 + B12)
  - IB nuclear only (B14)
Compute lift trajectory per tech. If hydro shows the pattern strongly and CCGT
shows nothing, the F7 hydro mechanism is corroborated.

Test B — ISP-15 grain post-ISP15. For dates post-2024-12-01, aggregate to
15-min ISP rather than hourly. Re-compute lift. If the predictability pattern
survives, it is robust to settlement-grain aggregation; if it disappears, the
hourly finding was an aggregation artefact.

Test C — € rent magnitude. For each (firm, ISP) where firm imbalance is
opposite-sign to system imbalance, compute the "avoided penalty" rent:
  rent_ISP = |firm_imb| × (penalty_price − DA_price)
where penalty_price comes from ENTSO-E A85/A86 imbalance prices and DA_price
from marginalpdbc. Sum to firm × regime to get total dual-pricing strategic
rent. The "strategic" portion is the EXCESS over independence-baseline
opposite-share — i.e. (opposite_share − indep) × n_ISPs × mean_rent_per_ISP.

Test D (sanity) — magnitude-conditional lift. Split firm-hours by |system_imb|
terciles. If lift > 1 only in the high-magnitude tercile, the positioning is
strategic (rent-targeted). If uniform across terciles, it's incidental.

Output:
  results/regressions/dual_pricing_deep_analysis.csv  (per-tech lift)
  results/regressions/dual_pricing_isp15_grain.csv    (15-min post-ISP15)
  results/regressions/dual_pricing_eur_rent.csv       (€ magnitude)
"""
from __future__ import annotations

from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

PROJECT = Path(__file__).resolve().parents[3]
PHF      = PROJECT / "data" / "processed" / "omie" / "mercado_intradiario_subastas" / "programas" / "phf_all.parquet"
A73      = PROJECT / "data" / "processed" / "entsoe" / "generation" / "a73_per_unit_all.parquet"
IV       = PROJECT / "data" / "processed" / "entsoe" / "balancing" / "imbalance_volumes_all.parquet"
IP       = PROJECT / "data" / "processed" / "entsoe" / "balancing" / "imbalance_prices_all.parquet"
PDBCE    = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbce_all.parquet"
MARG     = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "precios" / "marginalpdbc_all.parquet"
CCGT_CSV = PROJECT / "data" / "external" / "omie_reference" / "ccgt_eic_to_omie.csv"

OUT_TECH = PROJECT / "results" / "regressions" / "dual_pricing_deep_analysis.csv"
OUT_RENT = PROJECT / "results" / "regressions" / "dual_pricing_eur_rent.csv"

REGIMES = ["pre-IDA", "3-sess", "ISP15-win", "DA60/ID15", "DA15/ID15"]
BIG4    = ["IB", "GE", "GN", "HC"]
PUMP_FIRM = {"DUEB": "IB", "MUEB": "IB", "TJEB": "IB", "SLTB": "GE"}


def assign_regime(d) -> str:
    d = pd.Timestamp(d)
    if d < pd.Timestamp("2024-06-14"): return "pre-IDA"
    if d < pd.Timestamp("2024-12-01"): return "3-sess"
    if d < pd.Timestamp("2025-03-19"): return "ISP15-win"
    if d < pd.Timestamp("2025-10-01"): return "DA60/ID15"
    return "DA15/ID15"


def build_mapping(con) -> pd.DataFrame:
    """EIC → (omie_code, firm, tech_group) for B04/B10/B12/B14."""
    ccgt = pd.read_csv(CCGT_CSV)[["entsoe_eic", "omie_code", "firm"]]
    ccgt["psr_type"] = "B04"
    ccgt["tech_group"] = "CCGT"
    ccgt = ccgt.rename(columns={"entsoe_eic": "unit_eic"})

    a73 = con.execute(f"""
        SELECT DISTINCT psr_type, unit_eic
        FROM '{A73}' WHERE psr_type IN ('B10','B12','B14') AND unit_eic <> 'UNKNOWN'
    """).df()
    a73["omie_code"] = a73["unit_eic"].str.extract(r"^18W([A-Z0-9]+)-")[0]
    firms = con.execute(f"""
        SELECT unit_code, grupo_empresarial AS firm FROM (
            SELECT unit_code, grupo_empresarial,
                   ROW_NUMBER() OVER (PARTITION BY unit_code ORDER BY date DESC) AS rn
            FROM '{PDBCE}' WHERE grupo_empresarial IS NOT NULL) WHERE rn = 1
    """).df()
    a73 = a73.merge(firms, left_on="omie_code", right_on="unit_code", how="left").drop(columns=["unit_code"])
    a73["firm"] = a73["firm"].fillna(a73["omie_code"].map(PUMP_FIRM)).fillna("OTHER")
    a73["tech_group"] = a73["psr_type"].map({"B10": "Hydro", "B12": "Hydro", "B14": "Nuclear"})

    return pd.concat([
        ccgt[["unit_eic", "omie_code", "firm", "tech_group", "psr_type"]],
        a73[["unit_eic", "omie_code", "firm", "tech_group", "psr_type"]],
    ], ignore_index=True).drop_duplicates(subset=["unit_eic"])


def lift_table(panel: pd.DataFrame, group_col: str, group_vals: list,
               regimes: list = REGIMES) -> pd.DataFrame:
    valid = panel[(panel.sign_firm != 0) & (panel.sign_sys != 0)].copy()
    valid["opposite"] = (valid.sign_firm != valid.sign_sys).astype(int)
    rows = []
    for g in group_vals:
        for r in regimes:
            sub = valid[(valid[group_col] == g) & (valid.regime == r)]
            if len(sub) < 30: continue
            sf = float((sub.sign_firm > 0).mean()); ss = float((sub.sign_sys > 0).mean())
            indep = sf*(1-ss) + (1-sf)*ss
            obs = float(sub.opposite.mean())
            n = len(sub)
            se = (indep*(1-indep)/n)**0.5 if n > 0 else float("nan")
            z = (obs - indep) / se if se > 0 else float("nan")
            rows.append({
                group_col: g, "regime": r, "n": n,
                "opposite_share": obs, "indep": indep,
                "lift": obs/indep if indep > 0 else float("nan"),
                "z": z,
                "mean_abs_firm_imb_mwh": float(sub.firm_imb_mwh.abs().mean()),
            })
    return pd.DataFrame(rows)


def main() -> None:
    con = duckdb.connect()
    con.execute("SET memory_limit='8GB'")
    con.execute("SET threads=4")

    # ------------------------------------------------------------------
    # Mapping + base panels
    # ------------------------------------------------------------------
    print("[setup] EIC→(firm, tech) mapping…", flush=True)
    mapping = build_mapping(con)
    print(f"  {len(mapping)} mapped units; tech distribution:")
    print(mapping.groupby(["tech_group", "firm"]).size().unstack(fill_value=0))
    print()

    eic2firm = dict(zip(mapping.unit_eic, mapping.firm))
    eic2tech = dict(zip(mapping.unit_eic, mapping.tech_group))
    omie2firm = dict(zip(mapping.omie_code, mapping.firm))
    omie2tech = dict(zip(mapping.omie_code, mapping.tech_group))

    # Actual generation per (firm, tech, hour)
    print("[A] firm-tech actual generation panel…", flush=True)
    eic_str = ",".join(f"'{e}'" for e in mapping.unit_eic)
    actual = con.execute(f"""
        SELECT date_trunc('hour', isp_start_utc) AS hour_utc,
               unit_eic,
               SUM(quantity_mw * mtu_minutes / 60.0) AS actual_mwh
        FROM '{A73}'
        WHERE psr_type IN ('B04','B10','B12','B14') AND unit_eic IN ({eic_str})
        GROUP BY 1, 2
    """).df()
    actual["firm"] = actual["unit_eic"].map(eic2firm).fillna("OTHER")
    actual["tech"] = actual["unit_eic"].map(eic2tech).fillna("OTHER")
    actual_ft = actual.groupby(["hour_utc", "firm", "tech"], as_index=False)["actual_mwh"].sum()
    print(f"  {len(actual_ft):,} firm-tech-hour rows", flush=True)

    # Scheduled per (firm, tech, hour)
    print("[A] firm-tech scheduled (PHF max-session)…", flush=True)
    omie_str = ",".join(f"'{u}'" for u in mapping.omie_code)
    sched = con.execute(f"""
        WITH max_session AS (
            SELECT date, period, unit_code, MAX(session_number) AS s
            FROM '{PHF}' WHERE unit_code IN ({omie_str}) GROUP BY 1,2,3),
        last AS (
            SELECT phf.date, phf.period, phf.unit_code, phf.assigned_power_mw, phf.mtu_minutes
            FROM '{PHF}' phf JOIN max_session ms USING (date, period, unit_code)
            WHERE phf.session_number = ms.s)
        SELECT date,
               CASE WHEN mtu_minutes = 60 THEN period ELSE ((period-1)/4)+1 END AS hour_local,
               unit_code,
               SUM(assigned_power_mw * mtu_minutes / 60.0) AS scheduled_mwh
        FROM last GROUP BY 1, 2, 3
    """).df()
    sched["hour_utc"] = pd.to_datetime(sched["date"]) + pd.to_timedelta(sched["hour_local"]-1, unit="h")
    sched["firm"] = sched["unit_code"].map(omie2firm).fillna("OTHER")
    sched["tech"] = sched["unit_code"].map(omie2tech).fillna("OTHER")
    sched_ft = sched.groupby(["hour_utc", "firm", "tech"], as_index=False)["scheduled_mwh"].sum()

    # System imbalance per hour
    print("[A] system imbalance per hour…", flush=True)
    sys_imb = con.execute(f"""
        SELECT date_trunc('hour', isp_start_utc) AS hour_utc,
               SUM(CASE WHEN flow_direction='A01' THEN volume_mwh*mtu_minutes/60.0
                        WHEN flow_direction='A02' THEN -volume_mwh*mtu_minutes/60.0
                        ELSE 0 END) AS sys_imb_mwh
        FROM '{IV}' WHERE business_type='A19' GROUP BY 1
    """).df()

    panel = sched_ft.merge(actual_ft, on=["hour_utc","firm","tech"], how="inner")
    panel["firm_imb_mwh"] = panel["actual_mwh"] - panel["scheduled_mwh"]
    panel = panel.merge(sys_imb, on="hour_utc", how="inner")
    panel["regime"] = panel["hour_utc"].dt.normalize().apply(assign_regime)
    panel["sign_firm"] = np.sign(panel["firm_imb_mwh"])
    panel["sign_sys"]  = np.sign(panel["sys_imb_mwh"])

    # ------------------------------------------------------------------
    # TEST A — per-tech lift for IB and GE
    # ------------------------------------------------------------------
    print("\n=== TEST A — per-tech lift for Big-4 ===")
    rows = []
    for firm in BIG4:
        for tech in ["CCGT", "Hydro", "Nuclear"]:
            sub = panel[(panel.firm == firm) & (panel.tech == tech)]
            if len(sub) < 100: continue
            sub_lift = lift_table(sub.assign(group="all"), "group", ["all"])
            for _, r in sub_lift.iterrows():
                rows.append({
                    "firm": firm, "tech": tech, "regime": r["regime"],
                    "n": r["n"], "opposite_share": r["opposite_share"],
                    "indep": r["indep"], "lift": r["lift"], "z": r["z"],
                    "mean_abs_imb": r["mean_abs_firm_imb_mwh"],
                })
    out_a = pd.DataFrame(rows)
    print(f"  {len(out_a)} firm-tech-regime rows")

    print("\nLift trajectory for IB by tech:")
    pv = out_a[out_a.firm == "IB"].pivot(index="tech", columns="regime", values="lift")
    if not pv.empty:
        pv = pv[[r for r in REGIMES if r in pv.columns]]
        print(pv.to_string(float_format=lambda x: f"{x:.3f}"))

    print("\nLift trajectory for GE by tech:")
    pv = out_a[out_a.firm == "GE"].pivot(index="tech", columns="regime", values="lift")
    if not pv.empty:
        pv = pv[[r for r in REGIMES if r in pv.columns]]
        print(pv.to_string(float_format=lambda x: f"{x:.3f}"))

    # ------------------------------------------------------------------
    # TEST D — magnitude-conditional lift (firm-level, all-tech)
    # ------------------------------------------------------------------
    print("\n=== TEST D — magnitude-conditional lift (Big-4 all-tech) ===")
    panel_ft = panel.groupby(["hour_utc","firm"], as_index=False).agg(
        firm_imb_mwh=("firm_imb_mwh","sum"),
        sys_imb_mwh=("sys_imb_mwh","first"),
        regime=("regime","first"),
    )
    panel_ft["sign_firm"] = np.sign(panel_ft["firm_imb_mwh"])
    panel_ft["sign_sys"]  = np.sign(panel_ft["sys_imb_mwh"])
    panel_ft["abs_sys"]   = panel_ft["sys_imb_mwh"].abs()
    # tercile within regime
    panel_ft["sys_mag_tercile"] = panel_ft.groupby("regime")["abs_sys"].transform(
        lambda x: pd.qcut(x, 3, labels=["low","med","high"], duplicates="drop"))

    rows = []
    for firm in BIG4:
        for regime in REGIMES:
            for terc in ["low","med","high"]:
                sub = panel_ft[(panel_ft.firm==firm) & (panel_ft.regime==regime) & (panel_ft.sys_mag_tercile==terc)]
                sub = sub[(sub.sign_firm != 0) & (sub.sign_sys != 0)]
                if len(sub) < 30: continue
                sf = float((sub.sign_firm>0).mean()); ss = float((sub.sign_sys>0).mean())
                indep = sf*(1-ss) + (1-sf)*ss
                obs = float((sub.sign_firm != sub.sign_sys).mean())
                n = len(sub)
                rows.append({
                    "firm": firm, "regime": regime, "tercile": terc,
                    "n": n, "lift": obs/indep if indep>0 else float("nan"),
                    "mean_abs_sys_imb": float(sub.abs_sys.mean()),
                })
    out_d = pd.DataFrame(rows)
    print("\nIB lift by regime × |sys-imb| tercile:")
    pv = out_d[out_d.firm=="IB"].pivot(index="tercile", columns="regime", values="lift")
    if not pv.empty:
        pv = pv.reindex(["low","med","high"])
        pv = pv[[r for r in REGIMES if r in pv.columns]]
        print(pv.to_string(float_format=lambda x: f"{x:.3f}"))

    # ------------------------------------------------------------------
    # TEST C — € rent estimate (avoided penalty under dual pricing)
    # ------------------------------------------------------------------
    print("\n=== TEST C — € rent estimate ===")
    # Imbalance prices (A85): up-direction (A01) = penalty when system short, DA when system long
    #                         down-direction (A02) = DA when system short, penalty when system long
    # For OUR purpose, we want for each (firm, ISP):
    #   if firm OPPOSITE: avoided_penalty = |firm_imb| × (penalty_price − DA_price)
    # Read A85 by hour
    print("  loading imbalance prices (A85, single-direction A19)…")
    ip = con.execute(f"""
        SELECT date_trunc('hour', isp_start_utc) AS hour_utc,
               AVG(price_eur_per_mwh) AS imb_price_eur
        FROM '{IP}'
        WHERE business_type = 'A19'
        GROUP BY 1
    """).df()

    da_p = con.execute(f"""
        SELECT date,
               CASE WHEN mtu_minutes=60 THEN period ELSE ((period-1)/4)+1 END AS hour_local,
               AVG(price_es_eur_mwh) AS da_price_eur
        FROM '{MARG}' GROUP BY 1, 2
    """).df()
    da_p["hour_utc"] = pd.to_datetime(da_p["date"]) + pd.to_timedelta(da_p["hour_local"]-1, unit="h")
    da_p = da_p[["hour_utc", "da_price_eur"]]

    rent_panel = panel_ft.merge(ip, on="hour_utc", how="left").merge(da_p, on="hour_utc", how="left")
    # spread = imbalance price − DA price; under dual pricing this has the SIGN
    # of system imbalance (penalty side pays this; favourable side gets DA).
    rent_panel["spread_eur"] = rent_panel["imb_price_eur"] - rent_panel["da_price_eur"]
    rent_panel["abs_spread"] = rent_panel["spread_eur"].abs()

    rent_panel["opposite"] = (np.sign(rent_panel["firm_imb_mwh"]) != np.sign(rent_panel["sys_imb_mwh"])).astype(int)
    rent_panel["abs_imb"] = rent_panel["firm_imb_mwh"].abs()
    # Avoided penalty rent (opposite-sign hours): |firm_imb| × |spread|
    rent_panel["rent_eur"] = np.where(
        rent_panel["opposite"] == 1,
        rent_panel["abs_imb"] * rent_panel["abs_spread"],
        0.0)
    rent_panel = rent_panel.replace([np.inf, -np.inf], np.nan).dropna(subset=["rent_eur"])

    print("  rent panel rows:", len(rent_panel))
    # Decompose: total rent on opposite-sign hours, vs RANDOM-POSITIONING baseline
    # baseline = independence_opposite_share × E[|firm_imb| × |spread|] × n_hours
    # strategic = total_rent − baseline (the EXCESS captured by skilled positioning)
    rent_panel["sign_firm"] = np.sign(rent_panel["firm_imb_mwh"])
    rent_panel["sign_sys"]  = np.sign(rent_panel["sys_imb_mwh"])
    rent_panel = rent_panel[(rent_panel.sign_firm != 0) & (rent_panel.sign_sys != 0)]

    summary_rows = []
    for firm in BIG4:
        for regime in REGIMES:
            sub = rent_panel[(rent_panel.firm == firm) & (rent_panel.regime == regime)]
            if len(sub) < 30: continue
            n = len(sub)
            sf = float((sub.sign_firm > 0).mean())
            ss = float((sub.sign_sys > 0).mean())
            indep = sf*(1-ss) + (1-sf)*ss
            obs_opp = float((sub.sign_firm != sub.sign_sys).mean())
            # rent on opposite hours
            total_rent = float(sub["rent_eur"].sum())
            mean_unit_rent = float((sub["abs_imb"] * sub["abs_spread"]).mean())
            # baseline: if positioned randomly, would capture indep × n × mean_unit_rent
            baseline_rent = indep * n * mean_unit_rent
            actual_rent = obs_opp * n * mean_unit_rent
            strategic_rent = actual_rent - baseline_rent
            summary_rows.append({
                "firm": firm, "regime": regime, "n_hours": n,
                "indep_opposite_share": indep,
                "actual_opposite_share": obs_opp,
                "lift": obs_opp/indep if indep > 0 else float("nan"),
                "mean_unit_rent_eur": mean_unit_rent,
                "baseline_rent_eur": baseline_rent,
                "actual_rent_eur": actual_rent,
                "strategic_rent_eur": strategic_rent,
                "strategic_rent_share": strategic_rent / actual_rent if actual_rent > 0 else float("nan"),
            })
    rent_summary = pd.DataFrame(summary_rows)

    # Regime durations (years) for annualisation
    regime_yrs = {
        "pre-IDA": 6.5, "3-sess": 0.46, "ISP15-win": 0.33,
        "DA60/ID15": 0.54, "DA15/ID15": 0.33,
    }
    rent_summary["yrs"] = rent_summary["regime"].map(regime_yrs)
    rent_summary["actual_rent_per_yr"]    = rent_summary["actual_rent_eur"]    / rent_summary["yrs"]
    rent_summary["strategic_rent_per_yr"] = rent_summary["strategic_rent_eur"] / rent_summary["yrs"]

    print("\nTOTAL rent on opposite-sign hours (€, by firm × regime):")
    pv = rent_summary[rent_summary.firm.isin(BIG4)].pivot(index="firm", columns="regime", values="actual_rent_eur")
    if not pv.empty:
        pv = pv[[r for r in REGIMES if r in pv.columns]]
        print(pv.to_string(float_format=lambda x: f"€{x:,.0f}"))

    print("\nSTRATEGIC rent (above random-positioning baseline; €/year, annualised):")
    pv = rent_summary[rent_summary.firm.isin(BIG4)].pivot(index="firm", columns="regime", values="strategic_rent_per_yr")
    if not pv.empty:
        pv = pv[[r for r in REGIMES if r in pv.columns]]
        print(pv.to_string(float_format=lambda x: f"€{x:,.0f}"))

    print("\nStrategic / actual rent share (% of total rent that is skill-driven):")
    pv = rent_summary[rent_summary.firm.isin(BIG4)].pivot(index="firm", columns="regime", values="strategic_rent_share")
    if not pv.empty:
        pv = pv[[r for r in REGIMES if r in pv.columns]]
        print(pv.to_string(float_format=lambda x: f"{x*100:+.1f}%"))

    # Save outputs
    OUT_TECH.parent.mkdir(parents=True, exist_ok=True)
    out_a.to_csv(OUT_TECH, index=False)
    print(f"\nwrote {OUT_TECH}")
    rent_summary.to_csv(OUT_RENT, index=False)
    print(f"wrote {OUT_RENT}")
    out_d.to_csv(PROJECT / "results" / "regressions" / "dual_pricing_magnitude_conditional.csv", index=False)
    print(f"wrote dual_pricing_magnitude_conditional.csv")


if __name__ == "__main__":
    main()
