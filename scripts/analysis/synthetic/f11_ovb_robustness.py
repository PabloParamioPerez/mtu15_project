# STATUS: ALIVE
# LAST-AUDIT: 2026-04-27
# FEEDS: F11 robustness — does β(|gap|) sign flip when we add VRE/load/scarcity controls?
# CLAIM: F11 textbook-rejection survives or fails after controlling for omitted scarcity correlates.

"""F11 OVB-robustness check.

Original F11 spec: mp_IB ~ gap + |gap| + hour FE + month FE + regime FE
Result: β(|gap|) = −0.046 (p<0.0001), opposite of textbook prediction.

Concern: |gap| may be correlated with Spain-scarcity hours (low VRE,
high load) where the marginal price-setter is NOT IB but a peaker from
another firm. Omitting VRE/load biases β(|gap|) toward negative.

Build progressively richer specs:

  Spec 1: mp_IB ~ gap + |gap| + hour FE + month FE + regime FE
          (original — sparse controls)

  Spec 2: Spec 1 + VRE_mw (Spain wind+solar actual hourly)

  Spec 3: Spec 2 + VRE_mw²  (allow non-linear VRE effect)

  Spec 4: Spec 3 + p_actual (control for price level — partials out
          mechanical "high prices = high mp magnitude" relation)

  Spec 5: Spec 4 + p_actual_sq  (allow non-linear price effect)

  Spec 6: Spec 5 + IB_share (IB cleared / total Big-4 cleared) —
          residual-demand state proxy

If β(|gap|) flips from negative to positive across specs, OVB drove the
F11 negative result and the textbook prediction holds. If it stays
negative, F11 is robust.

Output: prints regression table by spec.
"""
from __future__ import annotations

from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import statsmodels.api as sm

PROJECT = Path(__file__).resolve().parents[3]
F7_ISP = PROJECT / "data" / "derived" / "results" / "synthetic_firm_per_firm_isp.csv"
PRICE_SP = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "precios" / "marginalpdbc_all.parquet"
PRICE_FR = PROJECT / "data" / "processed" / "entsoe" / "prices" / "fr_da_all.parquet"
VRE = PROJECT / "data" / "processed" / "entsoe" / "generation" / "wind_solar_actual_all.parquet"
PDBCE = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbce_all.parquet"


def main() -> None:
    print("[1/3] Build base panel (F7 hourly mp_IB + SP-FR gap + VRE + IB share)...")

    con = duckdb.connect()
    con.execute("SET memory_limit='2GB'")

    # SP DA hourly
    sp = con.sql(f"""
        SELECT date, period,
               CASE WHEN MAX(mtu_minutes) = 15 THEN CEIL(period / 4.0)::INTEGER
                    ELSE period END AS hour,
               AVG(price_es_eur_mwh) AS p_sp
        FROM '{PRICE_SP}'
        WHERE price_es_eur_mwh IS NOT NULL
          AND CAST(date AS DATE) >= DATE '2024-06-01'
        GROUP BY 1, 2
    """).df()
    sp["date"] = pd.to_datetime(sp["date"])
    sp["isp_start"] = sp["date"] + pd.to_timedelta(sp["hour"] - 1, unit="h")

    # FR DA hourly
    fr = pd.read_parquet(PRICE_FR)
    fr["isp_start"] = pd.to_datetime(fr["isp_start_utc"]).dt.tz_localize(None).dt.floor("h")
    fr_h = fr.groupby("isp_start", as_index=False)["price_eur_per_mwh"].mean().rename(columns={"price_eur_per_mwh": "p_fr"})

    # VRE Spain hourly
    print("   loading VRE (B01+B16+B18+B19)...")
    vre = pd.read_parquet(VRE, columns=["isp_start_utc", "psr_type", "quantity_mw", "mtu_minutes"])
    vre = vre[vre["psr_type"].isin(["B01", "B16", "B18", "B19"])].copy()
    vre["isp_start"] = pd.to_datetime(vre["isp_start_utc"]).dt.tz_localize(None).dt.floor("h")
    vre["mwh"] = vre["quantity_mw"] * (vre["mtu_minutes"] / 60.0)
    vre_h = vre.groupby("isp_start", as_index=False)["mwh"].sum().rename(columns={"mwh": "vre_mw"})

    # IB DA cleared per ISP from pdbce
    print("   loading IB DA cleared volume from pdbce...")
    ib_q = con.sql(f"""
        SELECT date, period,
               SUM(assigned_power_mw) / CASE WHEN MAX(mtu_minutes) = 15 THEN 4.0 ELSE 1.0 END AS q_ib
        FROM '{PDBCE}'
        WHERE grupo_empresarial = 'IB' AND offer_type = 1 AND assigned_power_mw > 0
          AND CAST(date AS DATE) >= DATE '2024-06-01'
        GROUP BY 1, 2
    """).df()
    ib_q["date"] = pd.to_datetime(ib_q["date"])

    # Big-4 cleared total per ISP
    big4_q = con.sql(f"""
        SELECT date, period,
               SUM(assigned_power_mw) / CASE WHEN MAX(mtu_minutes) = 15 THEN 4.0 ELSE 1.0 END AS q_big4
        FROM '{PDBCE}'
        WHERE grupo_empresarial IN ('GE','IB','GN','HC')
          AND offer_type = 1 AND assigned_power_mw > 0
          AND CAST(date AS DATE) >= DATE '2024-06-01'
        GROUP BY 1, 2
    """).df()
    big4_q["date"] = pd.to_datetime(big4_q["date"])

    # F7 hourly mp_IB
    iso = pd.read_csv(F7_ISP)
    iso["date"] = pd.to_datetime(iso["date"])
    iso = iso[(iso["regime"].isin(["DA60/ID15", "DA15/ID15"])) & (iso["p_actual"] > 0)].copy()
    iso["hour"] = np.where(iso["regime"] == "DA60/ID15", iso["period"], np.ceil(iso["period"] / 4.0).astype(int))
    iso["isp_start"] = iso["date"] + pd.to_timedelta(iso["hour"] - 1, unit="h")

    # Merge everything
    panel = iso.merge(sp[["isp_start", "p_sp"]], on="isp_start", how="inner") \
               .merge(fr_h, on="isp_start", how="inner") \
               .merge(vre_h, on="isp_start", how="left") \
               .merge(ib_q, on=["date", "period"], how="left") \
               .merge(big4_q, on=["date", "period"], how="left")
    panel["gap_sp_fr"] = panel["p_sp"] - panel["p_fr"]
    panel["abs_gap"] = panel["gap_sp_fr"].abs()
    panel["q_ib"] = panel["q_ib"].fillna(0)
    panel["q_big4"] = panel["q_big4"].fillna(0)
    panel["ib_share"] = np.where(panel["q_big4"] > 0, panel["q_ib"] / panel["q_big4"], 0)
    panel["hour_of_day"] = panel["isp_start"].dt.hour + 1
    panel["cal_month"] = panel["isp_start"].dt.month
    panel["vre_mw"] = panel["vre_mw"].fillna(panel["vre_mw"].mean())
    panel["vre_mw_sq"] = panel["vre_mw"] ** 2
    panel["p_actual_sq"] = panel["p_actual"] ** 2
    print(f"   final panel: {len(panel):,} ISPs, "
          f"VRE merge rate {(iso.merge(vre_h, on='isp_start').shape[0]/len(iso)*100):.1f}%")

    # FE dummies
    hod = pd.get_dummies(panel["hour_of_day"], prefix="hod", drop_first=True, dtype=float)
    cm = pd.get_dummies(panel["cal_month"], prefix="cm", drop_first=True, dtype=float)
    rg = pd.get_dummies(panel["regime"], prefix="rg", drop_first=True, dtype=float)

    print()
    print("[2/3] Specifications (HC3 SE):")
    print()

    specs = [
        ("Spec 1 (original sparse FE)",                       ["gap_sp_fr", "abs_gap"]),
        ("Spec 2 (+ VRE)",                                    ["gap_sp_fr", "abs_gap", "vre_mw"]),
        ("Spec 3 (+ VRE²)",                                   ["gap_sp_fr", "abs_gap", "vre_mw", "vre_mw_sq"]),
        ("Spec 4 (+ p_actual)",                               ["gap_sp_fr", "abs_gap", "vre_mw", "vre_mw_sq", "p_actual"]),
        ("Spec 5 (+ p_actual²)",                              ["gap_sp_fr", "abs_gap", "vre_mw", "vre_mw_sq", "p_actual", "p_actual_sq"]),
        ("Spec 6 (+ IB-share residual-demand proxy)",         ["gap_sp_fr", "abs_gap", "vre_mw", "vre_mw_sq", "p_actual", "p_actual_sq", "ib_share"]),
    ]

    y = panel["mp_IB"].astype(float)

    print(f"{'Spec':<55}  {'β(gap)':>11}  {'p':>6}  {'β(|gap|)':>11}  {'p':>6}  {'β(VRE)':>11}  {'β(p_act)':>11}  {'β(IB_sh)':>11}  {'R²':>6}")
    print("-" * 165)

    for name, regs in specs:
        X = pd.concat([panel[regs], hod, cm, rg], axis=1).astype(float)
        X = sm.add_constant(X)
        res = sm.OLS(y, X).fit(cov_type="HC3")
        b_gap = res.params.get("gap_sp_fr", float("nan"))
        p_gap = res.pvalues.get("gap_sp_fr", float("nan"))
        b_abs = res.params.get("abs_gap", float("nan"))
        p_abs = res.pvalues.get("abs_gap", float("nan"))
        b_vre = res.params.get("vre_mw", float("nan"))
        b_pa = res.params.get("p_actual", float("nan"))
        b_ib = res.params.get("ib_share", float("nan"))
        r2 = res.rsquared
        print(f"{name:<55}  {b_gap:>+11.4f}  {p_gap:>6.3f}  {b_abs:>+11.4f}  {p_abs:>6.3f}  "
              f"{b_vre:>+11.5f}  {b_pa:>+11.5f}  {b_ib:>+11.4f}  {r2:>6.3f}")

    print()
    print("[3/3] Reading:")
    print("  If β(|gap|) flips from negative to positive across specs, OVB drove the F11 negative result.")
    print("  If β(|gap|) stays negative across all specs, F11's textbook-rejection is robust to OVB.")
    print()
    print("Pairwise correlations (|gap| with potential omitted vars):")
    cors = panel[["abs_gap", "vre_mw", "p_actual", "ib_share", "q_ib", "q_big4"]].corr().iloc[0, 1:]
    print(cors.round(3).to_string())


if __name__ == "__main__":
    main()
