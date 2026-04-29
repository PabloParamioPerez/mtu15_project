# STATUS: ALIVE
# LAST-AUDIT: 2026-04-27
# FEEDS: F10/F11 mechanism — does competitive density at the cleared price absorb IB pivotality?
# CLAIM: When more non-IB tranches sit within ±€5 of cleared price, mp_IB is lower; mid-day pivotality concentration explained by thinner non-IB density.

"""Competitive marginal density vs IB price-setting power.

Reasoning before running:
  If many non-IB unit-tranches sit close to the cleared price, IB has
  near substitutes at the margin -> substituting IB-with-Fringe doesn't
  move price much -> mp_IB low.
  If IB is alone on the margin, mp_IB high.
  Predict: beta(non_ib_density) < 0 in regression of mp_IB on density.

  OVB direction:
    - Hour-of-day: evening has more units active overall (more density)
      AND more IB pivotality. Both correlations same sign -> omitting
      hour FE biases beta_short positive (toward zero or positive).
      Adding hour FE should make beta MORE NEGATIVE if true mechanism is
      density-driven.
    - VRE: low VRE -> more thermal active near margin AND IB more pivotal.
      Same OVB direction (positive) -> adding VRE should make beta more
      negative.

  Magnitude sanity: average mp_IB ~= 7-9 EUR/MWh. Density variation
  spans 0 (no competitors) to ~20+ tranches near cleared price. If
  beta = -0.5 EUR/MWh per extra tranche, going from 0 to 20 tranches
  shifts mp_IB by -10 EUR/MWh, plausible.

Output:
  results/regressions/marginal_density_vs_mp_ib.csv
"""
from __future__ import annotations

from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import statsmodels.api as sm

PROJECT = Path(__file__).resolve().parents[3]
PRICE = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "precios" / "marginalpdbc_all.parquet"
DET = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "ofertas" / "det_all.parquet"
CAB = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "ofertas" / "cab_all.parquet"
PDBCE = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbce_all.parquet"
F7_ISP = PROJECT / "results" / "regressions" / "synthetic_firm_per_firm_isp.csv"
VRE = PROJECT / "data" / "processed" / "entsoe" / "generation" / "wind_solar_actual_all.parquet"
OUT = PROJECT / "results" / "regressions" / "marginal_density_vs_mp_ib.csv"

BAND_EUR = 5.0


def main() -> None:
    print("[1/4] Compute non-IB marginal density per ISP, post-MTU15-IDA...")
    con = duckdb.connect()
    con.execute("SET memory_limit='3GB'")
    con.execute("SET threads=4")

    # Hourly cleared price + the (date, period) panel
    con.execute(f"""
        CREATE TEMP TABLE cleared AS
        SELECT date, period, AVG(price_es_eur_mwh) AS p_cleared
        FROM '{PRICE}'
        WHERE price_es_eur_mwh IS NOT NULL
          AND CAST(date AS DATE) >= DATE '2025-03-19'
        GROUP BY 1, 2
    """)

    # IB unit list (any unit with grupo_empresarial='IB' in 2024+)
    con.execute(f"""
        CREATE TEMP TABLE ib_units AS
        SELECT DISTINCT unit_code
        FROM '{PDBCE}'
        WHERE grupo_empresarial = 'IB'
          AND CAST(date AS DATE) >= DATE '2024-01-01'
    """)
    n_ib = con.sql("SELECT COUNT(*) FROM ib_units").fetchone()[0]
    print(f"   IB units: {n_ib}")

    # Big-4 unit list (for comparison)
    con.execute(f"""
        CREATE TEMP TABLE big4_units AS
        SELECT DISTINCT unit_code
        FROM '{PDBCE}'
        WHERE grupo_empresarial IN ('GE','IB','GN','HC')
          AND CAST(date AS DATE) >= DATE '2024-01-01'
    """)
    n_big4 = con.sql("SELECT COUNT(*) FROM big4_units").fetchone()[0]
    print(f"   Big-4 units: {n_big4}")

    # Density: count of unit-tranches within ±BAND_EUR of cleared price, by ISP
    print(f"   Computing density within ±€{BAND_EUR} of cleared price (this may take a minute)...")
    con.execute(f"""
        CREATE TEMP TABLE density AS
        WITH offer_unit AS (
            SELECT d.date, d.period, c.unit_code, d.price_eur_mwh AS price, d.quantity_mw AS qty
            FROM '{DET}' d
            JOIN '{CAB}' c
              ON d.date = c.date AND d.offer_code = c.offer_code AND d.version = c.version
            WHERE c.buy_sell = 'V'
              AND CAST(d.date AS DATE) >= DATE '2025-03-19'
              AND d.price_eur_mwh > 0
              AND d.quantity_mw > 0
        ),
        joined AS (
            SELECT o.date, o.period, o.unit_code, o.price, o.qty, c.p_cleared
            FROM offer_unit o
            JOIN cleared c USING (date, period)
        )
        SELECT date, period,
               COUNT(DISTINCT CASE WHEN unit_code NOT IN (SELECT unit_code FROM ib_units)
                                     AND ABS(price - p_cleared) <= {BAND_EUR}
                                THEN unit_code END) AS n_non_ib_marginal_units,
               COUNT(DISTINCT CASE WHEN unit_code NOT IN (SELECT unit_code FROM big4_units)
                                     AND ABS(price - p_cleared) <= {BAND_EUR}
                                THEN unit_code END) AS n_fringe_marginal_units,
               SUM(CASE WHEN unit_code NOT IN (SELECT unit_code FROM ib_units)
                          AND ABS(price - p_cleared) <= {BAND_EUR}
                        THEN qty ELSE 0 END) AS non_ib_marginal_mw,
               SUM(CASE WHEN unit_code NOT IN (SELECT unit_code FROM big4_units)
                          AND ABS(price - p_cleared) <= {BAND_EUR}
                        THEN qty ELSE 0 END) AS fringe_marginal_mw
        FROM joined
        GROUP BY 1, 2
    """)

    dens = con.sql("SELECT * FROM density").df()
    dens["date"] = pd.to_datetime(dens["date"])
    print(f"   density panel: {len(dens):,} ISPs")
    print(f"   non-IB units near margin: mean {dens.n_non_ib_marginal_units.mean():.1f}, "
          f"median {dens.n_non_ib_marginal_units.median():.0f}, "
          f"p10/p90 {dens.n_non_ib_marginal_units.quantile(0.1):.0f}/{dens.n_non_ib_marginal_units.quantile(0.9):.0f}")
    print(f"   non-IB MW near margin: mean {dens.non_ib_marginal_mw.mean():.0f}, "
          f"p10/p90 {dens.non_ib_marginal_mw.quantile(0.1):.0f}/{dens.non_ib_marginal_mw.quantile(0.9):.0f}")

    print("[2/4] Join F7 hourly mp_IB...")
    iso = pd.read_csv(F7_ISP)
    iso["date"] = pd.to_datetime(iso["date"])
    iso = iso[(iso["regime"].isin(["DA60/ID15", "DA15/ID15"])) & (iso["p_actual"] > 0)].copy()
    panel = iso.merge(dens, on=["date", "period"], how="inner")
    panel["hour_of_day"] = np.where(panel["regime"] == "DA60/ID15",
                                     panel["period"],
                                     np.ceil(panel["period"] / 4.0).astype(int))
    panel["cal_month"] = panel["date"].dt.month

    print("[3/4] Add VRE (Spanish wind+solar hourly)...")
    vre = pd.read_parquet(VRE, columns=["isp_start_utc", "psr_type", "quantity_mw", "mtu_minutes"])
    vre = vre[vre["psr_type"].isin(["B01", "B16", "B18", "B19"])].copy()
    vre["isp_start"] = pd.to_datetime(vre["isp_start_utc"]).dt.tz_localize(None)
    vre["date"] = vre["isp_start"].dt.normalize()
    vre["hour"] = vre["isp_start"].dt.hour + 1
    vre["mwh"] = vre["quantity_mw"] * (vre["mtu_minutes"] / 60.0)
    vre_h = vre.groupby(["date", "hour"], as_index=False)["mwh"].sum().rename(columns={"mwh": "vre_mw"})
    panel = panel.merge(vre_h, left_on=["date", "hour_of_day"], right_on=["date", "hour"], how="left")
    panel["vre_mw"] = panel["vre_mw"].fillna(panel["vre_mw"].mean())
    print(f"   final panel: {len(panel):,} ISPs")

    print()
    print(f"[4/4] Regressions of mp_IB on non-IB marginal density (HC3 SE):")
    print(f"      Predicted sign: NEGATIVE (more density → less IB power)")
    print()

    # Specs build progressively
    hod = pd.get_dummies(panel["hour_of_day"], prefix="hod", drop_first=True, dtype=float)
    cm = pd.get_dummies(panel["cal_month"], prefix="cm", drop_first=True, dtype=float)
    rg = pd.get_dummies(panel["regime"], prefix="rg", drop_first=True, dtype=float)

    y = panel["mp_IB"].astype(float)

    specs = [
        ("Spec 1: sparse (regime FE)",                        ["n_non_ib_marginal_units"],                        []),
        ("Spec 2: + hour FE",                                  ["n_non_ib_marginal_units"],                        ["hod"]),
        ("Spec 3: + month FE",                                 ["n_non_ib_marginal_units"],                        ["hod", "cm"]),
        ("Spec 4: + VRE",                                      ["n_non_ib_marginal_units", "vre_mw"],              ["hod", "cm"]),
        ("Spec 5: switch to MW measure",                       ["non_ib_marginal_mw", "vre_mw"],                   ["hod", "cm"]),
        ("Spec 6: Fringe-only (excl Big-4)",                   ["n_fringe_marginal_units", "vre_mw"],              ["hod", "cm"]),
    ]

    print(f"{'Spec':<48}  {'β(density)':>12}  {'p':>6}  {'β(VRE)':>10}  {'N':>6}  {'R²':>6}")
    print("-" * 95)
    for name, regs, fe_groups in specs:
        X_parts = [panel[regs].astype(float), rg]
        if "hod" in fe_groups:
            X_parts.append(hod)
        if "cm" in fe_groups:
            X_parts.append(cm)
        X = pd.concat(X_parts, axis=1)
        X = sm.add_constant(X)
        res = sm.OLS(y, X).fit(cov_type="HC3")
        b_d_name = regs[0]
        b_d = res.params.get(b_d_name, np.nan)
        p_d = res.pvalues.get(b_d_name, np.nan)
        b_vre = res.params.get("vre_mw", np.nan)
        print(f"{name:<48}  {b_d:>+12.5f}  {p_d:>6.3f}  {b_vre:>+10.5f}  {len(panel):>6,}  {res.rsquared:>6.3f}")

    OUT.parent.mkdir(parents=True, exist_ok=True)
    panel[["date", "period", "regime", "hour_of_day", "mp_IB", "p_actual",
           "n_non_ib_marginal_units", "n_fringe_marginal_units",
           "non_ib_marginal_mw", "fringe_marginal_mw", "vre_mw"]].to_csv(OUT, index=False)
    print(f"\nwrote {OUT}")


if __name__ == "__main__":
    main()
