# STATUS: ALIVE
# LAST-AUDIT: 2026-04-27
# FEEDS: F8 mechanism — Bushnell (2003) water-value test
# CLAIM: IB hydro Q4 dispatch concentration tracks Spanish reservoir scarcity. Bushnell's water-value model predicts: low reservoirs → high shadow price of stored water → tighter dispatch concentration in high-price hours. We test the prediction with monthly Spanish reservoir filling (ENTSO-E A72) vs IB monthly Q4 share.
"""F8 Bushnell water-value model: IB Q4 hydro share ~ reservoir scarcity.

The classical Bushnell (2003) hydro-thermal model: a strategic hydro
firm with a reservoir storing water W solves an intertemporal
optimization. Each MWh released competes against:
  - releasing it now at spot p_t
  - holding it for an expected future spot price E[p_{t+k}]

The shadow price of water V_w equates to expected discounted future
spot price over the remaining release horizon. Optimal: release at
hours where p_t > V_w.

Predictions:
  - Reservoir LOW (drought, scarce water): V_w HIGH → tighter
    dispatch concentration in highest-price hours → HIGHER Q4 share
  - Reservoir HIGH (wet year, near spillage): V_w LOW → broad
    dispatch (to avoid spillage) → LOWER Q4 share

Test: Pearson ρ between monthly Spanish reservoir filling level
(MWh stored, ENTSO-E A72) and IB Q4 hydro share, 2018–2026.

If ρ < 0 (negative): Bushnell water-value mechanism confirmed.
If ρ ≈ 0: IB's Q4 share is regime-invariant in some other dimension.
If ρ > 0: contrary to Bushnell.

Output: results/regressions/f8_bushnell_water_value.csv
"""
from __future__ import annotations

from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import statsmodels.api as sm

PROJECT = Path(__file__).resolve().parents[3]
PDBCE = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbce_all.parquet"
PRICE = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "precios" / "marginalpdbc_all.parquet"
RES = PROJECT / "data" / "processed" / "entsoe" / "generation" / "reservoir_filling_es_weekly.parquet"
REF = PROJECT / "data" / "external" / "omie_reference" / "lista_unidades.csv"
OUT = PROJECT / "results" / "regressions" / "f8_bushnell_water_value.csv"


def main() -> None:
    print("[1/4] Reservoir filling (Spanish system, weekly → monthly)...")
    res = pd.read_parquet(RES)
    res["week_start"] = pd.to_datetime(res["week_start"])
    res["month"] = res["week_start"].dt.to_period("M").dt.to_timestamp()
    monthly_res = (
        res.groupby("month", as_index=False)["reservoir_twh"].mean()
    )
    print(f"   monthly reservoir panel: {len(monthly_res)} months,"
          f" mean {monthly_res.reservoir_twh.mean():.2f} TWh,"
          f" min {monthly_res.reservoir_twh.min():.2f}, max {monthly_res.reservoir_twh.max():.2f}")

    print("[2/4] IB monthly Q4 share + Fringe Q4 share (2018-2026)...")
    ref = pd.read_csv(REF, encoding="latin1")
    ref["tech_low"] = ref["technology"].fillna("").astype(str).str.lower()
    hydro_units = ref[ref["tech_low"].str.contains("hidr", regex=False)]["unit_code"].tolist()
    con = duckdb.connect()
    con.execute("SET memory_limit='2GB'")
    con.execute("SET threads=4")
    con.register("hydro_units", pd.DataFrame({"unit_code": hydro_units}))

    con.execute(f"""
        CREATE TEMP TABLE px AS
        SELECT date,
               CASE WHEN mtu_minutes = 15 THEN CEIL(period / 4.0)::INTEGER
                    ELSE period END AS hour,
               AVG(price_es_eur_mwh) AS p_da
        FROM '{PRICE}'
        WHERE price_es_eur_mwh IS NOT NULL
          AND CAST(date AS DATE) >= DATE '2018-01-01'
        GROUP BY 1, 2
    """)
    con.execute("""
        CREATE TEMP TABLE px_q AS
        SELECT date, hour, p_da,
               DATE_TRUNC('month', CAST(date AS DATE)) AS month,
               NTILE(4) OVER (PARTITION BY DATE_TRUNC('month', CAST(date AS DATE))
                              ORDER BY p_da) AS price_q_in_month
        FROM px
    """)
    con.execute(f"""
        CREATE TEMP TABLE hydro_clr AS
        SELECT p.unit_code,
               CASE WHEN p.grupo_empresarial IN ('GE','IB','GN','HC') THEN p.grupo_empresarial ELSE 'Fringe' END AS firm_group,
               p.date,
               CASE WHEN p.mtu_minutes = 15 THEN CEIL(p.period / 4.0)::INTEGER
                    ELSE p.period END AS hour,
               SUM(p.assigned_power_mw)
                 / CASE WHEN p.mtu_minutes = 15 THEN 4.0 ELSE 1.0 END AS q_mwh
        FROM '{PDBCE}' p
        JOIN hydro_units h USING (unit_code)
        WHERE p.offer_type = 1 AND p.assigned_power_mw IS NOT NULL
          AND p.assigned_power_mw > 0
          AND CAST(p.date AS DATE) >= DATE '2018-01-01'
        GROUP BY p.unit_code, firm_group, p.date, hour, p.mtu_minutes
    """)
    df = con.sql("""
        SELECT h.firm_group, q.month, q.price_q_in_month, SUM(h.q_mwh) AS q_mwh
        FROM hydro_clr h
        JOIN px_q q USING (date, hour)
        GROUP BY 1, 2, 3
    """).df()
    df["month"] = pd.to_datetime(df["month"])

    pivot = df.pivot_table(
        index=["firm_group", "month"], columns="price_q_in_month",
        values="q_mwh", aggfunc="sum",
    ).reset_index().fillna(0)
    pivot.columns.name = None
    pivot["total"] = pivot[1] + pivot[2] + pivot[3] + pivot[4]
    pivot["Q4_pct"] = pivot[4] / pivot["total"] * 100
    pivot["total_gwh"] = pivot["total"] / 1e3

    ib = pivot[pivot["firm_group"] == "IB"][["month", "Q4_pct", "total_gwh"]].rename(
        columns={"Q4_pct": "ib_q4_pct", "total_gwh": "ib_hydro_gwh"})
    fringe = pivot[pivot["firm_group"] == "Fringe"][["month", "Q4_pct"]].rename(
        columns={"Q4_pct": "fringe_q4_pct"})

    panel = ib.merge(fringe, on="month", how="inner").merge(monthly_res, on="month", how="inner")
    panel["gap_pp"] = panel["ib_q4_pct"] - panel["fringe_q4_pct"]
    panel = panel.sort_values("month")
    print(f"   joined panel: {len(panel)} months,"
          f" range {panel.month.min().date()} → {panel.month.max().date()}")

    print()
    print("[3/4] Pearson correlations (predicted: ρ < 0):")
    print(f"   IB Q4 % ~ reservoir TWh:           ρ = {panel['ib_q4_pct'].corr(panel['reservoir_twh']):>+.3f}")
    print(f"   gap (IB-Fringe) ~ reservoir TWh:   ρ = {panel['gap_pp'].corr(panel['reservoir_twh']):>+.3f}")
    print(f"   Fringe Q4 % ~ reservoir TWh:       ρ = {panel['fringe_q4_pct'].corr(panel['reservoir_twh']):>+.3f}  (placebo — non-strategic firms shouldn't track water value)")
    print(f"   IB hydro GWh ~ reservoir TWh:      ρ = {panel['ib_hydro_gwh'].corr(panel['reservoir_twh']):>+.3f}  (sanity: more reservoir → more IB hydro generation)")

    # Regression with year FE to remove secular trend
    print()
    print("[4/4] Regression: IB_Q4_share = α + β·reservoir + γ·calendar_month_FE + δ·year_FE")
    panel["year"] = panel["month"].dt.year
    panel["cal_month"] = panel["month"].dt.month
    cm = pd.get_dummies(panel["cal_month"], prefix="cm", drop_first=True, dtype=float)
    yr = pd.get_dummies(panel["year"], prefix="yr", drop_first=True, dtype=float)
    X_full = pd.concat([panel[["reservoir_twh"]], cm, yr], axis=1).astype(float).assign(const=1.0)
    y = panel["ib_q4_pct"].astype(float)
    res_full = sm.OLS(y, X_full).fit(cov_type="HC3")
    print(f"   N={len(panel)}; R²={res_full.rsquared:.3f}")
    print(f"   β(reservoir_twh): {res_full.params['reservoir_twh']:>+.3f} pp / TWh of stored water"
          f"   SE={res_full.bse['reservoir_twh']:.3f}   p={res_full.pvalues['reservoir_twh']:.3f}")
    sd_res = panel['reservoir_twh'].std()
    sd_q4 = panel['ib_q4_pct'].std()
    print(f"   1-SD shift (reservoir +{sd_res:.2f} TWh) ⇒ "
          f"{res_full.params['reservoir_twh']*sd_res:+.2f} pp Q4 share"
          f" (Q4 SD = {sd_q4:.2f} pp)")

    # Same regression for the IB-Fringe gap
    y2 = panel["gap_pp"].astype(float)
    res_gap = sm.OLS(y2, X_full).fit(cov_type="HC3")
    print(f"\n   Gap regression: β(reservoir_twh) = {res_gap.params['reservoir_twh']:+.3f} pp / TWh"
          f"  SE={res_gap.bse['reservoir_twh']:.3f}  p={res_gap.pvalues['reservoir_twh']:.3f}")

    # Quartile-decile look: extreme low vs high reservoir periods
    print()
    print("Decile cuts: IB Q4 share by reservoir-decile (D1=lowest reservoir, D10=highest)")
    panel["res_decile"] = pd.qcut(panel["reservoir_twh"], 10, labels=False) + 1
    dec = panel.groupby("res_decile").agg(
        n_months=("month", "size"),
        mean_reservoir_twh=("reservoir_twh", "mean"),
        mean_ib_q4=("ib_q4_pct", "mean"),
        mean_fringe_q4=("fringe_q4_pct", "mean"),
        mean_gap=("gap_pp", "mean"),
    ).round(2)
    print(dec.to_string())

    OUT.parent.mkdir(parents=True, exist_ok=True)
    panel.to_csv(OUT, index=False)
    print(f"\nwrote {OUT}")


if __name__ == "__main__":
    main()
