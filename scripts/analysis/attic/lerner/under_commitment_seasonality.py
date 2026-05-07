# STATUS: ALIVE
# LAST-AUDIT: 2026-04-27
# FEEDS: B9 seasonality robustness — cal-month FE + same-calendar-month comparisons
# CLAIM: Test whether B9 regime patterns survive seasonal controls.

"""B9 seasonality robustness.

Reasoning: regimes span different calendar windows — DA60/ID15 is
Apr-Sep 2025 (summer/early-fall), DA15/ID15 is Oct 2025-Jan 2026
(fall/early-winter). Raw regime yields conflate behavior change with
seasonal mix.

Two complementary checks:

  (A) Same-calendar-month comparison: take only Apr-Sep observations
      from pre-IDA (2018-2023) and compare yields to DA60/ID15
      (also Apr-Sep). Take only Oct-Jan from pre-IDA and compare to
      DA15/ID15. ISP15-win is Dec-Mar; compare to pre-IDA Dec-Mar.
      This absorbs seasonality directly via window matching.

  (B) Regression with cal-month FE: run profit_per_day ~ regime FE +
      cal_month FE + firm-tech FE; compare regime coefficients.

If patterns from raw means survive both checks, the firm-tech
typology is reform-driven. If they collapse, what we attributed to
the reform is a seasonal mix artefact.

Output: results/regressions/under_commitment_seasonality.csv
"""
from __future__ import annotations

from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import statsmodels.api as sm

PROJECT = Path(__file__).resolve().parents[3]
PIBCIE = PROJECT / "data" / "processed" / "omie" / "mercado_intradiario_subastas" / "programas" / "pibcie_all.parquet"
PRICE_DA = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "precios" / "marginalpdbc_all.parquet"
PRICE_IDA = PROJECT / "data" / "processed" / "omie" / "mercado_intradiario_subastas" / "precios" / "marginalpibc_all.parquet"
REF = PROJECT / "data" / "external" / "omie_reference" / "lista_unidades.csv"

REGIME_CAL = {
    "DA60/ID15": [4, 5, 6, 7, 8, 9],     # Apr-Sep
    "ISP15-win": [12, 1, 2, 3],          # Dec-Mar
    "DA15/ID15": [10, 11, 12, 1],        # Oct-Jan
    "3-sess":     [6, 7, 8, 9, 10, 11],  # Jun-Nov
}

BIG4 = ["GE", "IB", "GN", "HC"]


def assign_regime(d) -> str:
    if d < pd.Timestamp("2024-06-14"): return "1.pre-IDA"
    if d < pd.Timestamp("2024-12-01"): return "2.3-sess"
    if d < pd.Timestamp("2025-03-19"): return "3.ISP15-win"
    if d < pd.Timestamp("2025-10-01"): return "4.DA60/ID15"
    return "5.DA15/ID15"


def tech_bucket(t) -> str:
    if pd.isna(t): return "Other"
    s = str(t)
    if "Ciclo Combinado" in s: return "CCGT"
    if "Hidr" in s and "Bombeo" not in s and "Consumo" not in s: return "Hydro"
    return "Other"


def main() -> None:
    print("[1/4] Build firm-tech-day panel with profit + ΔQ + month...")
    ref = pd.read_csv(REF, encoding="latin1")
    ref["tech"] = ref["technology"].apply(tech_bucket)
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    con.execute("SET threads=4")
    con.execute(f"""
        CREATE TEMP TABLE da AS
        WITH hp AS (
            SELECT date, CASE WHEN mtu_minutes = 15 THEN CEIL(period / 4.0)::INTEGER ELSE period END AS hour,
                   price_es_eur_mwh AS p
            FROM '{PRICE_DA}' WHERE price_es_eur_mwh IS NOT NULL
              AND CAST(date AS DATE) >= DATE '2018-01-01'
        )
        SELECT date, hour, AVG(p) AS p_da FROM hp GROUP BY 1, 2
    """)
    con.execute(f"""
        CREATE TEMP TABLE ida AS
        WITH hp AS (
            SELECT date, CASE WHEN mtu_minutes = 15 THEN CEIL(period / 4.0)::INTEGER ELSE period END AS hour,
                   price_es_eur_mwh AS p
            FROM '{PRICE_IDA}' WHERE price_es_eur_mwh IS NOT NULL
              AND CAST(date AS DATE) >= DATE '2018-01-01'
        )
        SELECT date, hour, AVG(p) AS p_ida FROM hp GROUP BY 1, 2
    """)
    con.register("ref", ref[["unit_code", "tech"]])
    con.execute(f"""
        CREATE TEMP TABLE dq AS
        WITH hf AS (
            SELECT CAST(p.date AS DATE) AS date,
                   CASE WHEN p.mtu_minutes = 15 THEN CEIL(p.period / 4.0)::INTEGER ELSE p.period END AS hour,
                   CASE WHEN p.grupo_empresarial IN ('GE','IB','GN','HC') THEN p.grupo_empresarial ELSE 'Fringe' END AS firm_group,
                   COALESCE(r.tech, 'Other') AS tech,
                   p.assigned_power_mw * p.mtu_minutes / 60.0 AS dq_mwh
            FROM '{PIBCIE}' p LEFT JOIN ref r ON p.unit_code = r.unit_code
            WHERE p.assigned_power_mw IS NOT NULL
              AND CAST(p.date AS DATE) >= DATE '2018-01-01'
        )
        SELECT date, hour, firm_group, tech, SUM(dq_mwh) AS dq_mwh
        FROM hf GROUP BY 1, 2, 3, 4
    """)
    panel = con.sql("""
        SELECT q.date, q.hour, q.firm_group, q.tech, q.dq_mwh,
               d.p_da, i.p_ida,
               q.dq_mwh * (d.p_da - i.p_ida) AS arb_profit_eur
        FROM dq q JOIN da d ON q.date = d.date AND q.hour = d.hour
                  JOIN ida i ON q.date = i.date AND q.hour = i.hour
    """).df()
    panel["date"] = pd.to_datetime(panel["date"])
    panel["regime"] = panel["date"].apply(assign_regime)
    panel["cal_month"] = panel["date"].dt.month
    daily = panel.groupby(["date", "firm_group", "tech", "regime", "cal_month"], as_index=False).agg(
        profit_eur=("arb_profit_eur", "sum"),
        dq_mwh=("dq_mwh", "sum"),
    )
    daily["abs_dq"] = daily["dq_mwh"].abs()
    print(f"   panel: {len(daily):,} firm-tech-day rows")

    print()
    print("[2/4] Same-calendar-month comparison (Big-4 main techs):")
    print()
    main_techs = ["CCGT", "Hydro"]
    sub = daily[daily["firm_group"].isin(BIG4) & daily["tech"].isin(main_techs)].copy()

    rows = []
    for fg in BIG4:
        for tech in main_techs:
            cell = sub[(sub["firm_group"] == fg) & (sub["tech"] == tech)]
            if len(cell) == 0:
                continue
            for reg, months in REGIME_CAL.items():
                regime_full = {"DA60/ID15": "4.DA60/ID15", "ISP15-win": "3.ISP15-win",
                               "DA15/ID15": "5.DA15/ID15", "3-sess": "2.3-sess"}[reg]
                # Comparator: pre-IDA, same calendar months
                pre = cell[(cell["regime"] == "1.pre-IDA") & cell["cal_month"].isin(months)]
                post = cell[cell["regime"] == regime_full]
                if pre["abs_dq"].sum() == 0 or post["abs_dq"].sum() == 0:
                    continue
                pre_yield = pre["profit_eur"].sum() / pre["abs_dq"].sum()
                post_yield = post["profit_eur"].sum() / post["abs_dq"].sum()
                rows.append({
                    "firm": fg, "tech": tech, "regime": reg,
                    "pre_same_cal_yield": pre_yield,
                    "post_yield": post_yield,
                    "delta_yield": post_yield - pre_yield,
                    "n_pre_days": len(pre),
                    "n_post_days": len(post),
                })
    same_cal = pd.DataFrame(rows)
    print("Yield (€/MWh) — post-reform vs pre-IDA SAME calendar months:")
    print(same_cal.to_string(index=False, float_format=lambda x: f"{x:+.2f}"))

    print()
    print("[3/4] Regression with cal-month FE (per firm-tech, regime + cal-month FE):")
    print()
    print(f"{'firm':<5} {'tech':<7} {'β(3-sess)':>12} {'β(ISP15)':>12} {'β(DA60)':>12} {'β(DA15)':>12} {'β with cal-FE':>15}    {'N':>6} {'R²':>6}")
    print("-" * 130)
    for fg in BIG4:
        for tech in main_techs:
            cell = sub[(sub["firm_group"] == fg) & (sub["tech"] == tech)].copy()
            if len(cell) < 100:
                continue
            cell["yield_eur_per_mwh"] = np.where(cell["abs_dq"] > 0, cell["profit_eur"] / cell["abs_dq"], 0)
            cell = cell[cell["abs_dq"] > 0]
            if len(cell) < 100:
                continue
            rd = pd.get_dummies(pd.Categorical(cell["regime"],
                                                categories=["1.pre-IDA","2.3-sess","3.ISP15-win","4.DA60/ID15","5.DA15/ID15"],
                                                ordered=False),
                                prefix="rg", dtype=float).drop(columns="rg_1.pre-IDA")
            cm = pd.get_dummies(cell["cal_month"], prefix="cm", drop_first=True, dtype=float)
            X = pd.concat([rd, cm], axis=1).reset_index(drop=True)
            y = cell["yield_eur_per_mwh"].astype(float).reset_index(drop=True)
            keep = (~X.isna().any(axis=1)) & (~y.isna()) & np.isfinite(y) & np.isfinite(X).all(axis=1)
            X = X.loc[keep].copy()
            y = y.loc[keep].copy()
            X = sm.add_constant(X)
            if len(y) < 50:
                continue
            res = sm.OLS(y, X).fit(cov_type="HC3")
            b3 = res.params.get("rg_2.3-sess", np.nan)
            bisp = res.params.get("rg_3.ISP15-win", np.nan)
            bda60 = res.params.get("rg_4.DA60/ID15", np.nan)
            bda15 = res.params.get("rg_5.DA15/ID15", np.nan)
            print(f"{fg:<5} {tech:<7} {b3:>+12.2f} {bisp:>+12.2f} {bda60:>+12.2f} {bda15:>+12.2f} {'(cal-month FE)':>15}    {len(cell):>6} {res.rsquared:>6.3f}")

    print()
    print("[4/4] Compare verdicts: raw means vs cal-month-FE regression")
    print()
    print("If raw-mean rankings flip under cal-month FE → seasonal artefact.")
    print("If raw-mean rankings hold → reform-driven.")

    OUT = PROJECT / "results" / "regressions" / "under_commitment_seasonality.csv"
    OUT.parent.mkdir(parents=True, exist_ok=True)
    same_cal.to_csv(OUT, index=False)
    print(f"\nwrote {OUT}")


if __name__ == "__main__":
    main()
