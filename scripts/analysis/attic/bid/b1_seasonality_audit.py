# STATUS: ALIVE
# LAST-AUDIT: 2026-04-27
# FEEDS: B1 seasonality audit
# CLAIM: GE bid-shading regime peaks robust to cal-month FE + same-calendar comparison.

"""B1 seasonality audit — GE IDA bid-shading regime peaks.

Original B1: GE IDA bid-shading peaks +€250 (3-sess) and +€218 (ISP15)
above pre-IDA mean €22; normalises to −€12 at MTU15-DA.

Per user 2026-04-27 standard, audit with cal-month FE + same-calendar
comparison.

Reasoning: shade = offer_price − clearing_price. Both prices are
seasonal. Difference may or may not be — depends on whether offers and
clearings shift seasonally in lockstep. Pre-IDA mean €22 averages over
all months/years; 3-sess regime is Jun-Dec 2024; ISP15-win is
Dec 2024-Mar 2025. Same-calendar test: compare each post-window to
pre-IDA in the same calendar months.

Output: results/regressions/b1_seasonality_audit.csv
"""
from __future__ import annotations

from pathlib import Path
import duckdb
import numpy as np
import pandas as pd
import statsmodels.api as sm

PROJECT = Path(__file__).resolve().parents[3]
ICAB = PROJECT / "data" / "processed" / "omie" / "mercado_intradiario_subastas" / "ofertas" / "icab_all.parquet"
IDET = PROJECT / "data" / "processed" / "omie" / "mercado_intradiario_subastas" / "ofertas" / "idet_all.parquet"
PIBCIE = PROJECT / "data" / "processed" / "omie" / "mercado_intradiario_subastas" / "programas" / "pibcie_all.parquet"
PRICE_IDA = PROJECT / "data" / "processed" / "omie" / "mercado_intradiario_subastas" / "precios" / "marginalpibc_all.parquet"

REGIME_ORDER = ["1.pre-IDA", "2.3-sess", "3.ISP15-win", "4.DA60/ID15", "5.DA15/ID15"]
REGIME_CAL = {
    "2.3-sess":     [6, 7, 8, 9, 10, 11],
    "3.ISP15-win":  [12, 1, 2, 3],
    "4.DA60/ID15":  [4, 5, 6, 7, 8, 9],
    "5.DA15/ID15":  [10, 11, 12, 1],
}


def assign_regime(d) -> str:
    if d < pd.Timestamp("2024-06-14"): return "1.pre-IDA"
    if d < pd.Timestamp("2024-12-01"): return "2.3-sess"
    if d < pd.Timestamp("2025-03-19"): return "3.ISP15-win"
    if d < pd.Timestamp("2025-10-01"): return "4.DA60/ID15"
    return "5.DA15/ID15"


def main() -> None:
    print("[1/3] Build daily quantity-weighted shade panel by firm-group...")
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    con.execute("SET threads=4")

    # Build shade panel (replicating the original logic)
    con.execute(f"""
        CREATE TEMP TABLE clr AS
        WITH hp AS (
            SELECT date, period,
                   CASE WHEN mtu_minutes = 15 THEN CEIL(period / 4.0)::INTEGER ELSE period END AS hour,
                   price_es_eur_mwh AS p,
                   session_number
            FROM '{PRICE_IDA}' WHERE price_es_eur_mwh IS NOT NULL
              AND CAST(date AS DATE) >= DATE '2018-01-01'
        )
        SELECT date, hour, session_number, AVG(p) AS clearing_p
        FROM hp GROUP BY 1, 2, 3
    """)

    # idet: offer_code → tranches with price + qty
    # icab: offer_code → unit + buy_sell
    # pibcie: cleared per (unit, session, period)
    # firm_group: from pibcie grupo_empresarial
    con.execute(f"""
        CREATE TEMP TABLE shade AS
        WITH offers AS (
            SELECT d.date, d.offer_code, d.version, d.session_number,
                   CASE WHEN d.mtu_minutes = 15 THEN CEIL(d.period / 4.0)::INTEGER ELSE d.period END AS hour,
                   d.price_eur_mwh AS offer_p,
                   d.quantity_mw AS offered_q,
                   c.unit_code, c.buy_sell
            FROM '{IDET}' d
            JOIN '{ICAB}' c ON d.date = c.date AND d.offer_code = c.offer_code AND d.version = c.version AND d.session_number = c.session_number
            WHERE c.buy_sell = 'V'
              AND d.price_eur_mwh > 0 AND d.quantity_mw > 0
              AND CAST(d.date AS DATE) >= DATE '2018-01-01'
        ),
        firmlbl AS (
            SELECT DISTINCT unit_code,
                   CASE WHEN grupo_empresarial IN ('GE','IB','GN','HC') THEN grupo_empresarial ELSE 'Fringe' END AS firm_group
            FROM '{PIBCIE}'
            WHERE assigned_power_mw IS NOT NULL
              AND CAST(date AS DATE) >= DATE '2024-01-01'
        )
        SELECT o.date, o.session_number, o.hour, o.unit_code,
               COALESCE(f.firm_group, 'Fringe') AS firm_group,
               o.offer_p, o.offered_q, c.clearing_p
        FROM offers o
        LEFT JOIN firmlbl f ON o.unit_code = f.unit_code
        JOIN clr c ON o.date = c.date AND o.hour = c.hour AND o.session_number = c.session_number
    """)

    # Quantity-weighted daily shade per firm-group
    con.execute("""
        CREATE TEMP TABLE shade_daily AS
        SELECT date, firm_group,
               SUM(offered_q * (offer_p - clearing_p)) / SUM(offered_q) AS wavg_shade,
               SUM(offered_q) AS total_q,
               AVG(clearing_p) AS mean_clearing_p
        FROM shade
        GROUP BY 1, 2
    """)
    df = con.sql("SELECT * FROM shade_daily WHERE wavg_shade BETWEEN -1000 AND 4000").df()
    df["date"] = pd.to_datetime(df["date"])
    df["regime"] = df["date"].apply(assign_regime)
    df["cal_month"] = df["date"].dt.month
    print(f"   panel: {len(df):,} firm-day rows")

    print()
    print("[2/3] GE shade by regime — raw mean vs same-calendar-month vs cal-month-FE regression:")
    ge = df[df["firm_group"] == "GE"].copy()

    print()
    print("(a) Raw mean by regime:")
    raw = ge.groupby("regime")["wavg_shade"].agg(["mean", "median", "count"]).round(1)
    raw = raw.reindex(REGIME_ORDER)
    print(raw.to_string())

    print()
    print("(b) Same-calendar-month comparison (post-reform regime mean vs pre-IDA same cal-months):")
    rows = []
    for reg, months in REGIME_CAL.items():
        post = ge[ge["regime"] == reg]
        pre = ge[(ge["regime"] == "1.pre-IDA") & ge["cal_month"].isin(months)]
        if len(post) == 0 or len(pre) == 0:
            continue
        rows.append({
            "regime": reg,
            "cal_months": str(months),
            "post_mean_shade": post["wavg_shade"].mean(),
            "pre_same_cal_mean": pre["wavg_shade"].mean(),
            "raw_pre_mean": ge[ge["regime"] == "1.pre-IDA"]["wavg_shade"].mean(),
            "delta_vs_same_cal": post["wavg_shade"].mean() - pre["wavg_shade"].mean(),
            "delta_vs_raw_pre": post["wavg_shade"].mean() - ge[ge["regime"] == "1.pre-IDA"]["wavg_shade"].mean(),
            "n_post": len(post), "n_pre_same_cal": len(pre),
        })
    same_cal = pd.DataFrame(rows)
    print(same_cal.to_string(index=False, float_format=lambda x: f"{x:+.1f}"))

    print()
    print("(c) OLS: wavg_shade ~ regime + cal_month FE (HC3 SE):")
    rd = pd.get_dummies(pd.Categorical(ge["regime"], categories=REGIME_ORDER, ordered=False),
                        prefix="rg", dtype=float).drop(columns="rg_1.pre-IDA").reset_index(drop=True)
    cm = pd.get_dummies(ge["cal_month"], prefix="cm", drop_first=True, dtype=float).reset_index(drop=True)
    X = pd.concat([rd, cm], axis=1)
    y = ge["wavg_shade"].astype(float).reset_index(drop=True)
    keep = (~X.isna().any(axis=1)) & (~y.isna()) & np.isfinite(y)
    X = X.loc[keep].reset_index(drop=True)
    y = y.loc[keep].reset_index(drop=True)
    X = sm.add_constant(X)
    res = sm.OLS(y, X).fit(cov_type="HC3")
    print(f"  β(3-sess)    = {res.params.get('rg_2.3-sess', np.nan):+.1f}  (p={res.pvalues.get('rg_2.3-sess', np.nan):.3f})")
    print(f"  β(ISP15-win) = {res.params.get('rg_3.ISP15-win', np.nan):+.1f}  (p={res.pvalues.get('rg_3.ISP15-win', np.nan):.3f})")
    print(f"  β(DA60/ID15) = {res.params.get('rg_4.DA60/ID15', np.nan):+.1f}  (p={res.pvalues.get('rg_4.DA60/ID15', np.nan):.3f})")
    print(f"  β(DA15/ID15) = {res.params.get('rg_5.DA15/ID15', np.nan):+.1f}  (p={res.pvalues.get('rg_5.DA15/ID15', np.nan):.3f})")
    print(f"  N={len(ge)}, R²={res.rsquared:.3f}")

    print()
    print("[3/3] Comparison: original B1 ledger numbers vs each spec:")
    print()
    print(f"  Original B1 ledger:  3-sess +250, ISP15-win +218, DA15 −12 (vs raw pre-IDA mean €22)")
    print(f"  Raw mean by regime:  see (a)")
    print(f"  Same-calendar:        see (b)")
    print(f"  Cal-month FE:         see (c)")

    OUT = PROJECT / "results" / "regressions" / "b1_seasonality_audit.csv"
    OUT.parent.mkdir(parents=True, exist_ok=True)
    same_cal.to_csv(OUT, index=False)
    print(f"\nwrote {OUT}")


if __name__ == "__main__":
    main()
