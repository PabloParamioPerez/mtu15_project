# STATUS: ALIVE
# LAST-AUDIT: 2026-04-25
# FEEDS: (modelling-track §3 Pigouvian Run 2026-04-25)
# CLAIM: Per-segment marginal imbalance cost EUR/MWh with month-of-year + hour-of-day FE; per-regime Pigouvian-misalignment test
"""Pigouvian clean regression (modelling-track §3).

Theory. The ESIOS imbalance-settlement rule allocates total imbalance
amount across BRPs in proportion to each BRP's signed deviation,
without conditioning on the per-MWh marginal cost that BRP's segment
imposes. If different segments (free-market retailers, conv-RZ, wind,
hydro, etc.) have heterogeneous per-MWh marginal contributions to total
imbalance amount, the current allocation is non-Pigouvian: high-marginal-
cost segments do not face the marginal price; they face the rule's
average price.

Spec. Multivariate OLS per regime (post-ISP15 only, since pre-ISP15
settles at hour-resolution rather than ISP-resolution):
    |imp_eur|_t = const + sum_seg beta_seg * |MWh_seg|_t
                + month-of-year FE + hour-of-day FE + epsilon_t
HC3 SE. Compare beta_seg across segments and across regimes.

If the rule were truly Pigouvian, beta_seg would track the per-segment
shadow cost. If beta_seg is highly heterogeneous (some segments have
beta close to zero despite large volume; others have very high beta
despite small volume), the rule is misaligned.

Outputs:
- Per-(regime, segment) coefficient table.
- Comparison to A87 segment shares (using esios_a87_cross.py).
"""
from __future__ import annotations

from pathlib import Path
import duckdb
import numpy as np
import pandas as pd
import statsmodels.api as sm

PROJECT = Path(__file__).resolve().parents[3]
LIQ = PROJECT / "data" / "processed" / "esios" / "liquicomun_all.parquet"

SEGS = [
    "conv_rz", "conv_nrz", "wind", "hydro", "thermal_re",
    "cor_ret", "lib_ret", "export_u", "import_u",
]
SEG_NAMES = {
    "conv_rz": "conv (regulation zones)",
    "conv_nrz": "conv (non-reg zones)",
    "wind": "RE wind",
    "hydro": "RE hydro",
    "thermal_re": "RE thermal",
    "cor_ret": "regulated retailers (COR)",
    "lib_ret": "free retailers (LIB)",
    "export_u": "export units",
    "import_u": "import units",
}

REGIMES_POST = ["ISP15 win", "DA60/ID15", "DA15/ID15"]


def assign_regime(d):
    d = pd.Timestamp(d)
    if d < pd.Timestamp("2024-06-14"):
        return "pre-IDA"
    if d < pd.Timestamp("2024-12-01"):
        return "3-sess"
    if d < pd.Timestamp("2025-03-19"):
        return "ISP15 win"
    if d < pd.Timestamp("2025-10-01"):
        return "DA60/ID15"
    return "DA15/ID15"


def load_wide() -> pd.DataFrame:
    con = duckdb.connect()
    con.execute("SET memory_limit='6GB'")
    wide = con.sql(f"""
        SELECT date, hour, quarter,
               MAX(CASE WHEN family='impdsvqh'  THEN value END) AS imp_eur,
               MAX(CASE WHEN family='endrozrqh' THEN value END) AS conv_rz,
               MAX(CASE WHEN family='endronzqh' THEN value END) AS conv_nrz,
               MAX(CASE WHEN family='endreeoqh' THEN value END) AS wind,
               MAX(CASE WHEN family='endrehiqh' THEN value END) AS hydro,
               MAX(CASE WHEN family='endretqh'  THEN value END) AS thermal_re,
               MAX(CASE WHEN family='endcurqh'  THEN value END) AS cor_ret,
               MAX(CASE WHEN family='endlibqh'  THEN value END) AS lib_ret,
               MAX(CASE WHEN family='endexpqh'  THEN value END) AS export_u,
               MAX(CASE WHEN family='endimpqh'  THEN value END) AS import_u
        FROM '{LIQ}'
        GROUP BY 1, 2, 3
    """).df()
    wide["date"] = pd.to_datetime(wide["date"])
    wide = wide.dropna(subset=["imp_eur"]).copy()
    for s in SEGS:
        wide[s] = wide[s].fillna(0)
        wide[f"abs_{s}"] = wide[s].abs()
    wide["abs_imp"] = wide["imp_eur"].abs()
    wide["regime"] = wide["date"].apply(assign_regime)
    wide["cal_month"] = wide["date"].dt.month
    return wide


def fit_one(sub: pd.DataFrame) -> sm.regression.linear_model.RegressionResultsWrapper:
    seg_cols = [f"abs_{s}" for s in SEGS]
    X = sub[seg_cols].astype(float).copy()
    cm = pd.get_dummies(sub["cal_month"], prefix="cm", drop_first=True, dtype=float)
    hr = pd.get_dummies(sub["hour"], prefix="hr", drop_first=True, dtype=float)
    X = pd.concat([X, cm, hr], axis=1).assign(const=1.0)
    y = sub["abs_imp"].astype(float)
    return sm.OLS(y, X).fit(cov_type="HC3")


def average_volume_share(sub: pd.DataFrame) -> pd.Series:
    abs_segs = sub[[f"abs_{s}" for s in SEGS]].sum(axis=0)
    return abs_segs / abs_segs.sum()


def main() -> None:
    wide = load_wide()
    print(f"Wide panel: {len(wide):,} rows. Date range {wide['date'].min().date()} -> {wide['date'].max().date()}.")
    print(f"Regime distribution:\n{wide.groupby('regime').size().reindex(['pre-IDA','3-sess','ISP15 win','DA60/ID15','DA15/ID15'])}")

    print()
    print("=" * 110)
    print("PIGOUVIAN CLEAN REGRESSION")
    print("Spec: |imp_eur| = const + sum_seg beta_seg * |MWh_seg| + cal-month FE + hour FE; HC3 SE")
    print("Sample: post-ISP15 only (segment-level settlement requires ISP resolution).")
    print("=" * 110)

    # Per-regime multivariate
    rows = []
    for r in REGIMES_POST:
        sub = wide[wide["regime"] == r]
        if len(sub) < 200:
            print(f"\n{r}: too few obs ({len(sub)}), skip.")
            continue
        res = fit_one(sub)
        share = average_volume_share(sub)
        print(f"\n--- {r} (n={len(sub):,}, R²={res.rsquared:.3f}) ---")
        print(f"  {'segment':<26}  {'beta (€/MWh)':>14}  {'se':>8}  {'p':>7}  {'volume share':>14}")
        for s in SEGS:
            col = f"abs_{s}"
            b = res.params[col]
            se = res.bse[col]
            p = res.pvalues[col]
            sig = "***" if p < 0.001 else ("**" if p < 0.01 else (" *" if p < 0.05 else "  "))
            sh = share.get(col, np.nan)
            print(f"  {SEG_NAMES[s]:<26}  {b:>+11.2f}{sig}  {se:>8.2f}  {p:>7.3f}  {sh*100:>12.1f}%")
            rows.append({
                "regime": r, "segment": s, "n": len(sub),
                "beta": float(b), "se": float(se), "p": float(p),
                "volume_share": float(sh),
                "r2": float(res.rsquared),
            })

    tab = pd.DataFrame(rows)

    # Pigouvian misalignment summary: per-segment beta sorted; segments with high beta but low share
    # are subsidised under the rule; segments with low beta but high share are over-charged.
    print()
    print("=" * 110)
    print("Pigouvian misalignment (per regime): segments ranked by β")
    print("=" * 110)
    print("Reading: high β + low volume share = under-charged under uniform rule")
    print("         low β  + high volume share = over-charged under uniform rule")

    for r in REGIMES_POST:
        sub = tab[tab["regime"] == r].sort_values("beta", ascending=False)
        if sub.empty:
            continue
        print(f"\n--- {r} ---")
        print(f"  {'segment':<26}  {'beta (€/MWh)':>14}  {'volume share':>14}  {'misalignment':>16}")
        for _, row in sub.iterrows():
            mis = row["beta"] * row["volume_share"]  # beta-weighted-share index
            print(f"  {SEG_NAMES[row['segment']]:<26}  {row['beta']:>+11.2f}    {row['volume_share']*100:>12.1f}%  {mis:>+14.2f}")

    out = PROJECT / "results" / "regressions" / "pigouvian_clean_results.csv"
    tab.to_csv(out, index=False)
    print(f"\nwrote {out}")


if __name__ == "__main__":
    main()
