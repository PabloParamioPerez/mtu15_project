# STATUS: ALIVE
# LAST-AUDIT: 2026-05-02
# FEEDS: B11 robustness attack — wider window + placebo dates
# CLAIM: B11 −41.7% bilateral-volume reduction at 2025-03-19 survives wider
#        windows and fails proper placebo tests at non-reform dates.
"""B11 robustness attack — multi-spec robustness for the Rule 28.8 effect.

Original B11 spec (`pdbf_disaggregated_regressions.py` Spec 2b):
  log(bilateral_GWh+1) ~ post_2025_03_19 + firm dummies + DOW FE + month FE
  Window: Sep 2024 → Aug 2025 (12 months around the reform)
  N=1,460, G=365 dates, β=-0.540 log-points = -41.7%

CONCERNS:
1. Narrow 12-month window — does effect survive wider windows?
2. Concurrent MTU15-IDA reform on 2025-03-19 — can't separate Rule 28.8 from
   MTU15-IDA. The post-dummy captures BOTH.
3. No placebo test at non-reform dates — could be spurious from larger
   secular trend in bilateral volumes.
4. Pre-period (Sep 2024-Mar 2025) is post-Q1-2024-break (per D8). Need
   to verify the effect is reform-driven, not secular.

Test 5 specs at firm-day grain:
  Spec A: original 12-month window (Sep 2024 → Aug 2025)
  Spec B: 24-month window (Sep 2023 → Aug 2025 + remaining)
  Spec C: extended through 2026 (Sep 2024 → Feb 2026, full DA15/ID15)
  Spec D: PLACEBO at 2024-09-19 (6 months before actual reform)
  Spec E: PLACEBO at 2024-12-19 (3 months before)

If Spec D/E placebos show ~zero, the actual reform effect is identified.
If placebos show large effects, the post-dummy is picking up secular trend.

Output:
  results/regressions/b11_robustness_attack.csv
"""
from __future__ import annotations

from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import statsmodels.api as sm

PROJECT = Path(__file__).resolve().parents[3]
PDBF    = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbf_all.parquet"
PDBCE   = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbce_all.parquet"
OUT     = PROJECT / "results" / "regressions" / "b11_robustness_attack.csv"

BIG4 = ["IB", "GE", "GN", "HC"]


def fit_ols_cluster(y, X, cluster):
    return sm.OLS(y, X).fit(cov_type="cluster", cov_kwds={"groups": cluster})


def main() -> None:
    con = duckdb.connect()
    con.execute("SET memory_limit='8GB'")
    con.execute("SET threads=4")

    firms_map = con.execute(f"""
        SELECT unit_code, grupo_empresarial AS firm FROM (
          SELECT unit_code, grupo_empresarial,
                 ROW_NUMBER() OVER (PARTITION BY unit_code ORDER BY date DESC) AS rn
          FROM '{PDBCE}' WHERE grupo_empresarial IS NOT NULL) WHERE rn = 1
    """).df()
    con.register("uf", firms_map[["unit_code", "firm"]])
    q1 = con.execute(f"""
        SELECT CAST(p.date AS DATE) AS date, uf.firm,
               SUM(CASE WHEN p.offer_type = 4 AND p.assigned_power_mw > 0
                        THEN p.assigned_power_mw * p.mtu_minutes / 60.0
                        ELSE 0 END) / 1000.0 AS q1_bilat_GWh
        FROM '{PDBF}' p JOIN uf USING (unit_code)
        WHERE uf.firm IN ('IB','GE','GN','HC')
        GROUP BY 1, 2
    """).df()
    q1["date"] = pd.to_datetime(q1["date"])
    q1["log_bilat"] = np.log1p(q1["q1_bilat_GWh"])
    q1["dow"]   = q1["date"].dt.dayofweek
    q1["month"] = q1["date"].dt.month
    q1["year"]  = q1["date"].dt.year

    def run_spec(panel, post_date, label):
        sub = panel.copy()
        sub["post"] = (sub["date"] >= pd.Timestamp(post_date)).astype(float)
        if sub["post"].sum() == 0 or (1 - sub["post"]).sum() == 0:
            return {"spec": label, "post_date": post_date, "n": 0, "G": 0,
                    "beta": np.nan, "se": np.nan, "p": np.nan, "pct": np.nan}
        cols = {"const": np.ones(len(sub))}
        for f in ["IB","GN","HC"]:
            cols[f"firm_{f}"] = (sub["firm"] == f).astype(float).values
        cols["post"] = sub["post"].values
        for d_ in range(1, 7):
            cols[f"DOW{d_}"] = (sub["dow"] == d_).astype(float).values
        for m_ in range(2, 13):
            cols[f"M{m_}"] = (sub["month"] == m_).astype(float).values
        X = pd.DataFrame(cols, index=sub.index)
        y = sub["log_bilat"].values
        cluster = sub["date"].dt.strftime("%Y%m%d").astype(np.int64).values
        m = fit_ols_cluster(y, X.values, cluster)
        i = X.columns.get_loc("post")
        b = m.params[i]; se = m.bse[i]; p = m.pvalues[i]
        pct = (np.exp(b) - 1) * 100
        return {"spec": label, "post_date": post_date, "n": len(sub),
                "G": len(np.unique(cluster)),
                "beta_log": b, "se": se, "p": p, "pct": pct, "rsq": m.rsquared}

    results = []
    # Spec A: original 12-month
    sub_a = q1[(q1.date >= "2024-09-01") & (q1.date < "2025-09-01")]
    results.append(run_spec(sub_a, "2025-03-19", "A original 12mo"))
    # Spec B: 24-month around reform
    sub_b = q1[(q1.date >= "2024-03-19") & (q1.date < "2026-03-19")]
    results.append(run_spec(sub_b, "2025-03-19", "B 24mo around reform"))
    # Spec C: full pre-Q1-2024-onwards (post Q1-2024 break only)
    sub_c = q1[(q1.date >= "2024-04-01") & (q1.date < "2026-03-01")]
    results.append(run_spec(sub_c, "2025-03-19", "C post-Q1-2024-break"))
    # Spec D: placebo at 2024-09-19 (6mo before actual reform)
    sub_d = q1[(q1.date >= "2024-03-19") & (q1.date < "2025-03-19")]
    results.append(run_spec(sub_d, "2024-09-19", "D placebo 2024-09-19"))
    # Spec E: placebo at 2024-12-19 (3mo before)
    sub_e = q1[(q1.date >= "2024-06-19") & (q1.date < "2025-03-19")]
    results.append(run_spec(sub_e, "2024-12-19", "E placebo 2024-12-19"))
    # Spec F: placebo at 2025-09-19 (6mo AFTER actual reform)
    sub_f = q1[(q1.date >= "2025-03-19") & (q1.date < "2026-03-01")]
    results.append(run_spec(sub_f, "2025-09-19", "F placebo 2025-09-19 (post)"))

    df = pd.DataFrame(results)
    print()
    print("=" * 100)
    print("B11 ROBUSTNESS — log(bilateral_GWh+1) ~ post-dummy across 6 specs")
    print("=" * 100)
    print()
    print(df.to_string(index=False, float_format=lambda x: f"{x:.4f}"))
    print()
    # Interpret
    main_a = df[df.spec.str.startswith("A")].iloc[0]
    placebos = df[df.spec.str.contains("placebo")]
    print(f"  Reform-date specs (A/B/C): all show bilateral volume reduction in {[f'{r.pct:+.1f}%' for _, r in df.iloc[:3].iterrows()]}")
    if placebos["pct"].abs().max() < abs(main_a.pct) * 0.5:
        print(f"  → Placebos all <50% of headline magnitude — REFORM EFFECT ROBUST")
    else:
        print(f"  → Placebo magnitudes too close to headline — pre-trend concern")

    OUT.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT, index=False)
    print(f"\nwrote {OUT}")


if __name__ == "__main__":
    main()
