# STATUS: ALIVE
# LAST-AUDIT: 2026-04-30
# FEEDS: S8 same-calendar-month robustness (CLAUDE.md mandatory cross-regime test)
# CLAIM: S8's daily-level positive regime coefficients survive the
#        same-calendar-month restriction (each post-IDA regime compared
#        only to the matching pre-IDA calendar months).
"""S8 same-calendar-month robustness — minimum acceptable cross-regime test.

CLAUDE.md mandates: every cross-regime claim must show same-calendar-month
robustness. The reform regimes span very different calendar windows:

  3-sess     : Jun-Nov 2024
  ISP15-win  : Dec 2024 - Mar 2025  (winter)
  DA60/ID15  : Apr-Sep 2025          (summer / early fall)
  DA15/ID15  : Oct 2025 - Jan 2026   (fall / early winter)

The omnibus daily spec in `s8_daily_disaggregated.py` controls for cal-month
FE, but functional cal-month restriction (where pre-IDA observations are
restricted to the matching calendar months only) is a stricter test that
absorbs seasonality non-parametrically through the sample window.

For each post-IDA regime, we run a paired regression:

    rz_gwh ~ const + D[regime] + DOW + VRE  +  cluster SE by year-month

on the sub-panel restricted to:
    - pre-IDA observations in calendar months matching the regime
    - the regime's own observations (entirely within those cal-months)

If S8's daily alive coefficients survive, all four post-IDA regimes should
show positive significant β when compared against same-cal-month pre-IDA
baselines.

Output:
    results/regressions/s8_same_cal_month.csv
"""
from __future__ import annotations

from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import statsmodels.api as sm

PROJECT = Path(__file__).resolve().parents[3]
RP48    = PROJECT / "data" / "processed" / "esios" / "restricciones" / "totalrp48preccierre_all.parquet"
ACTUAL  = PROJECT / "data" / "processed" / "entsoe" / "generation" / "wind_solar_actual_all.parquet"
OUT     = PROJECT / "results" / "regressions" / "s8_same_cal_month.csv"

REGIMES = ["pre-IDA", "3-sess", "ISP15 win", "DA60/ID15", "DA15/ID15"]

# Calendar-month windows per post-IDA regime (per CLAUDE.md framing).
# Each tuple is the inclusive month range covered by that regime.
REGIME_CAL_MONTHS = {
    "3-sess":    [6, 7, 8, 9, 10, 11],          # Jun - Nov
    "ISP15 win": [12, 1, 2, 3],                  # Dec - Mar
    "DA60/ID15": [4, 5, 6, 7, 8, 9],             # Apr - Sep
    "DA15/ID15": [10, 11, 12, 1],                # Oct - Jan
}


def assign_regime(d: pd.Timestamp) -> str:
    if d < pd.Timestamp("2024-06-14"): return "pre-IDA"
    if d < pd.Timestamp("2024-12-01"): return "3-sess"
    if d < pd.Timestamp("2025-03-19"): return "ISP15 win"
    if d < pd.Timestamp("2025-10-01"): return "DA60/ID15"
    return "DA15/ID15"


def build_daily_panel() -> pd.DataFrame:
    con = duckdb.connect()
    con.execute("SET memory_limit='6GB'")
    con.execute("SET threads=4")

    print("[1/3] Building daily RZ-61 activations…", flush=True)
    rz_daily = con.execute(f"""
        SELECT  CAST(period_start_utc AS DATE) AS date,
                SUM(COALESCE(qty_up_mwh, 0) + COALESCE(qty_down_mwh, 0)) AS rz_mwh
        FROM '{RP48}'
        WHERE tipo_redespacho = '61'
        GROUP BY 1
        ORDER BY 1
    """).df()
    rz_daily["date"] = pd.to_datetime(rz_daily["date"])
    print(f"   daily RZ-61 panel: {len(rz_daily):,} days, "
          f"range {rz_daily.date.min().date()} → {rz_daily.date.max().date()}", flush=True)

    print("[2/3] Building daily wind+solar generation (B16+B18+B19)…", flush=True)
    vre_daily = con.execute(f"""
        SELECT  CAST(isp_start_utc AS DATE) AS date,
                SUM(quantity_mw * mtu_minutes / 60.0) AS vre_mwh
        FROM '{ACTUAL}'
        WHERE psr_type IN ('B16', 'B18', 'B19')
        GROUP BY 1
        ORDER BY 1
    """).df()
    vre_daily["date"] = pd.to_datetime(vre_daily["date"])
    print(f"   daily VRE panel: {len(vre_daily):,} days, "
          f"avg {vre_daily.vre_mwh.mean()/1000:.0f} GWh/day", flush=True)

    print("[3/3] Joining and assigning regime…", flush=True)
    df = rz_daily.merge(vre_daily, on="date", how="inner")
    df["regime"]    = df["date"].apply(assign_regime)
    df["year"]      = df["date"].dt.year
    df["cal_month"] = df["date"].dt.month
    df["dow"]       = df["date"].dt.dayofweek
    df = df.dropna(subset=["rz_mwh", "vre_mwh"])
    print(f"   joined panel: {len(df):,} daily observations", flush=True)
    print()
    return df


def fit_paired(sub: pd.DataFrame, regime: str) -> dict:
    """Paired regression: pre-IDA (same-cal-month) vs the post-IDA regime."""
    y = sub["rz_mwh"].values / 1000.0  # GWh
    cols = {"const": 1.0,
            f"D[{regime}]": (sub["regime"] == regime).astype(float).values,
            "vre_gwh":     (sub["vre_mwh"] / 1000.0).values}
    # day-of-week FE (drop Monday=0)
    for d_ in range(1, 7):
        cols[f"DOW[{d_}]"] = (sub["dow"] == d_).astype(float).values
    # cal-month FE within the restricted set (drop the smallest-numbered month)
    months = sorted(sub["cal_month"].unique())
    for m in months[1:]:
        cols[f"M[{m}]"] = (sub["cal_month"] == m).astype(float).values

    X = pd.DataFrame(cols, index=sub.index)
    cluster = (sub["year"] * 100 + sub["cal_month"]).values
    m = sm.OLS(y, X.values).fit(cov_type="cluster", cov_kwds={"groups": cluster})
    j = list(X.columns).index(f"D[{regime}]")
    j_vre = list(X.columns).index("vre_gwh")
    return {
        "regime":            regime,
        "n":                 int(len(sub)),
        "n_pre":             int((sub["regime"] == "pre-IDA").sum()),
        "n_post":            int((sub["regime"] == regime).sum()),
        "cal_months":        ",".join(str(x) for x in sorted(sub["cal_month"].unique())),
        "beta":              float(m.params[j]),
        "se":                float(m.bse[j]),
        "t":                 float(m.params[j] / m.bse[j]),
        "p":                 float(m.pvalues[j]),
        "vre_beta":          float(m.params[j_vre]),
        "vre_se":            float(m.bse[j_vre]),
        "vre_p":             float(m.pvalues[j_vre]),
        "r2":                float(m.rsquared),
        "n_clusters":        int(np.unique(cluster).size),
        "pre_mean_gwh":      float(y[(sub["regime"] == "pre-IDA").values].mean()),
        "post_mean_gwh":     float(y[(sub["regime"] == regime).values].mean()),
    }


def main() -> None:
    df = build_daily_panel()

    rows = []
    for regime, months in REGIME_CAL_MONTHS.items():
        sub = df[
            ((df["regime"] == "pre-IDA") & df["cal_month"].isin(months))
            |
            ((df["regime"] == regime) & df["cal_month"].isin(months))
        ].copy()
        if sub.empty or (sub["regime"] == regime).sum() < 5:
            print(f"  [SKIP] {regime}: insufficient post-regime obs", flush=True)
            continue
        r = fit_paired(sub, regime)
        rows.append(r)

    print("=" * 100)
    print("S8 SAME-CALENDAR-MONTH ROBUSTNESS")
    print("=" * 100)
    print(f"  Outcome: daily RZ-61 activations (GWh/day)")
    print(f"  Each row: pre-IDA observations restricted to the regime's calendar months,")
    print(f"            paired with the post-IDA regime's own observations.")
    print(f"  Controls: cal-month FE + DOW FE + daily VRE generation (GWh).")
    print(f"  SEs: cluster-robust by year-month.")
    print()
    fmt = "{:<14}  {:<22}  {:>7}  {:>7}  {:>9}  {:>9}  {:>10}  {:>9}  {:>10}"
    print(fmt.format("Regime", "Cal months", "n_pre", "n_post",
                     "β GWh/d", "SE", "p", "R²",
                     "post-pre"))
    print("-" * 110)
    for r in rows:
        post_minus_pre = r["post_mean_gwh"] - r["pre_mean_gwh"]
        print(fmt.format(
            r["regime"], r["cal_months"],
            f"{r['n_pre']:,}", f"{r['n_post']:,}",
            f"{r['beta']:+.2f}", f"{r['se']:.2f}",
            f"{r['p']:.4f}", f"{r['r2']:.3f}",
            f"{post_minus_pre:+.2f}",
        ))
    print()

    n_pos_sig = sum(1 for r in rows if r["beta"] > 0 and r["p"] < 0.05)
    n_total = len(rows)
    print("=" * 100)
    print("Honest reading:")
    print("=" * 100)
    print(f"  Positive AND significant (p<0.05) regime coefficients: {n_pos_sig}/{n_total}")
    print()
    if n_pos_sig == n_total:
        print("  S8 alive holds under same-calendar-month restriction:")
        print("  every post-IDA regime shows higher daily RZ-61 activations than")
        print("  same-calendar-month pre-IDA observations after controlling for VRE+DOW+cal-month.")
        print("  Wounding under the original monthly-aggregated spec was indeed an")
        print("  aggregation artefact, NOT a seasonality artefact.")
    elif n_pos_sig >= n_total - 1:
        print("  S8 alive mostly survives the same-calendar-month restriction.")
        print("  Note any regime with non-significant coefficient as a partial caveat.")
    else:
        print("  S8 alive survives only partially under the same-calendar-month test.")
        print(f"  Only {n_pos_sig}/{n_total} regimes show positive significant β.")
        print("  Consider re-wounding S8 — the daily disaggregated alive result may")
        print("  itself reflect residual seasonality not absorbed by cal-month FE alone.")
    print()

    OUT.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(OUT, index=False)
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
