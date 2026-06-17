# STATUS: ALIVE
# LAST-AUDIT: 2026-04-27
# FEEDS: S6 baseline sensitivity (red-team audit item 8)
# CLAIM: Sensitivity test of S6 BRP→TSO settlement transfer under crisis-excluded baseline. Bounds the audit-B2 attack on the pre-IDA reference period being contaminated by the 2022-23 European energy crisis.
"""S6 baseline sensitivity: exclude 2022–2023 European energy crisis from pre-IDA baseline.

Red-team audit item 8 (B2 attack). The current S6 estimate uses
2018-01 → 2024-05 as the pre-IDA baseline. That period includes the
European gas/Ukraine crisis of 2022–2023, when imbalance prices spiked
to historically anomalous levels. Calendar-month FE handles seasonality
but not regime-shifting volatility — so the bootstrap CI uses
heteroskedastic residuals that may be inappropriate for the post-IDA
period.

This script runs S6 under two baseline windows:

  (a) FULL pre-IDA            2018-01 → 2024-05 (78 months)  — current default
  (b) EXCL_CRISIS pre-IDA     2018-01 → 2021-12 + 2024-01 → 2024-05
                              (52 months) — excludes 24 months of crisis

Reports the regime contrast for each, with bootstrap CI under each
baseline. The headline number to track: how much does the +€1,094.9M
asymmetric-window cumulative excess change?
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import statsmodels.api as sm

PROJECT = Path(__file__).resolve().parents[3]
A87 = PROJECT / "data" / "processed" / "entsoe" / "balancing" / "financial_balance_all.parquet"
OUT = PROJECT / "results" / "regressions" / "s6_baseline_sensitivity.csv"

REGIME_ORDER = ["pre-IDA", "3-sess", "ISP15 win", "DA60/ID15", "DA15/ID15"]


def assign_regime(d: pd.Timestamp) -> str:
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


def load_a87_net() -> pd.DataFrame:
    """A87 NET = A02 income (BRP→TSO) − A01 expenses (TSO→BSPs), monthly, EUR-millions."""
    df = pd.read_parquet(A87)
    df["month"] = pd.to_datetime(df["month"])
    a02 = df[df["direction_code"] == "A02"][["month", "amount_eur"]].rename(columns={"amount_eur": "a02"})
    a01 = df[df["direction_code"] == "A01"][["month", "amount_eur"]].rename(columns={"amount_eur": "a01"})
    m = a02.merge(a01, on="month", how="outer").fillna(0)
    m["y"] = (m["a02"] - m["a01"]) / 1e6
    return m[["month", "y"]].sort_values("month").reset_index(drop=True)


def fit_one(df: pd.DataFrame, label: str):
    df = df.copy()
    df["regime"] = df["month"].apply(assign_regime)
    df["cal_month"] = df["month"].dt.month
    df["regime_cat"] = pd.Categorical(df["regime"], categories=REGIME_ORDER, ordered=False)
    rd = pd.get_dummies(df["regime_cat"], prefix="regime", drop_first=False, dtype=float)
    if "regime_pre-IDA" in rd.columns:
        rd = rd.drop(columns="regime_pre-IDA")
    cm = pd.get_dummies(df["cal_month"], prefix="cm", drop_first=True, dtype=float)
    X = pd.concat([rd, cm], axis=1).assign(const=1.0)
    y = df["y"].astype(float)
    res = sm.OLS(y, X).fit(cov_type="HC3")
    months = df.groupby("regime").size().reindex(REGIME_ORDER, fill_value=0).to_dict()

    print(f"\n=== {label} ===")
    pre_n = (df["regime"] == "pre-IDA").sum()
    print(f"  pre-IDA window: n={pre_n} months ({df[df['regime']=='pre-IDA']['month'].min().date()} → {df[df['regime']=='pre-IDA']['month'].max().date()})")
    print(f"  Regime contrasts vs pre-IDA (β coef in €M/mo, with HC3 SE):")
    contrasts = {}
    for r in ["3-sess", "ISP15 win", "DA60/ID15", "DA15/ID15"]:
        col = f"regime_{r}"
        if col in res.params.index:
            b, se, p = res.params[col], res.bse[col], res.pvalues[col]
            contrasts[r] = float(b)
            print(f"    {r:<14}  β={b:>+8.2f}  SE={se:>5.2f}  p={p:.3f}  (n_months={months.get(r,0)})")
    integ = {r: contrasts.get(r, 0.0) * months.get(r, 0) for r in contrasts}
    asym = integ.get("ISP15 win", 0) + integ.get("DA60/ID15", 0)
    integ["asymmetric_window"] = asym
    print(f"  Cumulative integral (β × months):")
    for k, v in integ.items():
        print(f"    {k:<22}  €{v:>+8.1f}M")
    return integ, contrasts, months


def bootstrap_ci(df: pd.DataFrame, n_reps: int = 1000, seed: int = 42):
    rng = np.random.default_rng(seed)
    df = df.copy()
    df["regime"] = df["month"].apply(assign_regime)
    df["cal_month"] = df["month"].dt.month
    pre = df[df["regime"] == "pre-IDA"].copy()
    cm_means = pre.groupby("cal_month")["y"].mean().to_dict()
    pre["resid"] = pre["y"] - pre["cal_month"].map(cm_means)
    resid = pre["resid"].values
    months = df.groupby("regime").size().reindex(REGIME_ORDER, fill_value=0).to_dict()

    asym_samples = []
    for _ in range(n_reps):
        boot_resid = rng.choice(resid, size=len(df), replace=True)
        y_b = df["cal_month"].map(cm_means).fillna(np.mean(list(cm_means.values()))) + boot_resid
        df_b = df.copy()
        df_b["y"] = y_b
        df_b["regime_cat"] = pd.Categorical(df_b["regime"], categories=REGIME_ORDER, ordered=False)
        rd = pd.get_dummies(df_b["regime_cat"], prefix="regime", drop_first=False, dtype=float)
        if "regime_pre-IDA" in rd.columns:
            rd = rd.drop(columns="regime_pre-IDA")
        cm_d = pd.get_dummies(df_b["cal_month"], prefix="cm", drop_first=True, dtype=float)
        X = pd.concat([rd, cm_d], axis=1).assign(const=1.0)
        try:
            res_b = sm.OLS(df_b["y"].astype(float), X).fit()
            i = float(res_b.params.get("regime_ISP15 win", 0)) * months["ISP15 win"]
            d = float(res_b.params.get("regime_DA60/ID15", 0)) * months["DA60/ID15"]
            asym_samples.append(i + d)
        except Exception:
            continue
    return float(np.percentile(asym_samples, 2.5)), float(np.percentile(asym_samples, 97.5))


def main() -> None:
    df = load_a87_net()
    print(f"A87 NET panel: {len(df)} months, {df['month'].min().date()} → {df['month'].max().date()}")

    # (a) FULL baseline
    df_full = df.copy()
    integ_full, _, _ = fit_one(df_full, "FULL pre-IDA baseline (default)")
    ci_full = bootstrap_ci(df_full)
    print(f"  Bootstrap 95% CI for asymmetric_window (i.i.d.): [{ci_full[0]:+.1f}, {ci_full[1]:+.1f}] €M")

    # (b) EXCL_CRISIS baseline: drop 2022 + 2023 from baseline
    df_excl = df[~df["month"].dt.year.isin([2022, 2023])].copy()
    integ_excl, _, _ = fit_one(df_excl, "EXCL_CRISIS pre-IDA baseline (drop 2022+2023)")
    ci_excl = bootstrap_ci(df_excl)
    print(f"  Bootstrap 95% CI for asymmetric_window (i.i.d.): [{ci_excl[0]:+.1f}, {ci_excl[1]:+.1f}] €M")

    # (c) ALSO: 2018-2021 only (very clean baseline)
    df_clean = df[df["month"].dt.year.isin([2018, 2019, 2020, 2021]) | (df["month"] >= pd.Timestamp("2024-06-14"))].copy()
    integ_clean, _, _ = fit_one(df_clean, "PRE-2022 pre-IDA baseline (2018–2021 only)")
    ci_clean = bootstrap_ci(df_clean)
    print(f"  Bootstrap 95% CI for asymmetric_window (i.i.d.): [{ci_clean[0]:+.1f}, {ci_clean[1]:+.1f}] €M")

    # Comparison table
    print("\n" + "=" * 90)
    print("S6 baseline-window sensitivity comparison")
    print("=" * 90)
    rows = [
        ("FULL (2018-2024-05, default)",
         integ_full["asymmetric_window"], ci_full[0], ci_full[1]),
        ("EXCL_CRISIS (drop 2022+2023)",
         integ_excl["asymmetric_window"], ci_excl[0], ci_excl[1]),
        ("PRE-2022 (2018-2021 only)",
         integ_clean["asymmetric_window"], ci_clean[0], ci_clean[1]),
    ]
    print(f"  {'baseline window':<35}{'asymm. window €M':>20}{'95% CI lower':>15}{'95% CI upper':>15}")
    for name, val, lo, hi in rows:
        print(f"  {name:<35}{val:>+20.1f}{lo:>+15.1f}{hi:>+15.1f}")

    out = pd.DataFrame([{
        "baseline_window": name,
        "asymmetric_window_excess_eur_m": val,
        "ci_lo": lo, "ci_hi": hi,
    } for name, val, lo, hi in rows])
    OUT.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT, index=False)
    print(f"\nwrote {OUT}")


if __name__ == "__main__":
    main()
