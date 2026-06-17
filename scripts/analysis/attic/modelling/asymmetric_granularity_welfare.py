# STATUS: ALIVE
# LAST-AUDIT: 2026-04-25
# FEEDS: S1, S2, S3, S5; modelling-track §4 asymmetric-granularity Run 2026-04-25
# CLAIM: Welfare proxy from A87 deviation vs same-calendar pre-IDA baseline; cross-validated against A86/A85
"""Asymmetric-granularity welfare proxy (modelling-track §4).

Theory. The asymmetric-granularity friction model says reform-induced
welfare loss is concentrated in regimes where DA, IDA, and ISP have
mismatched granularities. The granularity table:

  pre-IDA      DA=60  ID=60(6sess)  ISP=60   symmetric
  3-sess       DA=60  ID=60(3sess)  ISP=60   symmetric
  ISP15 win    DA=60  ID=60(3sess)  ISP=15   ISP-asymmetric
  DA60/ID15    DA=60  ID=15         ISP=15   DA-vs-ID asymmetric (peak friction)
  DA15/ID15    DA=15  ID=15         ISP=15   re-symmetrised

Spec. For each system-level monthly outcome y in {A87 net income, A86
mean |V_imb|, A85 imbalance-price std, A84 mean activation price},
regress
    y_m = alpha_{cal-month} + sum_r beta_r * 1{m in regime r} + epsilon_m
with pre-IDA as the dropped regime baseline. beta_r is the average
monthly excess of regime r over the same-calendar pre-IDA baseline.

Welfare proxy = beta_{DA60/ID15} * months_in_DA60/ID15
              + beta_{ISP15 win} * months_in_ISP15_win

(DA60/ID15 is the strict 'peak friction' regime; ISP15 win is the
partial-asymmetry regime. Sum them for the "asymmetric window" total.)

Bootstrap 95% CI: resample pre-IDA residuals (y_m - alpha_m) with
replacement, refit, recompute the integral. 1000 reps.
"""
from __future__ import annotations

from pathlib import Path
import numpy as np
import pandas as pd
import statsmodels.api as sm

PROJECT = Path(__file__).resolve().parents[3]
A87 = PROJECT / "data" / "processed" / "entsoe" / "balancing" / "financial_balance_all.parquet"
A86 = PROJECT / "data" / "processed" / "entsoe" / "balancing" / "imbalance_volumes_all.parquet"
A85 = PROJECT / "data" / "processed" / "entsoe" / "balancing" / "imbalance_prices_all.parquet"
A84 = PROJECT / "data" / "processed" / "entsoe" / "balancing" / "activated_prices_all.parquet"

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


def load_a87_monthly() -> pd.DataFrame:
    """A87 net income (A02 direction): BRP -> TSO cash flow per month, in EUR-millions."""
    df = pd.read_parquet(A87)
    df = df[df["direction_code"] == "A02"].copy()
    df["month"] = pd.to_datetime(df["month"])
    out = df[["month", "amount_eur"]].rename(columns={"amount_eur": "y"}).copy()
    out["y"] = out["y"] / 1e6  # to EUR-millions for legibility
    return out


def load_a87_expenses_monthly() -> pd.DataFrame:
    """A87 expenses (A01 direction): TSO -> BSPs cash flow per month, EUR-millions."""
    df = pd.read_parquet(A87)
    df = df[df["direction_code"] == "A01"].copy()
    df["month"] = pd.to_datetime(df["month"])
    out = df[["month", "amount_eur"]].rename(columns={"amount_eur": "y"}).copy()
    out["y"] = out["y"] / 1e6
    return out


def load_a87_net_monthly() -> pd.DataFrame:
    """A87 NET fiscal balance = A02 net income - A01 expenses. Positive means
    TSO is collecting more from BRPs than it pays BSPs in reserves; this is the
    fiscal surplus / 'system rent' that the asymmetric-granularity model
    predicts will rise during periods of high friction."""
    a02 = load_a87_monthly().rename(columns={"y": "a02"})
    a01 = load_a87_expenses_monthly().rename(columns={"y": "a01"})
    df = a02.merge(a01, on="month", how="outer").fillna(0)
    df["y"] = df["a02"] - df["a01"]
    return df[["month", "y"]]


def load_a86_monthly() -> pd.DataFrame:
    """Mean daily |imbalance volume| in MWh, monthly. Aggregating to the
    daily level removes the MTU mismatch (ISP volumes mechanically halve
    when MTU shifts 60->15)."""
    df = pd.read_parquet(A86)
    df["isp_start_utc"] = pd.to_datetime(df["isp_start_utc"])
    df["abs_v"] = df["volume_mwh"].abs()
    df["date"] = df["isp_start_utc"].dt.normalize()
    daily = df.groupby("date", as_index=False)["abs_v"].sum()
    daily["month"] = daily["date"].values.astype("datetime64[M]").astype("datetime64[ns]")
    return (
        daily.groupby("month", as_index=False)["abs_v"]
        .mean()
        .rename(columns={"abs_v": "y"})
    )


def load_a85_monthly() -> pd.DataFrame:
    df = pd.read_parquet(A85)
    df["isp_start_utc"] = pd.to_datetime(df["isp_start_utc"])
    df = df.dropna(subset=["price_eur_per_mwh"])
    df["month"] = df["isp_start_utc"].values.astype("datetime64[M]").astype("datetime64[ns]")
    return (
        df.groupby("month", as_index=False)["price_eur_per_mwh"]
        .std()
        .rename(columns={"price_eur_per_mwh": "y"})
    )


def load_a84_monthly() -> pd.DataFrame:
    df = pd.read_parquet(A84)
    df["isp_start_utc"] = pd.to_datetime(df["isp_start_utc"])
    df = df.dropna(subset=["price_eur_per_mwh"])
    df["month"] = df["isp_start_utc"].values.astype("datetime64[M]").astype("datetime64[ns]")
    return (
        df.groupby("month", as_index=False)["price_eur_per_mwh"]
        .mean()
        .rename(columns={"price_eur_per_mwh": "y"})
    )


def fit_panel(df: pd.DataFrame) -> tuple[sm.regression.linear_model.RegressionResultsWrapper, pd.DataFrame]:
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
    return res, df


def regime_months(df: pd.DataFrame) -> dict[str, int]:
    return df.groupby("regime").size().reindex(REGIME_ORDER, fill_value=0).to_dict()


def integral(res, months: dict[str, int]) -> dict[str, float]:
    out = {}
    for r in ["3-sess", "ISP15 win", "DA60/ID15", "DA15/ID15"]:
        col = f"regime_{r}"
        if col in res.params.index:
            out[r] = float(res.params[col]) * months.get(r, 0)
        else:
            out[r] = 0.0
    out["asymmetric (ISP15 win + DA60/ID15)"] = out["ISP15 win"] + out["DA60/ID15"]
    return out


def bootstrap_ci(df: pd.DataFrame, n_reps: int = 1000, seed: int = 42):
    rng = np.random.default_rng(seed)
    df = df.copy()
    df["regime"] = df["month"].apply(assign_regime)
    df["cal_month"] = df["month"].dt.month

    pre = df[df["regime"] == "pre-IDA"].copy()
    if len(pre) == 0:
        return None
    cm_means = pre.groupby("cal_month")["y"].mean().to_dict()
    pre["resid"] = pre["y"] - pre["cal_month"].map(cm_means)
    resid = pre["resid"].values
    months_dict = regime_months(df)

    integrals = {r: [] for r in ["3-sess", "ISP15 win", "DA60/ID15", "DA15/ID15", "asymmetric (ISP15 win + DA60/ID15)"]}
    for _ in range(n_reps):
        df_b = df.copy()
        boot_resid = rng.choice(resid, size=len(df_b), replace=True)
        df_b["y"] = df_b["cal_month"].map(cm_means).fillna(np.mean(list(cm_means.values()))) + boot_resid
        try:
            res_b, _ = fit_panel(df_b)
            integ_b = integral(res_b, months_dict)
            for k, v in integ_b.items():
                integrals[k].append(v)
        except Exception:
            continue
    cis = {}
    for k, vs in integrals.items():
        if not vs:
            cis[k] = (np.nan, np.nan)
            continue
        arr = np.asarray(vs)
        cis[k] = (float(np.percentile(arr, 2.5)), float(np.percentile(arr, 97.5)))
    return cis


def run_one(name: str, df: pd.DataFrame, unit: str, do_bootstrap: bool = True) -> None:
    if df.empty:
        print(f"{name}: no data")
        return
    print()
    print("=" * 90)
    print(f"{name} — monthly panel ({unit})")
    print("=" * 90)
    res, df_with_regime = fit_panel(df)
    months = regime_months(df_with_regime)
    print(f"  regime months covered: {months}")
    print(f"  regime contrast vs pre-IDA (with calendar-month FE):")
    for r in ["3-sess", "ISP15 win", "DA60/ID15", "DA15/ID15"]:
        col = f"regime_{r}"
        if col in res.params.index:
            b = res.params[col]
            se = res.bse[col]
            p = res.pvalues[col]
            sig = "***" if p < 0.001 else ("**" if p < 0.01 else (" *" if p < 0.05 else "  "))
            print(f"    {r:<12}  {b:>+10.3f} {sig}  (se {se:.3f}, p={p:.3f})")
        else:
            print(f"    {r:<12}  (no observations)")
    integ = integral(res, months)
    print(f"  cumulative excess over each regime ({unit}-months):")
    for r in ["3-sess", "ISP15 win", "DA60/ID15", "DA15/ID15"]:
        print(f"    {r:<12}  {integ[r]:>+12.1f}")
    print(f"    {'asymmetric':<12}  {integ['asymmetric (ISP15 win + DA60/ID15)']:>+12.1f}  (ISP15 win + DA60/ID15)")
    if do_bootstrap:
        print("  bootstrap 95% CI on cumulative excess (1000 reps, resampling pre-IDA residuals):")
        cis = bootstrap_ci(df_with_regime[["month", "y"]])
        if cis is not None:
            for r in ["3-sess", "ISP15 win", "DA60/ID15", "DA15/ID15", "asymmetric (ISP15 win + DA60/ID15)"]:
                lo, hi = cis[r]
                print(f"    {r:<12}  [{lo:>+10.1f}, {hi:>+10.1f}]")


def main() -> None:
    a87 = load_a87_monthly()
    a87_exp = load_a87_expenses_monthly()
    a87_net = load_a87_net_monthly()
    a86 = load_a86_monthly()
    a85 = load_a85_monthly()
    a84 = load_a84_monthly()

    run_one("A87 net income (A02; BRP -> TSO)", a87, "EUR-millions / month")
    run_one("A87 expenses (A01; TSO -> BSPs)", a87_exp, "EUR-millions / month")
    run_one("A87 NET fiscal balance (A02 - A01)", a87_net, "EUR-millions / month")
    run_one("A86 mean |V_imb|", a86, "MWh / ISP")
    run_one("A85 imbalance-price std", a85, "EUR/MWh / month")
    run_one("A84 mean activation price", a84, "EUR/MWh / month")


if __name__ == "__main__":
    main()
