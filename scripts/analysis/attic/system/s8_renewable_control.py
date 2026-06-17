# STATUS: ALIVE
# LAST-AUDIT: 2026-04-27
# FEEDS: S8 mechanism robustness (red-team audit attack C1)
# CLAIM: S8's RZ-activation post-IDA elevation survives controlling for renewable-share growth (Spanish solar capacity grew 15 → 41 GW 2021-2025). The reform-coincident shift is robust to the leading uncontrolled alternative.
"""S8 mechanism sensitivity: control for renewable-share growth.

Red-team audit attack C1. The baseline S8 finding ("RZ system-security
activations roughly doubled post-IDA, persisting post-MTU15-DA") shows
the level shift but doesn't rule out alternative mechanisms. The
strongest uncontrolled alternative is **renewable-share growth** —
Spanish solar capacity (PSR B16) grew from 14.6 GW in 2021 to 41.4 GW
in 2025 (nearly tripled), wind from 27.7 to 32.5 GW. More variable
renewable supply → more residual demand variance → more redispatch
needed structurally, independent of the IDA reform.

This script regresses monthly RZ activations on:
  (a) regime dummies (the existing approach)
  (b) regime dummies + monthly Spanish wind+solar actual generation
      (captures both capacity growth AND seasonal availability)
  (c) regime dummies + wind+solar generation + month-of-year FE

If regime coefficients survive substantially after adding renewable
controls, the reform-coincident shift is robust to the leading
alternative. If they collapse, the renewable-growth story explains
S8 mostly.

Output:
    results/regressions/s8_renewable_control.csv
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import statsmodels.api as sm

PROJECT = Path(__file__).resolve().parents[3]
RP48 = PROJECT / "data" / "processed" / "esios" / "restricciones" / "totalrp48preccierre_all.parquet"
ACTUAL = PROJECT / "data" / "processed" / "entsoe" / "generation" / "wind_solar_actual_all.parquet"
OUT = PROJECT / "results" / "regressions" / "s8_renewable_control.csv"

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


def build_panel() -> pd.DataFrame:
    # RZ activations (TipoRedespacho 61) per month, total GWh
    print("[1/3] Loading RZ activations (TipoRedespacho 61)...")
    rp = pd.read_parquet(RP48, columns=["date", "tipo_redespacho", "qty_up_mwh", "qty_down_mwh"])
    rp["date"] = pd.to_datetime(rp["date"], errors="coerce")
    rp = rp.dropna(subset=["date"])
    rp = rp[rp["tipo_redespacho"] == "61"]
    rp["qty_mwh"] = rp["qty_up_mwh"].combine_first(rp["qty_down_mwh"])
    rp["month"] = rp["date"].dt.to_period("M").dt.to_timestamp()
    monthly = rp.groupby("month", as_index=False).agg(rz_gwh=("qty_mwh", lambda x: x.sum() / 1e3))
    print(f"   monthly RZ panel: {len(monthly)} months")

    # Wind+solar actual generation per month, TWh (Spain B16 solar + B19 onshore wind + B18 offshore wind)
    print("[2/3] Loading wind+solar actual generation...")
    ws = pd.read_parquet(ACTUAL, columns=["isp_start_utc", "psr_type", "quantity_mw"])
    ws["isp_start_utc"] = pd.to_datetime(ws["isp_start_utc"])
    # Filter to renewables: B16 Solar, B19 Wind onshore, B18 Wind offshore
    ws = ws[ws["psr_type"].isin(["B16", "B19", "B18"])]
    # Convert MW per ISP to MWh: each row is an ISP, MW × duration (60 min for legacy hourly,
    # 15 min for newer). Per-row energy = quantity_mw × hours-per-isp.
    # For simplicity, use the MEDIAN ISP duration in the data.
    # Easier: aggregate to monthly mean MW and multiply by hours per month.
    ws["month"] = ws["isp_start_utc"].dt.to_period("M").dt.to_timestamp()
    # Monthly average MW per psr_type, then sum across types
    monthly_ws = ws.groupby(["month", "psr_type"], as_index=False)["quantity_mw"].mean()
    monthly_ws_total = monthly_ws.groupby("month", as_index=False)["quantity_mw"].sum()
    monthly_ws_total = monthly_ws_total.rename(columns={"quantity_mw": "renew_mw_avg"})
    # Approximate monthly TWh: avg-MW × ~720 hours/month / 1e6
    monthly_ws_total["renew_twh_approx"] = monthly_ws_total["renew_mw_avg"] * 720 / 1e6
    print(f"   monthly wind+solar panel: {len(monthly_ws_total)} months, "
          f"avg MW range {monthly_ws_total['renew_mw_avg'].min():.0f} → {monthly_ws_total['renew_mw_avg'].max():.0f}")

    # Merge
    df = monthly.merge(monthly_ws_total, on="month", how="left")
    df["regime"] = df["month"].apply(assign_regime)
    df["cal_month"] = df["month"].dt.month
    print(f"\n   joined panel: {len(df)} months, range {df['month'].min().date()} → {df['month'].max().date()}")
    return df


def fit_model(df: pd.DataFrame, name: str, controls: list[str]) -> dict:
    """Fit regime-dummy regression with given controls."""
    df = df.copy()
    df["regime_cat"] = pd.Categorical(df["regime"], categories=REGIME_ORDER, ordered=False)
    rd = pd.get_dummies(df["regime_cat"], prefix="regime", drop_first=False, dtype=float)
    if "regime_pre-IDA" in rd.columns:
        rd = rd.drop(columns="regime_pre-IDA")

    cols = [rd]
    if "cal_month" in controls:
        cm = pd.get_dummies(df["cal_month"], prefix="cm", drop_first=True, dtype=float)
        cols.append(cm)
    if "renew_mw_avg" in controls:
        cols.append(df[["renew_mw_avg"]])
    X = pd.concat(cols, axis=1).assign(const=1.0)
    y = df["rz_gwh"].astype(float)

    res = sm.OLS(y, X).fit(cov_type="HC3")

    out = {"spec": name, "n_obs": len(df), "r2": res.rsquared, "controls": ",".join(controls)}
    for r in ["3-sess", "ISP15 win", "DA60/ID15", "DA15/ID15"]:
        col = f"regime_{r}"
        if col in res.params.index:
            out[f"{r}_beta"] = float(res.params[col])
            out[f"{r}_se"] = float(res.bse[col])
            out[f"{r}_p"] = float(res.pvalues[col])
    if "renew_mw_avg" in res.params.index:
        out["renew_beta"] = float(res.params["renew_mw_avg"])
        out["renew_se"] = float(res.bse["renew_mw_avg"])
        out["renew_p"] = float(res.pvalues["renew_mw_avg"])
    return out


def main() -> None:
    df = build_panel()
    df = df.dropna(subset=["renew_mw_avg"])
    print()
    print("=" * 95)
    print("S8 mechanism sensitivity: regime dummies + renewable-share controls")
    print("=" * 95)
    print(f"  Outcome: monthly RZ-61 activations (GWh/month)")
    print(f"  Renewable control: monthly average wind+solar generation (MW)")
    print()

    # Baseline trends
    print("Pre-IDA renewable growth diagnostic:")
    pre = df[df["regime"] == "pre-IDA"].sort_values("month")
    if len(pre) > 1:
        first = pre.iloc[0]
        last = pre.iloc[-1]
        print(f"  pre-IDA renewable mean MW: {first['month'].date()} {first['renew_mw_avg']:.0f} → "
              f"{last['month'].date()} {last['renew_mw_avg']:.0f}  "
              f"({(last['renew_mw_avg']/first['renew_mw_avg']-1)*100:+.0f}%)")
    post = df[df["regime"] != "pre-IDA"].sort_values("month")
    if len(post) > 0:
        print(f"  Post-IDA mean: {post['renew_mw_avg'].mean():.0f} MW  vs  "
              f"pre-IDA mean {pre['renew_mw_avg'].mean():.0f} MW  "
              f"(+{(post['renew_mw_avg'].mean()/pre['renew_mw_avg'].mean()-1)*100:.0f}%)")

    print()
    specs = [
        ("Spec 1: regime dummies only",         []),
        ("Spec 2: + cal-month FE",               ["cal_month"]),
        ("Spec 3: + cal-month FE + renew_mw",    ["cal_month", "renew_mw_avg"]),
        ("Spec 4: regime + renew_mw (no cal-FE)", ["renew_mw_avg"]),
    ]

    rows = []
    for name, ctrl in specs:
        r = fit_model(df, name, ctrl)
        rows.append(r)
        print(f"--- {name}  (R²={r['r2']:.3f}) ---")
        for reg in ["3-sess", "ISP15 win", "DA60/ID15", "DA15/ID15"]:
            b = r.get(f"{reg}_beta")
            se = r.get(f"{reg}_se")
            p = r.get(f"{reg}_p")
            if b is not None:
                print(f"  {reg:<14}  β={b:>+8.1f} GWh/mo  SE={se:>5.1f}  p={p:.3f}")
        if "renew_beta" in r:
            print(f"  renew_mw_avg    β={r['renew_beta']:>+8.4f} (per MW of avg renewable)  "
                  f"SE={r['renew_se']:.4f}  p={r['renew_p']:.3f}")
        print()

    # Headline comparison
    print("=" * 95)
    print("HEADLINE: regime dummy DA60/ID15 across specifications (post-IDA, asymmetric window):")
    print("=" * 95)
    print(f"  {'specification':<45}{'DA60/ID15 β':>15}{'p-value':>10}{'survival':>12}")
    base_b = rows[1].get("DA60/ID15_beta")  # Spec 2 as baseline (regime + cal-FE, matches existing S8)
    for r in rows:
        b = r.get("DA60/ID15_beta")
        p = r.get("DA60/ID15_p")
        if b is None:
            continue
        survival = b / base_b * 100 if base_b else float("nan")
        print(f"  {r['spec']:<45}{b:>+14.1f}{p:>10.3f}{survival:>11.0f}%")

    print()
    print("Same for DA15/ID15 (post-MTU15-DA, n=3):")
    print(f"  {'specification':<45}{'DA15/ID15 β':>15}{'p-value':>10}{'survival':>12}")
    base_b = rows[1].get("DA15/ID15_beta")
    for r in rows:
        b = r.get("DA15/ID15_beta")
        p = r.get("DA15/ID15_p")
        if b is None:
            continue
        survival = b / base_b * 100 if base_b else float("nan")
        print(f"  {r['spec']:<45}{b:>+14.1f}{p:>10.3f}{survival:>11.0f}%")

    OUT.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(OUT, index=False)
    print(f"\nwrote {OUT}")


if __name__ == "__main__":
    main()
