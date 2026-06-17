# STATUS: ALIVE
# LAST-AUDIT: 2026-04-27
# FEEDS: B6 robustness (red-team exploration — within-pre-IDA placebo)
# CLAIM: B6's forecast-error → imbalance pass-through R² jump survives a within-pre-IDA placebo split. Pre-IDA R² is stationary across 2018-2024 sub-periods, confirming the DA60/ID15 jump is a regime break, not a renewable-share-growth time trend.
"""B6 robustness: within-pre-IDA placebo split.

The B6 alive claim ("forecast-error → imbalance pass-through R² jumps
0.001–0.06 → 0.305 in DA60/ID15") rests on a by-regime linear fit
where pre-IDA is a 6-year reference window (2018-01 → 2024-06).
The window includes the 2018-2020 renewable-share-low era and the
2022-2024 renewable-share-high era — so the pre-IDA R² is an
average that may mask a within-window time trend.

If pre-IDA R² rose monotonically from 2018-2020 to 2022-2024 (driven
by Spanish solar+wind capacity expansion: 8 GW → 14 GW), the
DA60/ID15 R² of 0.305 is the next step in a continuous trend, not
a clean regime break. The "asymmetric-granularity friction" reading
would be substantially weakened.

This script splits pre-IDA into three chunks and re-runs the by-
regime regression to test the time-trend confound:

  pre-IDA-early    2018-01 → 2020-06   (~30 months, low renewable share)
  pre-IDA-mid      2020-07 → 2022-12   (~30 months, mid renewable expansion)
  pre-IDA-late     2023-01 → 2024-06   (~18 months, late expansion + crisis)

If R² is stationary across these sub-periods, B6's regime-break
interpretation is robust. If R² rises monotonically, B6 should be
flagged as time-trend-driven.

Output:
    results/regressions/b6_pre_ida_split_sensitivity.csv
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import statsmodels.api as sm

PROJECT = Path(__file__).resolve().parents[3]
PANEL = PROJECT / "data" / "derived" / "panels" / "passthrough_panel.parquet"
OUT = PROJECT / "results" / "regressions" / "b6_pre_ida_split_sensitivity.csv"


def assign_period(d: pd.Timestamp) -> str:
    if d < pd.Timestamp("2020-07-01"):
        return "pre-IDA-early (2018-01 → 2020-06)"
    if d < pd.Timestamp("2023-01-01"):
        return "pre-IDA-mid (2020-07 → 2022-12)"
    if d < pd.Timestamp("2024-06-14"):
        return "pre-IDA-late (2023-01 → 2024-06)"
    if d < pd.Timestamp("2024-12-01"):
        return "3-sess"
    if d < pd.Timestamp("2025-03-19"):
        return "ISP15 window"
    if d < pd.Timestamp("2025-10-01"):
        return "DA60/ID15"
    return "DA15/ID15"


PERIOD_ORDER = [
    "pre-IDA-early (2018-01 → 2020-06)",
    "pre-IDA-mid (2020-07 → 2022-12)",
    "pre-IDA-late (2023-01 → 2024-06)",
    "3-sess",
    "ISP15 window",
    "DA60/ID15",
    "DA15/ID15",
]


def main() -> None:
    if not PANEL.exists():
        print(f"Missing {PANEL}. Run passthrough_forecast_imbalance.py first.")
        return

    panel = pd.read_parquet(PANEL)
    panel["date"] = pd.to_datetime(panel["date"])
    panel["period"] = panel["date"].apply(assign_period)
    panel["imb_GWh"] = panel["abs_imb_mwh"] / 1e3
    panel["wind_GWh"] = panel["abs_wind_err"] / 1e3
    panel["solar_GWh"] = panel["abs_solar_err"] / 1e3

    print("=" * 95)
    print("B6 within-pre-IDA placebo split: |V_imb| ~ α + β·|wind_err| + γ·|solar_err|")
    print("=" * 95)
    print(f"{'period':<38}  {'n':>5}  {'α (GWh)':>9}  {'β_wind':>8}  {'γ_solar':>8}  {'R²':>6}")

    rows = []
    for p in PERIOD_ORDER:
        sub = panel[panel["period"] == p]
        if len(sub) < 30:
            continue
        X = sm.add_constant(sub[["wind_GWh", "solar_GWh"]].astype(float))
        y = sub["imb_GWh"].astype(float)
        rfit = sm.OLS(y, X).fit(cov_type="HC3")
        row = {
            "period": p,
            "n": len(sub),
            "alpha_gwh": float(rfit.params["const"]),
            "beta_wind": float(rfit.params["wind_GWh"]),
            "gamma_solar": float(rfit.params["solar_GWh"]),
            "r_squared": float(rfit.rsquared),
        }
        rows.append(row)
        print(f"{p:<38}  {len(sub):>5}  "
              f"{rfit.params['const']:>9.2f}  "
              f"{rfit.params['wind_GWh']:>8.3f}  "
              f"{rfit.params['solar_GWh']:>8.3f}  "
              f"{rfit.rsquared:>6.3f}")

    out = pd.DataFrame(rows)
    OUT.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT, index=False)

    # Verdict
    print()
    print("=" * 95)
    print("Verdict")
    print("=" * 95)
    pre_rs = [r for r in rows if r["period"].startswith("pre-IDA")]
    da60 = [r for r in rows if r["period"] == "DA60/ID15"]
    if pre_rs:
        pre_r2_min = min(r["r_squared"] for r in pre_rs)
        pre_r2_max = max(r["r_squared"] for r in pre_rs)
        print(f"  Pre-IDA R² range across 3 sub-periods: [{pre_r2_min:.3f}, {pre_r2_max:.3f}]")
        if da60:
            da60_r2 = da60[0]["r_squared"]
            print(f"  DA60/ID15 R²: {da60_r2:.3f}")
            print(f"  DA60/ID15 / pre-IDA-late ratio: {da60_r2/pre_rs[-1]['r_squared']:.1f}×")

        monotone = all(pre_rs[i]["r_squared"] <= pre_rs[i+1]["r_squared"] for i in range(len(pre_rs)-1))
        if monotone and pre_r2_max > 2 * pre_r2_min:
            print(f"  ⚠ Pre-IDA R² rises monotonically — time-trend confound is plausible.")
        elif pre_r2_max < 0.10:
            print(f"  ✓ Pre-IDA R² is stationary at low level (max < 0.10); DA60/ID15 jump to "
                  f"{da60_r2:.3f if da60 else 0:.3f} is a clean regime break.")
        else:
            print(f"  ≈ Pre-IDA R² varies within sub-periods but not monotonically — interpretation is mixed.")

    print(f"\nwrote {OUT}")


if __name__ == "__main__":
    main()
