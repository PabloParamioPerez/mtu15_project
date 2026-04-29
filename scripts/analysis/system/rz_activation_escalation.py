# STATUS: WOUNDED
# LAST-AUDIT: 2026-04-27
# FEEDS: S8 (wounded — RZ activations doubled post-IDA, but renewable-control regression collapses the persistent post-MTU15-DA effect; only ISP15-window 4-month elevation survives)
# CLAIM: WOUNDED. Original framing "RZ activations roughly doubled post-IDA, persisting post-MTU15-DA — structural reform effect" retracted 2026-04-27 PM after `s8_renewable_control.py` showed renewable-share growth (+80% in pre-IDA window alone) statistically explains most of the post-IDA RZ elevation. Only the ISP15-window-specific elevation (+156 GWh/mo, p=0.022) survives the renewable control.
"""RZ activation escalation across reform regimes.

Side-finding from the S7 anchor cross-check (`s7_rz_anchor_validation.py`):
RZ system-security activations (TipoRedespacho 61 in ESIOS
totalrp48preccierre) escalated from ~270 GWh/month pre-IDA to
~415–500 GWh/month across all post-IDA regimes including
post-MTU15-DA.

The persistence post-MTU15-DA (when DA granularity matches ID/ISP)
suggests this is NOT a granularity-friction effect (those collapsed
in S6) and NOT a blackout-only effect (DA15/ID15 is post-blackout
but the elevation persists). The leading interpretation is:

  (a) Asymmetric IDA reform 2024-06: 6 → 3 sessions reduced
      operator flexibility to incrementally rebalance positions
      between DA and real-time, increasing the residual that REE
      must redispatch via RZ; OR
  (b) ISP15 reform 2024-12 broke the previous matching of imbalance-
      settlement granularity to DA dispatch, increasing the gap
      between cleared programs and feasible dispatch; OR
  (c) Both, plus continuing renewable penetration.

Whatever the mechanism, the empirical pattern is robust: roughly
2× system redispatch volume is an operational cost not internalised
by the price-setting market (DA + IDA + XBID). The redispatch is
done by REE at PT15M granularity at average closure price ~€60–90/MWh,
implying ~€25–45M/month direct redispatch cost vs the pre-IDA
baseline.

This script:
  1. Computes per-month RZ activation volume by direction (up/down).
  2. Assigns each month to a regime; tests for level shifts using
     same-calendar-month pre-IDA baseline (controls for seasonality).
  3. Reports the regime-mean elevation with bootstrap CI.

Output:
    results/regressions/rz_activation_escalation.csv
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

PROJECT = Path(__file__).resolve().parents[3]
RP48 = PROJECT / "data" / "processed" / "esios" / "restricciones" / "totalrp48preccierre_all.parquet"
OUT = PROJECT / "results" / "regressions" / "rz_activation_escalation.csv"

IDA_REFORM   = pd.Timestamp("2024-06-14")
ISP15_REFORM = pd.Timestamp("2024-12-01")
MTU15_IDA    = pd.Timestamp("2025-03-19")
MTU15_DA     = pd.Timestamp("2025-10-01")


def assign_regime(d: pd.Timestamp) -> str:
    if d < IDA_REFORM:
        return "pre-IDA"
    if d < ISP15_REFORM:
        return "3-sess"
    if d < MTU15_IDA:
        return "ISP15 win"
    if d < MTU15_DA:
        return "DA60/ID15"
    return "DA15/ID15"


def main() -> None:
    print(f"[load] {RP48}")
    df = pd.read_parquet(RP48)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"]).copy()

    # Filter to RZ system-security (code 61)
    rz = df[df["tipo_redespacho"] == "61"].copy()
    rz["qty_mwh"] = rz["qty_up_mwh"].combine_first(rz["qty_down_mwh"])
    rz = rz.dropna(subset=["qty_mwh"])
    rz["month"] = rz["date"].dt.to_period("M").dt.to_timestamp()
    rz["regime"] = rz["date"].apply(assign_regime)
    rz["cal_month"] = rz["date"].dt.month

    # Per-month total + per-direction split
    monthly = (
        rz.groupby(["month", "regime", "cal_month"])
        .agg(
            total_gwh=("qty_mwh", lambda s: s.sum() / 1e3),
            up_gwh=("qty_up_mwh", lambda s: s.sum() / 1e3),
            down_gwh=("qty_down_mwh", lambda s: s.sum() / 1e3),
            mean_price=("price_up_eur", "mean"),
        )
        .reset_index()
    )

    # Same-calendar-month pre-IDA baseline
    pre = monthly[monthly["regime"] == "pre-IDA"]
    baseline = pre.groupby("cal_month")["total_gwh"].mean().to_dict()

    monthly["baseline_gwh"] = monthly["cal_month"].map(baseline)
    monthly["excess_gwh"] = monthly["total_gwh"] - monthly["baseline_gwh"]
    monthly["excess_pct"] = monthly["excess_gwh"] / monthly["baseline_gwh"] * 100

    # Bootstrap pre-IDA residuals to get null CI
    pre_resid = []
    for cal_m, sub in pre.groupby("cal_month"):
        b = sub["total_gwh"].mean()
        pre_resid.extend((sub["total_gwh"] - b).tolist())
    pre_resid = np.array(pre_resid)

    rng = np.random.default_rng(42)
    N_BOOT = 1000
    null_means = np.empty(N_BOOT)
    for i in range(N_BOOT):
        # Sample a 6-month window of pre-IDA residuals (matches typical post-IDA-regime span)
        sample = rng.choice(pre_resid, size=6, replace=True)
        null_means[i] = sample.mean()
    null_lo, null_hi = np.percentile(null_means, [2.5, 97.5])

    # Aggregate per regime
    print()
    print(f"{'regime':<14}  {'months':>6}  {'mean_total':>11}  {'mean_excess':>12}  {'excess_pct':>10}")
    print("-" * 65)
    rows = []
    REGIME_ORDER = ["pre-IDA", "3-sess", "ISP15 win", "DA60/ID15", "DA15/ID15"]
    for reg in REGIME_ORDER:
        sub = monthly[monthly["regime"] == reg]
        if sub.empty:
            continue
        mean_total = sub["total_gwh"].mean()
        mean_excess = sub["excess_gwh"].mean()
        mean_pct = sub["excess_pct"].mean()
        rows.append({
            "regime": reg,
            "n_months": len(sub),
            "mean_total_gwh": mean_total,
            "mean_excess_gwh": mean_excess,
            "mean_excess_pct": mean_pct,
        })
        print(f"{reg:<14}  {len(sub):>6}  {mean_total:>11.1f}  {mean_excess:>+12.1f}  {mean_pct:>+9.1f}%")

    print()
    print(f"Bootstrap null CI (95%, 6-month window): [{null_lo:+.1f}, {null_hi:+.1f}] GWh/mo")
    print()
    print("Interpretation: any post-IDA regime mean_excess outside the null CI is a")
    print("significant level shift. Pre-IDA baseline = same-calendar-month average over")
    print("~9 years of historical data.")

    out = pd.DataFrame(rows)
    OUT.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT, index=False)
    print(f"\nWrote {OUT}")


if __name__ == "__main__":
    main()
