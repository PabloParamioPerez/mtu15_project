# STATUS: ALIVE
# LAST-AUDIT: 2026-04-27
# FEEDS: F7 (hour-of-day decomposition of IB price-setting power)
# CLAIM: When in the day does IB's price-setting power concentrate? Peak vs off-peak.
"""When does IB set prices? Hour-of-day decomposition of the F7 per-firm transfer.

If IB's market-power transfer concentrates in peak demand hours
(CCGT-margin hours), it ties F7 + B8 (IB CCGT complexification) + F5
(IB peak-hour Allaz-Vila signal) into a coherent peak-hour CCGT story.
If it's spread evenly across the day, it suggests a different mechanism
(infra-marginal portfolio composition, settlement-rule effect).

Spec. Reads `synthetic_firm_per_firm_isp.csv` (per-ISP actual + per-firm
synthetic prices). Converts ISP period to hour-of-day (post-MTU15-IDA
period 1-96 → hour-of-day = ceil(period/4)). Aggregates per (firm,
hour-of-day, regime) the mean market-power index.
"""
from __future__ import annotations

from pathlib import Path
import numpy as np
import pandas as pd

PROJECT = Path(__file__).resolve().parents[3]
ISP_FILE = PROJECT / "results" / "regressions" / "synthetic_firm_per_firm_isp.csv"
OUT = PROJECT / "results" / "regressions" / "synthetic_firm_hour_of_day.csv"


def main() -> None:
    df = pd.read_csv(ISP_FILE)
    df["date"] = pd.to_datetime(df["date"])

    # Convert period to hour-of-day (1-24)
    # Pre-MTU15-IDA: period 1-24 = hour 1-24
    # Post-MTU15-IDA: period 1-96 (15-min) → hour = ceil(period/4)
    # We can detect by max period; or use a simple rule: if period > 24, divide by 4.
    df["hour_of_day"] = np.where(
        df["period"] > 24, np.ceil(df["period"] / 4).astype(int), df["period"]
    )

    # Restrict to post-MTU15-IDA (where data is interpretable)
    post = df[df["date"] >= pd.Timestamp("2025-03-19")].copy()

    # Mean per-firm market power per hour-of-day
    print()
    print("=" * 110)
    print("Per-firm market power by hour-of-day (post-MTU15-IDA pooled, EUR/MWh)")
    print("=" * 110)
    print()
    print(f"{'hour':<5}  {'mp_GE':>10}  {'mp_IB':>10}  {'mp_GN':>10}  {'mp_HC':>10}  {'mean p_actual':>15}")
    rows = []
    for h in range(1, 25):
        sub = post[post["hour_of_day"] == h]
        if len(sub) == 0:
            continue
        mp_ge = sub["mp_GE"].mean()
        mp_ib = sub["mp_IB"].mean()
        mp_gn = sub["mp_GN"].mean()
        mp_hc = sub["mp_HC"].mean()
        p_act = sub["p_actual"].mean()
        rows.append({
            "hour_of_day": h, "n_isps": len(sub),
            "mp_GE": mp_ge, "mp_IB": mp_ib,
            "mp_GN": mp_gn, "mp_HC": mp_hc,
            "mean_p_actual": p_act,
        })
        print(
            f"  {h:>2}  {mp_ge:>+10.2f}  {mp_ib:>+10.2f}  {mp_gn:>+10.2f}  {mp_hc:>+10.2f}  {p_act:>15.2f}"
        )

    out_df = pd.DataFrame(rows)

    # Headline test: peak (h11-22) vs off-peak (h1-10 + h23-24) IB market power
    peak = out_df[out_df["hour_of_day"].between(11, 22)]
    off = out_df[~out_df["hour_of_day"].between(11, 22)]
    ib_peak = (peak["mp_IB"] * peak["n_isps"]).sum() / peak["n_isps"].sum()
    ib_off = (off["mp_IB"] * off["n_isps"]).sum() / off["n_isps"].sum()
    p_peak = (peak["mean_p_actual"] * peak["n_isps"]).sum() / peak["n_isps"].sum()
    p_off = (off["mean_p_actual"] * off["n_isps"]).sum() / off["n_isps"].sum()
    print()
    print("=" * 80)
    print("Peak (h11–22) vs off-peak (h1–10 + h23–24) IB market power")
    print("=" * 80)
    print(f"  IB peak    : {ib_peak:+.3f} EUR/MWh   (mean p_actual = {p_peak:.2f})  rel = {ib_peak/p_peak*100:+.2f}%")
    print(f"  IB off-peak: {ib_off:+.3f} EUR/MWh   (mean p_actual = {p_off:.2f})  rel = {ib_off/p_off*100:+.2f}%")
    print(f"  Peak / off-peak ratio: {ib_peak/ib_off:.2f}×")
    print()
    if ib_peak > 1.5 * ib_off:
        print("  ✓ IB's price-setting power IS strongly concentrated in peak (CCGT-margin) hours.")
        print("  → ties F7 + B8 + F5 into a coherent peak-hour CCGT mechanism story.")
    elif ib_peak > ib_off:
        print("  ≈ IB's price-setting power is somewhat concentrated in peak hours, but not dramatically.")
    else:
        print("  ✗ IB's price-setting power is NOT concentrated in peak hours; spread across the day.")

    # Same comparison for GE (should be near zero across all hours per F7)
    ge_peak = (peak["mp_GE"] * peak["n_isps"]).sum() / peak["n_isps"].sum()
    ge_off = (off["mp_GE"] * off["n_isps"]).sum() / off["n_isps"].sum()
    print()
    print(f"  GE peak    : {ge_peak:+.3f}; GE off-peak: {ge_off:+.3f}  (both near zero per F7 per-firm finding)")

    OUT.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(OUT, index=False)
    print(f"\nwrote {OUT}")


if __name__ == "__main__":
    main()
