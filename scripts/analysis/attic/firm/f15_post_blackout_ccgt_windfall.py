# STATUS: ALIVE
# LAST-AUDIT: 2026-04-27
# FEEDS: F15
# CLAIM: Apr-Jun 2024 vs 2025 per-firm CCGT TWh + share comparison; post-blackout windfall
"""F15 audit script — post-blackout CCGT windfall went to Naturgy, not IB.

Tests the vertical-integration moral-hazard reading by computing per-firm
CCGT generation in two windows:
  - Apr-Jun 2024 (control)
  - Apr-Jun 2025 (post-2025-04-28 blackout, operación reforzada active)

And compares CCGT share by firm across:
  - Jan-Apr 27 2025 (pre-blackout, DA60/ID15 clean window)
  - Apr 28 - Jun 30 2025 (post-blackout)

If "IB strategically reduces nuclear (causing voltage problems) so its
CCGT can capture the recommitment windfall," IB's CCGT share should JUMP
post-blackout. Empirical result: IB CCGT lost share (-2 pp) while
Naturgy gained +7.1 pp. Rejects the moral-hazard sole-cause aggregate
reading. (Does not close per-event Article 64.37 conduct around the
specific blackout-day voltage events at Cofrentes.)

Output: results/regressions/f15_post_blackout_ccgt_windfall.csv
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

PROJECT = Path(__file__).resolve().parents[3]


def main() -> None:
    df = pd.read_parquet(PROJECT / "data/processed/entsoe/generation/ccgt_per_firm_panel.parquet")
    df["ts"] = pd.to_datetime(df["isp_start_utc"])
    df["year"] = df["ts"].dt.year
    df["month"] = df["ts"].dt.month

    print("=" * 60)
    print("F15a: Apr-Jun 2024 vs 2025 CCGT TWh by firm")
    print("=" * 60)
    m24 = (df.year == 2024) & df.month.isin([4, 5, 6])
    m25 = (df.year == 2025) & df.month.isin([4, 5, 6])
    a24 = df[m24].groupby("firm")["mwh"].sum() / 1e6
    a25 = df[m25].groupby("firm")["mwh"].sum() / 1e6
    out = pd.DataFrame({"2024 Apr-Jun TWh": a24.round(2), "2025 Apr-Jun TWh": a25.round(2)})
    out["delta_TWh"] = (out["2025 Apr-Jun TWh"] - out["2024 Apr-Jun TWh"]).round(2)
    out["delta_pct"] = ((out["2025 Apr-Jun TWh"] / out["2024 Apr-Jun TWh"] - 1) * 100).round(1)
    print(out.to_string())

    print()
    print("=" * 60)
    print("F15b: CCGT share by firm pre- vs post-blackout (2025)")
    print("=" * 60)
    pre_blackout = df[(df.ts >= "2025-01-01") & (df.ts < "2025-04-28")]
    post_blackout = df[(df.ts >= "2025-04-28") & (df.ts < "2025-07-01")]
    pre_share = pre_blackout.groupby("firm")["mwh"].sum() / pre_blackout["mwh"].sum() * 100
    post_share = post_blackout.groupby("firm")["mwh"].sum() / post_blackout["mwh"].sum() * 100
    share = pd.DataFrame({"pre-blackout share %": pre_share.round(1),
                          "post-blackout share %": post_share.round(1)})
    share["delta_pp"] = (share["post-blackout share %"] - share["pre-blackout share %"]).round(1)
    print(share.to_string())

    out_path = PROJECT / "results/regressions/f15_post_blackout_ccgt_windfall.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    combined = out.join(share)
    combined.to_csv(out_path)
    print(f"\nwrote {out_path}")


if __name__ == "__main__":
    main()
