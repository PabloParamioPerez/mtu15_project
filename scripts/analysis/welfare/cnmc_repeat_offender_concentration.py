# STATUS: ALIVE
# LAST-AUDIT: 2026-04-27
# FEEDS: F15 (post-blackout CCGT windfall) + cnmc_historical_sanctions doc
# CLAIM: Sanction-history CCGT plants behave differently across reform / blackout regimes vs clean plants
"""B: Repeat-offender concentration test.

Inspired by docs/regulation/cnmc_historical_sanctions_2013-2026.md.

Three Spanish CCGT plants have a CNMC sanction history for wholesale-
market manipulation (Article 65.33 LSE):
  - SBO3 (Naturgy)  — 2023 EUR 41.5M, manipulation Mar 2019 - Dec 2020
  - BES3 (Endesa)   — 2019 EUR 5.8M (joint w/ BES5), Oct 2016 - Jan 2017
  - BES5 (Endesa)   — 2019 EUR 5.8M (joint w/ BES3), Oct 2016 - Jan 2017
  (Naturgy's earlier 2019 EUR 19.5M case did not name specific plants)

Test: do these three plants behave differently in the post-MTU15-IDA
regimes than other CCGT plants?

Specifically:
  - Generation share (% of system CCGT TWh) — did sanction conduct die?
  - Capacity factor by regime (TWh / nameplate-hours)
  - Pre/post-blackout share shift — did sanctioned plants benefit more
    from operación reforzada or behave more conservatively?

Caveats:
  1. Sanction window doesn't cover our F15 panel (which starts 2018).
     The 2019 Naturgy/Endesa sanction is for 2016-17; the 2023 Naturgy
     sanction is for 2019-20. Both pre-date most of our regime windows.
  2. We see ONLY plant generation in MWh (A73 per-unit), not the
     bidding behaviour the CNMC actually sanctioned — which would
     require det_all DA bid prices vs technical-restrictions market
     bid prices (not in this analysis).
  3. This is a descriptive cross-tab, not a causal estimate. Sanctioned
     plants may differ from clean plants for many reasons (location,
     size, vintage, voltage-zone) unrelated to sanction history.

Output: data/derived/results/repeat_offender_concentration.csv
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

PROJECT = Path(__file__).resolve().parents[3]

SANCTIONED = {"SBO3", "BES3", "BES5"}


def assign_regime(d: pd.Timestamp) -> str:
    if d < pd.Timestamp("2024-06-14"):
        return "pre-IDA"
    if d < pd.Timestamp("2024-12-01"):
        return "3-sess"
    if d < pd.Timestamp("2025-03-19"):
        return "ISP15-win"
    if d < pd.Timestamp("2025-04-28"):
        return "DA60 PRE-blackout"
    if d < pd.Timestamp("2025-10-01"):
        return "DA60 POST-blackout"
    return "DA15/ID15"


def main() -> None:
    df = pd.read_parquet(PROJECT / "data/processed/entsoe/generation/ccgt_per_firm_panel.parquet")
    df["ts"] = pd.to_datetime(df["isp_start_utc"])
    df["sanctioned"] = df["omie_code"].isin(SANCTIONED)
    df["regime"] = df["ts"].apply(assign_regime)

    print("=" * 70)
    print("Sanctioned CCGT plants vs others — TWh and share by regime")
    print("=" * 70)
    twh = df.groupby(["regime", "sanctioned"])["mwh"].sum().div(1e6).round(2)
    pivot = twh.unstack(fill_value=0)
    pivot.columns = ["clean (TWh)", "sanctioned (TWh)"]
    pivot["sanctioned share %"] = (pivot["sanctioned (TWh)"] /
                                   (pivot["clean (TWh)"] + pivot["sanctioned (TWh)"]) * 100).round(1)
    print(pivot.to_string())

    print()
    print("=" * 70)
    print("Per-plant TWh by regime (focused — sanctioned + 5 size-comparable)")
    print("=" * 70)
    # Pick a few non-sanctioned IB/GN/GE plants as comparison
    focus = list(SANCTIONED) + ["ARCOS1", "ARCOS2", "PALOS1", "PALOS2", "CTGN1", "CTN3", "STC4"]
    fdf = df[df.omie_code.isin(focus)]
    plant_twh = fdf.groupby(["regime", "omie_code"])["mwh"].sum().div(1e6).round(2)
    plant_pivot = plant_twh.unstack("omie_code", fill_value=0)
    print(plant_pivot.to_string())

    print()
    print("=" * 70)
    print("Pre/post-blackout share shift (plants in DA60/ID15 only)")
    print("=" * 70)
    pre_b = df[(df.ts >= "2025-03-19") & (df.ts < "2025-04-28")]
    post_b = df[(df.ts >= "2025-04-28") & (df.ts < "2025-10-01")]
    da15 = df[df.ts >= "2025-10-01"]
    rows = []
    for plant in sorted(focus):
        pre_mwh = pre_b[pre_b.omie_code == plant]["mwh"].sum() / 1e3  # GWh
        post_mwh = post_b[post_b.omie_code == plant]["mwh"].sum() / 1e3
        da15_mwh = da15[da15.omie_code == plant]["mwh"].sum() / 1e3
        # Share within window
        pre_share = pre_mwh / (pre_b["mwh"].sum() / 1e3) * 100 if pre_b["mwh"].sum() > 0 else 0
        post_share = post_mwh / (post_b["mwh"].sum() / 1e3) * 100 if post_b["mwh"].sum() > 0 else 0
        da15_share = da15_mwh / (da15["mwh"].sum() / 1e3) * 100 if da15["mwh"].sum() > 0 else 0
        rows.append({
            "plant": plant,
            "sanction_history": "yes" if plant in SANCTIONED else "no",
            "pre_blackout_GWh": round(pre_mwh, 1),
            "post_blackout_GWh": round(post_mwh, 1),
            "DA15_GWh": round(da15_mwh, 1),
            "pre_share_%": round(pre_share, 2),
            "post_share_%": round(post_share, 2),
            "post_minus_pre_pp": round(post_share - pre_share, 2),
            "DA15_share_%": round(da15_share, 2),
        })
    out = pd.DataFrame(rows).sort_values("post_minus_pre_pp", ascending=False)
    print(out.to_string(index=False))

    out_path = PROJECT / "data/derived/results/repeat_offender_concentration.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_path, index=False)
    pivot.to_csv(out_path.with_name("repeat_offender_aggregate.csv"))
    print(f"\nwrote {out_path}")
    print(f"wrote {out_path.with_name('repeat_offender_aggregate.csv')}")


if __name__ == "__main__":
    main()
