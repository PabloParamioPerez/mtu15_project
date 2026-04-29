# STATUS: ALIVE
# LAST-AUDIT: 2026-04-27
# FEEDS: F14 (nuclear unaccounted reduction system-wide) + cnmc_historical_sanctions
# CLAIM: CCGT-equivalent of F14 — identify plants with high unaccounted reduction in 2024-25
"""C: CCGT Article-65.27 availability sweep.

Inspired by the 2023 CNMC sanctions of Engie Castelnou (CTNU) and Ignis
(ECT2) under Article 65.27 LSE for "failure to maintain availability of
production units." This is the SERIOUS version of the VERY-SERIOUS
Article 64.37 charges in the 2026 post-blackout batch (IB Cofrentes,
Almaraz-Trillo).

For each Spanish CCGT plant:
  - actual generation TWh per year (from A73)
  - reported planned-outage TWh per year (from A80 B53)
  - reported forced-outage TWh per year (from A80 B54)
  - implied "unaccounted reduction" % = (nameplate*hours - actual - outages) / nameplate*hours

If a CCGT plant has unaccounted reduction > some baseline threshold
(say > nameplate * 50% — many CCGTs run at low CF normally), the
remaining CF might still be a candidate for CNMC scrutiny. CCGTs are
peakers so low CF is normal — thresholds must be calibrated against
the historical baseline of THAT plant.

Output: results/regressions/ccgt_availability_sweep.csv

Caveats:
  1. CCGTs are peakers; low CF is normal. We rank plants by 2024-25 vs
     2018-21 BASELINE (within-plant), not against nameplate.
  2. A80 outages have sparse forced-event coverage (28/100 months for ES);
     missing forced events would inflate "unaccounted" estimate.
  3. We do not have plant-level nameplate capacity in the parquets;
     we approximate from max observed MW × 1.05 buffer.
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

PROJECT = Path(__file__).resolve().parents[3]


def main() -> None:
    panel = pd.read_parquet(PROJECT / "data/processed/entsoe/generation/ccgt_per_firm_panel.parquet")
    panel["ts"] = pd.to_datetime(panel["isp_start_utc"])
    panel["year"] = panel["ts"].dt.year

    # Approximate nameplate per plant (max observed MW + 5% buffer)
    nameplate = panel.groupby("omie_code")["quantity_mw"].max() * 1.05

    # Annual TWh per plant
    plant_year_twh = (panel.groupby(["omie_code", "year"])["mwh"].sum() / 1e6).unstack(fill_value=0)

    # Outages: A80 forced + planned, filter to B04 (CCGT)
    plan = pd.read_parquet(PROJECT / "data/processed/entsoe/outages/outages_planned_all.parquet")
    forc = pd.read_parquet(PROJECT / "data/processed/entsoe/outages/outages_forced_all.parquet")

    out_rows = []
    for df, label in [(plan, "planned"), (forc, "forced")]:
        d = df[df["psr_type"] == "B04"].copy()
        d["start_utc"] = pd.to_datetime(d["start_utc"], errors="coerce", utc=True).dt.tz_localize(None)
        d["end_utc"] = pd.to_datetime(d["end_utc"], errors="coerce", utc=True).dt.tz_localize(None)
        # Match unit_eic against panel's unit_eic
        panel_eics = set(panel["unit_eic"].dropna().unique())
        d = d[d["unit_eic"].isin(panel_eics)]
        d["outage_hours"] = (d["end_utc"] - d["start_utc"]).dt.total_seconds() / 3600
        d["curtailed_mw"] = d["nominal_mw"].fillna(0) - d["min_avail_mw"].fillna(d["nominal_mw"].fillna(0))
        d["curtailed_mwh"] = d["curtailed_mw"] * d["outage_hours"]
        d["type"] = label
        out_rows.append(d[["unit_eic", "start_utc", "end_utc",
                           "outage_hours", "curtailed_mwh", "type"]])
    out = pd.concat(out_rows, ignore_index=True)

    # EIC -> omie mapping
    eic_to_omie = panel.dropna(subset=["unit_eic", "omie_code"]).drop_duplicates("unit_eic").set_index("unit_eic")["omie_code"].to_dict()
    out["omie_code"] = out["unit_eic"].map(eic_to_omie)
    out = out.dropna(subset=["omie_code"])

    # Annual outage TWh per (plant, year, type) — allocate by overlap
    YEARS = list(range(2018, 2027))
    rows = []
    for unit in plant_year_twh.index:
        nm = nameplate.get(unit, np.nan)
        if pd.isna(nm) or nm < 100:
            continue
        for yr in YEARS:
            actual = plant_year_twh.loc[unit].get(yr, 0)
            days = 366 if yr in (2020, 2024) else 365
            if yr == 2026:
                days = 117  # through April 27
            cap_twh = nm * 24 * days / 1e6  # nameplate * hours
            cf = actual / cap_twh * 100 if cap_twh > 0 else np.nan

            yr_start = pd.Timestamp(f"{yr}-01-01")
            yr_end = pd.Timestamp(f"{yr+1}-01-01")
            o = out[out.omie_code == unit].copy()
            o = o[(o.start_utc < yr_end) & (o.end_utc > yr_start)]
            if not o.empty:
                o["ovl_start"] = o["start_utc"].clip(lower=yr_start)
                o["ovl_end"] = o["end_utc"].clip(upper=yr_end)
                o["ovl_hours"] = (o["ovl_end"] - o["ovl_start"]).dt.total_seconds() / 3600
                o["ovl_curtailed_mwh"] = (o["curtailed_mwh"] / o["outage_hours"].replace(0, np.nan) * o["ovl_hours"]).fillna(0)
                planned_twh = o[o.type == "planned"]["ovl_curtailed_mwh"].sum() / 1e6
                forced_twh = o[o.type == "forced"]["ovl_curtailed_mwh"].sum() / 1e6
            else:
                planned_twh = forced_twh = 0
            unaccounted = cap_twh - actual - planned_twh - forced_twh
            rows.append({
                "unit": unit, "year": yr,
                "nameplate_MW": round(nm, 0),
                "actual_TWh": round(actual, 2),
                "cap_TWh": round(cap_twh, 2),
                "cf_%": round(cf, 1),
                "planned_outage_TWh": round(planned_twh, 2),
                "forced_outage_TWh": round(forced_twh, 2),
                "unaccounted_TWh": round(unaccounted, 2),
                "unaccounted_pct_of_cap": round(unaccounted / cap_twh * 100 if cap_twh > 0 else np.nan, 1),
            })
    df = pd.DataFrame(rows)

    # For each plant, compute baseline (2018-2021 mean) vs 2024-2025 mean of cf_%
    print("=" * 70)
    print("CCGT capacity factor: 2018-21 baseline vs 2024-25 mean (% of nameplate)")
    print("=" * 70)
    cf_pivot = df.pivot(index="unit", columns="year", values="cf_%")
    cf_pivot["base_2018_21"] = cf_pivot[[2018, 2019, 2020, 2021]].mean(axis=1).round(1)
    cf_pivot["mean_2024_25"] = cf_pivot[[2024, 2025]].mean(axis=1).round(1)
    cf_pivot["delta_pp"] = (cf_pivot["mean_2024_25"] - cf_pivot["base_2018_21"]).round(1)
    cf_summary = cf_pivot[["base_2018_21", "mean_2024_25", "delta_pp"]].sort_values("delta_pp")
    print(cf_summary.to_string())

    print()
    print("=" * 70)
    print("Plants with LARGEST CF DROP 2024-25 vs 2018-21 baseline")
    print("=" * 70)
    print("(Candidates for further availability investigation; does NOT prove conduct)")
    print()
    biggest_drops = cf_summary.head(10)
    print(biggest_drops.to_string())

    print()
    print("Sanctioned plants (already CNMC-sanctioned for 65.27 / 65.33):")
    for unit, label in [("CTNU", "Engie Castelnou — Art 65.27 sanctioned 2023-Jun"),
                        ("ECT2", "Ignis Escatrón 2 — Art 65.27 sanctioned 2023-Oct"),
                        ("SBO3", "Naturgy Sabón 3 — Art 65.33 sanctioned 2023-Jul"),
                        ("BES3", "Endesa Besós 3 — Art 65.33 sanctioned 2019"),
                        ("BES5", "Endesa Besós 5 — Art 65.33 sanctioned 2019")]:
        if unit in cf_summary.index:
            row = cf_summary.loc[unit]
            print(f"  {unit:8} base={row['base_2018_21']:.1f}%  2024-25={row['mean_2024_25']:.1f}%  Δ={row['delta_pp']:+.1f}pp  ({label})")

    out_path = PROJECT / "results/regressions/ccgt_availability_sweep.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)
    cf_summary.to_csv(out_path.with_name("ccgt_cf_baseline_vs_recent.csv"))
    print(f"\nwrote {out_path}")
    print(f"wrote {out_path.with_name('ccgt_cf_baseline_vs_recent.csv')}")


if __name__ == "__main__":
    main()
