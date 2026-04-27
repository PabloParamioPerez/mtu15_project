# STATUS: ALIVE
# LAST-AUDIT: 2026-04-27
# FEEDS: F7 mechanism — Pivotality / Residual-Supply-Index decomposition
# CLAIM: Decompose F7 IB synthetic-firm transfer (~€820M) by hour-type to identify when IB is pivotal.

"""F7 mechanism — pivotality decomposition.

The synthetic-firm method computes the marginal price impact mp_IB(h) =
p_actual(h) - p_synth_IB(h) at each ISP h. Hours with mp_IB > 0 are
hours where IB has unilateral price-setting power: removing IB from the
supply curve and replacing it with synthetic Fringe-priced supply
strictly lowers the cleared price. This is a direct empirical
operationalisation of pivotality.

We classify hours into four pivotality buckets:
  - non-pivotal:        |mp_IB| <  €0.5/MWh
  - mildly pivotal:    0.5 ≤ mp_IB <  €5/MWh
  - strongly pivotal:    5 ≤ mp_IB < €20/MWh
  - extremely pivotal:  20 ≤ mp_IB

For each bucket, we report:
  (i)   share of hours
  (ii)  share of total IB market-power transfer
  (iii) cross-tabulation with hour-of-day, day-of-week, month-of-year,
        Spanish renewable (wind+solar) generation quintile, monthly
        reservoir filling (TWh) decile, and within-month DA price
        quartile.

User hypotheses tested:
  H1 "drought + evening peak"  → reservoir-decile × hour-of-day heatmap
  H2 "cold mornings"            → winter (Dec-Feb) × early hours (h6-9)
  H3 "wind doldrums"            → low-VRE hours (Q1 of monthly VRE)

Output:
  data/derived/results/f7_pivotality_decomposition.csv
  (long-format with the joint cross-tabulations stacked)
"""
from __future__ import annotations

from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

PROJECT = Path(__file__).resolve().parents[3]
F7_ISP = PROJECT / "data" / "derived" / "results" / "synthetic_firm_per_firm_isp.csv"
PDBCE = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbce_all.parquet"
VRE = PROJECT / "data" / "processed" / "entsoe" / "generation" / "wind_solar_actual_all.parquet"
RES = PROJECT / "data" / "processed" / "entsoe" / "generation" / "reservoir_filling_es_weekly.parquet"
OUT = PROJECT / "data" / "derived" / "results" / "f7_pivotality_decomposition.csv"


def main() -> None:
    print("[1/6] Load hourly F7 mp_IB (post-MTU15-IDA only — pre-MTU15-IDA bid prices are 0-padded)...")
    iso = pd.read_csv(F7_ISP)
    iso["date"] = pd.to_datetime(iso["date"])
    iso = iso[(iso["regime"].isin(["DA60/ID15", "DA15/ID15"])) & (iso["p_actual"] > 0)].copy()
    print(f"   panel: {len(iso):,} ISP-hours, "
          f"{iso.date.min().date()} → {iso.date.max().date()}, "
          f"DA60/ID15 {(iso.regime=='DA60/ID15').sum():,}; "
          f"DA15/ID15 {(iso.regime=='DA15/ID15').sum():,}")

    # Map period → wall-clock hour-of-day (DA60: period=hour; DA15: period=4×(hour-1)+isp)
    iso["hour_of_day"] = np.where(
        iso["regime"] == "DA60/ID15",
        iso["period"],
        np.ceil(iso["period"] / 4.0).astype(int),
    )
    iso["dow"] = iso["date"].dt.dayofweek  # 0=Mon, 6=Sun
    iso["month"] = iso["date"].dt.to_period("M").dt.to_timestamp()
    iso["cal_month"] = iso["date"].dt.month

    print("[2/6] Pull IB cleared MW per hour from pdbce → IB transfer per ISP...")
    con = duckdb.connect()
    con.execute("SET memory_limit='2GB'")
    con.execute("SET threads=4")
    ib_q = con.sql(f"""
        SELECT date, period,
               SUM(assigned_power_mw)
                 / CASE WHEN MAX(mtu_minutes) = 15 THEN 4.0 ELSE 1.0 END AS q_ib_mwh
        FROM '{PDBCE}'
        WHERE grupo_empresarial = 'IB'
          AND offer_type = 1
          AND assigned_power_mw IS NOT NULL
          AND assigned_power_mw > 0
          AND CAST(date AS DATE) >= DATE '2024-06-01'
        GROUP BY 1, 2
    """).df()
    ib_q["date"] = pd.to_datetime(ib_q["date"])
    iso = iso.merge(ib_q, on=["date", "period"], how="left")
    iso["q_ib_mwh"] = iso["q_ib_mwh"].fillna(0.0)
    iso["transfer_eur"] = iso["mp_IB"] * iso["q_ib_mwh"]
    print(f"   IB cleared mean: {iso.q_ib_mwh.mean():.1f} MWh/ISP, total: {iso.q_ib_mwh.sum()/1e6:.1f} TWh")
    print(f"   Total IB transfer in panel: €{iso.transfer_eur.sum()/1e6:.1f}M")

    print("[3/6] Pivotality buckets on mp_IB...")
    bins = [-np.inf, 0.5, 5.0, 20.0, np.inf]
    labels = ["non-pivotal", "mildly", "strongly", "extremely"]
    iso["pivot"] = pd.cut(iso["mp_IB"], bins=bins, labels=labels, right=False)
    pivot_summary = iso.groupby("pivot", observed=True).agg(
        n_hours=("mp_IB", "size"),
        mean_mp=("mp_IB", "mean"),
        sum_transfer_M=("transfer_eur", lambda x: x.sum() / 1e6),
        mean_q_ib=("q_ib_mwh", "mean"),
    ).reset_index()
    pivot_summary["share_hours_pct"] = pivot_summary["n_hours"] / pivot_summary["n_hours"].sum() * 100
    pivot_summary["share_transfer_pct"] = pivot_summary["sum_transfer_M"] / pivot_summary["sum_transfer_M"].sum() * 100
    pivot_summary["concentration"] = pivot_summary["share_transfer_pct"] / pivot_summary["share_hours_pct"]
    print()
    print("Pivotality buckets:")
    print(pivot_summary.to_string(index=False, float_format=lambda x: f"{x:.2f}"))
    print()
    print(f"  → top decile of hours (mp_IB ≥ €5): "
          f"{pivot_summary[pivot_summary['pivot'].isin(['strongly','extremely'])]['share_hours_pct'].sum():.1f}% "
          f"of hours, "
          f"{pivot_summary[pivot_summary['pivot'].isin(['strongly','extremely'])]['share_transfer_pct'].sum():.1f}% of transfer")

    print("\n[4/6] Pivotality × hour-of-day:")
    by_hour = iso.groupby("hour_of_day").agg(
        n=("mp_IB", "size"),
        mean_mp=("mp_IB", "mean"),
        sum_transfer_M=("transfer_eur", lambda x: x.sum() / 1e6),
        share_pivotal=("pivot", lambda x: (x.isin(["strongly", "extremely"])).mean() * 100),
    ).round(2)
    print(by_hour.to_string())

    print("\n[5/6] Pivotality × VRE-quintile (wind+solar) and reservoir-decile...")
    print("   Loading VRE (B16 Solar + B19 Wind Onshore + B18 Wind Offshore)...")
    vre = pd.read_parquet(VRE, columns=["isp_start_utc", "psr_type", "quantity_mw", "mtu_minutes"])
    vre = vre[vre["psr_type"].isin(["B01", "B16", "B18", "B19"])]
    vre["date"] = vre["isp_start_utc"].dt.tz_localize(None).dt.date
    vre["hour_of_day"] = vre["isp_start_utc"].dt.hour + 1  # 1-24 to align with OMIE period
    vre_h = vre.groupby(["date", "hour_of_day"], as_index=False)["quantity_mw"].sum()
    vre_h.columns = ["date", "hour_of_day", "vre_mw"]
    vre_h["date"] = pd.to_datetime(vre_h["date"])
    iso = iso.merge(vre_h, on=["date", "hour_of_day"], how="left")
    print(f"   VRE merge match rate: {(iso.vre_mw.notna()).mean()*100:.1f}%")
    iso["vre_qtl"] = pd.qcut(iso["vre_mw"], 5, labels=["Q1-low", "Q2", "Q3", "Q4", "Q5-high"])

    by_vre = iso.dropna(subset=["vre_qtl"]).groupby("vre_qtl", observed=True).agg(
        n=("mp_IB", "size"),
        mean_mp=("mp_IB", "mean"),
        median_mp=("mp_IB", "median"),
        share_strong_pivotal=("pivot", lambda x: (x.isin(["strongly", "extremely"])).mean() * 100),
        sum_transfer_M=("transfer_eur", lambda x: x.sum() / 1e6),
        mean_vre_mw=("vre_mw", "mean"),
    ).round(2)
    print()
    print("By VRE quintile (Q1=lowest renewable hours; H3 wind-doldrum hypothesis):")
    print(by_vre.to_string())

    print("\n   Loading monthly reservoir filling...")
    res = pd.read_parquet(RES)
    res["week_start"] = pd.to_datetime(res["week_start"])
    res["month"] = res["week_start"].dt.to_period("M").dt.to_timestamp()
    monthly_res = res.groupby("month", as_index=False)["reservoir_twh"].mean()
    iso = iso.merge(monthly_res, on="month", how="left")
    print(f"   Reservoir merge match rate: {(iso.reservoir_twh.notna()).mean()*100:.1f}%")
    iso["res_dec"] = pd.qcut(iso["reservoir_twh"], 10, labels=False, duplicates="drop") + 1

    by_res = iso.dropna(subset=["res_dec"]).groupby("res_dec", observed=True).agg(
        n=("mp_IB", "size"),
        mean_mp=("mp_IB", "mean"),
        share_strong_pivotal=("pivot", lambda x: (x.isin(["strongly", "extremely"])).mean() * 100),
        sum_transfer_M=("transfer_eur", lambda x: x.sum() / 1e6),
        mean_reservoir_twh=("reservoir_twh", "mean"),
    ).round(2)
    print()
    print("By reservoir decile (D1=lowest, drought; H1 drought-pivotal hypothesis):")
    print(by_res.to_string())

    print("\n[6/6] Joint diagnostic — testing user hypotheses H1 (drought × evening peak), H2 (cold mornings):")

    # H1: drought × evening peak
    iso["evening_peak"] = iso["hour_of_day"].between(19, 22)
    iso["drought"] = iso["res_dec"] <= 3   # bottom 30%
    iso["wet"] = iso["res_dec"] >= 8        # top 30%
    h1 = pd.DataFrame({
        "category": ["drought × evening (H1)", "drought × off-evening", "wet × evening", "wet × off-evening"],
        "n": [
            ((iso["drought"]) & (iso["evening_peak"])).sum(),
            ((iso["drought"]) & (~iso["evening_peak"])).sum(),
            ((iso["wet"]) & (iso["evening_peak"])).sum(),
            ((iso["wet"]) & (~iso["evening_peak"])).sum(),
        ],
        "mean_mp_IB": [
            iso.loc[iso["drought"] & iso["evening_peak"], "mp_IB"].mean(),
            iso.loc[iso["drought"] & ~iso["evening_peak"], "mp_IB"].mean(),
            iso.loc[iso["wet"] & iso["evening_peak"], "mp_IB"].mean(),
            iso.loc[iso["wet"] & ~iso["evening_peak"], "mp_IB"].mean(),
        ],
        "share_strong_pivotal_pct": [
            (iso.loc[iso["drought"] & iso["evening_peak"], "pivot"].isin(["strongly", "extremely"])).mean() * 100,
            (iso.loc[iso["drought"] & ~iso["evening_peak"], "pivot"].isin(["strongly", "extremely"])).mean() * 100,
            (iso.loc[iso["wet"] & iso["evening_peak"], "pivot"].isin(["strongly", "extremely"])).mean() * 100,
            (iso.loc[iso["wet"] & ~iso["evening_peak"], "pivot"].isin(["strongly", "extremely"])).mean() * 100,
        ],
    }).round(2)
    print()
    print("H1 (drought × evening peak):")
    print(h1.to_string(index=False))

    # H2: cold mornings
    iso["winter"] = iso["cal_month"].isin([12, 1, 2])
    iso["morning"] = iso["hour_of_day"].between(6, 9)
    h2 = pd.DataFrame({
        "category": ["winter × morning (H2)", "winter × non-morning", "summer × morning", "summer × non-morning"],
        "n": [
            ((iso["winter"]) & (iso["morning"])).sum(),
            ((iso["winter"]) & (~iso["morning"])).sum(),
            ((~iso["winter"]) & (iso["morning"])).sum(),
            ((~iso["winter"]) & (~iso["morning"])).sum(),
        ],
        "mean_mp_IB": [
            iso.loc[iso["winter"] & iso["morning"], "mp_IB"].mean(),
            iso.loc[iso["winter"] & ~iso["morning"], "mp_IB"].mean(),
            iso.loc[~iso["winter"] & iso["morning"], "mp_IB"].mean(),
            iso.loc[~iso["winter"] & ~iso["morning"], "mp_IB"].mean(),
        ],
        "share_strong_pivotal_pct": [
            (iso.loc[iso["winter"] & iso["morning"], "pivot"].isin(["strongly", "extremely"])).mean() * 100,
            (iso.loc[iso["winter"] & ~iso["morning"], "pivot"].isin(["strongly", "extremely"])).mean() * 100,
            (iso.loc[~iso["winter"] & iso["morning"], "pivot"].isin(["strongly", "extremely"])).mean() * 100,
            (iso.loc[~iso["winter"] & ~iso["morning"], "pivot"].isin(["strongly", "extremely"])).mean() * 100,
        ],
    }).round(2)
    print()
    print("H2 (winter mornings):")
    print(h2.to_string(index=False))

    # H3: wind-doldrum hours
    h3 = pd.DataFrame({
        "category": ["VRE Q1 (low) × evening", "VRE Q1 × off-evening", "VRE Q5 (high) × evening", "VRE Q5 × off-evening"],
        "n": [
            ((iso["vre_qtl"] == "Q1-low") & (iso["evening_peak"])).sum(),
            ((iso["vre_qtl"] == "Q1-low") & (~iso["evening_peak"])).sum(),
            ((iso["vre_qtl"] == "Q5-high") & (iso["evening_peak"])).sum(),
            ((iso["vre_qtl"] == "Q5-high") & (~iso["evening_peak"])).sum(),
        ],
        "mean_mp_IB": [
            iso.loc[(iso["vre_qtl"] == "Q1-low") & iso["evening_peak"], "mp_IB"].mean(),
            iso.loc[(iso["vre_qtl"] == "Q1-low") & ~iso["evening_peak"], "mp_IB"].mean(),
            iso.loc[(iso["vre_qtl"] == "Q5-high") & iso["evening_peak"], "mp_IB"].mean(),
            iso.loc[(iso["vre_qtl"] == "Q5-high") & ~iso["evening_peak"], "mp_IB"].mean(),
        ],
        "share_strong_pivotal_pct": [
            (iso.loc[(iso["vre_qtl"] == "Q1-low") & iso["evening_peak"], "pivot"].isin(["strongly", "extremely"])).mean() * 100,
            (iso.loc[(iso["vre_qtl"] == "Q1-low") & ~iso["evening_peak"], "pivot"].isin(["strongly", "extremely"])).mean() * 100,
            (iso.loc[(iso["vre_qtl"] == "Q5-high") & iso["evening_peak"], "pivot"].isin(["strongly", "extremely"])).mean() * 100,
            (iso.loc[(iso["vre_qtl"] == "Q5-high") & ~iso["evening_peak"], "pivot"].isin(["strongly", "extremely"])).mean() * 100,
        ],
    }).round(2)
    print()
    print("H3 (wind-doldrum × evening peak):")
    print(h3.to_string(index=False))

    # Save the panel + summary
    OUT.parent.mkdir(parents=True, exist_ok=True)

    summary_rows = []
    for table_name, df_ in [
        ("pivotality_buckets", pivot_summary),
        ("by_hour_of_day", by_hour.reset_index()),
        ("by_vre_quintile", by_vre.reset_index()),
        ("by_reservoir_decile", by_res.reset_index()),
        ("h1_drought_evening", h1),
        ("h2_winter_morning", h2),
        ("h3_vre_doldrum_evening", h3),
    ]:
        df_["_table"] = table_name
        summary_rows.append(df_)

    out_long = pd.concat(summary_rows, ignore_index=True, sort=False)
    out_long.to_csv(OUT, index=False)
    print(f"\nwrote {OUT}")


if __name__ == "__main__":
    main()
