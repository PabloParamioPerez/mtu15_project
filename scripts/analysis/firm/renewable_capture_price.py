# STATUS: ALIVE
# LAST-AUDIT: 2026-04-27
# FEEDS: New economic angle — renewable capture-price cannibalization by regime
# CLAIM: Renewable producers face systematically lower realized prices than the spot average; the cannibalization ratio is regime-sensitive.

"""Renewable capture-price (cannibalization) by regime.

Solar and wind producers can only sell when the wind blows / sun shines.
Their realized average price (capture price) is therefore production-
weighted across hours, while a baseload generator with flat dispatch
would face the time-weighted average (spot mean).

If RES production correlates negatively with hourly DA price (e.g.
solar mid-day saturation lowers prices when solar produces most), the
capture-price ratio = capture_price / spot_mean is less than 1 — the
"cannibalization effect."

Predictions:
  - Solar capture ratio drops below 1 at high solar penetration (mid-day
    saturation). Stronger cannibalization in summer.
  - Wind capture ratio is typically less negative (wind production is
    spread across hours and seasons).
  - Reform impact: finer trading granularity (post-MTU15-DA) lets RES
    producers capture intra-hour price variation more precisely. If
    intra-hour prices spike when RES drops mid-period, finer granularity
    should LOWER cannibalization (capture ratio rises).
  - Conversely, asymmetric-granularity windows (DA60/ID15) might worsen
    cannibalization because DA-cleared 60-min RES is averaged across
    finer-priced ISPs.

Compute:
  - Hourly DA price (Spain)
  - Hourly Spanish wind production (B18 + B19)
  - Hourly Spanish solar production (B01 + B16)
  - capture_price_tech_regime = sum(P_h × Q_h_tech) / sum(Q_h_tech)
  - cannibalization_ratio = capture_price / spot_mean

Output: results/regressions/renewable_capture_price.csv
"""
from __future__ import annotations

from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

PROJECT = Path(__file__).resolve().parents[3]
PRICE = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "precios" / "marginalpdbc_all.parquet"
VRE = PROJECT / "data" / "processed" / "entsoe" / "generation" / "wind_solar_actual_all.parquet"
OUT = PROJECT / "results" / "regressions" / "renewable_capture_price.csv"

WIND = ["B18", "B19"]
SOLAR = ["B01", "B16"]


def assign_regime(d) -> str:
    if d < pd.Timestamp("2024-06-14"):
        return "1.pre-IDA"
    if d < pd.Timestamp("2024-12-01"):
        return "2.3-sess"
    if d < pd.Timestamp("2025-03-19"):
        return "3.ISP15-win"
    if d < pd.Timestamp("2025-10-01"):
        return "4.DA60/ID15"
    return "5.DA15/ID15"


def main() -> None:
    print("[1/4] Hourly Spain DA prices...")
    con = duckdb.connect()
    con.execute("SET memory_limit='2GB'")
    px = con.sql(f"""
        SELECT date,
               CASE WHEN mtu_minutes = 15 THEN CEIL(period / 4.0)::INTEGER
                    ELSE period END AS hour,
               AVG(price_es_eur_mwh) AS p_da
        FROM '{PRICE}'
        WHERE price_es_eur_mwh IS NOT NULL
          AND CAST(date AS DATE) >= DATE '2018-01-01'
        GROUP BY 1, 2
    """).df()
    px["date"] = pd.to_datetime(px["date"])
    px["isp_start"] = px["date"] + pd.to_timedelta(px["hour"] - 1, unit="h")
    print(f"   hourly DA panel: {len(px):,} hours, "
          f"{px.date.min().date()} → {px.date.max().date()}")

    print("[2/4] Hourly Spanish wind + solar production...")
    vre = pd.read_parquet(VRE, columns=["isp_start_utc", "psr_type", "quantity_mw", "mtu_minutes"])
    vre = vre[vre["psr_type"].isin(WIND + SOLAR)].copy()
    # Strip tz, aggregate to hourly
    vre["isp_start_utc"] = pd.to_datetime(vre["isp_start_utc"]).dt.tz_localize(None)
    vre["hour_start"] = vre["isp_start_utc"].dt.floor("h")
    vre["mwh"] = vre["quantity_mw"] * (vre["mtu_minutes"] / 60.0)
    vre["tech"] = np.where(vre["psr_type"].isin(WIND), "Wind", "Solar")
    g = vre.groupby(["hour_start", "tech"], as_index=False)["mwh"].sum()
    wide = g.pivot(index="hour_start", columns="tech", values="mwh").fillna(0).reset_index()
    wide.columns.name = None
    print(f"   hourly wind+solar panel: {len(wide):,} hours")

    print("[3/4] Merge prices × VRE production, regime-tag...")
    panel = px[["isp_start", "p_da"]].merge(wide, left_on="isp_start", right_on="hour_start", how="inner")
    panel["regime"] = panel["isp_start"].dt.normalize().apply(assign_regime)
    panel["month"] = panel["isp_start"].dt.to_period("M").dt.to_timestamp()
    panel["hour_of_day"] = panel["isp_start"].dt.hour + 1
    print(f"   joined panel: {len(panel):,} hours")
    print(f"   wind range: {panel.Wind.min():.0f} - {panel.Wind.max():.0f} MWh; "
          f"solar range: {panel.Solar.min():.0f} - {panel.Solar.max():.0f} MWh")

    print("[4/4] Capture price + cannibalization ratio by regime + tech...")

    rows = []
    for regime, sub in panel.groupby("regime"):
        spot_mean = sub["p_da"].mean()
        for tech in ["Wind", "Solar"]:
            qcol = sub[tech]
            if qcol.sum() == 0:
                continue
            cap_price = (sub["p_da"] * qcol).sum() / qcol.sum()
            cann_ratio = cap_price / spot_mean
            rows.append({
                "regime": regime,
                "tech": tech,
                "n_hours": len(sub),
                "spot_mean_eur_mwh": spot_mean,
                "capture_price_eur_mwh": cap_price,
                "cannibalization_ratio": cann_ratio,
                "total_mwh_million": qcol.sum() / 1e6,
                "tech_share_of_load_pct": qcol.sum() / (qcol.sum() + sub["Wind"].sum() + sub["Solar"].sum() - qcol.sum()) * 100 if False else None,
            })

    out = pd.DataFrame(rows).sort_values(["regime", "tech"])
    print()
    print("Capture price by regime × tech:")
    print(out[["regime", "tech", "n_hours", "spot_mean_eur_mwh", "capture_price_eur_mwh", "cannibalization_ratio", "total_mwh_million"]]
          .to_string(index=False, float_format=lambda x: f"{x:.2f}"))

    # Hour-of-day decomposition for the post-MTU15-DA regime (most thesis-relevant)
    print()
    print("Hour-of-day capture profile (DA15/ID15 only):")
    da15 = panel[panel["regime"] == "5.DA15/ID15"]
    if len(da15) > 0:
        hod = da15.groupby("hour_of_day").agg(
            n=("p_da", "size"),
            mean_p=("p_da", "mean"),
            mean_wind_mw=("Wind", "mean"),
            mean_solar_mw=("Solar", "mean"),
        ).round(1)
        print(hod.to_string())

    # Reform-impact: did cannibalization improve at MTU15-DA?
    print()
    print("Reform-impact summary (cannibalization ratio movement across regimes):")
    pivot = out.pivot(index="regime", columns="tech", values="cannibalization_ratio").round(3)
    print(pivot.to_string())
    print()

    # Same-calendar-month robustness: compare DA60/ID15 (Mar-Sept) to DA15/ID15 (Oct-Dec) NOT directly comparable
    # Better: pull same-month-of-year pre-reform reference
    print("Same-calendar-month comparison (capture ratio in DA60/ID15-Apr-Sep vs same months pre-IDA, 2019-2023 mean):")
    panel["cal_month"] = panel["isp_start"].dt.month
    panel["year"] = panel["isp_start"].dt.year
    target_months = [4, 5, 6, 7, 8, 9]  # Apr-Sep
    rows2 = []
    for tech in ["Wind", "Solar"]:
        for label, mask in [
            ("DA60/ID15 Apr-Sep 2025", (panel["regime"] == "4.DA60/ID15") & panel["cal_month"].isin(target_months)),
            ("pre-IDA Apr-Sep 2019-2023", (panel["regime"] == "1.pre-IDA") & panel["cal_month"].isin(target_months) & (panel["year"] >= 2019) & (panel["year"] <= 2023)),
        ]:
            sub = panel[mask]
            if len(sub) == 0:
                continue
            spot_mean = sub["p_da"].mean()
            qcol = sub[tech]
            if qcol.sum() == 0:
                continue
            cap_price = (sub["p_da"] * qcol).sum() / qcol.sum()
            cann_ratio = cap_price / spot_mean
            rows2.append({
                "comparison": label,
                "tech": tech,
                "n_hours": len(sub),
                "spot_mean": spot_mean,
                "capture_price": cap_price,
                "cann_ratio": cann_ratio,
            })
    cal = pd.DataFrame(rows2)
    print(cal.to_string(index=False, float_format=lambda x: f"{x:.3f}"))

    OUT.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT, index=False)
    print(f"\nwrote {OUT}")


if __name__ == "__main__":
    main()
