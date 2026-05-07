# STATUS: ALIVE
# LAST-AUDIT: 2026-04-27
# FEEDS: New economic angle — cross-border interconnection congestion mediation of IB market power
# CLAIM: IB's pivotality and rent extraction are higher when Spain-France interconnection is congested.

"""F11 — Cross-border interconnection congestion mediation.

When SP-FR interconnection is uncongested, French imports cap Spanish
prices: IB's unilateral price-setting power is bounded by what France
can supply. When the interconnection saturates ("Spain-as-an-island"),
imports cannot increase further — IB's market power is unbounded by
external supply.

Empirical proxy for interconnection congestion: |SP - FR| price gap.
When the gap is ≈ 0, the interconnection is uncongested (markets are
coupled and prices converge after transmission losses ~€2-3/MWh). When
the gap is large (positive: SP > FR + losses, indicating import
saturation; negative: SP < FR - losses, indicating export saturation),
the interconnection is at capacity.

Predictions:
  - Strong direction prediction: import-congestion (SP >> FR) ⇒
    higher mp_IB (IB's market power un-disciplined by French supply).
  - Symmetric: export-congestion (SP << FR) might also reduce IB's
    market power if Spanish prices are pulled down by export demand.
  - Uncoupled hours (large |gap|) > coupled hours (|gap| ≈ 0): mp_IB
    higher in uncoupled regime.

Output:
  results/regressions/f11_interconnection_mediation.csv
"""
from __future__ import annotations

from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import statsmodels.api as sm

PROJECT = Path(__file__).resolve().parents[3]
F7_ISP = PROJECT / "results" / "regressions" / "synthetic_firm_per_firm_isp.csv"
PRICE_SP = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "precios" / "marginalpdbc_all.parquet"
PRICE_FR = PROJECT / "data" / "processed" / "entsoe" / "prices" / "fr_da_all.parquet"
OUT = PROJECT / "results" / "regressions" / "f11_interconnection_mediation.csv"


def main() -> None:
    print("[1/4] Hourly Spain DA prices...")
    con = duckdb.connect()
    con.execute("SET memory_limit='2GB'")
    sp = con.sql(f"""
        SELECT date,
               CASE WHEN mtu_minutes = 15 THEN CEIL(period / 4.0)::INTEGER
                    ELSE period END AS hour,
               AVG(price_es_eur_mwh) AS p_sp
        FROM '{PRICE_SP}'
        WHERE price_es_eur_mwh IS NOT NULL
          AND CAST(date AS DATE) >= DATE '2024-06-01'
        GROUP BY 1, 2
    """).df()
    sp["date"] = pd.to_datetime(sp["date"])
    sp["isp_start"] = sp["date"] + pd.to_timedelta(sp["hour"] - 1, unit="h")
    print(f"   SP DA hourly: {len(sp):,} hours")

    print("[2/4] Hourly France DA prices (60-min product)...")
    fr = pd.read_parquet(PRICE_FR)
    fr["isp_start"] = pd.to_datetime(fr["isp_start_utc"]).dt.tz_localize(None)
    # FR prices appear at 60-min granularity. Aggregate any 15-min noise to hour.
    fr["isp_start"] = fr["isp_start"].dt.floor("h")
    fr_h = fr.groupby("isp_start", as_index=False)["price_eur_per_mwh"].mean().rename(columns={"price_eur_per_mwh": "p_fr"})
    print(f"   FR DA hourly: {len(fr_h):,} hours")

    print("[3/4] Merge SP-FR + F7 hourly mp_IB...")
    px = sp[["isp_start", "p_sp"]].merge(fr_h, on="isp_start", how="inner")
    px["gap_sp_fr"] = px["p_sp"] - px["p_fr"]
    px["abs_gap"] = px["gap_sp_fr"].abs()

    iso = pd.read_csv(F7_ISP)
    iso["date"] = pd.to_datetime(iso["date"])
    iso = iso[(iso["regime"].isin(["DA60/ID15", "DA15/ID15"])) & (iso["p_actual"] > 0)].copy()
    iso["hour"] = np.where(iso["regime"] == "DA60/ID15", iso["period"], np.ceil(iso["period"] / 4.0).astype(int))
    iso["isp_start"] = iso["date"] + pd.to_timedelta(iso["hour"] - 1, unit="h")
    panel = iso.merge(px, on="isp_start", how="inner")
    print(f"   joined panel: {len(panel):,} ISPs")
    print(f"   gap (SP-FR) summary: mean {panel.gap_sp_fr.mean():.2f}, "
          f"median {panel.gap_sp_fr.median():.2f}, "
          f"p10/p90 {panel.gap_sp_fr.quantile(0.1):.2f}/{panel.gap_sp_fr.quantile(0.9):.2f}")

    print()
    print("[4/4] Pivotality conditional on interconnection state:")
    # Bucket by gap
    panel["gap_bucket"] = pd.cut(
        panel["gap_sp_fr"],
        bins=[-np.inf, -10, -3, 3, 10, np.inf],
        labels=["1.export-cong (SP<<FR)", "2.export-cpld", "3.coupled (≈0)", "4.import-cpld", "5.import-cong (SP>>FR)"],
    )
    by_bucket = panel.groupby("gap_bucket", observed=True).agg(
        n=("mp_IB", "size"),
        mean_p_sp=("p_sp", "mean"),
        mean_p_fr=("p_fr", "mean"),
        mean_gap=("gap_sp_fr", "mean"),
        mean_mp_IB=("mp_IB", "mean"),
        median_mp_IB=("mp_IB", "median"),
        rel_markup_pct=("mp_IB", lambda x: x.mean() / panel.loc[x.index, "p_sp"].mean() * 100),
        share_strong_pivotal=("mp_IB", lambda x: ((x >= 5)).mean() * 100),
    ).round(2)
    print(by_bucket.to_string())
    print()

    # Coupled vs Uncoupled comparison
    panel["coupled"] = (panel["abs_gap"] <= 3).astype(int)
    coupled_summary = panel.groupby("coupled").agg(
        n=("mp_IB", "size"),
        mean_p_sp=("p_sp", "mean"),
        mean_mp_IB=("mp_IB", "mean"),
        rel_markup_pct=("mp_IB", lambda x: x.mean() / panel.loc[x.index, "p_sp"].mean() * 100),
        share_strong_pivotal=("mp_IB", lambda x: ((x >= 5)).mean() * 100),
    ).round(2)
    print("Coupled (|gap|≤€3) vs Uncoupled comparison:")
    print(coupled_summary.to_string())
    print()

    # Regression: mp_IB on gap with regime + hour FE
    panel["hour_of_day"] = panel["isp_start"].dt.hour + 1
    panel["cal_month"] = panel["isp_start"].dt.month
    hod = pd.get_dummies(panel["hour_of_day"], prefix="hod", drop_first=True, dtype=float)
    cm = pd.get_dummies(panel["cal_month"], prefix="cm", drop_first=True, dtype=float)
    rg = pd.get_dummies(panel["regime"], prefix="rg", drop_first=True, dtype=float)
    X = pd.concat([panel[["gap_sp_fr", "abs_gap"]], hod, cm, rg], axis=1).astype(float)
    X = sm.add_constant(X)
    y = panel["mp_IB"].astype(float)
    res = sm.OLS(y, X).fit(cov_type="HC3")
    print(f"Regression: mp_IB ~ gap + |gap| + hour FE + month FE + regime FE")
    print(f"  N={len(panel)}; R²={res.rsquared:.3f}")
    print(f"  β(gap_sp_fr): {res.params['gap_sp_fr']:>+.4f}  SE={res.bse['gap_sp_fr']:.4f}  p={res.pvalues['gap_sp_fr']:.4f}")
    print(f"  β(|gap|):     {res.params['abs_gap']:>+.4f}  SE={res.bse['abs_gap']:.4f}  p={res.pvalues['abs_gap']:.4f}")
    print()

    # Counterfactual estimate: if Spain-France always coupled (gap=0), what would IB's transfer be?
    # Use the gap coefficient × actual gap × q_IB
    # But this requires q_IB: pull from pdbce
    print("Counterfactual: if interconnection always coupled (gap_sp_fr → 0), how much smaller would F7 IB transfer be?")
    pdbce = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbce_all.parquet"
    ib_q = con.sql(f"""
        SELECT date, period,
               SUM(assigned_power_mw) / CASE WHEN MAX(mtu_minutes) = 15 THEN 4.0 ELSE 1.0 END AS q_ib
        FROM '{pdbce}'
        WHERE grupo_empresarial = 'IB' AND offer_type = 1 AND assigned_power_mw > 0
          AND CAST(date AS DATE) >= DATE '2025-03-19'
        GROUP BY 1, 2
    """).df()
    ib_q["date"] = pd.to_datetime(ib_q["date"])
    panel = panel.merge(ib_q, on=["date", "period"], how="left")
    panel["q_ib"] = panel["q_ib"].fillna(0)
    panel["transfer_eur"] = panel["mp_IB"] * panel["q_ib"]

    # Counterfactual mp_IB if gap_sp_fr = 0
    beta_gap = res.params["gap_sp_fr"]
    panel["counterfactual_mp_IB"] = panel["mp_IB"] - beta_gap * panel["gap_sp_fr"]
    panel["counterfactual_transfer"] = panel["counterfactual_mp_IB"] * panel["q_ib"]
    actual_total_M = panel["transfer_eur"].sum() / 1e6
    cf_total_M = panel["counterfactual_transfer"].sum() / 1e6
    delta_M = actual_total_M - cf_total_M
    print(f"  Actual total IB transfer in panel: €{actual_total_M:.1f}M")
    print(f"  Counterfactual (if always coupled): €{cf_total_M:.1f}M")
    print(f"  Implied gap-mediated rent: €{delta_M:.1f}M ({delta_M/actual_total_M*100:.1f}% of total)")
    print()

    # Save bucketed output
    OUT.parent.mkdir(parents=True, exist_ok=True)
    summary_rows = []
    by_bucket = by_bucket.reset_index()
    by_bucket["_table"] = "by_gap_bucket"
    summary_rows.append(by_bucket)
    coupled_summary = coupled_summary.reset_index()
    coupled_summary["_table"] = "coupled_vs_uncoupled"
    summary_rows.append(coupled_summary)
    pd.concat(summary_rows, ignore_index=True, sort=False).to_csv(OUT, index=False)
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
