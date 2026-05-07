# STATUS: ALIVE
# LAST-AUDIT: 2026-04-27
# FEEDS: F8 mechanism refinement / IB-canonical synthesis (within-sample mechanism test)
# CLAIM: IB's monthly market-power transfer scales with IB's monthly hydro generation share. Direct within-sample test of the F8 mechanism (Bushnell-style strategic hydro dispatch).
"""F8 mechanism within-sample test: IB Lerner ~ IB hydro availability (monthly).

If F8 is right ("IB strategically dispatches its hydro fleet into
high-price hours"), then IB's market-power signal should track IB's
hydro-generation availability month by month. In wet/high-reservoir
months, IB has more dispatchable hydro to optimize → larger Lerner.
In dry/low-reservoir months, less hydro flexibility → smaller Lerner.

Window: post-MTU15-IDA (March 2025 – present), 11 monthly observations.
The 2020–2021 hydrologic anomaly (where IB > GE in sell-side cleared
volume due to wet years) suggests IB's market position is meaningfully
hydrology-dependent.

Test:
  Regress monthly IB synthetic-firm market-power transfer (mp_IB,
  EUR-millions/month) on monthly IB hydro cleared GWh.

Hypothesis F8 (mechanism): β > 0 — wetter months → larger IB transfer.
Hypothesis alt (structural): β ≈ 0 — IB transfer is constant regardless
of hydrology.

Output:
    results/regressions/f8_hydrologic_correlation.csv
"""
from __future__ import annotations

from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import statsmodels.api as sm

PROJECT = Path(__file__).resolve().parents[3]
ISP_FILE = PROJECT / "results" / "regressions" / "synthetic_firm_per_firm_isp.csv"
PDBCE = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbce_all.parquet"
REF = PROJECT / "data" / "external" / "omie_reference" / "lista_unidades.csv"
OUT = PROJECT / "results" / "regressions" / "f8_hydrologic_correlation.csv"


def main() -> None:
    print("[1/4] Load IB synthetic-firm per-ISP market-power data...")
    df = pd.read_csv(ISP_FILE)
    df["date"] = pd.to_datetime(df["date"])
    # Restrict to post-MTU15-IDA (mp data is meaningful only here per F7 caveat 1)
    df = df[df["date"] >= pd.Timestamp("2025-03-19")].copy()
    df = df.dropna(subset=["mp_IB"])
    print(f"   post-MTU15-IDA ISPs: {len(df):,}")

    # Per-ISP transfer: mp_IB € × 25 GWh / 4 (ISP15 = 25/4 GWh per period)
    df["transfer_eur"] = df["mp_IB"] * 25_000 / 4
    df["month"] = df["date"].dt.to_period("M").dt.to_timestamp()
    monthly_mp = df.groupby("month", as_index=False).agg(
        ib_transfer_eur=("transfer_eur", "sum"),
        ib_mp_mean=("mp_IB", "mean"),
        n_isps=("mp_IB", "size"),
    )
    monthly_mp["ib_transfer_meur"] = monthly_mp["ib_transfer_eur"] / 1e6
    print(f"   monthly IB mp panel: {len(monthly_mp)} months")

    # IB hydro cleared GWh per month
    print()
    print("[2/4] IB hydro cleared GWh per month from pdbce...")
    ref = pd.read_csv(REF, encoding="latin1")
    ref["tech_low"] = ref["technology"].fillna("").astype(str).str.lower()
    hydro_units = ref[
        ref["tech_low"].str.contains("hidr", regex=False)
    ]["unit_code"].tolist()

    con = duckdb.connect()
    con.execute("SET memory_limit='2GB'")
    con.execute("SET threads=4")
    con.register("hydro_units", pd.DataFrame({"unit_code": hydro_units}))

    ib_hy = con.sql(f"""
        SELECT DATE_TRUNC('month', CAST(p.date AS DATE)) AS month,
               p.grupo_empresarial AS firm,
               SUM(p.assigned_power_mw)
                 / CASE WHEN p.mtu_minutes = 15 THEN 4.0 ELSE 1.0 END AS q_mwh
        FROM '{PDBCE}' p
        JOIN hydro_units h USING (unit_code)
        WHERE p.offer_type = 1
          AND p.assigned_power_mw IS NOT NULL
          AND p.assigned_power_mw > 0
          AND CAST(p.date AS DATE) >= DATE '2025-03-01'
          AND p.grupo_empresarial = 'IB'
        GROUP BY month, firm, p.mtu_minutes
    """).df()
    ib_hy = ib_hy.groupby("month", as_index=False)["q_mwh"].sum()
    ib_hy["ib_hydro_gwh"] = ib_hy["q_mwh"] / 1e3
    ib_hy["month"] = pd.to_datetime(ib_hy["month"])
    print(f"   IB hydro panel: {len(ib_hy)} months")

    # Also: IB total cleared GWh per month (for share)
    ib_total = con.sql(f"""
        SELECT DATE_TRUNC('month', CAST(p.date AS DATE)) AS month,
               SUM(p.assigned_power_mw)
                 / CASE WHEN p.mtu_minutes = 15 THEN 4.0 ELSE 1.0 END AS q_mwh
        FROM '{PDBCE}' p
        WHERE p.offer_type = 1
          AND p.assigned_power_mw IS NOT NULL
          AND p.assigned_power_mw > 0
          AND CAST(p.date AS DATE) >= DATE '2025-03-01'
          AND p.grupo_empresarial = 'IB'
        GROUP BY month, p.mtu_minutes
    """).df()
    ib_total = ib_total.groupby("month", as_index=False)["q_mwh"].sum()
    ib_total["ib_total_gwh"] = ib_total["q_mwh"] / 1e3
    ib_total["month"] = pd.to_datetime(ib_total["month"])

    # Spain-system hydro generation per month (proxy for hydrology — wet vs dry)
    sys_hy = con.sql(f"""
        SELECT DATE_TRUNC('month', CAST(p.date AS DATE)) AS month,
               SUM(p.assigned_power_mw)
                 / CASE WHEN p.mtu_minutes = 15 THEN 4.0 ELSE 1.0 END AS q_mwh
        FROM '{PDBCE}' p
        JOIN hydro_units h USING (unit_code)
        WHERE p.offer_type = 1 AND p.assigned_power_mw > 0
          AND CAST(p.date AS DATE) >= DATE '2025-03-01'
        GROUP BY month, p.mtu_minutes
    """).df()
    sys_hy = sys_hy.groupby("month", as_index=False)["q_mwh"].sum()
    sys_hy["sys_hydro_gwh"] = sys_hy["q_mwh"] / 1e3
    sys_hy["month"] = pd.to_datetime(sys_hy["month"])

    # Merge all
    panel = monthly_mp.merge(ib_hy[["month", "ib_hydro_gwh"]], on="month", how="left")
    panel = panel.merge(ib_total[["month", "ib_total_gwh"]], on="month", how="left")
    panel = panel.merge(sys_hy[["month", "sys_hydro_gwh"]], on="month", how="left")
    panel["ib_hydro_share_of_ib"] = panel["ib_hydro_gwh"] / panel["ib_total_gwh"] * 100
    panel["ib_hydro_share_of_sys"] = panel["ib_hydro_gwh"] / panel["sys_hydro_gwh"] * 100

    print()
    print("=" * 100)
    print("Monthly IB market-power transfer vs IB hydro generation (post-MTU15-IDA)")
    print("=" * 100)
    print()
    print(panel[["month", "n_isps", "ib_transfer_meur", "ib_mp_mean", "ib_hydro_gwh",
                "ib_total_gwh", "ib_hydro_share_of_ib", "sys_hydro_gwh", "ib_hydro_share_of_sys"]].round(2).to_string(index=False))

    # Correlation tests
    print()
    print("[3/4] Pearson correlations:")
    print(f"  ib_transfer_meur ~ ib_hydro_gwh:           ρ = {panel['ib_transfer_meur'].corr(panel['ib_hydro_gwh']):.3f}")
    print(f"  ib_transfer_meur ~ ib_hydro_share_of_ib:   ρ = {panel['ib_transfer_meur'].corr(panel['ib_hydro_share_of_ib']):.3f}")
    print(f"  ib_transfer_meur ~ sys_hydro_gwh:          ρ = {panel['ib_transfer_meur'].corr(panel['sys_hydro_gwh']):.3f}")
    print(f"  ib_mp_mean ~ ib_hydro_gwh:                 ρ = {panel['ib_mp_mean'].corr(panel['ib_hydro_gwh']):.3f}")

    # Regression
    print()
    print("[4/4] Regression: IB monthly transfer ~ IB hydro GWh")
    Y = panel["ib_transfer_meur"].astype(float)
    X = panel[["ib_hydro_gwh"]].astype(float).assign(const=1.0)
    res = sm.OLS(Y, X).fit(cov_type="HC3")
    print(f"  N: {len(panel)} months")
    print(f"  β(ib_hydro_gwh): {res.params['ib_hydro_gwh']:>+8.4f} M€ per GWh hydro")
    print(f"     SE:           {res.bse['ib_hydro_gwh']:.4f}")
    print(f"     p-value:      {res.pvalues['ib_hydro_gwh']:.3f}")
    print(f"  R²: {res.rsquared:.3f}")
    print()
    print("  Multi-control regression (IB hydro + IB total cleared):")
    X2 = panel[["ib_hydro_gwh", "ib_total_gwh"]].astype(float).assign(const=1.0)
    res2 = sm.OLS(Y, X2).fit(cov_type="HC3")
    for col in ["ib_hydro_gwh", "ib_total_gwh"]:
        b, se, p = res2.params[col], res2.bse[col], res2.pvalues[col]
        print(f"  β({col:<18}): {b:>+8.4f}  SE={se:.4f}  p={p:.3f}")
    print(f"  R² = {res2.rsquared:.3f}")

    print()
    print("=" * 100)
    print("Verdict")
    print("=" * 100)
    coef = res.params["ib_hydro_gwh"]
    p = res.pvalues["ib_hydro_gwh"]
    if coef > 0 and p < 0.10:
        print(f"  ✓ POSITIVE significant: β = €{coef:.3f}M per GWh of IB hydro (p={p:.3f}).")
        print(f"    Months with more IB hydro generation see larger IB market-power transfer.")
        print(f"    F8 mechanism (Bushnell-style strategic hydro dispatch) supported within-sample.")
    elif coef > 0:
        print(f"  ≈ POSITIVE but not significant: β = €{coef:.3f}M per GWh (p={p:.3f}).")
    else:
        print(f"  ✗ NULL or NEGATIVE: β = €{coef:.3f}M per GWh (p={p:.3f}).")
        print(f"    F8 mechanism not visible in within-sample monthly variation.")
        print(f"    IB's market power may be more structural (capacity-based) than dispatch-dependent.")

    OUT.parent.mkdir(parents=True, exist_ok=True)
    panel.to_csv(OUT, index=False)
    print(f"\nwrote {OUT}")


if __name__ == "__main__":
    main()
