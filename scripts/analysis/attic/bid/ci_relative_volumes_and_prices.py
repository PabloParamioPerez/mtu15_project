# STATUS: ALIVE
# LAST-AUDIT: 2026-05-19
# CLAIM: Continuous-intraday volume and price evidence in relative terms:
#        (a) CI volume / (DA + IDA + CI) per tech per regime — share of trading
#            in OMIE auctions vs continuous;
#        (b) Bilateral share (PDBF - PDBC) / PDBF per tech per regime;
#        (c) Mean CI trade price per tech per regime, compared to DA price;
#        (d) CI price relative to DA price (the CI premium/discount).
#
#        Per-tech, per-regime, daily aggregation, seasonality control via
#        Fourier(K=4) FWL regression.
#
# OUT: results/regressions/bid/ci_relative/
#        per_tech_shares.csv       — DA / IDA / CI / Bilateral shares per regime
#        per_tech_prices.csv       — CI mean price + DA price + ratio per regime

from __future__ import annotations
from pathlib import Path
import duckdb
import numpy as np
import pandas as pd
import statsmodels.api as sm

REPO = Path(__file__).resolve().parents[3]
TRADES = REPO / "data/processed/omie/mercado_intradiario_continuo/transacciones/trades_all.parquet"
PDBC = REPO / "data/processed/omie/mercado_diario/programas/pdbc_all.parquet"
PDBF = REPO / "data/processed/omie/mercado_diario/programas/pdbf_all.parquet"
PIBCI = REPO / "data/processed/omie/mercado_intradiario_subastas/programas/pibci_all.parquet"
MPDBC = REPO / "data/processed/omie/mercado_diario/precios/marginalpdbc_all.parquet"
UNITS = REPO / "data/external/omie_reference/lista_unidades.csv"
OUT_DIR = REPO / "results/regressions/bid/ci_relative"

START = "2022-01-01"
END = "2026-05-15"

REGIME_DATES = [
    ("3sess",         pd.Timestamp("2024-06-14"), pd.Timestamp("2024-11-30")),
    ("ISP15win",      pd.Timestamp("2024-12-01"), pd.Timestamp("2025-03-18")),
    ("MTU15IDA_pre",  pd.Timestamp("2025-03-19"), pd.Timestamp("2025-04-27")),
    ("MTU15IDA_post", pd.Timestamp("2025-04-28"), pd.Timestamp("2025-09-30")),
    ("DA15_ID15",     pd.Timestamp("2025-10-01"), pd.Timestamp("2026-05-15")),
]


def tech_bucket(t):
    if t is None: return "Other"
    t = str(t).lower()
    if "ciclo combinado" in t: return "CCGT"
    if "nuclear" in t: return "Nuclear"
    if "hidráulica generación" in t: return "Hydro"
    if "bombeo" in t: return "Hydro_pump"
    if "re mercado eólica" in t: return "Wind"
    if "re mercado solar fotovolt" in t: return "Solar_PV"
    return "Other"


def regime_of(d):
    d = pd.Timestamp(d).date()
    for label, lo, hi in REGIME_DATES:
        if lo.date() <= d <= hi.date():
            return label
    return "preIDA"


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute("SET memory_limit='16GB'")
    con.execute("SET threads=4")
    units = pd.read_csv(UNITS)
    units["tech"] = units["technology"].apply(tech_bucket)
    con.register("u", units[["unit_code", "tech"]])

    # ============ DA cleared MWh per tech per day (PDBC, no bilaterals) ============
    print("Computing DA cleared MWh per tech per day from PDBC...")
    da = con.execute(f"""
    SELECT CAST(date AS DATE) AS d, u.tech,
           SUM(assigned_power_mw * COALESCE(mtu_minutes, 60)/60.0) AS da_mwh
    FROM read_parquet('{PDBC}') p
    JOIN u ON p.unit_code = u.unit_code
    WHERE date >= '{START}' AND date <= '{END}' AND assigned_power_mw > 0
    GROUP BY 1, 2 ORDER BY 1, 2
    """).fetchdf()
    da["d"] = pd.to_datetime(da["d"])
    print(f"  da: {len(da):,} rows")

    # ============ PDBF cleared MWh per tech per day (includes bilaterals) ============
    print("Computing PDBF cleared MWh per tech per day...")
    pdbf = con.execute(f"""
    SELECT CAST(date AS DATE) AS d, u.tech,
           SUM(assigned_power_mw * COALESCE(mtu_minutes, 60)/60.0) AS pdbf_mwh
    FROM read_parquet('{PDBF}') p
    JOIN u ON p.unit_code = u.unit_code
    WHERE date >= '{START}' AND date <= '{END}' AND assigned_power_mw > 0
    GROUP BY 1, 2 ORDER BY 1, 2
    """).fetchdf()
    pdbf["d"] = pd.to_datetime(pdbf["d"])
    print(f"  pdbf: {len(pdbf):,} rows")

    # ============ IDA cleared MWh per tech per day (PIBCI, all sessions) ============
    print("Computing IDA cleared (gross) MWh per tech per day...")
    ida = con.execute(f"""
    SELECT CAST(date AS DATE) AS d, u.tech,
           SUM(ABS(assigned_power_mw) * COALESCE(mtu_minutes, 60)/60.0) AS ida_mwh
    FROM read_parquet('{PIBCI}') p
    JOIN u ON p.unit_code = u.unit_code
    WHERE date >= '{START}' AND date <= '{END}'
    GROUP BY 1, 2 ORDER BY 1, 2
    """).fetchdf()
    ida["d"] = pd.to_datetime(ida["d"])
    print(f"  ida: {len(ida):,} rows")

    # ============ CI volume per tech per day (trades, gross MWh per seller's tech) ============
    print("Computing CI volume + mean price per tech per day...")
    ci = con.execute(f"""
    SELECT CAST(delivery_date AS DATE) AS d, u.tech,
           SUM(quantity_mw * mtu_minutes/60.0) AS ci_mwh,
           SUM(quantity_mw * mtu_minutes/60.0 * price_eur_mwh) /
               NULLIF(SUM(quantity_mw * mtu_minutes/60.0), 0) AS ci_price_wavg
    FROM read_parquet('{TRADES}') t
    JOIN u ON t.seller_unit = u.unit_code
    WHERE delivery_date >= '{START}' AND delivery_date <= '{END}'
    GROUP BY 1, 2 ORDER BY 1, 2
    """).fetchdf()
    ci["d"] = pd.to_datetime(ci["d"])
    print(f"  ci: {len(ci):,} rows")

    # ============ DA price daily mean ============
    da_p = con.execute(f"""
    SELECT CAST(date AS DATE) AS d, AVG(price_es_eur_mwh) AS da_price
    FROM read_parquet('{MPDBC}')
    WHERE date >= '{START}' AND date <= '{END}'
    GROUP BY 1 ORDER BY 1
    """).fetchdf()
    da_p["d"] = pd.to_datetime(da_p["d"])

    # ============ Merge per (d, tech) ============
    df = da.merge(pdbf, on=["d", "tech"], how="outer").merge(ida, on=["d", "tech"], how="outer").merge(ci, on=["d", "tech"], how="outer")
    df = df.merge(da_p, on="d", how="left")
    df["regime"] = df["d"].apply(regime_of)

    for c in ["da_mwh", "pdbf_mwh", "ida_mwh", "ci_mwh"]:
        df[c] = df[c].fillna(0)
    df["bilat_mwh"] = (df["pdbf_mwh"] - df["da_mwh"]).clip(lower=0)
    df["omie_total_mwh"] = df["da_mwh"] + df["ida_mwh"] + df["ci_mwh"]
    df["ci_share_omie"] = df["ci_mwh"] / df["omie_total_mwh"].replace(0, np.nan)
    df["bilat_share_pdbf"] = df["bilat_mwh"] / df["pdbf_mwh"].replace(0, np.nan)
    df["ci_price_vs_da"] = df["ci_price_wavg"] - df["da_price"]

    focus_techs = ["CCGT", "Nuclear", "Hydro", "Hydro_pump", "Wind", "Solar_PV"]
    print("\n" + "=" * 100)
    print("VOLUMES PER TECH PER REGIME (GWh/day means, raw — Fourier-deseasonalization not applied to ratios)")
    print("=" * 100)
    for tech in focus_techs:
        sub = df[df["tech"] == tech]
        agg = sub.groupby("regime").agg(
            da_gwh=("da_mwh", lambda x: x.mean()/1000),
            ida_gwh=("ida_mwh", lambda x: x.mean()/1000),
            ci_gwh=("ci_mwh", lambda x: x.mean()/1000),
            bilat_gwh=("bilat_mwh", lambda x: x.mean()/1000),
            ci_share=("ci_share_omie", "mean"),
            bilat_share=("bilat_share_pdbf", "mean"),
            ci_price=("ci_price_wavg", "mean"),
            da_price=("da_price", "mean"),
            n=("d", "count"),
        )
        agg = agg.reindex(["preIDA", "3sess", "ISP15win", "MTU15IDA_pre", "MTU15IDA_post", "DA15_ID15"])
        print(f"\n--- {tech} ---")
        print(agg.round(2).to_string())

    # System-wide totals
    print("\n--- SYSTEM TOTAL (all techs) ---")
    sys = df.groupby(["d", "regime"]).agg(
        da_mwh=("da_mwh", "sum"), ida_mwh=("ida_mwh", "sum"),
        ci_mwh=("ci_mwh", "sum"), pdbf_mwh=("pdbf_mwh", "sum"),
        bilat_mwh=("bilat_mwh", "sum"), da_price=("da_price", "first"),
    ).reset_index()
    sys["omie_total_mwh"] = sys["da_mwh"] + sys["ida_mwh"] + sys["ci_mwh"]
    sys["ci_share_omie"] = sys["ci_mwh"] / sys["omie_total_mwh"]
    sys["bilat_share_pdbf"] = sys["bilat_mwh"] / sys["pdbf_mwh"].replace(0, np.nan)
    sys_agg = sys.groupby("regime").agg(
        da_gwh=("da_mwh", lambda x: x.mean()/1000),
        ida_gwh=("ida_mwh", lambda x: x.mean()/1000),
        ci_gwh=("ci_mwh", lambda x: x.mean()/1000),
        bilat_gwh=("bilat_mwh", lambda x: x.mean()/1000),
        pdbf_gwh=("pdbf_mwh", lambda x: x.mean()/1000),
        ci_share_omie=("ci_share_omie", "mean"),
        bilat_share_pdbf=("bilat_share_pdbf", "mean"),
    ).reindex(["preIDA", "3sess", "ISP15win", "MTU15IDA_pre", "MTU15IDA_post", "DA15_ID15"])
    print(sys_agg.round(3).to_string())

    df.to_csv(OUT_DIR / "per_tech_daily.csv", index=False)
    sys_agg.to_csv(OUT_DIR / "system_shares_per_regime.csv")
    print(f"\nwrote {OUT_DIR}/")


if __name__ == "__main__":
    main()
