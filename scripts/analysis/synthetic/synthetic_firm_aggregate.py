# STATUS: ALIVE
# LAST-AUDIT: 2026-04-26
# FEEDS: F7 (synthetic-firm method, Stage 4: regime aggregation + welfare)
# CLAIM: Per-regime market-power index (synthetic method) + consumer-surplus welfare estimates
"""Synthetic-firm method, Stage 4: per-regime aggregation.

Reads `synthetic_firm_clearing.csv` (per-ISP actual vs synthetic price)
and aggregates to per-regime statistics:

  * mean and weighted-mean market-power index (p_actual - p_synth)
  * relative market-power: (p_actual - p_synth) / p_actual
  * consumer-surplus loss approximation (Ciarreta-Espinosa Eq. 7-style)

Cross-check with published OMIE clearing price (marginalpdbc) as a
sanity test that our re-clearing reproduces the actual auction.
"""
from __future__ import annotations

from pathlib import Path
import duckdb
import numpy as np
import pandas as pd

PROJECT = Path(__file__).resolve().parents[3]
RES = PROJECT / "data" / "derived" / "results" / "synthetic_firm_clearing.csv"
PRICE = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "precios" / "marginalpdbc_all.parquet"
OUT = PROJECT / "data" / "derived" / "results" / "synthetic_firm_regime.csv"


def assign_regime(d):
    d = pd.Timestamp(d)
    if d < pd.Timestamp("2024-06-14"):
        return "pre-IDA"
    if d < pd.Timestamp("2024-12-01"):
        return "3-sess"
    if d < pd.Timestamp("2025-03-19"):
        return "ISP15 win"
    if d < pd.Timestamp("2025-10-01"):
        return "DA60/ID15"
    return "DA15/ID15"


def main() -> None:
    df = pd.read_csv(RES)
    df["date"] = pd.to_datetime(df["date"])
    df["regime"] = df["date"].apply(assign_regime)
    df = df.dropna(subset=["p_actual", "p_synth"]).copy()
    df["mp"] = df["p_actual"] - df["p_synth"]

    # ---- Sanity check: our p_actual vs published OMIE marginalpdbc ----
    print("[1/3] Sanity check — does our re-clearing reproduce published OMIE prices?")
    con = duckdb.connect()
    con.execute("SET memory_limit='1GB'")
    pub = con.sql(f"""
        SELECT date,
               period,
               price_es_eur_mwh AS p_published
        FROM '{PRICE}'
        WHERE price_es_eur_mwh IS NOT NULL
          AND CAST(date AS DATE) >= DATE '2024-06-01'
    """).df()
    pub["date"] = pd.to_datetime(pub["date"])
    cmp = df.merge(pub, on=["date", "period"], how="inner")
    cmp["err"] = cmp["p_actual"] - cmp["p_published"]
    print(f"  matched ISPs: {len(cmp):,}")
    print(f"  p_actual - p_published: mean={cmp['err'].mean():+.3f}, median={cmp['err'].median():+.3f}, std={cmp['err'].std():.3f}")
    print(f"  |err| <= 1 EUR/MWh: {(cmp['err'].abs() <= 1).mean()*100:.1f}% of ISPs")
    print(f"  |err| <= 5 EUR/MWh: {(cmp['err'].abs() <= 5).mean()*100:.1f}% of ISPs")

    # ---- Per-regime aggregation ----
    print()
    print("[2/3] Per-regime market-power index (synthetic method):")
    print()
    print(f"{'regime':<14}  {'n ISPs':>8}  {'mean p_actual':>13}  {'mean p_synth':>13}  {'mean MP':>10}  {'median MP':>10}  {'MP/p (%)':>10}")
    rows = []
    for r in ["pre-IDA", "3-sess", "ISP15 win", "DA60/ID15", "DA15/ID15"]:
        sub = df[df["regime"] == r]
        if len(sub) == 0:
            continue
        mean_p = sub["p_actual"].mean()
        mean_synth = sub["p_synth"].mean()
        mean_mp = sub["mp"].mean()
        median_mp = sub["mp"].median()
        rel_mp = (mean_mp / mean_p * 100) if mean_p > 0 else float("nan")
        rows.append({
            "regime": r, "n_isps": len(sub),
            "mean_p_actual": mean_p, "mean_p_synth": mean_synth,
            "mean_mp": mean_mp, "median_mp": median_mp,
            "rel_mp_pct": rel_mp,
        })
        print(
            f"{r:<14}  {len(sub):>8,}  {mean_p:>13.2f}  {mean_synth:>13.2f}  "
            f"{mean_mp:>+10.3f}  {median_mp:>+10.3f}  {rel_mp:>9.2f}%"
        )

    tab = pd.DataFrame(rows)

    # ---- Welfare approximation (Ciarreta-Espinosa Eq. 7) ----
    # ΔCS ≈ (p^s - p) × q + 0.5 × (p^s - p) × (q^s - q)
    # First term ≈ producer surplus shift; second ≈ deadweight loss.
    # We don't have q_actual / q_synth in the per-ISP file; would require
    # stage-2 to also output the cleared quantity at each price. For now,
    # produce per-regime monthly aggregates and a back-of-envelope
    # transfer estimate using mean prices and a typical ISP volume.
    print()
    print("[3/3] Back-of-envelope welfare transfer per regime:")
    print(
        "  Using mean q ≈ 25 GWh per hourly ISP (Spanish DA market 2024-2026 typical), "
        "ISP15 q ≈ 25/4 GWh per 15-min ISP."
    )
    df["q_proxy_mwh"] = np.where(
        df["date"] >= pd.Timestamp("2024-12-01"), 25_000 / 4, 25_000
    )
    df["transfer_eur"] = df["mp"] * df["q_proxy_mwh"]

    print(
        f"  {'regime':<14}  {'n ISPs':>8}  {'transfer (mean MP × q_proxy) (M€/regime)':>40}"
    )
    for r in ["pre-IDA", "3-sess", "ISP15 win", "DA60/ID15", "DA15/ID15"]:
        sub = df[df["regime"] == r]
        if len(sub) == 0:
            continue
        total_eur = sub["transfer_eur"].sum() / 1e6
        print(f"  {r:<14}  {len(sub):>8,}  {total_eur:>40.1f}")

    OUT.parent.mkdir(parents=True, exist_ok=True)
    tab.to_csv(OUT, index=False)
    print(f"\nwrote {OUT}")


if __name__ == "__main__":
    main()
