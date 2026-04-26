# STATUS: ALIVE
# LAST-AUDIT: 2026-04-27
# FEEDS: D6 candidate (aFRR offer-curve depth across regimes)
# CLAIM: aFRR system-aggregate offer-curve depth/slope evolves across reform regimes — descriptive supplement to S5/S6/S8
"""aFRR offer-curve depth across reform regimes (curvas_ofertas_afrr).

The ESIOS Curvas_Ofertas_aFRR panel is the system-aggregate aFRR
offer curve at PT15M resolution from 2024-11-20 onwards. NOT per-firm
— REE publishes the aggregate up/down offer curve per ISP, with each
row being one tranche of the merit order.

This script computes per-regime descriptive statistics on the
offer curve to characterise market depth, granularity, and price
level across the post-ISP15 reform regimes:

  - ISP15-win  (2024-12-01 → 2025-03-18, ~108 days in our window)
  - DA60/ID15  (2025-03-19 → 2025-09-30, ~196 days)
  - DA15/ID15  (2025-10-01 → 2026-04-26, ~210 days)

Plus the partial pre-ISP15 (2024-11-20 → 2024-11-30, ~11 days,
labelled "3-sess").

Metrics per (regime, direction):
  - mean_total_mw         — total MW offered per ISP (volume depth)
  - mean_n_tranches       — median tranches per ISP (offer granularity)
  - median_price          — median tranche price (level)
  - q90_minus_q10_eur     — within-ISP 90-10 price spread (slope proxy)
  - frac_zero_price       — share of MW offered at €0 (capacity-show)

Output:
    data/derived/results/afrr_offer_depth.csv

Reading: this is descriptive — confirms or contradicts the S5/S6/S8
system-layer narrative from the *supply-curve* angle. If aFRR depth
shrinks at MTU15-DA, this complements S6 (asymmetric-granularity
fiscal shift collapses). If depth STAYS thin post-MTU15-DA, it
complements S8 (redispatch escalation persists).
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

PROJECT = Path(__file__).resolve().parents[3]
PANEL = PROJECT / "data" / "processed" / "esios" / "reservas" / "curvas_ofertas_afrr_all.parquet"
OUT = PROJECT / "data" / "derived" / "results" / "afrr_offer_depth.csv"

ISP15_REFORM = pd.Timestamp("2024-12-01")
MTU15_IDA    = pd.Timestamp("2025-03-19")
MTU15_DA     = pd.Timestamp("2025-10-01")


def assign_regime(d: pd.Timestamp) -> str:
    if d < ISP15_REFORM:
        return "3-sess (partial)"
    if d < MTU15_IDA:
        return "ISP15 win"
    if d < MTU15_DA:
        return "DA60/ID15"
    return "DA15/ID15"


def main() -> None:
    if not PANEL.exists():
        print(f"Missing {PANEL}. Run 20_build_curvas_ofertas_afrr_all.py first.")
        return

    print(f"[load] {PANEL}")
    df = pd.read_parquet(PANEL)
    df["date"] = pd.to_datetime(df["date"])
    df["regime"] = df["date"].apply(assign_regime)

    # Sanity: drop NaN price/mw rows (rare)
    df = df.dropna(subset=["price_eur_mw", "mw"]).copy()
    df = df[df["mw"] > 0]

    # Per-ISP aggregates
    isp_agg = df.groupby(["date", "isp", "direction", "regime"]).agg(
        total_mw=("mw", "sum"),
        n_tranches=("mw", "size"),
        median_price=("price_eur_mw", "median"),
        q10=("price_eur_mw", lambda s: s.quantile(0.10)),
        q90=("price_eur_mw", lambda s: s.quantile(0.90)),
        zero_mw=("mw", lambda s: ((s.values * (df.loc[s.index, "price_eur_mw"].values == 0).astype(int)).sum())),
    ).reset_index()
    isp_agg["q90_q10_spread"] = isp_agg["q90"] - isp_agg["q10"]
    isp_agg["frac_zero_mw"] = isp_agg["zero_mw"] / isp_agg["total_mw"] * 100

    # Per-regime per-direction means
    REGIME_ORDER = ["3-sess (partial)", "ISP15 win", "DA60/ID15", "DA15/ID15"]
    rows = []
    for direction in ("Subir", "Bajar"):
        print()
        print("=" * 90)
        print(f"  Direction: {direction}")
        print("=" * 90)
        print(f"{'regime':<22}{'n_isps':>9}{'mean_MW':>11}{'mean_tranches':>15}{'median_eur':>12}{'q90-q10':>10}{'%MW@0':>8}")
        for reg in REGIME_ORDER:
            sub = isp_agg[(isp_agg["regime"] == reg) & (isp_agg["direction"] == direction)]
            if sub.empty:
                continue
            row = {
                "direction": direction,
                "regime": reg,
                "n_isps": len(sub),
                "mean_total_mw": sub["total_mw"].mean(),
                "mean_n_tranches": sub["n_tranches"].mean(),
                "median_price": sub["median_price"].median(),
                "q90_q10_spread": sub["q90_q10_spread"].median(),
                "frac_zero_mw_pct": sub["frac_zero_mw"].median(),
            }
            rows.append(row)
            print(
                f"{reg:<22}{len(sub):>9,}"
                f"{row['mean_total_mw']:>11.1f}"
                f"{row['mean_n_tranches']:>15.1f}"
                f"{row['median_price']:>12.2f}"
                f"{row['q90_q10_spread']:>10.2f}"
                f"{row['frac_zero_mw_pct']:>7.1f}%"
            )

    out = pd.DataFrame(rows)
    OUT.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT, index=False)
    print()
    print(f"Wrote {OUT}")


if __name__ == "__main__":
    main()
