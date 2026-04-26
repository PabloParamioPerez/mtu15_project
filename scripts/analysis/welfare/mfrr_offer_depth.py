# STATUS: ALIVE
# LAST-AUDIT: 2026-04-27
# FEEDS: F9 supplement / S5 supplement (mFRR offer-curve depth across reform regimes)
# CLAIM: mFRR system-aggregate offer-curve depth/slope responds to the 2024-06 IDA reform; supplements aFRR finding
"""mFRR offer-curve depth across reform regimes (REE_BalancingEnerBids).

Analogue of `afrr_offer_depth.py` but for tertiary regulation (mFRR).
ESIOS REE_BalancingEnerBids (id=181) provides the system-aggregate
mFRR offer curve at PT15M resolution from 2022-05-24 to 2024-12-10.

The window covers: pre-IDA (2022-05-24 → 2024-06-13, ~750 days) +
3-sess (2024-06-14 → 2024-11-30, ~170 days) + a sliver of ISP15-win
(2024-12-01 → 2024-12-10, 10 days, too short to be meaningful).

Question: does the IDA reform (2024-06-14, 6 → 3 sessions in IDA)
shift the mFRR offer curve depth/level/granularity? mFRR is a
separate balancing market from IDA, but the IDA-session reduction
could indirectly affect mFRR via residual-imbalance dynamics.

Metrics per (regime, direction):
  - mean_total_mw         — total MW offered per ISP (volume depth)
  - mean_n_tranches       — tranches per ISP (offer granularity)
  - median_price          — median tranche price (€/MWh)
  - q90_minus_q10_eur     — within-ISP 90-10 price spread (slope proxy)

Output:
    data/derived/results/mfrr_offer_depth.csv
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

PROJECT = Path(__file__).resolve().parents[3]
PANEL = PROJECT / "data" / "processed" / "esios" / "reservas" / "balancing_bids_all.parquet"
OUT = PROJECT / "data" / "derived" / "results" / "mfrr_offer_depth.csv"

IDA_REFORM   = pd.Timestamp("2024-06-14")
ISP15_REFORM = pd.Timestamp("2024-12-01")


def assign_regime(d: pd.Timestamp) -> str:
    if d < IDA_REFORM:
        return "pre-IDA"
    if d < ISP15_REFORM:
        return "3-sess"
    return "ISP15 win (partial)"


def main() -> None:
    if not PANEL.exists():
        print(f"Missing {PANEL}. Run 20_build_balancing_bids_all.py first.")
        return

    print(f"[load] {PANEL}")
    df = pd.read_parquet(PANEL)
    df["date"] = pd.to_datetime(df["date"])
    df["regime"] = df["date"].apply(assign_regime)
    df = df.dropna(subset=["price_eur_mwh", "mw"]).copy()
    df = df[df["mw"] > 0]

    # Map bid_type_id → direction
    df["direction"] = df["bid_type_id"].map({679: "Subir", 678: "Bajar"})
    df = df.dropna(subset=["direction"])

    # Per-ISP aggregates
    isp_agg = (
        df.groupby(["date", "hour", "isp_in_hour", "direction", "regime"])
        .agg(
            total_mw=("mw", "sum"),
            n_tranches=("mw", "size"),
            median_price=("price_eur_mwh", "median"),
            q10=("price_eur_mwh", lambda s: s.quantile(0.10)),
            q90=("price_eur_mwh", lambda s: s.quantile(0.90)),
        )
        .reset_index()
    )
    isp_agg["q90_q10_spread"] = isp_agg["q90"] - isp_agg["q10"]

    REGIME_ORDER = ["pre-IDA", "3-sess", "ISP15 win (partial)"]
    rows = []
    for direction in ("Subir", "Bajar"):
        print()
        print("=" * 90)
        print(f"  Direction: {direction}")
        print("=" * 90)
        print(f"{'regime':<22}{'n_isps':>9}{'mean_MW':>11}{'mean_tranches':>15}{'median_eur':>12}{'q90-q10':>10}")
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
            }
            rows.append(row)
            print(
                f"{reg:<22}{len(sub):>9,}"
                f"{row['mean_total_mw']:>11.1f}"
                f"{row['mean_n_tranches']:>15.1f}"
                f"{row['median_price']:>12.2f}"
                f"{row['q90_q10_spread']:>10.2f}"
            )

    out = pd.DataFrame(rows)
    OUT.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT, index=False)
    print()
    print(f"Wrote {OUT}")


if __name__ == "__main__":
    main()
