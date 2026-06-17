# STATUS: ALIVE
# LAST-AUDIT: 2026-05-15
# FEEDS: thesis paper.tex Appendix §A.x (price-setter day-illustrations)
# CLAIM: For two illustrative days (one high-demand evening, one flat Sunday),
#        identify the price-setting unit in a chosen critical hour and plot
#        its full bid ladder for each of the four 15-min quarters. If
#        quarters differ near the clearing-price band, the within-hour
#        strategic shaping that aggregate plots smooth is visible here.

from __future__ import annotations

from pathlib import Path
import sys

import duckdb
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "src"))

DET   = REPO / "data" / "processed" / "omie" / "mercado_diario" / "ofertas" / "det_all.parquet"
CAB   = REPO / "data" / "processed" / "omie" / "mercado_diario" / "ofertas" / "cab_all.parquet"
MPDBC = REPO / "data" / "processed" / "omie" / "mercado_diario" / "precios" / "marginalpdbc_all.parquet"
FIGDIR = REPO / "figures" / "thesis"

# Pick two illustrative days. High-demand evening (winter weekday with
# elevated DA prices); flat / low-demand Sunday.
DAYS = [
    ("high_demand", "2025-12-04", "Thu 4 Dec 2025 (high-demand winter day)"),
    ("flat_day",    "2025-10-26", "Sun 26 Oct 2025 (low-demand flat Sunday)"),
]
HOUR_OF_INTEREST = 20  # 20:00–21:00 — peak evening, almost always a critical hour


def find_price_setter(date_iso: str, hour: int) -> dict:
    """For each of the 4 quarters in the chosen (date, hour), return the
    accepted sell tranche whose price equals the period's clearing price
    (within EPS). That row identifies the price-setting unit and tranche.
    """
    EPS = 0.5
    rows = duckdb.execute(f"""
        WITH cab AS (
            SELECT date::DATE AS d, offer_code, version, unit_code, buy_sell,
                   ROW_NUMBER() OVER (PARTITION BY date::DATE, offer_code, unit_code
                                      ORDER BY version DESC) AS rn
            FROM '{CAB}'
            WHERE date::DATE = DATE '{date_iso}' AND buy_sell = 'V'
        ),
        cab_l AS (SELECT * FROM cab WHERE rn = 1),
        prices AS (
            SELECT period, price_es_eur_mwh AS p_clear
            FROM '{MPDBC}'
            WHERE date::DATE = DATE '{date_iso}'
        ),
        det AS (
            SELECT date::DATE AS d, offer_code, version, period,
                   price_eur_mwh AS price, quantity_mw AS qty
            FROM '{DET}'
            WHERE date::DATE = DATE '{date_iso}'
              AND period BETWEEN 1 AND 96
              AND quantity_mw IS NOT NULL AND quantity_mw > 0
              AND price_eur_mwh IS NOT NULL
        )
        SELECT d.period,
               (((d.period - 1) // 4))::INT AS hour,
               (((d.period - 1) % 4) + 1)::INT AS quarter,
               c.unit_code, d.price AS bid_price, d.qty, p.p_clear,
               abs(d.price - p.p_clear) AS gap
        FROM det d
          JOIN cab_l c USING (d, offer_code, version)
          JOIN prices p ON d.period = p.period
        WHERE (((d.period - 1) // 4))::INT = {hour}
          AND d.price <= p.p_clear + {EPS}
        QUALIFY ROW_NUMBER() OVER (PARTITION BY d.period ORDER BY d.price DESC) = 1
    """).df()
    return rows


def fetch_unit_bid_ladder(date_iso: str, hour: int, unit_code: str) -> pd.DataFrame:
    """Pull ALL sell tranches for a given unit on the four quarters of the
    chosen (date, hour). Each row = one (period, tranche)."""
    df = duckdb.execute(f"""
        WITH cab AS (
            SELECT date::DATE AS d, offer_code, version, unit_code, buy_sell,
                   ROW_NUMBER() OVER (PARTITION BY date::DATE, offer_code, unit_code
                                      ORDER BY version DESC) AS rn
            FROM '{CAB}'
            WHERE date::DATE = DATE '{date_iso}' AND buy_sell = 'V'
              AND unit_code = '{unit_code}'
        ),
        cab_l AS (SELECT * FROM cab WHERE rn = 1)
        SELECT d.period,
               (((d.period - 1) // 4))::INT AS hour,
               (((d.period - 1) % 4) + 1)::INT AS quarter,
               d.price_eur_mwh AS price, d.quantity_mw AS qty
        FROM '{DET}' d
          JOIN cab_l c USING (offer_code, version)
        WHERE date::DATE = DATE '{date_iso}'
          AND (((d.period - 1) // 4))::INT = {hour}
          AND quantity_mw IS NOT NULL AND quantity_mw > 0
          AND price_eur_mwh IS NOT NULL
        ORDER BY period, price
    """).df()
    return df


def plot_unit_quarters(unit_ladder: pd.DataFrame, p_clear_by_q: dict,
                        unit_code: str, day_label: str, out_stem: str):
    """4-panel figure: one cumulative bid ladder per quarter, with the period's
    clearing price marked. Y-axis zoomed to the strategic band."""
    fig, axes = plt.subplots(1, 4, figsize=(13, 4), sharey=True)
    p_lows  = []
    p_highs = []
    for q, ax in zip((1, 2, 3, 4), axes):
        sub = unit_ladder[unit_ladder["quarter"] == q].sort_values("price")
        if sub.empty:
            ax.text(0.5, 0.5, "(no bids)", ha="center", va="center", transform=ax.transAxes)
            continue
        cum = sub["qty"].cumsum().values
        prices = sub["price"].values
        ax.step(cum, prices, where="post", color="C0", linewidth=1.6)
        ax.scatter(cum, prices, color="C0", s=14)
        p_clear = p_clear_by_q.get(q)
        if p_clear is not None:
            ax.axhline(p_clear, color="C3", linestyle=":", linewidth=1.2)
            ax.text(cum.max(), p_clear, f"  clear: {p_clear:.0f}",
                    fontsize=8, color="C3", va="center", clip_on=False)
        p_lows.append(prices.min()); p_highs.append(prices.max())
        ax.set_title(f"Q{q}  ({(q-1)*15:02d}--{q*15:02d} min)", fontsize=10)
        ax.set_xlabel("MW cumulative")
        ax.grid(alpha=0.3)
    axes[0].set_ylabel("Bid price (EUR/MWh)")
    # Common y-band: 25 below lowest bid to 35 above clearing, clamped.
    if p_lows and p_highs:
        p_clears = [v for v in p_clear_by_q.values() if v is not None]
        y_lo = min(min(p_lows), min(p_clears) if p_clears else 0) - 10
        y_hi = max(max(p_clears) if p_clears else max(p_highs), max(p_highs)) + 20
        y_hi = min(y_hi, 250)  # clamp price-cap tail
        for ax in axes:
            ax.set_ylim(y_lo, y_hi)
    fig.suptitle(f"Price-setting unit {unit_code} — bid ladder by quarter\n{day_label}, hour {HOUR_OF_INTEREST:02d}:00--{HOUR_OF_INTEREST+1:02d}:00",
                 fontsize=11, y=1.02)
    fig.tight_layout()
    for ext in ("pdf", "png"):
        fig.savefig(f"{out_stem}.{ext}", bbox_inches="tight",
                    dpi=140 if ext == "png" else None)
    plt.close(fig)
    print(f"  saved {out_stem}.pdf")


def main() -> None:
    for slug, date_iso, label in DAYS:
        print(f"\n=== {label} ===")
        ps = find_price_setter(date_iso, HOUR_OF_INTEREST)
        if ps.empty:
            print(f"  no price-setting tranche found for hour {HOUR_OF_INTEREST}")
            continue
        print(ps[["quarter", "unit_code", "bid_price", "p_clear", "gap"]].to_string(index=False))
        # Use the unit that sets the price in the most quarters of this hour
        unit_code = ps["unit_code"].mode().iloc[0]
        print(f"  → focal price-setting unit: {unit_code}")
        p_clear_by_q = dict(zip(ps["quarter"], ps["p_clear"]))
        ladder = fetch_unit_bid_ladder(date_iso, HOUR_OF_INTEREST, unit_code)
        plot_unit_quarters(ladder, p_clear_by_q, unit_code, label,
                            str(FIGDIR / f"fig_price_setter_{slug}"))


if __name__ == "__main__":
    main()
