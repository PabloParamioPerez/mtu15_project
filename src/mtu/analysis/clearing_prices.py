"""Mean DA / IDA clearing prices by hour-class, used to overlay on bid-curve plots.

Cached on first call so each script computes once even when running plots over
multiple techs / panels.
"""
from __future__ import annotations

from functools import lru_cache
from pathlib import Path

import duckdb

REPO = Path(__file__).resolve().parents[3]
MPDBC = REPO / "data" / "processed" / "omie" / "mercado_diario" / "precios" / "marginalpdbc_all.parquet"
MPIBC = REPO / "data" / "processed" / "omie" / "mercado_intradiario_subastas" / "precios" / "marginalpibc_all.parquet"

CRITICAL_HOURS = (5, 6, 7, 8, 16, 17, 18, 19, 20, 21, 22)
FLAT_HOURS = (1, 2, 3)
MIDDAY_HOURS = (11, 12, 13, 14)


def _hour_expr() -> str:
    """SQL expression that recovers clock-hour-of-day from a (date, period) row.

    Works for both MTU60 and MTU15 data because `(period-1) * 15 minutes` lands
    on the correct hour for both regimes (period=1 → 0:00, period=24 / 96 → 23:xx).
    """
    return ("EXTRACT(hour FROM (CAST(date AS TIMESTAMP) + (period-1) * INTERVAL '15 minute'))::INT")


@lru_cache(maxsize=16)
def mean_clearing_prices(
    market: str,
    window_start: str,
    window_end: str,
) -> dict[str, float]:
    """Mean clearing price (€/MWh) per hour-class in a date window.

    market: "da" → marginalpdbc; "ida" → marginalpibc (pooled across sessions).
    window_*: ISO YYYY-MM-DD; end is exclusive.

    Returns: {"critical": x, "midday": y, "flat": z}.
    """
    path = MPDBC if market == "da" else MPIBC
    h = _hour_expr()
    out: dict[str, float] = {}
    for label, hours in (("critical", CRITICAL_HOURS),
                          ("midday",   MIDDAY_HOURS),
                          ("flat",     FLAT_HOURS)):
        df = duckdb.execute(f"""
            SELECT AVG(price_es_eur_mwh) FROM '{path}'
            WHERE date::DATE >= DATE '{window_start}'
              AND date::DATE <  DATE '{window_end}'
              AND price_es_eur_mwh IS NOT NULL
              AND {h} IN {hours}
        """).df()
        v = df.iloc[0, 0]
        if v is not None and not (isinstance(v, float) and v != v):  # not NaN
            out[label] = float(v)
    return out


def overlay_clearing_prices(
    ax,
    market: str,
    window_start: str,
    window_end: str,
    mode: str,
    hour_class: str | None = None,
    annotate: bool = True,
) -> None:
    """Draw dashed horizontal lines for the mean clearing price(s) on `ax`.

    mode:
      - "per_hour":      three lines (critical/midday/flat), color-matched to
                         the hour-class palette used elsewhere
                         (critical=C3 red, midday=tab:green, flat=C0 blue).
      - "per_quarter":   one black line for the hour_class shown (must pass it).

    Skips drawing any line whose y-value falls outside the current axis ylim.
    """
    means = mean_clearing_prices(market, window_start, window_end)
    if not means:
        return
    ymin, ymax = ax.get_ylim()
    xmin, xmax = ax.get_xlim()
    palette = {"critical": "C3", "midday": "tab:green", "flat": "C0"}

    if mode == "per_hour":
        for hc, color in palette.items():
            mp = means.get(hc)
            if mp is None or not (ymin <= mp <= ymax):
                continue
            ax.axhline(mp, color=color, linestyle=":", linewidth=1.1, alpha=0.7)
            if annotate:
                ax.text(xmax, mp, f" {mp:.0f}", ha="left", va="center",
                        fontsize=7, color=color, alpha=0.9, clip_on=False)
    elif mode == "per_quarter":
        mp = means.get(hour_class)
        if mp is None or not (ymin <= mp <= ymax):
            return
        ax.axhline(mp, color="black", linestyle=":", linewidth=1.1, alpha=0.6)
        if annotate:
            label = market.upper()
            ax.text(xmax, mp, f" {label} avg: {mp:.0f}", ha="left", va="center",
                    fontsize=7, color="black", alpha=0.8, clip_on=False)
