"""Critical-hours classification for the within-day DiD identification.

Replaces the inline `CRITICAL_HOURS = [7, 8, 16, 17, 18]` constant duplicated
across `scripts/analysis/firm/critical_hours_*.py`.

## Why multiple definitions

The original critical-hours set was built from σ²_within(net-load) ranking —
hours with the steepest within-hour demand-supply ramps. That captures the
morning ramp (h7-8) and afternoon transition (h16-18) but **misses the
evening peak (h19-21) where DA bidding is empirically most elaborate**:
CCGT-Dominant n_tranches in Oct-Dec 2025 was 9.5-10.0 tranches/quarter at
h19-21 vs 4.1-8.5 at h{7,8,16,17,18}. Clearing prices also peak at h20.

Solar floods h11-15, so demand peak (h17-19) and *price* peak (h19-21) are
genuinely different hours. Strategic price extraction lives at the price
peak; the early-evening demand-peak hours have less strategic surplus
because solar still partially clears the market.

Three definitions are therefore supported:

- `'supply_ramp'` (HEADLINE / pre-pivot): h{7, 8, 16, 17, 18}. Top 5 by
  σ²_within(net-load) on the 2024-2025 calibration window. Used for B12,
  B13, B14 main results.
- `'price_peak'`: top 5 hours by DA clearing-price level. Empirically
  h{18, 19, 20, 21, 22} on Spanish Oct-Dec 2025 data — see
  `notebooks/memos/_critical_hours_calibration.md` for the underlying
  ranking. Captures the evening-peak strategic-extraction window where
  demand is high AND solar is gone (high net-load).
- `'demand_peak'`: top 5 hours by raw load level (ENTSO-E A65).
  Empirically h{16, 17, 18, 19, 20} on Spanish Oct-Dec 2025 data. Earlier
  than `'price_peak'` because solar still partially clears the late
  afternoon. Provided for completeness; `'price_peak'` is the more
  economically meaningful demand-side trigger for strategic conduct.
- `'joint'`: union of supply_ramp and price_peak: h{7, 8, 16, 17, 18,
  19, 20, 21, 22}. Robustness check, broader coverage.

All definitions share the same flat-hours control: h{3, 4, 5} (overnight,
low demand, low ramp, low price).

## Usage policy

The HEADLINE results (B12 β₃ = +58.6 MWh/firm-hour, B13 fringe placebo
β₃ = -24.3, B14 ladder enrichment) use `'supply_ramp'`. This is
pre-specified and should not be changed retrospectively.

`'price_peak'`, `'demand_peak'`, and `'joint'` are SECONDARY robustness
specs. New analyses that want to pre-specify a different definition can
do so explicitly via the `definition` argument; old analyses keep their
hard-coded behaviour (the function default is `'supply_ramp'`).

## Empirical anchor

Both `'price_peak'` and `'demand_peak'` were calibrated on Oct-Dec 2025
data:

- `'price_peak'`: hourly mean DA clearing price (Spain, ES side):
  h20=113.3, h19=106.4, h21=105.7, h18=93.4, h22=93.3 €/MWh.
- `'demand_peak'`: hourly mean Spanish actual load (ENTSO-E A65):
  h19=32.0, h18=31.7, h20=30.9, h17=30.7, h16=29.3 GW.

See `notebooks/memos/_modelling_track.md` §X for the empirical design
rationale and `notebooks/memos/_within_market_granularity_model.md` for
the theoretical anchor (σ²_within drives strategic value of granularity).
"""
from __future__ import annotations

from typing import Literal

CRITICAL_HOURS_SUPPLY_RAMP: tuple[int, ...] = (7, 8, 16, 17, 18)
CRITICAL_HOURS_PRICE_PEAK: tuple[int, ...] = (18, 19, 20, 21, 22)
CRITICAL_HOURS_DEMAND_PEAK: tuple[int, ...] = (16, 17, 18, 19, 20)
CRITICAL_HOURS_JOINT: tuple[int, ...] = (7, 8, 16, 17, 18, 19, 20, 21, 22)
FLAT_HOURS: tuple[int, ...] = (3, 4, 5)

Definition = Literal["supply_ramp", "price_peak", "demand_peak", "joint"]


def critical_hour_set(definition: Definition = "supply_ramp") -> tuple[int, ...]:
    """Return the set of critical hours under the requested definition."""
    if definition == "supply_ramp":
        return CRITICAL_HOURS_SUPPLY_RAMP
    if definition == "price_peak":
        return CRITICAL_HOURS_PRICE_PEAK
    if definition == "demand_peak":
        return CRITICAL_HOURS_DEMAND_PEAK
    if definition == "joint":
        return CRITICAL_HOURS_JOINT
    raise ValueError(
        f"Unknown definition {definition!r}. Use one of: 'supply_ramp', "
        f"'price_peak', 'demand_peak', 'joint'."
    )


def classify_hour(hour: int, *, definition: Definition = "supply_ramp") -> str:
    """Classify a single hour-of-day into 'critical' / 'flat' / 'other'.

    Parameters
    ----------
    hour : int
        Hour of day in [0, 23].
    definition : {'supply_ramp', 'price_peak', 'demand_peak', 'joint'}
        Which definition to apply. Default `'supply_ramp'` matches the
        pre-pivot headline and existing `critical_hours_*.py` scripts.

    Returns
    -------
    {'critical', 'flat', 'other'}
    """
    crit = critical_hour_set(definition)
    if hour in crit:
        return "critical"
    if hour in FLAT_HOURS:
        return "flat"
    return "other"


def hour_class_series(hours, *, definition: Definition = "supply_ramp"):
    """Vectorised version. Accepts any iterable of hours; returns a list."""
    crit = critical_hour_set(definition)
    flat = FLAT_HOURS
    return [
        "critical" if h in crit else ("flat" if h in flat else "other")
        for h in hours
    ]
