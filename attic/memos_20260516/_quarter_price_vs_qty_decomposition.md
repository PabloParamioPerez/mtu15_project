# Granularity-exploitation mechanism: price ladder vs quantity reallocation

**Created:** 2026-05-08

**Purpose.** When a CCGT's bids differ across the 4 quarters of an hour
post-MTU15-DA (i.e., `mech_strict = 0`), is the variation in PRICE
(different ladder shapes per quarter) or in QUANTITY (same ladder
shape, different MW per quarter)? Decomposes the existing mech_strict
measure to identify the strategic instrument used.

**Window.** Oct-Dec 2025 DA, post-MTU15-DA, competitive zone (≤250 €/MWh).

**Method.** For each (unit, date, hour) compute three "uniformity" rates
across the 4 quarters:
- `mech_full`: identical (price, quantity) tuples
- `mech_price`: identical sorted price tuples
- `mech_qty`: identical sorted quantity tuples (sorted by price within
  tranche to preserve ladder-position)

Then for unit-hours where `mech_full=0` (bids differ), classify as
price-only / quantity-only / both.

## Headline: three distinct strategic types

| Firm | hour_class | mech_full | mech_price | mech_qty | When differs: |
|---|---|---:|---:|---:|---|
| **EDP-PT** | critical | 0.016 | 0.016 | **1.000** | **100% price-only** |
| **GN** | critical | 0.826 | 0.856 | 0.826 | **83% both, 17% qty-only** |
| **Other-fringe** | critical | 0.769 | **0.989** | 0.769 | **95% qty-only** |
| GE | critical | 0.989 | 0.989 | 1.000 | (rare) 100% price-only |
| HC | critical | 1.000 | 1.000 | 1.000 | never |
| IB | critical | 0.998 | 0.998 | 0.998 | (rare) 100% both |

Reading the columns: a firm uses *only* price-variation if mech_qty = 1
but mech_price < 1; uses *only* quantity-variation if mech_price = 1
but mech_qty < 1; uses both if both are < 1.

### Type A — Pure ladder-shape variation (EDP-Portugal)

EDP-Portugal CCGTs (LARES, RIBATE) post **the same MW per quarter** but
**different price ladders** in critical hours. Their mech_qty is
exactly 1.000 — they never reallocate MW across quarters. But their
mech_price is 0.016 — ladder shape varies in 98.4% of unit-hours.

Strategic interpretation: each quarter gets a different price profile
covering the same offered capacity. The firm uses within-hour price
heterogeneity as the strategic instrument; the quantity offered is held
constant (a fleet-fixed quantity per quarter, varying just the price
schedule).

### Type B — Joint price + quantity reallocation (Naturgy)

GN's pattern is qualitatively different. When their bids differ across
quarters (17.4% of critical-hour unit-days, since mech_full = 0.826):
- 82.5% of cases: BOTH price and quantity vary
- 17.5% of cases: only quantity varies
- 0.0% of cases: only price varies

GN **never** uses pure price-only variation. They either reshape the
ladder along both dimensions simultaneously, or they reshuffle MW
without changing prices. This is more sophisticated than EDP-PT's
pattern — GN is optimizing on a 2-instrument margin (price × quantity)
rather than just one.

### Type C — Pure MW reallocation (Other mid-fringe CCGTs)

The "Other-fringe-CCGT" group (Engie's ESCCC1/2/3 + CTNU, AXPO Iberia,
BBE, ABO2G, etc. — 4 distinct CCGTs in this analysis) shows the
opposite of EDP-PT: **same prices, different MW per quarter** (95.2%
quantity-only). Mech_price stays high (0.989) while mech_qty drops to
0.769.

Strategic interpretation: a fixed price ladder, with MW reallocated
across the 4 quarters to ride within-hour demand peaks. This is
analogous to physical-dispatch scheduling — bid the same prices but
sell more in the quarters with higher expected clearing.

### Type D — No quarter variation (IB, GE, HC)

IB and HC: mech_full ≈ 1.0 in critical hours — they essentially never
vary bids across the 4 quarters. They use the LADDER dimension instead
(IB has 9.3 tranches/quarter; HC has 6) and hold that ladder constant
across the 4 quarters.

GE has mech_full ≈ 0.989 — their few cases of variation (27 unit-days
in critical hours) are 100% price-only. Their conduct is mostly
flat-mechanical with occasional price-only adjustments.

## What this tells us about the strategy

**The granularity-exploitation finding is real but heterogeneous in
mechanism.** When firms exploit the new MTU15 dimension, they choose
ONE of two instruments (or both):

- **Price-only ladder variation:** EDP-PT (always), GE (rarely)
- **Quantity-only MW reallocation:** Other-fringe-CCGT (always)
- **Joint price+quantity:** GN (always)
- **No quarter variation:** IB, HC (use ladder enrichment instead)

The unit-period mech_strict measure aggregated all of these into one
metric, which was misleading. The decomposition shows that **different
firms have different strategic technologies** for exploiting the
within-hour granularity, even when they all do "drop mech_strict in
critical hours."

## Critical-vs-flat differential (within firm)

The identification logic predicts more granularity exploitation in
critical hours than in flat hours. Empirical checks:

- **GN**: mech_full 0.979 flat → 0.826 critical (Δ = -0.153). Strong
  critical-vs-flat differential. ✓
- **IB**: mech_full 0.590 flat → 0.998 critical (Δ = +0.408). Going the
  WRONG way. IB's quarter variation is at flat hours (h{3-5}, presumably
  ramp-down/idle), not critical.
- **EDP-PT**: mech_full 0.041 flat → 0.016 critical (Δ = -0.025).
  EDP-PT varies bids at all hours; small intensification in critical.
- **HC**: 0.998 flat → 1.000 critical (Δ = +0.002). No movement either
  way. Confirms HC has no quarter-level strategic conduct.
- **GE**: 1.000 flat → 0.989 critical (Δ = -0.011). Trivial.
- **Other-fringe**: 0.777 flat → 0.769 critical (Δ = -0.008). Trivial.

**Two firms genuinely intensify quarter variation in critical hours: GN
(Δ = -0.153) and EDP-PT (Δ = -0.025).** EDP-PT's smaller magnitude is
because they vary at all hours, while GN's is concentrated in critical.

IB does the opposite — they vary more in flat hours than critical. This
is the morning-ramp pattern from `_per_firm_hourly_bidshape.md` (mech
drops to 0.41-0.46 at h4-5). It's not strategic; it's operational
(ramping up). Their critical-hour exploitation operates on the LADDER
dimension, not the quarter dimension.

## Implications for the within-market granularity model

The model needs to distinguish **two strategic instruments enabled by
MTU15-DA**:

1. **Within-hour ladder variation (EDP-PT type):** different price
   ladders per quarter, same quantity. Captures expected-clearing-price
   heterogeneity within the hour.

2. **Within-hour MW reallocation (Other-fringe type):** same price
   ladder, different MW per quarter. Captures expected-quantity-cleared
   heterogeneity within the hour.

3. **Joint variation (GN type):** both instruments simultaneously.

4. **Cross-tranche ladder enrichment (IB type, separate dimension):**
   not within-hour quarter variation; instead, a richer ladder that
   spans more price levels but is held identical across quarters.

A firm chooses the optimal mix of (1)+(2)+(4) based on its expected
hour-specific market structure. The model should support all three
instruments and let the equilibrium choice be empirical.

## Implications for B14 / B12 / B13 robustness

- The **mech_strict measure aggregates two distinct instruments** and
  blurs the strategic interpretation. Future B14 robustness specs
  should report mech_price and mech_qty separately to disambiguate.

- The "GN Δ = +2.95 tranches in critical hours" finding from B14 is
  consistent with GN's joint price+qty variation — they're enriching
  the ladder AND reshuffling MW.

- The "IB Δ = +5.32 tranches" finding is purely a ladder-enrichment
  effect (their mech_full ≈ 1, so tranches per quarter = tranches per
  hour). IB doesn't use the quarter dimension at all in critical hours.

- The fringe-CCGT placebo (B13) is now further questioned: the
  "Other-fringe" group exhibits Type-C quantity reallocation in critical
  hours. They have a clear mechanism. The placebo identifies on EDP-PT
  + Engie/Repsol/etc. + Other-fringe, all of which behave differently.
  Cleaner placebo: restrict to Type-D non-pivotal firms (Repsol, Engie
  España CTNU, TotalEnergies, Moeve) per `_pivotality_by_firm_critical_hours.md`.

## Sources

- `scripts/analysis/bid/quarter_price_vs_qty_decomposition.py`
- `results/regressions/bid/quarter_decomposition_per_parent_hourly.csv`
- `results/regressions/bid/quarter_decomposition_per_parent_hour_class.csv`
- `results/regressions/bid/quarter_decomposition_what_differs.csv`
