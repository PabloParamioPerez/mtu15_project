# Per-firm CCGT bid-shape at hour-of-day resolution

**Created:** 2026-05-07

**Purpose.** Extend `_per_firm_bid_shape.md` from critical-vs-flat aggregates
to the full 24-hour profile. Tests whether the IB-uniform-enricher /
GN-granularity-exploiter dichotomy is stable across the day. Resolves the
"GE Endesa anomaly" the user flagged in qty-weighted price means.

**Window.** DA market, Oct 1 2025 – Dec 31 2025, post-MTU15-DA, parser-fixed.
58 CCGT units in OMIE register: IB=10, GE=7, GN=18, HC=2, Fringe=21.

**Source.** `scripts/analysis/bid/per_firm_hourly_ccgt_bidshape.py`. Outputs
in `results/regressions/bid/perfirm_hourly_ccgt_*.csv`.

## Hour-by-hour ladder richness (n_tranches per quarter)

| Firm | h0-5 | h6-15 | h16-17 | h18-22 | h23 |
|---|---:|---:|---:|---:|---:|
| **GN (Naturgy)** | 1.3 | 1.4-1.7 | 2.2-5.8 | **8.5-13.0** | 4.7 |
| **IB (Iberdrola)** | 4.4 | 5.2-5.5 | 5.5-5.8 | 5.8-7.2 | 5.4 |
| **HC (EDP)** | 3.7 | 6.0 (from h7+) | 6.0 | 6.0 | 6.0 |
| **GE (Endesa)** | 1.8 | 1.82-1.85 | 1.94 | 2.0 | 1.95 |
| Fringe | 2.8-3.3 | 3.3-3.7 | 3.7-3.9 | 4.0 | 3.9 |

**Headline.** GN's ladder richness explodes at the evening peak — 13.0
tranches at h20 vs 1.3 at h0-5 (a 10× factor). All other firms hold
roughly constant ladder counts across the day.

## Hour-by-hour mechanical-repeat rate (fraction of unit-days where
all 4 quarters have identical bid hashes)

| Firm | h0-3 | h4-15 | h16-17 | h18-22 | h23 |
|---|---:|---:|---:|---:|---:|
| **GN (Naturgy)** | ~1.0 | 0.88-1.0 | **0.34 / 0.19** | 0.63→0.98 | 0.73 |
| **IB (Iberdrola)** | 0.97-0.99 | **0.41-0.46** at h4-5, then ~1.0 | ~1.0 | ~1.0 | ~1.0 |
| **HC (EDP)** | 0.99-1.0 | 1.0 | 1.0 | 1.0 | 1.0 |
| **GE (Endesa)** | ~1.0 | 0.93-1.0 | 0.91-0.98 | 0.95-1.0 | 0.96 |
| Fringe | 0.69-0.75 | 0.59-0.69 | 0.52-0.56 | 0.57-0.60 | 0.60 |

**Headline.** GN drops to 19% mechanical at h17 — at the transition into
the evening peak, they use the within-hour MTU15 granularity heavily.
By h20 they've recovered to 98% mechanical, having moved the strategy to
ladder richness instead. The shift across the late afternoon is unique
to Naturgy.

IB shows a separate, narrower window of granularity exploitation at
**h4-5** (mech drops to 41-46%) — likely the morning-ramp transition
when units are starting up. Otherwise IB is uniformly mechanical.

HC is mechanical at all hours, all 4 quarters. Period.

GE is mechanical at ~95-100% all hours — they don't engage with within-hour
granularity at all.

## Two distinct strategic instruments, deployed at different hours

GN uses BOTH instruments — but **at different hours of the day**:
- h16-17 (afternoon transition into evening): use within-hour granularity
  (mech_strict drops to 0.19-0.34) with modest tranche counts (~2-6).
  Strategy: vary the bid quarter-by-quarter as net-load ramps.
- h18-22 (evening peak): post very rich ladders (8.5-13.0 tranches/quarter)
  but consistent across the 4 quarters (mech_strict 0.63→0.98). Strategy:
  ladder richness, not quarter-variation.

This is more sophisticated than "GN exploits granularity, IB enriches
ladders." Naturgy chooses a *different* instrument for *different* hours
based on the local market structure: granularity when within-hour ramps
are steep (h16-17), ladders when level-matters-most (h18-22).

## The "GE Endesa anomaly" resolved

The user flagged that GE's qty-weighted mean prices flip wildly. The
explanation:

GE-CCGT bidding parameters, Oct-Dec 2025 average:
- n_tranches/quarter: ~1.8-2.0 (essentially one or two tranches)
- p_max: **2330-2375 €/MWh** (very near the 3000 €/MWh system cap)
- p_med: **1000-1190 €/MWh**
- mean qty per tranche: 254-281 MW

GE's CCGT fleet bids essentially **at the system cap**. Median bid is
1000+ €/MWh, max near 2375. They post 1-2 tranches and don't differentiate
the within-day ladder shape. The conduct is *strategic withholding via
price* — keep capacity nominally available so the unit doesn't appear
unavailable to the market or regulator, but price it so high that it only
clears in extreme system-stress events.

Three implications:

1. **Quantity-weighted price means (the user's "anomaly graph") collapse
   onto GE's near-cap p_med whenever GE has any committed quantity.**
   Comparing across firms in qty-weighted units is dominated by GE's
   near-cap bidding, not by ladder differentiation. Use unit-weighted
   or count-based metrics for cross-firm comparisons.

2. **GE's "no critical-flat differentiation" finding from B14 is correct
   but uninformative.** GE doesn't differentiate by hour because they
   don't *participate* in active price discovery. They're at the cap. The
   relevant strategic margin for GE is the choice of cap-bid quantity
   (extensive-margin), not the ladder shape (intensive-margin).

3. **The firm-specific ceiling-finding is sharper than B14 framed it.**
   Each firm's "ceiling" represents a different conduct stance:
   - IB 350: bid most MW within 100 €/MWh of ceiling → expects routine clearing.
   - HC 700: bid below half-ceiling → expects routine clearing at moderate prices.
   - GN 1000: bid most MW within 100 €/MWh of ceiling → expects partial clearing.
   - **GE 2350-2375: bid most MW at NEAR-SYSTEM-CAP → expects clearing only in stress events.**

   GE's "ceiling" is qualitatively different from the other three —
   they're not retaining residual rent on a routine basis; they're
   reserving capacity for stress events.

## Fringe-CCGT pattern is consistent with non-strategic shape

Fringe-CCGT (21 units) shows moderate n_tranches (2.8-4.1, peak at h20),
moderate mechanical-repeat (52-75%), and a flat firm ceiling at ~445
€/MWh. The hour-of-day pattern is much weaker than GN's: n_tranches at
h20 (4.07) is only 1.4× higher than at h0 (2.82), versus GN's 10×.

This is the cleanest version of the fringe placebo: fringe CCGTs have
some hour-of-day texture but no extreme granularity exploitation. The
B13 fringe-placebo coefficient is now anchored to a clearly different
conduct stance.

## Implications for the within-market granularity model

The model in `_within_market_granularity_model.md` should distinguish:

1. **Two different strategic instruments** — within-hour ladder richness
   and within-hour quarter-variation are separate margins firms can
   choose between. The model should support both.
2. **Hour-specific instrument choice** — Naturgy chooses *different*
   instruments at *different* hours of the same day. Static models that
   solve a single optimization per hour will miss this; the choice is
   conditional on hour-specific market structure.
3. **Strategic withholding via price** — GE's near-cap bidding is a
   distinct margin (intensive-extensive boundary) that the granularity
   model doesn't address. Possibly outside the scope of this model;
   would need a separate Bushnell-style withholding analysis.

## What the user wants next

- Continue understanding bidding patterns at finer resolution.
- DO NOT run DiD regressions; this is exploratory.
- Compare across regimes carefully (with respect to MW vs MWh discipline).
- Pre-MTU15-DA comparison should use parser-fixed det_all data;
  `notebooks/eda/15_bid_shape_atlas.ipynb` already has the Oct-Dec 2024 vs
  Oct-Dec 2025 framework.

## Files

- `scripts/analysis/bid/per_firm_hourly_ccgt_bidshape.py`
- `results/regressions/bid/perfirm_hourly_ccgt_bidshape_oct_dec_2025.csv`
- `results/regressions/bid/perfirm_hourly_ccgt_n_tranches_pivot.csv`
- `results/regressions/bid/perfirm_hourly_ccgt_mech_strict_pivot.csv`
- `results/regressions/bid/perfirm_hourly_ccgt_p_max_pivot.csv`
- `results/regressions/bid/perfirm_hourly_ccgt_p_med_pivot.csv`
- `results/regressions/bid/perfirm_hourly_ccgt_mean_qty_mw_pivot.csv`
- `results/regressions/bid/perunit_hourly_ccgt_bidshape_oct_dec_2025.csv`
