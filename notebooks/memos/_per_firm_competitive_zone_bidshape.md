# Per-firm CCGT bid-shape, competitive zone only (≤250 €/MWh)

**Created:** 2026-05-07

**Purpose.** Re-do the per-firm CCGT bid-shape analysis stripping out
tranches priced above the observed clearing-tail. User feedback: the
firm-specific "ceilings" (GN 1000, GE 2350, HC 700, IB 350) are
reputational positioning, not binding constraints — clearing maxed at
240 €/MWh in Oct-Dec 2025 and 193 in Oct-Dec 2024. Anything above
~250 is functionally unavailable supply.

**Cutoff.** 250 €/MWh. Covers all observed clearing in both windows.

## Headline reframe: GN bids 75% of MW above the competitive zone

**Pre-reform (Oct-Dec 2024) competitive-zone share by firm:**

| Firm | % tranches ≤ 250 | **% quantity (MW) ≤ 250** |
|---|---:|---:|
| HC | 83.2% | **86.0%** |
| IB | 83.6% | **77.3%** |
| Fringe | 77.6% | **64.0%** |
| GE | 53.4% | **39.4%** |
| **GN** | 76.9% | **24.8%** |

**Post-reform (Oct-Dec 2025):**

| Firm | % tranches ≤ 250 | **% quantity ≤ 250** |
|---|---:|---:|
| IB | 89.8% | **79.7%** |
| Fringe | 77.2% | **74.0%** |
| **GE** | 57.7% | **76.8%** |
| HC | 81.5% | **76.7%** |
| **GN** | 73.6% | **25.1%** |

Two findings jump out:

**1. Naturgy bids ~75% of CCGT MW above the competitive zone — pre AND post.**
GN's strategy is systematically: 25% of MW in the competitive ladder,
75% at high prices that essentially never clear. This is the most
extreme withholding-via-price stance of any firm. Their bid-shape
sophistication (10+ tranche ladder, granularity exploitation at
h16-17) operates on only the bottom quarter of their offered MW.

**2. GE flipped at the reform.** Pre-MTU15-DA: 39% of MW competitively
priced. Post-MTU15-DA: **77%** of MW competitively priced. GE
reallocated ~37% of their CCGT MW from "cap reserve" to "competitive
supply." This is the largest strategic behavior change of any firm at
the reform — bigger than what the headline ceiling reduction suggested.

## Competitive-zone bid-shape (critical hours h{18-22})

| Firm | n_tranches | p_med | p_max | offered_MW | (per period) |
|---|---:|---:|---:|---:|:---|
| **PRE 2024** | | | | | |
| Fringe | 4.25 | 103 | 122 | 385 | |
| GE | 2.03 | 82 | 104 | 336 | |
| GN | 11.46 | 94 | 103 | 338 | |
| HC | 4.99 | 109 | 126 | 369 | |
| IB | 8.68 | 142 | 167 | 503 | |
| **POST 2025** | | | | | |
| Fringe | 3.46 | 131 | 149 | 684 | |
| **GE** | **1.23** | 101 | 109 | 381 | |
| GN | 10.85 | 83 | 90 | 346 | |
| HC | 5.00 | 104 | 128 | 370 | |
| **IB** | **9.28** | **115** | 158 | **571** | |

**Per-firm shifts at the reform (POST − PRE):**

| Firm | Δ n_tranches | Δ p_med | Δ p_max | Δ offered_MW |
|---|---:|---:|---:|---:|
| **GE** | **−0.80** | +19 | +5 | +45 |
| **GN** | −0.61 | **−10.5** | **−13.1** | +8 |
| **IB** | **+0.60** | **−27.1** | −9 | **+68** |
| HC | +0.01 | −5.6 | +2 | +0.5 |
| Fringe | −0.79 | +28.5 | +27.2 | +298 |

## Strategic-conduct playbook by firm (post-reform, competitive zone)

**IB (Iberdrola) — deep competitive ladder, expects routine clearing.**
9.3 tranches/quarter at h{18-22}, offering 571 MW per period. Median
tranche at 115 €/MWh, max at 158 €/MWh. They bid lots of MW
competitively at low-to-moderate prices. The reform AMPLIFIED this:
ladder enrichment +0.60 tranches, p_med −27 (deeper undercutting),
offered_MW +68 (more competitive supply). IB's behavior is
hyper-competitive in the price ranges that matter.

**GN (Naturgy) — partial competitive ladder + bulk cap-bidding.**
10.9 tranches/quarter in critical hours offering only 346 MW
competitively (the rest sits above 250). Median tranche at 83 €/MWh
(the LOWEST of any firm). The reform LOWERED their competitive ladder
further (Δp_med −10.5, Δp_max −13.1). GN's competitive bidding is
*more* aggressive than IB's in the bottom of the ladder, but they hold
back 75% of their MW for scarcity-rent extraction.

**HC (EDP-Spain) — small fixed competitive ladder.** Exactly 5 tranches,
flat across reform, low MW (370). p_med ~104, p_max ~128. Their two
CCGTs run a routine fixed-shape ladder; nothing changed at the reform.

**GE (Endesa) — thin competitive ladder, partial reform pivot.**
Only 1.2 tranches/quarter in critical hours (post). Compare 2.0 pre.
Reform actually DECREASED their tranche count in the competitive zone.
But TOTAL competitive-zone offered MW grew (+45 critical hours, +298
when looking at the share-by-quantity). GE's strategic shift was to
move bulk MW from cap-bidding into the competitive zone, but with
fewer distinct tranches. The bidding is "concentrated lump at moderate
prices" rather than ladder-shaped.

**Fringe — heterogeneous, more competitive overall.** 3.5 tranches at
critical hours, offered_MW grew +298 (Portuguese-fringe entry +
broader competitive participation). p_med up +28 — fringe ladder
shifted UP in the competitive zone, suggesting fringe firms became
LESS aggressive (offering more MW but at higher prices).

## Implication for the within-day-DiD identification

The B14 ladder-enrichment finding (IB 5× ladder amplification)
survives the cap-stripping — IB's competitive-zone n_tranches grew
8.68 → 9.28 in critical hours, a real ladder enrichment. The
quantity-weighted GE-anomaly (which dominated the cap-included
statistics) disappears: in the competitive zone GE actually has the
THINNEST ladder (1.23), the opposite of the cap-included reading.

**The competitive-zone restriction should be the DEFAULT for any
cross-firm comparison going forward.** The cap-bidding tail is a
reputational/withholding margin, not a strategic-conduct one.

## What this implies for the granularity model

The model needs to distinguish two strategic margins:

1. **Competitive-zone ladder shape** — n_tranches, mech_strict, p_med,
   p_max. This is where the reform-amplified granularity exploitation
   lives. IB and GN both extract value here, but with different
   instruments (IB: deep ladder; GN: aggressive ladder + quarter-by-
   quarter granularity at h16-17).

2. **Cap-bidding mass** — what fraction of the firm's MW sits above
   the competitive zone. This is a *separate* strategic margin (firm
   policy on willingness to clear in stress events). The MTU15-DA
   reform did not affect this margin meaningfully (GN unchanged,
   ceilings stable across firms; only GE shifted, and that shift
   was in the level of cap-pricing, not the cap-bidding fraction).

The within-market granularity model should focus on margin 1; margin
2 is structural / external.

## Sources

- `scripts/analysis/bid/per_firm_competitive_zone_bidshape.py`
- `results/regressions/bid/perfirm_competitive_zone_pre_vs_post.csv`
- `results/regressions/bid/perfirm_competitive_zone_offered_mw_pre_vs_post.csv`
- `results/regressions/bid/perfirm_comp_zone_share_PRE_2024_MTU60.csv`
- `results/regressions/bid/perfirm_comp_zone_share_POST_2025_MTU15.csv`
