# Operational vs strategic decomposition + DA→IDA price wedge

**Created:** 2026-05-08

**Purpose.** Decompose firm-hour MWh changes through the OMIE/REE chain
into strategic IDA market activity (PIBCA − PDVD = sum(pibci across
sessions), RT-free per spec) vs RT2 (PHF(max session) − PIBCA(max
session), REE post-IDA RT). And compute the DA→IDA price wedge by
hour-class to test whether reforzada / MTU15-IDA / MTU15-DA created
a systematic divergence between DA and IDA clearing.

**Window:** 2024-01-01 to 2026-01-01. CCGT firms only.

**Source:** `scripts/analysis/firm/operational_vs_strategic_decomposition.py`.

## Headline: DA→IDA price wedge

The most informative finding. **Spain DA price minus IDA price**
(€/MWh, monthly mean by hour-class):

| Period | critical_h{18-22} | flat_h{3-5} | other |
|---|---:|---:|---:|
| 2024 mean | ~0-3 | ~0-4 | ~−0.5 to +1.5 |
| Jan-Feb 2025 | 1-1.5 | 0.4-0.7 | −0.5 to 0.5 |
| **Apr 2025** | **8.3** | 5.5 | 0.1 |
| **May 2025** | **9.3** | 1.4 | −3.4 |
| **Jun 2025** | **14.2** | 3.3 | −4.0 |
| **Jul 2025** | **19.8** | 1.3 | −6.9 |
| **Aug 2025** | **17.7** | 1.1 | −3.8 |
| **Sep 2025** | **16.3** | 2.6 | −3.7 |
| Oct 2025 | 3.6 | 2.0 | 2.0 |
| Nov-Dec 2025 | 3.3-4.3 | −1.7 to 0.3 | −0.5 to 0.8 |

**Interpretation:**

1. **Pre-blackout (through March 2025):** DA-IDA wedge is small and roughly
   symmetric across hour-classes. Markets are integrated.

2. **Post-blackout reforzada (April-September 2025):** Wedge **explodes
   in critical hours** (14-20 €/MWh) while staying small in flat hours
   and going **NEGATIVE in "other" hours** (−3 to −7 €/MWh). This
   pattern is consistent with REE forcing CCGTs/coal to run for voltage
   support, which:
   - Crowded out DA capacity in peak hours → DA prices spike critical
   - Released that forced production in IDA → IDA prices drag in critical
   - Created excess thermal supply in off-peak → IDA prices below DA off-peak

3. **After MTU15-DA (October 2025):** Pattern collapses back to
   pre-blackout levels. Either reforzada relaxed at the same time, or
   MTU15-DA's finer pricing absorbed the reforzada distortion (or both).

**This is exactly the kind of cross-hour heterogeneity the within-day
DiD design can exploit.** Even if q_2 magnitudes shrunk at MTU15-IDA,
the DA-IDA price wedge shows critical-vs-flat divergence is present
in the data — just on the price dimension instead of the quantity
dimension.

## Strategic IDA decomposition by firm

CCGT-firm strategic IDA (PIBCA − PDVD = sum pibci) MWh per (unit,
clock-hour), averaged over all operating cells (zeros included),
critical hours h{18-22}:

| Firm | 2024 mean | Jan-Feb 2025 | Apr-Aug 2025 | Oct-Dec 2025 |
|---|---:|---:|---:|---:|
| IB | 4-7 | 4-7 | 0.4-2.3 | 1.7-2.2 |
| GE | 3-18 | 7-9 | 0.4-3.8 | 0.6-7.6 |
| GN | 3-10 | 2-3 | 1.1-2.6 | 1.5-2.4 |
| HC | 1-23 | 10-12 | 1.3-6 | 2.8-5.1 |
| Repsol | 3-38 | 13-22 | 4.5-20 | 9.9-26 |

The drop at MTU15-IDA (March 2025) is visible across firms but the
magnitudes differ. Notable:
- **Repsol** has the largest strategic IDA throughout — consistent
  with the "scale without conduct" finding from the structural-
  dominance audit (large flex capacity but bids competitively in DA,
  uses IDA for normal-clearing repositioning).
- **HC**'s strategic IDA is large and volatile in 2024 H2-2025 H1,
  smaller post-MTU15-IDA.
- **EDP-PT shows zero strategic IDA throughout** — Portuguese units
  may not appear in OMIE pibci files (zone separation). This is a
  data limitation, not a behavioral finding.

## RT2 decomposition for CCGTs ≈ 0 — likely a real finding

The PHF − PIBCA difference (RT2 = REE post-IDA technical restrictions)
shows nearly zero values across the window for CCGT firms. **This is
plausibly a real finding**, not a bug: CCGTs are flexible enough to
handle most restrictions during IDA itself. Post-IDA REE reshuffling
likely concentrates on nuclear, hydro and coal — not CCGT.

The HEAVY_RUN_SUMMARY's "RT2 jumps to +13,639 MWh per firm-day in
DA15/ID15" figure is across the firm's full unit fleet (nuclear,
hydro, CCGT, coal). For CCGT specifically, near-zero RT2 is consistent
with their operational role — they're the units that *handle*
restrictions through their IDA bidding, rather than the units that
*get reshuffled* by REE's post-IDA RT.

This means the operational-vs-strategic split for CCGT-firm bidding
is essentially: **strategic IDA ≈ all of (PHF − PDBC) for CCGTs**.
The RT2 layer matters for nuclear/hydro analysis but is a sideshow
for CCGT-strategic-conduct work.

## DA cleared decomposition (consistency check)

DA cleared MWh per unit-clock-hour for CCGT firms in critical hours
(2024-2025 average): ranges from ~150 to ~600 MWh/clock-hour depending
on firm and month. Strategic IDA is 1-5% of DA cleared. So strategic
IDA is a small adjustment relative to DA position, but its sign and
hour-class concentration are what matter for the within-day DiD.

## Implications for next steps

1. **The DA-IDA price wedge is a cleaner outcome than q_2** for
   the within-day DiD because:
   - Less affected by the MTU15-IDA reform (which crushed q_2
     volumes).
   - Larger empirical signal in critical hours (14-20 €/MWh).
   - Direct measure of inter-market arbitrage opportunity.

2. **Reforzada is the dominant force in 2025 H2 critical-hour
   dynamics.** Any DiD on price-wedge or q_2 must condition on
   reforzada dummy (post-2025-04-28).

3. **For the headline thesis claim**, the user is right that
   absolute magnitudes matter less than the (critical − flat)
   variation. The price wedge shows huge (critical − flat) variation
   exists, just driven by reforzada more than MTU15-DA.

## Sources

- `scripts/analysis/firm/operational_vs_strategic_decomposition.py`
- `results/regressions/firm/operational_vs_strategic/operational_strategic_per_firm_month_hourclass.csv`
- `results/regressions/firm/operational_vs_strategic/da_ida_price_wedge_monthly.csv`
- `figures/working/operational_strategic_per_firm.png` and `.pdf`
- `figures/working/da_ida_price_wedge.png` and `.pdf`
