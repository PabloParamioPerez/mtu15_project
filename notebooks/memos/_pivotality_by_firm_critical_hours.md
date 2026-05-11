# Pivotality by firm in critical hours: empirical treatment-effect set

**Created:** 2026-05-08
**Updated:** 2026-05-11 — firm-classification audit (see [`_firm_classification_audit.md`](_firm_classification_audit.md))

> **2026-05-11 amendment.** The pivotality numbers below were computed against
> an earlier version of the firm-to-unit mapping that contained four bugs:
> triple-counted joint-owned nuclear (Almaraz, Trillo, Vandellós II, Ascó 2),
> a misclassified `IBERENERGIA, S.A.` (it holds EDP-Spain's 15.5% Trillo stake,
> not Iberdrola's), an inflated Repsol set that bundled in 42 third-party
> REPRESENTANTE-aggregated renewables (`REPSOL SERVICIOS RENOVABLES, S.A.`),
> and an Engie set polluted by the Belgian trading desk `ENGIE GLOBAL MARKETS,
> BELGIAN BRANCH`. Pivotality on CCGT specifically is robust to these bugs
> (none of the misclassified entities have CCGT capacity), so the
> firm-partition conclusions below are unchanged. The classification pipeline
> has since been centralized in `src/mtu/classification/units.py` with 36
> unit tests, and all downstream headline regressions (B1 q_2, B3 DA cleared,
> B4 CPT, B5 robustness) have been re-run under the corrected mapping. See
> the audit memo for details.

**Purpose.** The identification strategy treats critical-vs-flat hours as
the treatment axis, with firms acting as subjects. The expected magnitude
of the treatment effect on each firm depends on whether that firm is
*pivotal* in critical hours — firms that don't straddle clearing have no
strategic margin to exploit regardless of size. This memo computes
empirical pivotality (price-setter, the existing
`pivotal_indicator_DA_oct_dec_2025.parquet` panel) by parent firm × hour
class to identify the pivotality-based partition for the treatment-effect
set.

## Why pivotality, not firm size

The identification strategy:
- Treatment unit: critical hours (h{18-22}, treated by MTU15).
- Control unit: flat hours (h{3-5}).
- Outcome: q_2 (Ito-Reguant withdrawn quantity in IDA repositioning).
- Parallel-trends assumption: q_2 evolves identically in critical-vs-flat
  hours absent treatment, conditional on controls (esp. renewable
  capacity).

The dominant-vs-fringe split is *expected heterogeneity in who reacts*,
not the treatment dimension. A firm reacts if and only if it has a
strategic margin to exploit in critical hours — i.e., its bids straddle
clearing often enough to matter. Pivotality is the right empirical
test.

## CCGT pivotality rate (price-setter), Oct-Dec 2025

| Firm | flat_h3_5 | other | **critical_h18_22** | Δ critical−flat |
|---|---:|---:|---:|---:|
| **HC (EDP-Spain)** | 0.72 | 0.90 | **1.00** | +0.28 |
| **EDP-Portugal** | 0.79 | 0.82 | **0.88** | +0.09 |
| **GN (Naturgy)** | 0.02 | 0.15 | **0.84** | **+0.82** |
| Other (small fringe CCGT) | 0.26 | 0.27 | **0.40** | +0.14 |
| **GE (Endesa)** | 0.12 | 0.16 | **0.31** | +0.19 |
| **IB (Iberdrola)** | 0.004 | 0.04 | **0.25** | +0.25 |
| Engie España | 0.00 | 0.002 | 0.005 | 0.00 |
| Moeve | 0.005 | 0.004 | 0.005 | 0.00 |
| Repsol | 0.00 | 0.00 | 0.00 | 0.00 |
| TotalEnergies | 0.00 | 0.00 | 0.00 | 0.00 |

(Pivotal = unit's tranches straddle DA clearing price. Computed at
unit-period resolution then aggregated by parent firm × hour-class.)

## Empirical partition for the strategy

**Treatment-effect set (pivotality > ~10% in critical hours, capable of
exploiting the reform):**

- HC, EDP-Portugal, GN, GE, IB, plus "Other" mid-sized fringe CCGTs
  (ABO2G, ENGIE CARTAGENA's ESCCC1/2/3, AXPO Iberia AMBIETA, BBE,
  etc.).

**Empirical placebo (~0% pivotality, no strategic margin):**

- Repsol (17% flex-share but 0% pivotality — bids to clear cheaply,
  price-taker stance).
- Engie España (CTNU only at 0.5%; ENGIE CARTAGENA's units are in
  "Other" and DO show pivotality).
- TotalEnergies (CTJON1R/3R near-cap-only bidding, never straddles
  competitively-relevant prices).
- Moeve / Cepsa (ARRU1R/2R, similar pattern).

## Two findings that revise prior conclusions

### 1. EDP-Portugal is in the treatment set, not the placebo

EDP-Portugal CCGTs (LARES1/2, RIBATE1/2/3) have 88% pivotality in
critical hours — comparable to GN's 84% and HC's 100%. Their
granularity-exploitation behavior (mech 0.09, 6.4 tranches/quarter
in `_per_firm_hourly_bidshape.md`) is therefore strategic exploitation
of the reform, not a placebo violation.

This supersedes the recommendation in `_fringe_ccgt_heterogeneity.md`
to treat EDP-PT as part of the fringe placebo. The empirical partition
should put EDP-PT in the treatment set.

### 2. Repsol stays in the placebo despite passing structural scale

Repsol passes the flex-strategic share test (17% in critical hours)
but has **0% pivotality** — their tranches never straddle clearing.
This is consistent with the hypothesis from
`_structural_dominance_audit.md`: Repsol bids to clear cheaply as a
hedging-instrument strategy, not strategic-conduct. Scale without
pivotality means no strategic margin in DA, regardless of how large
their flex portfolio is.

Repsol is therefore the cleanest single-firm placebo: large flex
capacity (B14 control for "size effect") + zero pivotality (B14 zero
expected reform response). If the treatment-vs-Repsol DiD shows the
expected sign, that's strong identification.

## Implications

1. **The pivotality-based treatment set cuts orthogonally to the
   dominant-vs-fringe partition.** It includes "fringe" firms that ARE
   strategic (EDP-PT, mid-sized fringe CCGTs) and excludes "dominant"
   firms / candidates that AREN'T (Repsol).

2. **For B14 ladder-enrichment robustness:**
   - Drop Repsol from any "dominant scale" specification — they never
     contribute to the strategic margin.
   - Add EDP-PT to the treatment set (drop from fringe placebo).
   - Keep IB/GE/GN/HC as the canonical dominant tier; treat
     "Other" small fringe CCGT as a "weak-treatment" group with
     intermediate pivotality (~40%).

3. **For B12/B13 q_2 outcome regressions:** the right placebo is
   non-pivotal firms (Repsol + Engie España + TotalEnergies + Moeve).
   These have CCGT capacity but no DA pivotality, so q_2 should not
   move at the reform.

4. **For the within-market granularity model:** the stylized firm in
   the model corresponds to "pivotal in the strategic-conduct
   moments." All else equal, σ²_within(h) drives strategic value
   only when the firm is pivotal at h.

## Source

DuckDB script run on `data/derived/panels/bid_shape_critical_flat/pivotal_indicator_DA_oct_dec_2025.parquet`,
joined with `mtu.classification.units` for parent-firm aggregation.
Output kept inline (not committed as a CSV).
