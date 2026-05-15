# Structural dominance audit: which firms have scale × flexibility?

**Created:** 2026-05-07

**Purpose.** Test whether the administrative `firm_class` partition (IB/GE/GN/HC
vs Fringe) survives empirically, using exogenous structural markers — not
bidding behavior. Driven by the methodological concern that classifying
firms by bidding patterns and then testing claims about their bidding patterns
is circular.

**Window.** 2025-10-01 to 2025-12-31 (post-MTU15-DA), Spanish zone only,
DA-cleared programmes (`pdbce`).

**Critical hours.** h{18, 19, 20, 21, 22} (price-peak, per
`_critical_hours_calibration.md`).

## Why we don't use RSI<1 here

RSI<1 (residual supply index < 1) requires a single firm's available
capacity to exceed (system-supply − demand). Spain has ~120 GW installed
vs peak demand ~40 GW; the typical surplus is 75-100 GW. Even Iberdrola's
30 GW portfolio is below the typical surplus, so RSI<1 fires only in
narrow stress events (low VRE + high demand + dry hydro + thermal outages),
probably <2% of hours. Most of the panel would be zeros and the
discriminating power lives in tail events that don't represent routine
strategic conduct.

**Relaxed pivotality used here:** scale within the flex-strategic segment
only (CCGT + reservoir hydro + Hydro_pump + Coal + Hybrid_RES_thermal).
Stripping out renewables and nuclear — which can't strategically withhold
in DA — gives a tighter denominator. A firm with ≥10% of cleared
flex-strategic MWh in critical hours has the scale that actually competes
at the strategic margin.

## Verdict (Oct-Dec 2025 critical hours)

| Firm | Total share | CCGT share | **Flex share** | Flex composition | **Dominant?** |
|---|---:|---:|---:|---:|:---:|
| **IB (Iberdrola)** | 15.98% | 14.68% | **30.78%** | 19.3% | ✅ |
| **GE (Endesa)** | 18.25% | 18.62% | **28.55%** | 24.7% | ✅ |
| **Repsol** | 6.40% | 17.53% | **17.03%** | 45.6% | ✅ |
| **GN (Naturgy)** | 6.93% | 31.81% | **14.34%** | 26.6% | ✅ |
| TotalEnergies | 1.26% | 13.33% | 5.38% | 70.6% | ❌ |
| HC (EDP-Spain) | 3.23% | 1.59% | **0.94%** | 4.2% | ❌ |
| Other (representadas) | 15.68% | 1.52% | 1.46% | 1.2% | ❌ |
| Engie | 2.04% | 0.39% | 0.17% | 0.8% | ❌ |
| Acciona | 6.66% | 0% | 0% | 0% | ❌ |
| ... | | | | | |

**HHI on flex-strategic segment, critical hours: 2,292.** Moderately-to-
highly concentrated by anti-trust standards (anti-trust threshold for
"highly concentrated" is HHI > 2500).

## Three substantive findings

### 1. The dominant tier is IB / GE / Repsol / GN — not IB / GE / GN / HC

Under the flex-segment relaxed test, **the four firms with ≥10% of
strategic flex-supply in critical hours are IB / GE / Repsol / GN**.

Repsol replaces HC as a structurally dominant firm. Repsol's two Spanish
CCGTs (ALG3 in Algeciras, ECT3 in Escatron) clear enough flex-MWh to
constitute 17% of the segment in critical hours.

This is a different cut than CNMC's "operador dominante en generación"
list (IB, END, NAT, EDP), and it's empirically correct: HC's Spanish
flex capacity has shrunk dramatically since the Soto-Ribera and As Pontes
coal retirements.

### 2. HC (EDP-Spain) is structurally tiny — its "dominance" status is anachronistic

HC has only **0.94% of flex-strategic critical-hour MWh** and 4.2% flex
composition. That's an order of magnitude below any reasonable dominance
threshold.

Yet the bid-shape evidence (`_per_firm_hourly_bidshape.md`) shows HC
bids identically to IB-style firms — perfectly mechanical 6-tranche
ladder, anchored at 699 €/MWh, all 24 hours.

Two possible reconciliations:

(a) **HC's classification is anachronistic.** EDP-Spain's Big-4 status
dates to when it owned Soto-Ribera (coal, ~600 MW, retired 2017-20) and
held larger thermal capacity. Post-coal-phaseout, EDP-Spain is
structurally a small generator. Demoting HC from the dominant tier is
empirically defensible.

(b) **HC's bidding mimics dominant firms because it's part of a
broader EDP coordination** (with EDP-Portugal, who DO have flex-segment
share — see `_fringe_ccgt_heterogeneity.md`). The 6-tranche identical
ladder might reflect a corporate trading-desk policy applied to both
EDP-Spain (HC) and EDP-Portugal CCGTs.

Either way, the structural verdict is unambiguous: HC is not in the
"scale + flex" dominant tier on its own. Whether to keep HC as a
"behavioral dominant" or demote them depends on what claim we're making.

### 3. Repsol is structurally dominant but behaviorally compliant

The most striking discrepancy: Repsol passes the structural test (17%
flex-share in critical hours) but **does NOT exhibit dominant-firm
bidding behavior**. From `_per_firm_hourly_bidshape.md`:

- Repsol p_max: 170-189 €/MWh (the LOWEST ceiling of any fringe firm,
  lower than any Big-4 firm except IB at 350)
- Repsol n_tranches: 3.5 (mid-range)
- Repsol mech_strict: 0.90 (mostly mechanical, no granularity exploit)

A firm with ≥10% of strategic flex-supply could exercise market power
via withholding, ladder-shaping, or near-cap bidding. Repsol does
**none of these**. Their CCGTs bid like price-takers despite having
the scale to bid otherwise.

**Hypothesis:** Repsol's vertical integration with their gas/oil
business makes their CCGTs effectively a hedging instrument against
their gas-supply position. Bidding low ensures clearing, which monetizes
their gas inventory at clearing prices — same mechanism as any
gas-marketer, just applied to their own gas. Strategic conduct in the
narrow market-power sense is suppressed by a broader portfolio-hedging
incentive.

If correct, this is an important point about market-power
identification: **scale is not sufficient when offsetting incentives
in adjacent markets push the firm toward price-taking behavior.**

## Implications for the firm_class partition

Three options:

**Option A: Empirical four-firm dominant tier.** Replace HC with Repsol.
Use IB / GE / Repsol / GN as the dominant tier going forward. The
within-day-DiD (B12, B13, B14) would re-run with Repsol in the dominant
arm. **Risk:** Repsol's price-taking bid behavior would dilute the
dominant coefficient — exactly because Repsol has scale but not conduct.

**Option B: Five-firm dominant tier with conduct stratification.** Keep
IB / GE / GN / HC and add Repsol, but stratify analyses by "scale +
exercises conduct" (IB / GE / GN / HC) versus "scale only" (Repsol).
The B14 ladder-enrichment finding belongs with the conduct group.

**Option C: Status quo.** Keep IB / GE / GN / HC. Acknowledge HC is a
"small but allied" dominant firm, and Repsol is a "scale without conduct"
example that we deliberately exclude. Document the limitation.

I'd recommend **Option B**: it acknowledges the empirical asymmetry
(scale doesn't imply conduct) and lets us preserve the headline B12/B14
results while gaining a Repsol-as-counterfactual robustness test.

## Sources

- `scripts/analysis/firm/structural_dominance_markers.py`
- `results/regressions/firm/dominance_audit/marker1_generation_share_by_firm.csv`
- `results/regressions/firm/dominance_audit/marker2_ccgt_share_by_firm.csv`
- `results/regressions/firm/dominance_audit/marker2b_flex_share_by_firm.csv`
- `results/regressions/firm/dominance_audit/marker2c_hhi_flex.csv`
- `results/regressions/firm/dominance_audit/marker3_flexibility_composition_by_firm.csv`
- `results/regressions/firm/dominance_audit/structural_dominance_combined.csv`

## Next steps

1. **Investigate HC anachronism**: confirm via CNMC reports / ENTSO-E
   capacity data that EDP-Spain's flex capacity has shrunk to ~1 GW or
   below post-coal-phaseout. If yes, demote HC from administrative
   dominant tier (or keep with caveat).

2. **Re-run B14 ladder-enrichment with Repsol in dominant tier** as
   Option B robustness — does β change sign or magnitude?

3. **Investigate ABO2G**: per `_fringe_ccgt_heterogeneity.md`, this unit
   bids identically to HC's pattern. Is it operationally tied to HC?
   Confirm via Aboño plant ownership chain.

4. **Re-run B13 fringe-placebo with cleaner fringe** (drop Portuguese,
   drop ABO2G if HC-tied, drop Repsol if reclassified).
