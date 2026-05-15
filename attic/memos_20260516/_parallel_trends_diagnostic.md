# Parallel-trends visual diagnostic for DA cleared MWh outcome

**Created:** 2026-05-08

**Purpose.** Look at the (critical_h18_22 − flat_h3_5) differential in DA
cleared MWh per CCGT-firm × month, across 2024-01 to 2025-12, BEFORE
running any DiD. Identifies whether parallel trends are plausible for
this outcome and which firms are usable.

**Outcome:** MWh per clock-hour-of-class-per-day. Energy normalized so
that MTU60 (1h periods) and MTU15 (0.25h periods) are directly
comparable. Computed per firm × month × hour_class.

## What I see

### Treatment-set firms

- **IB (Iberdrola)** — clearest growing critical-flat differential
  Oct-Dec 2025: 198, 164, 299 MWh/clock-h-day. Pre-reform 2024 data is
  volatile/sparse, hard to compare cleanly. Visually consistent with a
  reform-induced shift; pre-trend assessment is hampered by intermittent
  CCGT operation in 2024.

- **GN (Naturgy)** — present in 2024 with positive differentials but
  lots of missing months (CCGTs not always running).

- **GE (Endesa)** — patchy.

- **HC (EDP-Spain)** — small positive differential, fairly stable.

- **EDP-PT** — large differentials in early 2024 (707 in Jan, 186 in
  Dec 2024) but only one observation in 2025 (13.6 Dec). Post-reform
  EDP-PT CCGTs have rarely run, making post-trend assessment
  impossible.

### Placebo-set firms

- **Repsol** — striking pattern: positive crit-flat differential through
  early 2025, then **flips to LARGE NEGATIVE** starting June 2025
  (-167, -325, -840, -355 MWh/clock-h-day for Jun-Sep 2025), continuing
  into Oct-Dec. The shift happens BEFORE MTU15-DA (Oct 2025) — coincides
  with post-blackout reforzada and MTU15-IDA. Repsol is responding to
  *something* mid-2025 but not specifically to DA15.

- **TotalEnergies** — similar pattern: positive differential through
  early 2025, flips negative from June 2025. Same as Repsol — a mid-2025
  regime change, not a Oct-2025 effect.

- **Engie** — sparse, when running their critical bidding spikes; few
  flat-hour observations to compute differential.

- **Moeve** — barely any operations.

## Two problems for using DA cleared MWh as outcome

### Problem 1: extensive-margin attrition

CCGTs cycle on/off based on gas prices. Many month-firm cells are NaN
because the unit didn't run that month. Computing means over running
periods only biases UP and creates spurious volatility. A proper test
would:
- Include zeros for non-running periods (LEFT JOIN with full grid)
- Or restrict to "always-on" CCGTs (probably a tiny set)
- Or aggregate to firm-level monthly totals (less granular but denser)

### Problem 2: pre-period structural breaks

The June 2025 break in Repsol's and TotalEnergies's trajectories is
inconvenient. Either:
(a) These placebo firms reacted to blackout-era regulation in mid-2025
    (MTU15-IDA + reforzada) — meaning their pre-MTU15-DA "trend" is
    already polluted.
(b) Or there's a confounding event (gas price spike, CCGT capacity
    addition) that affected mid-2025 bidding regardless of MTU15.

Either way, **Oct-Dec 2024 (pre-reform reference) and Oct-Dec 2025
(post-reform) bookend a turbulent middle**. Same-cal-month comparison
(Oct-Dec 2024 vs Oct-Dec 2025) already controls for seasonality and
straddles the noise — that's defensible. But a full pre-trend panel
showing 2-3 years of stability is NOT what the data look like.

## Implications

1. **DA cleared MWh per firm × month is a noisy outcome** with serious
   extensive-margin issues. It can support same-cal-month comparison
   (Oct-Dec 2024 vs Oct-Dec 2025) but probably cannot support a
   12-month event-study window.

2. **The placebo firms (Repsol, TotalEnergies) show MID-2025 regime
   shifts** that don't match the MTU15-DA timing. They're responding
   to pre-DA-reform events. This complicates "fringe placebo unaffected
   by treatment" interpretation but doesn't break the within-day-DiD
   logic — the (critical − flat) differential is what matters, and we
   can include MTU15-IDA + reforzada controls in the spec.

3. **For the within-day-DiD on q_2 (Ito-Reguant)**, the right move is
   to construct q_2 at the (firm, hour, day) level from pdbce + pibcie
   + pibcice. That's a more complex panel build but gives the
   identification-strategy outcome directly. The DA cleared MWh
   diagnostic shown here suggests the data is dense enough for the
   firm-month aggregate but noisy for hour-level dynamics, so q_2 work
   should:
   - Aggregate to firm × month × hour-class (not firm × hour × day) for
     parallel-trends visualization
   - Use the same Oct-Dec 2024 vs Oct-Dec 2025 same-cal-month window as
     the main test

4. **The outcome with the cleanest theoretical anchor is q_2** — the
   Ito-Reguant withdrawn quantity. DA cleared MWh shows whether firms
   bid more aggressively in critical hours, but q_2 directly measures
   the strategic withholding-via-IDA-buyback that the model predicts.

## Next step

Build the q_2 panel at firm × month × hour-class for 2024-01 to
2025-12. Plot the parallel-trends figure for q_2 specifically.

If q_2 also has the same extensive-margin attrition, pivot to a
firm-MONTHLY aggregate (sum over all units within the firm) which
should be much denser.

## Sources

- `scripts/analysis/firm/parallel_trends_diagnostic.py`
- `results/regressions/firm/parallel_trends/monthly_critical_minus_flat_DA_cleared.csv`
- `figures/working/parallel_trends_DA_cleared_per_firm.png` and `.pdf`
