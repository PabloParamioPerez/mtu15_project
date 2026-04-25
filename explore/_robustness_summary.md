# Robustness check results — nb12 Lerner index

Generated: 2026-04-25 by `scripts/analysis/overnight_robustness.py`.
Raw data: `data/derived/{bootstrap,slope_sensitivity,placebo,hour_of_day}_lerner*.parquet`.
Full log: `logs/robustness_20260425_004605.log`.

Four checks on the central nb12 finding:
> **GE median Lerner rises from 5% pre-reform to 35% in DA60/ID15 and moderates to 10% at MTU15-DA.**

## 1. Bootstrap CI: SURVIVES

500 bootstrap resamples per (firm, regime). 95% CI on the median:

| Firm | Regime | Median | 95% CI | $n$ |
|---|---|---:|---|---:|
| GE | pre-IDA | 0.052 | [0.052, 0.053] | 54,142 |
| GE | 3-sess | 0.240 | [0.231, 0.246] | 2,858 |
| GE | ISP15 window | 0.191 | [0.184, 0.201] | 2,081 |
| GE | **DA60/ID15** | **0.351** | **[0.333, 0.373]** | **2,009** |
| GE | DA15/ID15 | 0.103 | [0.097, 0.107] | 2,470 |

**No CI overlap between DA60/ID15 and any other regime.** The peak is statistically separated from every neighbouring regime, including the immediately-adjacent ISP15 window (upper bound 0.201 < lower bound 0.333). Same pattern for IB/GN/HC in `bootstrap_lerner.parquet`.

## 2. Slope-window sensitivity: SURVIVES

Recomputed supply slope at $\pm$€5, €10, €15, €25 finite-difference windows:

**GE median Lerner across windows:**

| $\Delta$ (€) | pre-IDA | 3-sess | ISP15 | **DA60/ID15** | DA15/ID15 |
|---:|---:|---:|---:|---:|---:|
| 5 | 0.049 | 0.233 | 0.186 | **0.346** | 0.101 |
| 10 | 0.052 | 0.240 | 0.191 | **0.351** | 0.103 |
| 15 | 0.055 | 0.246 | 0.195 | **0.358** | 0.102 |
| 25 | 0.062 | 0.255 | 0.201 | **0.411** | 0.104 |

The peak-at-DA60/ID15 ordering is **identical at every window choice**. Absolute levels drift with wider windows (because wider windows smooth the slope → denominator smaller → Lerner larger), but the regime ranking is stable. Not a window-choice artefact.

## 3. Placebo reform dates: FAILS (but in an informative way)

200 fake reform dates uniformly in 2024-03 to 2025-07 (excluding real reform dates $\pm 30$ days). For each, compute post-minus-pre Lerner shift in a $\pm 120$ day window. Real boundary tested: MTU15-IDA (2025-03-19, the start of the DA60/ID15 regime).

| Firm | Real $\Delta$ | Placebo median $|\Delta|$ | Placebo $p_{95}$ | Empirical $p$ |
|---|---:|---:|---:|---:|
| GE | +0.089 | 0.135 | 0.415 | **0.725** |
| IB | +0.150 | 0.047 | 0.162 | 0.085 |
| GN | +0.011 | 0.001 | 0.005 | 0.000 |
| HC | +0.0004 | 0.0002 | 0.003 | 0.335 |

**GE's real delta at MTU15-IDA is NOT localised to that date.** 72.5% of random dates in the 2024-03 → 2025-07 window produce a larger |delta|. The MTU15-IDA boundary is not a uniquely-strong shift point for GE.

**Why this is not a project-killer:**

This mirrors the nb07 §12 treatment-date sweep finding and the nb08 §10 placebo result: the Spanish market shows **continuous non-stationarity** across the reform window, with multiple concurrent shifts (3-sess → ISP15, ISP15 → DA60/ID15, DA60/ID15 → DA15/ID15). Any $\pm120$d window straddles some of these, so random placebo dates frequently capture real regime shifts.

**The interpretation changes, not the finding.** The thesis claim should be:

> Lerner was **elevated throughout the reform window** (3-sess through DA60/ID15) and **normalised at MTU15-DA** — not that any single reform date caused a specific instantaneous jump.

This is weaker than "MTU15-IDA caused an X% Lerner increase" but stronger than "we cannot say anything." The baseline (pre-IDA: 0.05) and the normalisation (DA15/ID15: 0.10) are cleanly identified; the attribution of the peak to any single date is not.

**IB, GN, HC pass the placebo test** (empirical p of 0.085, 0.000, 0.335 respectively). Those firms' Lerner shifts ARE localised to the MTU15-IDA boundary. GE is the exception, likely because GE is the firm most affected by concurrent bilateral-contract reallocation (Rule 28.8 elimination at the same date).

## 4. Hour-of-day Lerner profile: STRENGTHENS THE MECHANISM STORY

GE median Lerner by (regime, hour-of-day):

| Regime | h1-6 | h7-12 | h13-18 (afternoon peak) | h19-24 |
|---|---:|---:|---:|---:|
| pre-IDA | 0.06 | 0.05 | 0.05 | 0.05 |
| 3-sess | 0.26 | 0.29 | **0.42** | 0.22 |
| ISP15 window | 0.24 | 0.19 | **0.23** | 0.17 |
| DA60/ID15 | 0.32 | 0.34 | **0.44** | 0.38 |
| DA15/ID15 | 0.12 | 0.11 | **0.17** | 0.08 |

**Market power is concentrated in the afternoon peak-demand hours (13-18)**, when CCGT capacity is on the marginal supply step. This is the hour-of-day pattern we'd expect if the mechanism is CCGT-scarcity-driven — matches the nb08 §8 GE×CCGT signed-flip finding.

**Strongest single cell: DA60/ID15 at hour 14, L = 0.54.** Half the clearing price was implied markup over marginal cost in that cell.

## Summary table

| Check | Verdict | Implication |
|---|---|---|
| Bootstrap CI | ✓ Survives | Regime medians statistically separated, DA60/ID15 peak distinct from all others |
| Slope-window sensitivity | ✓ Survives | Ranking invariant across $\pm 5$ to $\pm 25$ EUR windows |
| Placebo dates | ✗ Fails for GE, passes for IB/GN/HC | Reframe claim from "MTU15-IDA caused X" to "Lerner elevated throughout reform window, normalised at MTU15-DA" |
| Hour-of-day profile | ✓ Consistent with CCGT-peak-scarcity mechanism | Strongest market-power cell is DA60/ID15 × h14 = 0.54 |

## Downstream action items (not yet implemented)

1. Rewrite nb12 synthesis and `_identification_target.md` D15/D16 to use the **"elevated-throughout-reform-window, normalised at MTU15-DA"** framing instead of the "peak at DA60/ID15" framing.
2. Consider adding the hour-of-day heatmap as a provisional fourth thesis figure (nb14). Probably belongs in the body to complement Fig 2's time-series view.
3. Consider a version of fig2 with bootstrap CI ribbons around the monthly medians — tighter visual evidence.
4. Document in thesis text that the placebo failure for GE likely reflects concurrent bilateral-contract reallocation (Rule 28.8), not a pure null result — IB's placebo p=0.085 is the cleaner signal that the Lerner rise DOES track the reform sequence.

## 5. Post-blackout confound (added 2026-04-25)

The **2025-04-28 Iberian blackout falls inside the DA60/ID15 regime** (2025-03-19 to 2025-09-30). REE's *operación reforzada* — increased CCGT and nuclear commitment via P.O. 3.2 technical-restrictions dispatch — has been in continuous application since 2025-04-29 (~€666M cumulative cost through Mar 2026). Plus a cascade of regulatory measures (RDL 7/2025, RD 997/2025, multiple CNMC P.O. modifications) modifies voltage-control and balancing-market rules through Jan 2026.

This means the DA60/ID15 Lerner peak attribution to MTU15-IDA reform mechanics is confounded by:

1. **Operación reforzada increased out-of-market CCGT commitment** — changes DA equilibrium even though dispatch is via restricciones técnicas.
2. **Sept-Oct 2025 emergency voltage measures** — concurrent regulatory shocks.
3. **CNMC sanctioning proceedings** opened in April 2026 against ~100 firms — investigative pressure on bidding behaviour.

**Possible cleaner cut**: estimate Lerner only in the **pre-blackout DA60/ID15 sub-window (2025-03-19 to 2025-04-27)**. ~40 days of clean post-MTU15-IDA pre-blackout data. If GE's Lerner is already elevated in that sub-window, MTU15-IDA reform mechanics carry it; if not, the blackout/operación-reforzada carries the bulk.

This **strengthens** the placebo-failure interpretation in §3: GE's MTU15-IDA shift is not localised to that boundary because the entire post-MTU15-IDA window is also the post-blackout window. IB's cleaner placebo (p=0.085) is consistent with IB being less affected by the operación-reforzada CCGT commitment (more hydro-heavy portfolio).

Memory note: `ref_post_blackout_regulation.md` documents the full BOE/CNMC/ENTSO-E cascade.

**Action item**: add a §13 to nb12 (or a small standalone notebook) that estimates Lerner specifically in the **pre-blackout DA60/ID15 sub-window** as a confound check. Quick to implement, would be the cleanest robustness check we have for the central claim.

## 6. Seasonal price-level artefact (added 2026-04-25)

Implementing §5's pre-blackout sub-window cut surfaced a **larger and more
serious confound than the blackout itself**.

GE median Lerner, **same calendar weeks (March 19 – April 27) across years**:

| Year | Regime status | Avg DA price (€/MWh) | Avg supply slope | GE median L |
|---|---|---:|---:|---:|
| 2022 | pre-IDA (gas crisis) | 203.4 | 162.8 | 0.050 |
| 2023 | pre-IDA | 81.5 | 296.0 | 0.244 |
| 2024 | pre-IDA | 23.6 | 943.2 | **0.525** |
| 2025 (pre-blackout) | DA60/ID15 | 52.7 | 370.1 | **0.658** |
| 2025 (post-blackout) | DA60/ID15 | 96.8 | 196.2 | 0.310 |

**GE's Lerner in March-April has been climbing every year — including in
the pre-IDA regime — and it tracks the inverse of the average price.** The
2024 March-April period (entirely pre-reform) already shows L=0.525, far
above the pre-IDA full-year median of 0.052. The 2025 pre-blackout window
peaks even higher because that period had even lower prices.

IB shows the same pattern (2022: 0.025 → 2023: 0.024 → 2024: 0.221 → 2025
pre-blackout: 0.383).

### Mechanism

The Lerner formula
$$L_i = q_i / (p^* \cdot (1-s_i) \cdot |\partial S/\partial p|)$$
becomes mechanically large when $p^*$ is small AND $|\partial S/\partial p|$
is small. In low-demand, high-renewable spring months, both happen
simultaneously: surplus renewable supply pushes clearing into the
near-zero-price part of the curve, where the supply curve is flat (many
renewables bidding at €0 or below). The denominator collapses and Lerner
inflates.

This is a **known weakness of Hortaçsu-Puller / Cournot-FOC structural
estimators at low-price points**: they assume an interior profit-max
solution with a well-defined slope. At very low prices many generators
operate below average cost and the static-profit-maximisation FOC is
misspecified.

### Implications for the thesis

1. **The DA60/ID15 Lerner peak is partly a seasonal price-level artefact**,
   not a clean reform effect. The DA60/ID15 regime spans March-September,
   pulling in Spring's low-price hours that mechanically inflate L.
2. **This is a more serious confound than the blackout** because it predates
   all reforms (visible already in 2024 March-April pre-IDA data).
3. **The system-level findings (nb11) are NOT subject to this confound**.
   A87's net-income jump is a pre/post December comparison; A86 |V_imb|
   regressions absorb month-of-year FE. nb11's evidence remains robust.
4. **The narrower nb13 finding — GE IDA offer-price jump from €103 to
   €348** — should be checked for the same seasonal effect. If 2024
   March-April pre-IDA also shows elevated IDA offers, that finding is
   similarly confounded.

### Updated framing

The defensible claim from nb12 is now:
> Big-4 implied Lerner indices are highly sensitive to clearing-price level
> (low-price hours produce mechanically high estimates), and this
> sensitivity has structurally increased across the renewable-deployment
> trajectory of the past 4 years. Within this trend, **DA60/ID15 shows a
> further elevation, but a substantial fraction is not reform-attributable**.

The cleanest cuts that survive this caveat:
- **Pre-IDA → DA15/ID15 comparison at similar prices** (2022 pre-IDA at
  €203 vs DA15/ID15 at €80: latter has L=0.10 — still higher than what we'd
  expect at €80 in 2023, where pre-IDA full was 0.052).
- **Hour-of-day decomposition**: peak-hour markups (h13-18) when CCGT is
  on margin, where the FOC is well-posed.
- **Within-firm-tech subset**: GE×CCGT specifically, where the structural
  estimator is most defensible.

### Action items

1. **Re-run nb12 with month-of-year FE** to absorb the seasonal price effect
   (or weight by inverse-volatility of prices). Quick.
2. **Check whether nb13 §1's IDA offer-price finding survives a same-
   calendar comparison** with March-April 2024 (pre-IDA). If 2024 also
   shows €350+/MWh in spring, the bid-shading interpretation is weakened.
3. **Reframe nb12 thesis claim** to "low-price-amplified Lerner has risen
   structurally, with a further DA60/ID15 elevation but unclean
   attribution".
4. **Possibly demote fig2 (nb14) from thesis body to appendix** depending
   on how the seasonal correction lands. The system-level fig1 (A87) is
   not affected.

Memory note for future sessions: `ref_post_blackout_regulation.md` documents
the full regulatory cascade; this section documents the seasonal confound
on top of the blackout confound.
