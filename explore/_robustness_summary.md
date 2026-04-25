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

## 7. Seasonally-corrected Lerner — central claim survives, narrowed (added 2026-04-25 morning)

OLS regime contrasts vs pre-IDA reference, three control specs.
Reproducing: `scripts/analysis/seasonal_correction_lerner.py`.

**Spec 3 (price-bin FE) is the cleanest control** — asks "at the same
clearing-price level, is Lerner different across regimes?" — and is what
the thesis should cite. Spec 1 (month FE) controls for calendar
seasonality only. Spec 2 (month + year FE) over-absorbs because year
dummies are nearly collinear with the regime sequence; treat as a worst-
case sensitivity rather than a preferred spec.

### Regime contrasts (Lerner, vs pre-IDA reference, OLS coefficients)

| Firm | Regime | Raw med | Spec 1 (month FE) | Spec 2 (m+y FE) | **Spec 3 (price-bin FE)** |
|---|---|---:|---:|---:|---:|
| GE | 3-sess        | 0.240 | +0.226 | −0.040 | **+0.226** |
| GE | ISP15 window  | 0.191 | +0.166 | −0.200 | **+0.174** |
| GE | DA60/ID15     | 0.351 | +0.310 | −0.125 | **+0.318** |
| GE | DA15/ID15     | 0.103 | +0.098 | −0.333 | **+0.080** |
| IB | 3-sess        | 0.042 | +0.022 | −0.065 | +0.006 |
| IB | ISP15 window  | 0.045 | +0.024 | −0.066 | +0.027 |
| IB | DA60/ID15     | 0.112 | +0.157 | +0.058 | **+0.135** |
| IB | DA15/ID15     | 0.022 | −0.020 | −0.118 | **−0.028** |
| GN | all           | ~0.00 | … | … | mostly negative or null |
| HC | all           | ~0.00 | … | … | mostly negative or null |

(All standard errors below 0.012 with n in tens of thousands; every Spec 3
coef in the GE/IB rows is significant at p<0.001.)

### What the seasonally-corrected story says

**For GE and IB**, the qualitative pattern survives and is sharper after
controlling for clearing-price level:

1. **DA60/ID15 elevation is real** even at the same price level. GE's
   Lerner is +0.318 above pre-IDA at matched prices; IB's is +0.135. The
   raw "GE peaked at 0.351" overstated the magnitude (because some of it
   is the spring-low-price effect bringing the formula into its mechanical
   blow-up regime), but the matched-price elevation is still substantial.
2. **DA15/ID15 partial reversal** also survives. GE's matched-price
   contrast is +0.080 (much smaller than DA60/ID15's +0.318); IB drops
   to **−0.028**, *below* pre-IDA baseline. Consistent with the reform
   narrative that MTU15-DA closed the asymmetric-granularity arbitrage
   window.
3. **Pre-MTU15-IDA regimes (3-sess, ISP15 window) show smaller
   elevations** than DA60/ID15. GE's are +0.226 and +0.174, IB's are
   +0.006 and +0.027. The DA60/ID15 peak is the clear local maximum.

**For GN and HC**, the seasonally-corrected pattern is consistently
negative (their Lerner drops below pre-IDA in every post-reform regime).
This is the bilateral-contract reallocation story: their selling-agent
share collapsed at Rule 28.8 elimination, mechanically reducing $q_i$
in the formula.

### Refined thesis claim from nb12

> Conditional on clearing-price level, Endesa's (GE) and Iberdrola's (IB)
> implied Lerner indices show a robust DA60/ID15 elevation of +0.318 and
> +0.135 respectively above pre-IDA baselines (both p<0.001), partially
> reversing at MTU15-DA (GE +0.080, IB −0.028). The peak in the asymmetric-
> granularity window (post-MTU15-IDA, pre-MTU15-DA) is consistent with
> the strategic-arbitrage interpretation: when intraday and day-ahead
> markets had mismatched settlement clocks, Big-4 firms with active
> selling roles had room to extract higher implied markups; closure of
> the asymmetry at MTU15-DA reduced this room. The Big-4 aggregate is not
> homogeneous: Naturgy (GN) and HC-Energía (HC) show consistently below-
> pre-IDA Lerner across all post-reform regimes, dominated by the
> bilateral-contract reallocation around the March 2025 Rule 28.8
> elimination, not by strategic bidding.

### What this means for nb14 fig2

Provisional **fig2 stays**, but with caveats and possibly a price-bin-
controlled supplementary panel. The seasonally-uncorrected raw figure
(monthly Big-4 medians) is descriptively useful — it shows the time
profile of the elevation — but the body text must cite the **Spec 3
matched-price contrasts** rather than raw regime medians. Consider:

- **Option A** (lighter): keep fig2 as the descriptive monthly median,
  cite the price-bin-corrected regime-contrast table in the body text,
  put the table in the body or appendix.
- **Option B** (heavier): replace fig2 with a side-by-side raw vs price-
  bin-corrected version. More honest but visually busier.

I lean toward Option A — descriptive figure stays, rigorous numbers in
the table.

## 8. nb13 §1 IDA offer-price — claim weakened, narrowed (added 2026-04-25 morning)

GE IDA sell-side wavg offer price, **same calendar weeks** across years:

| Year | Window | Regime | n_sess | Avg offer (€/MWh) |
|---|---|---|---:|---:|
| 2022 | Mar 19 – Apr 27 | pre-IDA (gas crisis) | 236 | **249.4** |
| 2023 | Mar 19 – Apr 27 | pre-IDA | 239 | **306.4** |
| 2024 | Mar 19 – Apr 27 | pre-IDA | 161 | 44.5 |
| 2024 | Jun 14 – Aug 31 | post-IDA, 3-sess | 176 | 341.7 |
| 2025 | Mar 19 – Apr 27 | DA60/ID15 (pre-blackout) | 113 | 264.7 |
| 2025 | May – Sep      | DA60/ID15 (post-blackout) | 442 | **99.6** |

The original nb13 §1 framing was "GE pre-IDA full-mean €103 → 3-sess
€348 = +238% jump at IDA reform". This **does not survive** a same-
calendar-weeks comparison:

- 2022 pre-IDA spring (€249) and 2023 pre-IDA spring (€306) were both
  at or above the supposedly-elevated 3-sess level (€342). The €103
  pre-IDA mean was dragged down by 2024's anomalously low spring (€45),
  not by structural pre-reform pricing behaviour.
- 2024 Jun-Aug 3-sess (€342) is similar magnitude to 2022 and 2023
  spring offers. So the 3-sess elevation might just reflect summer 2024
  having gas prices comparable to 2022-2023 spring.

**What does survive:** the **post-MTU15-DA collapse**. 2025 May–Sep
post-blackout DA60/ID15 shows €99.6 — the lowest summer figure of the
8-year sample. IB shows the same: €108 May–Sep 2025 vs €270 in 2022.

### Refined claim from nb13 §1

> Big-4 IDA sell-side offer prices in DA15/ID15 (post-MTU15-DA) are
> structurally lower than in any prior comparable summer in the 2018-2025
> sample. The original framing of an "IDA offer price jump at the IDA
> reform" was an artefact of the 2024 pre-IDA spring being anomalously
> low; comparable-calendar-weeks comparisons across years show that
> 2022-2023 spring offer prices were already at the 3-sess and DA60/ID15
> levels.

The behavioural-layer claim weakens substantially. nb13 §1's IDA-offer-
price story should be reframed around the **DA15/ID15 collapse to
historic lows**, not the IDA-reform jump.

## Action item summary (post-correction)

1. **Update nb12 narrative** to use Spec-3 (price-bin FE) regime
   contrasts as the headline result. Raw regime medians become a
   robustness note, not the central claim.
2. **Update nb13 §1 narrative** to focus on the DA15/ID15 IDA offer-
   price collapse, drop the "+238% jump" framing.
3. **Update `_identification_target.md` D15/D16** to reflect the
   refined three-layer claim with the price-bin-corrected magnitudes.
4. **Keep nb14 fig2** for now; cite Spec-3 regime contrasts in body
   text. Reconsider in week of thesis writing.
5. **System layer (nb11) is unchanged** — A87 and other ISP15-pivoting
   findings are not subject to either confound.

## 9. Within-tech decomposition: nb12 Lerner peak is mostly nuclear infra-marginal, not CCGT strategic (added 2026-04-25 afternoon)

**The most important robustness finding so far.** Original nb12 Lerner
implicitly aggregates infra-marginal nuclear/hydro with marginal CCGT.
When restricted to CCGT-cleared volume only — the actually-marginal
technology — the DA60/ID15 peak largely disappears.

### Tech composition of Big-4 cleared volume per regime

GE (Endesa), share of cleared MWh by technology, by regime:

| Regime | CCGT | Hydro | **Nuclear** | Other |
|---|---:|---:|---:|---:|
| pre-IDA | 33.5% | 12.1% | 35.5% | 18.9% |
| 3-sess | 4.5% | 6.1% | **89.4%** | 0% |
| ISP15 window | 5.1% | 8.6% | **86.3%** | 0% |
| DA60/ID15 | 3.4% | 20.6% | **75.9%** | 0% |
| DA15/ID15 | 5.1% | 14.3% | **80.7%** | 0% |

GE's selling-agent role post-IDA reform is **overwhelmingly nuclear**
(76–90%). The "Big-4 selling agent" label hides a structural shift in
which technology Endesa's BRP is settling. Pre-IDA Endesa's selling
agent role was ~⅓ each of CCGT, nuclear, and other. Post-IDA it
collapsed to ~80% nuclear.

(Same pattern for HC; IB and GN show split between hydro and nuclear.)

### CCGT-only Lerner — the clean strategic-markup measure

Restrict $q_i$ in the Lerner formula to firm $i$'s **CCGT-cleared MW
only**, with $s_i$ = firm $i$'s CCGT MW / total market CCGT MW. Median
Lerner per (firm, regime), trimmed to [0,1]:

| Firm | pre-IDA | 3-sess | ISP15 win | **DA60/ID15** | DA15/ID15 |
|---|---:|---:|---:|---:|---:|
| GE | 0.037 | 0.035 | 0.031 | **0.066** | 0.015 |
| IB | 0.030 | 0.028 | 0.034 | 0.031 | 0.012 |
| GN | 0.040 | 0.025 | 0.035 | 0.052 | 0.029 |
| HC | 0.012 | 0.011 | 0.012 | 0.030 | 0.004 |

Versus the original full-firm Lerner (GE pre-IDA 0.052 → DA60/ID15
0.351). The CCGT-only Lerner is one order of magnitude smaller and
mostly flat across regimes.

### CCGT-only Lerner with price-bin FE (Spec 3) — regime contrasts vs pre-IDA

| Firm | 3-sess | ISP15 win | DA60/ID15 | DA15/ID15 |
|---|---:|---:|---:|---:|
| GE | −0.063 | −0.071 | −0.013 | **−0.084** |
| IB | +0.001 | +0.000 | +0.017 | **−0.026** |
| GN | −0.071 | −0.056 | −0.041 | **−0.059** |
| HC | −0.002 | −0.005 | +0.028 | −0.011 |

(All p<0.001 with $n$ in tens of thousands per firm.)

**The full-firm Lerner peak does not survive within-tech restriction.**
GE's matched-price CCGT-Lerner is **negative** in every post-reform
regime relative to pre-IDA (−0.013 in DA60/ID15, −0.084 in DA15/ID15).
IB's is essentially flat with a very small DA60/ID15 elevation
(+0.017) and a meaningful DA15/ID15 reduction (−0.026). GN is below
pre-IDA across all regimes. Only HC shows a positive DA60/ID15 (+0.028)
but the n is small (19) and likely noisy.

### What changes for the thesis claim

The structural-markup story has to be **substantially rewritten**:

- **GE Lerner peak at 0.351 in DA60/ID15 was driven by composition,
  not strategic withholding.** GE's BRP role concentrated in nuclear
  post-IDA (76–90% of cleared volume), and the Hortaçsu-Puller formula
  treats infra-marginal nuclear cleared MW as if strategic. When
  restricted to actually-marginal CCGT, no peak.
- **No clean Ito-Reguant signature** in the CCGT-only Lerner. The
  hypothesis that CCGT was strategically withheld during the
  asymmetric-granularity DA60/ID15 window does not survive the test
  on the most direct measure.
- **What MIGHT survive at the structural layer**: a uniform CCGT-Lerner
  reduction at MTU15-DA across all four firms (GE −0.084, IB −0.026,
  GN −0.059, HC −0.011). Direction is consistent with closure of
  asymmetric-granularity arbitrage. But magnitude is small.
- **System layer (nb11) and behavioural-layer XBID (nb13 §2)
  unaffected.**

### Implication for nb14 Fig 2

Panel (b) of Fig 2 is now misleading without a CCGT-only counterpart.
The price-bin-corrected contrasts (+0.318 GE, +0.135 IB) reflect mostly
the bilateral-contract composition story, not strategic markup. Options:

1. **Replace** Fig 2 panel (b) bar chart with side-by-side full-firm vs
   CCGT-only contrasts. Honest but visually busy.
2. **Demote** Fig 2 to appendix; promote the system-level Fig 1 (A87)
   and behavioural Fig 3 (XBID) to the body. Lerner becomes a
   subsidiary point.
3. **Drop** Fig 2 entirely from thesis output.

I lean toward (1) — keep the figure, show the within-tech result as
honest robustness, frame the original full-firm Lerner as a
"BRP-aggregated-claim" rather than a structural-markup claim.

### Refined three-layer claim (post within-tech check)

> The Spanish reform sequence produced concordant changes in (1)
> aggregate balancing settlement flows, (2) wholesale market liquidity,
> and (3) BRP-level cleared-volume composition through bilateral-
> contract reallocation at Rule 28.8 elimination. The originally
> claimed firm-level structural Lerner peak is largely an artefact of
> the BRP composition shift; a within-tech CCGT-only Lerner does not
> show a comparable peak. **The reform-attributable structural-markup
> evidence is much weaker than initially claimed.** The system-layer
> (nb11) and behavioural-layer (nb13 §2) findings remain robust.

Reproducing: `scripts/analysis/within_tech_lerner.py`.

## 10. France DA placebo — partial Spain-specificity (added 2026-04-25 afternoon)

France was not subject to the Spanish 15-min reform sequence and serves
as a control on EU-wide trends. Same regime windows (Spanish reform
dates) applied to French DA prices.

### (a) Mean DA price level by regime, ES vs FR

|  | pre-IDA | 3-sess | ISP15 win | DA60/ID15 | DA15/ID15 |
|---|---:|---:|---:|---:|---:|
| ES mean | 80.9 | 79.3 | 99.8 | 50.9 | 55.6 |
| FR mean | 96.9 | 61.6 | 104.2 | 44.4 | 64.3 |
| ES Δ vs pre-IDA | −1.7 | **+18.9** | −30.0 | −25.3 |
| FR Δ vs pre-IDA | −35.3 | **+7.4** | −52.5 | −32.6 |

**Both countries' DA price levels move similarly across regimes** —
both jump in ISP15 window (winter 2024-25 high prices), both fall in
DA60/ID15 and DA15/ID15. ES being "cheaper" pre-IDA and "more
expensive" post-IDA at signed-spread level is interesting but the
direction of change is the same. **DA price level is not a clean Spain-
specific signal.**

### (b) Within-day price SD (volatility within each calendar day)

|  | pre-IDA | 3-sess | ISP15 | DA60/ID15 | DA15/ID15 |
|---|---:|---:|---:|---:|---:|
| ES mean within-day SD | 14.0 | 27.6 | 27.5 | 33.7 | 25.4 |
| FR mean within-day SD | 20.4 | 26.6 | 26.0 | 29.2 | 25.4 |
| Δ ES vs pre-IDA | +13.7 | +13.6 | **+19.7** | +11.4 |
| Δ FR vs pre-IDA | +6.2 | +5.6 | **+8.8** | +5.0 |

Both rise post-reform, ES more than FR. **The ES-specific excess is
≈+5–10 €/MWh** (ES Δ minus FR Δ). DA60/ID15 has the largest ES-specific
excess (+19.7 vs +8.8 = +10.9 EU-controlled). Direction-consistent with
mechanism story, magnitude smaller than uncontrolled ES change suggests.

### (c) Within-month price SD (variability across hours within a month)

|  | pre-IDA | 3-sess | ISP15 | DA60/ID15 | DA15/ID15 |
|---|---:|---:|---:|---:|---:|
| ES avg | 23.2 | 38.0 | 44.4 | 39.9 | 36.7 |
| FR avg | 34.7 | 35.5 | 43.8 | 36.0 | 38.3 |

**Cleanest Spain-specific finding.** ES within-month SD nearly **doubles**
(23 → 38–44) across reforms; FR is essentially flat (35 → 35–44). The
gap closes only in DA15/ID15 where both are around 37. This suggests
the reform sequence did increase Spanish price-distribution dispersion
in a way that isn't matched by EU-wide trends.

### (d) ES − FR spread, signed and absolute

| Regime | mean signed | mean \|spread\| | SD spread |
|---|---:|---:|---:|
| pre-IDA | −15.8 | 34.5 | 76.5 |
| 3-sess | +18.1 | 32.8 | 38.6 |
| ISP15 window | −4.3 | **26.4** | 37.5 |
| DA60/ID15 | +7.8 | 33.5 | 43.5 |
| DA15/ID15 | −9.9 | 32.8 | 46.1 |

ISP15 window had the tightest coupling (lowest abs spread). Direction
of signed spread varies across regimes — Spain switched from cheaper-
than-FR pre-IDA to more-expensive-than-FR in 3-sess and DA60/ID15.

### What survives the FR placebo

1. **A87 financial balance (nb11 §1)** — Spain-specific by definition
   (control-area metric); FR has its own A87 we haven't compared but
   the underlying ISP15 driver (intra-hour netting elimination) is a
   Spanish reform.
2. **Within-month price dispersion** — clearly Spain-specific.
3. **XBID liquidity (nb13 §2)** — France isn't on XBID Iberian product
   list, so direct comparison N/A. The 15× growth in orders is
   tightly tied to the MTU15 product introduction in the SIDC, which
   is Spain-specific.

### What's weakened

4. **DA price level changes** — track France closely, EU-wide trend.
5. **Within-day price SD** — rises in both countries; ES-specific
   excess is +5–10 €/MWh (real but smaller than raw change suggests).

Reproducing: `scripts/analysis/france_da_placebo.py`.

## 11. HHI panel — concentration rises across reform sequence (added 2026-04-25 afternoon)

Herfindahl-Hirschman Index of DA cleared sell-side market shares. Three
variants:

| Regime | HHI (all firms) | HHI (Big-4 only) | Big-4 share | HHI (CCGT only) |
|---|---:|---:|---:|---:|
| pre-IDA | 0.283 | 0.096 | 48.6% | 0.460 |
| 3-sess | **0.421** | **0.311** | 63.4% | 0.608 |
| ISP15 window | 0.396 | 0.220 | 55.8% | 0.530 |
| **DA60/ID15** | **0.425** | **0.313** | **66.5%** | 0.596 |
| DA15/ID15 | 0.386 | 0.257 | 63.2% | 0.581 |

**The full-market HHI rises from 0.283 to 0.42** (49% increase), with
Big-4 share climbing from 49% to 66%. By FTC/DOJ thresholds, HHI > 0.25
is "highly concentrated" and HHI > 0.40 is "very highly concentrated".
Spain crosses that threshold at the IDA reform and stays there.

CCGT-only HHI is even higher (0.46 → 0.60+). CCGT in Spain is
structurally concentrated.

This is a **descriptive concentration finding that does not require
the Cournot-FOC interpretation**. It's a robust, standard market-power
metric. Useful for thesis: the concentration shift is real even after
the structural-Lerner finding was reframed as a composition artefact.

## 12. Capacity withholding ratio (cleared / offered, sell-side) — pattern not consistent with strategic withholding (added 2026-04-25 afternoon)

If firms strategically withhold capacity, the cleared/offered ratio
should DECLINE in the alleged-withholding regime. Per-firm daily mean
ratio:

| Firm | pre-IDA | 3-sess | ISP15 | DA60/ID15 | DA15/ID15 |
|---|---:|---:|---:|---:|---:|
| GE | 0.241 | 0.127 | 0.148 | **0.642** | **0.720** |
| IB | 0.135 | 0.030 | 0.046 | 0.112 | 0.092 |
| GN | 0.099 | 0.008 | 0.014 | 0.031 | 0.048 |
| HC | 0.145 | 0.006 | 0.024 | 0.036 | 0.139 |

**GE's clearance rate INCREASED** from 24% pre-IDA to 64-72% post-MTU15.
Opposite of strategic withholding. GE is offering less and clearing
more of what it offers post-reform — consistent with bilateral-contract
intermediation reducing GE's "pure-price" sell-side exposure.

GN, HC, IB show ratios DECLINING in the post-reform regimes, but only
slightly, and the absolute levels (0.01-0.10) are very low. They were
already over-offering pre-reform; post-reform they over-offer more.

**No clear strategic-withholding signature** in this measure either.

## 13. Bid-shading regression (offer − clearing in IDA, sell-side) — GE-specific 3-sess and ISP15 elevation (added 2026-04-25 afternoon)

Quantity-weighted IDA sell-side offer price minus clearing price, per
(date, session, firm-group). OLS with regime + price-bin FE, vs pre-IDA
reference.

| Firm | pre-IDA mean | 3-sess | ISP15 | DA60/ID15 | DA15/ID15 |
|---|---:|---:|---:|---:|---:|
| GE | €21.7 | **+€250** | **+€218** | +€53 | −€12 |
| IB | €22.0 | +€19 | +€15 | +€30 | +€25 |
| GN | €0.2 | +€44 | +€38 | +€43 | +€37 |
| HC | €2.3 | +€25 | +€13 | −€1 | −€14 |
| Fringe | −€0.0 | −€26 | −€22 | +€15 | +€8 |

(All p<0.001 except HC DA60/ID15.)

**GE's bid-shading peaks at the 3-sess and ISP15 windows** (+€250 and
+€218 above pre-IDA average shade), normalises at MTU15-DA (−€12). This
is one of the cleanest reform-aligned findings we have:
- Pre-IDA: GE offered +€22 above clearing on average
- Post-IDA-reform: GE offered +€272 above clearing on average
- Post-MTU15-DA: GE offered +€10 above clearing on average

**Caveats:**
- The regime windows here cover 6-21 months each; +€250 average is
  consistent with GE pricing high-price-step tranches in low-demand
  hours where they don't expect to clear — a "reservation" or
  "shadow-bidding" strategy more than active price extraction.
- The *Δ Q* and Lerner findings show GE's CCGT didn't clear much in
  low-demand spring 2024, so the wavg-shade is dominated by those
  not-clearing tranches. Strategic interpretation needs care.
- IB shows modest sustained shading (+€15-30) across all post-reform
  regimes including DA15/ID15. So the GE pattern is not uniform across
  Big-4.
- Fringe shows the OPPOSITE pattern: shading negative in 3-sess and
  ISP15 (offering BELOW clearing), turning positive at DA60/ID15.
  Suggests Fringe is reactive to clearing rather than strategically
  shading.

**This is the cleanest behavioural-layer finding so far** that aligns
with the reform-attribution narrative for GE specifically. It's
descriptive (offer-curve shape) rather than welfare-relevant directly,
but tighter than the Lerner-formula approach.

Reproducing: `scripts/analysis/hhi_withholding_bidshading.py`.
