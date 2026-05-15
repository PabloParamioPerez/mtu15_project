# Within-market granularity model — stub

**Created:** 2026-05-05 (post-CEMFI workshop)

**Purpose.** Provide a stylised industrial-organisation model that justifies the empirical design of `_modelling_track.md` §X (the within-day critical-vs-flat-hours DiD on dominant-firm signed IDA repositioning). The model has two jobs:

1. **Justify $\sigma^2_{\text{within}}$ as the right ranker for critical hours.** Show that the strategic value of within-market granularity is increasing in the within-hour variance of conventional production (i.e. the residual-demand profile faced by dispatchable units).
2. **Justify flat hours as a valid counterfactual.** Show that, in equilibrium, the firm's optimal post-reform bid in flat hours coincides with the pre-reform single hourly bid — so the granularity reform has no economic content there. This is what licenses the within-day DiD to identify the granularity-mechanism causal effect.

**Scope.** This is a stylised model in the partial-equilibrium / IO tradition, not a fully estimated structural auction model. The output is closed-form comparative statics, not a calibrated counterfactual.

**Workshop feedback (2026-05-05).** The selection of critical hours via $\sigma^2_{\text{within}}$ ranking is currently empirical and lacks theoretical grounding. The workshop discussant pushed on this. The model should answer: why is $\sigma^2_{\text{within}}$ the right measure, and not (e.g.) $\sigma^2_{\text{across}}$, the within-hour derivative of net load, or hour-of-day position alone?

---

## Section headers (to be filled in)

### 1. Setup

- A residual monopolist (or oligopoly with one dominant firm) facing residual demand $D_h(p)$ in each hour $h$.
- Within hour $h$, residual demand has a within-hour profile: write $D_{h,q}(p)$ for the residual demand in quarter $q \in \{1,2,3,4\}$ of hour $h$.
- Define $\bar D_h = \frac{1}{4} \sum_q D_{h,q}$ and $\sigma^2_{\text{within}}(h) = \frac{1}{4} \sum_q (D_{h,q} - \bar D_h)^2$.
- Two market-design regimes:
  - **MTU60 (pre-DA15-reform):** firm submits ONE bid $b_h$ per hour; clearing happens at $\bar D_h$ at price $p_h$.
  - **MTU15 (post-DA15-reform):** firm submits FOUR bids $\{b_{h,1}, b_{h,2}, b_{h,3}, b_{h,4}\}$ per hour; clearing happens independently in each quarter at $D_{h,q}$ giving four prices $\{p_{h,q}\}$.
- Marginal cost of the firm: $c_h$ (or quarter-specific $c_{h,q}$ if relevant for ramp costs).

### 2. The pre-reform optimum

- In MTU60, firm chooses $b_h$ to maximise $E[\pi_h] = E[(p_h - c_h) Q_h(b_h)]$ where $Q_h(b_h)$ is cleared quantity.
- Standard Cournot-residual-demand FOC gives $b^*_h$. Single bid per hour.

### 3. The post-reform optimum and the role of $\sigma^2_{\text{within}}$

- In MTU15, firm chooses $\{b_{h,q}\}_q$ to maximise $E[\sum_q \pi_{h,q}]$.
- Each quarter's optimum is the standard FOC against $D_{h,q}(p)$ in isolation: $b^*_{h,q}$.
- **Key observation 1 (intensive margin):** when residual demand is constant within the hour ($\sigma^2_{\text{within}} = 0$, all $D_{h,q}$ identical), the four quarter-FOCs coincide, so $b^*_{h,1} = b^*_{h,2} = b^*_{h,3} = b^*_{h,4} = b^*_h$. The firm's optimal post-reform bid identically matches the pre-reform single hourly bid. **Granularity has no economic content for flat hours.**
- **Key observation 2 (granularity value):** when $\sigma^2_{\text{within}} > 0$, the four FOCs diverge. The firm's expected profit under MTU15 is strictly greater than under MTU60. The increment is increasing in $\sigma^2_{\text{within}}$.
- This justifies the empirical ranking: hours with high $\sigma^2_{\text{within}}$ are where the granularity reform should differentially bite.

### 4. Predictions for the within-day DiD

- The model implies that the post-reform bid $b^*_{h,q}$ differs across quarters of an hour only in critical hours. In flat hours, $b^*_{h,q}$ is identical across $q$ — exactly the "mechanical-repeat across quarters" pattern we observe in wind, solar, and nuclear bid data.
- The DiD coefficient $\beta_3$ identifies the average treatment effect of the granularity reform on the firm's strategic-conduct outcome (here, signed IDA repositioning), conditional on the within-hour profile being steep. In flat hours where the profile is constant, the treatment effect is zero by construction.
- This is the formal sense in which the within-day DiD identifies the *granularity-mechanism causal effect*, not a generic post-vs-pre comparison.

### 5. Relation to existing literatures

- **Allaz–Vila (1993, JET):** A–V adds more forward markets *for the same delivery good*, eroding the residual monopolist's quantity-withholding incentive through commitment. The within-market granularity mechanism is *related in spirit* (finer instruments dispel rents) but operates by *splitting one delivery good into four sub-goods*. Mathematically distinct: the A–V mechanism is about sequential commitment with a fixed good; ours is about simultaneous bidding on multiple sub-goods.
- **Ito–Reguant (2016, AER):** I–R is about *sequencing across markets* (DA → real-time arbitrage) and the strategic content of intra-day repositioning under market power. Our outcome variable $q_2$ is the empirical signature they propose (signed IDA repositioning above DA commitment), but our reform changes *granularity within a market*, not the sequencing across markets. We adopt their measure but apply it to a different question.
- **Chang (2026, working paper):** settlement-period vs dispatch-period mismatch and SFE bidding. Doesn't apply directly to DA15/ID15 (settlement and dispatch are both 15-min — no averaging). Applies only to the transitional ISP15-with-DA60 window, and only to quantities (imbalance settlement averaged within the hour).
- **Hortaçsu–Puller (2008, RAND):** inverse-FOC sophistication test. Our identification doesn't require FOC inversion or Cournot pivotality assumptions — the within-day DiD identifies the strategic-conduct effect directly via cross-sectional differential. Hortaçsu–Puller-style sophistication can be brought back as a diagnostic, not as the primary identification.

### 6. Robustness extensions (to be developed)

- **Ramp costs.** Add a quadratic ramp cost $\kappa (Q_{h,q} - Q_{h,q-1})^2$ across quarters. Predicts that critical-hour bid-ladder slopes should be steeper for high-$\kappa$ technologies (CCGT, hydro) than for low-$\kappa$ technologies (wind, solar) — matches the bid-shape evidence (B14).
- **Forward commitment.** Allow firms to take DA positions $q^{\text{DA}}_{h}$ before the IDA opens. The strategic content of $q_2$ (the IDA repositioning) emerges as the residual choice variable. Predicts that $q_2$ critical-flat differential is positive when within-hour profile is rising (scarcity within the hour increases late-quarter willingness to sell) and negative when falling — matches the per-hour DiD two-cluster pattern.
- **Asymmetric market power.** Two-firm game (dominant + fringe): show that the fringe firm's optimal response to the dominant firm's critical-hour repositioning is opposite-signed because the fringe is squeezed out of strategic positions when the dominant firm bids more aggressively. Matches the fringe placebo (B13).

### 7. What this model is NOT

- Not a calibrated counterfactual.
- Not a structural auction estimation.
- Not a welfare analysis (welfare lives in `_modelling_track.md` §3 and §4).
- Not a complete description of the Spanish electricity market — many features (nuclear must-run, hydro reservoir constraints, transmission constraints, REE technical-restrictions overlay, dual-pricing imbalance settlement) are abstracted away. The justification is that the within-day cross-sectional comparison absorbs these as daily-level confounders by construction; the model only needs to capture the *differential* response across critical and flat hours within a day.

---

## Status (2026-05-05)

Stub only. Section headers are populated with target content; the formal derivations have not been written. Next steps:

1. Write Section 1 (setup) and Section 2 (pre-reform optimum) — should be ~2 pages of standard residual-monopoly Cournot algebra.
2. Write Section 3 (granularity value) — the key result is straightforward: $E[\pi^{\text{MTU15}}] - E[\pi^{\text{MTU60}}]$ is increasing in $\sigma^2_{\text{within}}$ and equals zero at $\sigma^2_{\text{within}} = 0$. This is the load-bearing piece.
3. Write Section 5 (relation to existing literatures) carefully — this is the part the workshop discussant will scrutinise.
4. Sections 4, 6, 7 are scaffolding; fill in last.

Estimated effort: 1–2 weeks of focused writing once Section 3 is sketched.

## Connection to other docs

- `_modelling_track.md` §X (the empirical design this model justifies)
- `CLAIMS_LEDGER.md` rows B12, B13, B14 (the empirical results this model rationalises)
- `thesis/proposal.md` § "Identification at the firm layer" (the proposal's articulation of this model's role)

---

## Empirical evidence supporting the model — DA Oct-Dec 2025 (added 2026-05-04)

The bid-shape critical-vs-flat tests in
`scripts/analysis/bid/critical_vs_flat_bidshape.py` directly probe two predictions
of the model — that strategic value of granularity is increasing in within-hour
residual demand variance, and that this value is highest in tight-system
(critical) hours. The DA Oct-Dec 2025 evidence (full submitted curves of
sell-side simple + MIC sub-orders, ~2.7M unit-hour cells) yields three robust
empirical regularities:

### 1. Strategic ladder enrichment in critical hours — Dominant CCGT only

For dominant CCGT in DA (35 units, 2643 unit-days with both critical and flat
hours observed; n=21k unit-hour cells):

| Measure | Mean Δ (critical − flat) |
|---|---:|
| Number of tranches (n_tr) | **+3.09** |
| Median tranche price (p_50) | **−205.8** EUR/MWh |
| Top tranche price (p_max) | −0.45 EUR/MWh |
| Bottom tranche price (p_min) | −260.5 EUR/MWh |
| OTM share | −0.17 |
| MIC volume share | 0.00 |

Read-out: dominant CCGTs anchor their TOP reservation prices across the day
and EXTEND THE LADDER DOWNWARD in critical hours by adding ~3 cheaper
tranches. The MIC volume share doesn't shift, ruling out
order-type-substitution as the channel — the change happens within the
curve sub-orders themselves.

This pattern is universal across 32 of 35 dominant CCGTs (Δ_n_tr > 0). Range:
Δ_n_tr ∈ [+0.0, +9.6]. Strongest enrichers: CTN4 and ARCOS3 (+9.6 tranches in
critical vs flat).

### 2. Selective MTU15 granularity exploitation — half of dominant CCGTs

**Definition correction (2026-05-04 evening).** "Mechanical repetition" requires
checking the full per-period (price, quantity) tranche tuples, not just
aggregate descriptors. The original descriptor-based measure (STD of
{p_min, p_max, p_50, qty_total} across the 4 quarters = 0) overstated
mechanical-repeat by ~1% overall but up to **8pp in cells with thin ladders**
(CCGT-Dominant in flat hours had descriptor mech = 95.7% but strict mech =
88.7%). The strict tranche-level hash is the right measure. Headline numbers
below use the strict definition.

For dominant CCGT in DA, the mechanical-repeat rate drops by **7.8pp** in
critical hours under the strict definition: **88.7%** mechanical in flat
hours → **80.9%** mechanical in critical hours. Cross-quarter STD of the
median tranche price increases by +17.2 EUR/MWh in critical hours.

Decomposition by unit (35 dominant CCGTs, strict definition):
- **17 units** drop mech_strict by ≥20pp in critical hours (active
  granularity exploiters: SROQ1, CTGN1/2/3, MALA1, SAGU1/2/3, PALOS1/2/3,
  PBCN1/2, SBO3, BES4, ACE4, CAMGI10 — typically Naturgy and Iberdrola
  CCGTs; flat-hour mech_strict ≈ 99-100%, critical ≈ 56-66%)
- **5 units** show small or zero change (BES5, SROQ2, BES3, PGR5, PEGO3 —
  always near-mechanical)
- **13 units** show OPPOSITE pattern: more mechanical in critical hours
  (ARCOS1/2/3, CTN3/4, CTJON2, ESC6, STC4, TAPOWER — flat-hour mech_strict
  ≈ 40-90%, critical = 100%). These units bid variably across quarters in
  flat hours (idling/ramping behaviour) but bid identically across all 4
  quarters in critical hours (running steady at full output). They do
  enrich the LADDER (Δ_n_tr = +6 to +9.6) but not the cross-quarter
  variation.

Three operational profiles emerge:
- **Granularity exploiters (17 units)**: vary bids quarter-by-quarter in
  critical hours; mechanical in flat hours.
- **Uniform enrichers (13 units)**: enrich ladder in critical hours but
  repeat the rich ladder across all 4 quarters (mech_strict goes UP in
  critical because flat-hour bidding is more variable).
- **Always-mechanical (5 units)**: near-100% mechanical in both regimes.

Both granularity exploiters and uniform enrichers are strategic responses
to higher within-hour residual demand variance — the model's σ²_within
mechanism — but they use different instruments. Future work should treat
these as distinct strategic types rather than a single "granularity
exploitation" axis.

### 3. Null effects elsewhere

- **CCGT-Fringe** (15 units, 1323 unit-days): top of ladder uniformly LOWERS
  by ~19 EUR/MWh in critical hours (Δ p_max = −19.1). No ladder enrichment
  (Δ_n_tr = +0.12). No granularity exploitation (Δ mech = −0.04). Different
  pattern from dominants — consistent with simpler reservation-price
  bidding without strategic granularity.
- **Renewables, biomass, cogen, storage hydro**: |Δ_n_tr| ≤ 0.4, |Δ p_50| ≤ 1
  EUR/MWh, |Δ mech| ≤ 0.03. No critical/flat differentiation.
- **Solar PV**: cross-sectional Δ values appear large (−425 to −663 EUR/MWh
  in p_50) but disappear within-unit-day (Δ ≈ 0). The cross-sectional
  pattern is entirely a selection artifact (different solar units bid at
  night vs day).
- **Wind / Solar PV / Biomass / Hydro_RES are flagged as physical_variation,
  not strategic.** Capacity-utilization diagnostic (sample week 2025-12-10..16):
  Wind-Fringe median util = 17%, Solar PV-Fringe = 18%, Biomass-Fringe = 52%
  — all far below installed capacity. Wind-Fringe and Solar PV-Fringe submit
  87% and 95% of MWh respectively at prices below 1 EUR/MWh; mean p_min ≈
  −10 EUR/MWh. They are price-takers offering whatever physical production
  they have. Their cross-quarter variation in mech-repeat reflects physical
  wind/sun variation across quarters (especially at sunrise/sunset for solar),
  not strategic granularity exploitation. The discrete strategic-vs-physical
  flag in `run_test2_granularity()` puts these techs in `physical_variation`.
- **Coal-Dominant** (1 active unit, EDP As Pontes): mirrors dominant CCGT
  pattern but stronger (Δ_n_tr = +2.73, Δ p_50 = −253). Single unit, treat
  as anecdotal.

### 4. Why this matches the model

§3 of the model predicts that strategic value of within-market granularity
increases in σ²_within(h), the within-hour variance of residual demand
faced by the dispatchable firm. The empirical pattern matches:

- Dispatchable firms with market power (dominant CCGT, dominant coal) show
  the strategic enrichment. Mechanical bidders (renewables, cogen) do not.
- Within the dispatchable-with-market-power group, the pattern is
  concentrated in critical hours (where σ²_within is highest), not flat
  hours.
- The granularity exploitation specifically — different bids per quarter —
  is the empirical signature of σ²_within > 0 in §3 of the model. About
  half of dominant CCGTs exhibit it, the rest enrich the ladder uniformly.

### 5. What this rules out

- The "richer ladders in critical hours" pattern is NOT a compositional
  artifact (different firms in critical vs flat hours): cross-sectional and
  within-unit-day Δ values match to four decimal places.
- The pattern is NOT driven by order-type substitution: MIC share doesn't
  change between critical and flat hours.
- The pattern is NOT a selection artifact: most dominant CCGTs appear in
  both critical and flat hours of the same days.

### 6. Same-calendar-month robustness — pre vs post MTU15-DA

Pre-MTU15-DA (Oct-Dec 2024, MTU60 with one hourly ladder) vs post-MTU15-DA
(Oct-Dec 2025, MTU15 with four ladders summed across an hour). Note: DA bid
prices pre-2025-03-19 are 0-padded due to a parser artefact, so only tranche
counts are directly comparable across the reform.

| Tech × Class | PRE Δ_n_tr/hour | POST Δ_n_tr/hour | Ratio post/pre |
|---|---:|---:|---:|
| **CCGT-Dominant** | **+2.55** | **+12.38** | **4.85×** |
| **CCGT-Fringe**   | +1.70 | +0.48 | 0.28× |
| Coal-Dominant     | +0.00 | +10.92 | new |
| Hydro-Dominant    | -0.09 | -1.41 | (sign reversed, both small) |
| Hydro-Fringe      | +0.15 | +0.42 | 2.80× |
| Hydro_pump-Dominant | +0.06 | +0.31 | 5.17× |
| Nuclear-Dominant  | -0.00 | -0.08 | (negligible) |
| Wind / Solar / Biomass / Cogen / Hydro_RES | ~0 | ~0 | (no change) |

**Three new empirical claims emerge from this comparison:**

1. **The ladder-enrichment pattern PRE-EXISTED MTU15-DA.** Dominant CCGTs already
   added +2.55 more tranches in critical hours under MTU60. Fringe CCGTs +1.70.
   Critical-hour strategic bidding is not an artefact of MTU15 alone.

2. **MTU15-DA AMPLIFIED the dominant-CCGT pattern 4.85×** while leaving Wind /
   Solar / Cogen / Hydro_RES unchanged. The reform handed dominant CCGTs the
   ability to post 4 distinct ladders per hour and they are using it
   aggressively in critical hours.

3. **MTU15-DA SUPPRESSED the fringe-CCGT pattern to 0.28× (from 1.70 to 0.48).**
   The likely mechanism: fringe CCGTs substituted curve sub-orders for block
   orders post-reform. As of Dec 2025, 58% of fringe CCGT DA-sell volume is in
   block orders (vs 13% for dominant CCGT — see _euphemia_order_types_check.md
   §6.1). Block orders are not curves and don't appear in the n_tr count. So
   what looks like "less ladder enrichment" is actually substitution to a
   different institutional channel.

The asymmetric response to the reform — dominants 4.85×, fringes 0.28× — is
itself a strong empirical signature of asymmetric strategic capacity.

### 7. IDA mirror analysis — Session 1, Oct-Dec 2025

Same critical-vs-flat tests applied to IDA Session 1 (full-day coverage,
simple-curve sell offers only). IDA was already MTU15 throughout this
window (since March 2025).

| Tech × Class | DA Δ_n_tr | DA Δ_mech | IDA Δ_n_tr | IDA Δ_mech |
|---|---:|---:|---:|---:|
| CCGT-Dominant | +3.09 | -0.125 | **-1.75** | -0.151 |
| CCGT-Fringe   | +0.12 | -0.036 | -0.04 | **-0.200** |
| Hydro-Dominant | -0.35 | -0.030 | -0.53 | -0.182 |
| Hydro-Fringe   | +0.11 | -0.018 | -0.26 | -0.255 |
| Nuclear-Dominant | -0.02 | -0.033 | 0.00 | -0.281 |
| Cogen-Dominant   | 0.00 | 0.000 | +0.03 | -0.193 |
| Wind, Biomass, Solar PV, Hydro_RES | ~0 | ~0 | ~0 | ~0 |

**IDA shows an OPPOSITE ladder pattern to DA but a BROADER granularity
exploitation pattern.**

- **No ladder enrichment in IDA.** Dominant CCGTs reduce tranches in critical
  hours (-1.75) and slightly raise the median price (+1.55 EUR/MWh). The
  initial-price-discovery story that explains DA enrichment doesn't apply in
  IDA: by IDA opening, the unit's day-ahead position is set; IDA bids are
  reoptimisation under updated information, not a new strategic
  price-quantity schedule.
- **Granularity exploitation is broad-based in IDA.** Mechanical-repeat rate
  drops 15-28pp in critical hours for CCGT (both classes), hydro (both
  classes), nuclear, and cogen. In DA, only dominant CCGT showed strong
  granularity exploitation (-12.5pp). In IDA, fringe firms also exploit
  granularity (CCGT-Fringe -20pp, Hydro-Fringe -25.5pp), often more than
  dominants.

**Interpretation.** The two markets serve different strategic functions and
the empirical signatures differ accordingly. DA is the place to set the
strategic supply curve for the day; the ladder-enrichment pattern reflects
the value of within-hour residual-demand variance σ²_within(h) for the
strategic firm with market power. IDA is the place to update bids
quarter-by-quarter as system conditions resolve; the granularity pattern in
IDA reflects ALL participants tracking realised conditions more finely in
critical hours, regardless of market power.

The model in §3 of this memo maps cleanly to the DA finding (strategic value
of granularity scaled by σ²_within). For the IDA finding, the model would
need extending — the relevant variance is no longer ex-ante residual-demand
variance but ex-post information-arrival variance. Both produce
critical-vs-flat differentials, but the underlying economic content differs.

### 8. Open empirical questions

- **Is the dominant-CCGT MTU15-DA amplification driven by the firm's portfolio
  size?** Larger firms (more units) might exploit granularity more. Decompose
  by IB vs GE vs GN vs HC.
- **Does the IDA granularity exploitation track DA clearing-price tightness?**
  Within-day variation in DA clearing should pin down which IDA hours are
  "critical" in the system-tightness sense, sharpening the DA→IDA link.
- **Does fringe-CCGT block-order substitution actually happen under MTU15-DA?**
  Track per-fringe-unit pre/post Δ in (curve_share, block_share) and link
  to the n_tr suppression. (Probably the next test to run.)
- **Is the pattern bigger when reserves are tight?** Cross-day variation in
  reserve margin or clearing-price spread should amplify the test.
