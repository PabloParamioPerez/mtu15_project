# Master thesis proposal — economic structure for 44 alive findings

**Date:** 2026-04-28; revised 2026-05-02 (kill pass) and 2026-05-05 (post-CEMFI-workshop pivot — within-day DiD added as headline identification; B12, B13, B14 added to CLAIMS_LEDGER). Counts: **44 alive · 7 wounded · 20 dead · 71 active rows**. Working draft.

**One-sentence thesis claim.** Spain's 2024–2025 reform sequence (6→3 IDA sessions; MTU15-IDA; MTU15-DA) reveals a **firm-level strategic-conduct response to within-market granularity** — identified by a within-day critical-vs-flat-hours DiD on dominant-firm signed IDA repositioning ($\beta_3 = +58.6$ MWh per firm-hour, $p < 10^{-6}$), supported by unit-level bid-shape evidence (CCGT richer ladders in critical hours) and a fringe-firm placebo with the opposite-signed coefficient — alongside **three reform-driven mechanism-design observations** at the system layer: (1) **asymmetric-clock friction** during the 10-month asymmetric-granularity window generated a ~€1.1B BRP→TSO settlement transfer that collapses cleanly at MTU15-DA; (2) **non-Pigouvian segment incidence** under uniform dual-pricing settles wind + LIB free-market retailers with 60–65% of imbalance cost in every post-ISP15 regime regardless of clock symmetry; (3) **operational-regime overlay** — the post-2025-04-28 *operación reforzada* runs continuously alongside the market layer (~€666M direct cost, €3.77B RRTT in 2025) — a missing-market patch that the markets-side reform (MTU15) cannot reach. Underneath these reform-driven observations, firm-level market power persists regime-invariant: IB extracts ~€820M of DA-clearing rent through hydro Cournot dispatch; cross-market specialisation (IB in DA hydro, GE in aFRR, Naturgy in CCGT redispatch) sharpens after the blackout without reordering.

**The within-day DiD result is the project's primary causal identification at the firm level.** It is distinct from but related to Allaz–Vila (1993, JET) and Ito–Reguant (2016, AER): the granularity reform divides one delivery good into four sub-goods rather than adding more forward markets for the same good, and the strategic content is intra-hour repositioning rather than cross-market arbitrage. Defending the flat-hour control rigorously requires a stylised IO model anchored in the within-market granularity mechanism — stub for that model: `notebooks/memos/_within_market_granularity_model.md`.

---

## Why this title and not the simpler ones

Three simpler narratives I considered and rejected:

1. **"MTU15 reform reduces market power"** — REJECTED. F7 (€820M IB transfer post-MTU15-IDA) and F8 (IB hydro Q4 share regime-invariant at +21pp gap vs Fringe across 2018-2026) make this empirically wrong. Whatever good MTU15 did, it did not break IB's hydro Cournot mechanism.

2. **"IB undersupplies voltage control to capture CCGT windfalls"** — REJECTED. F14 shows nuclear unaccounted reduction is system-wide (22-38% across all 7 reactors). F15 shows the post-blackout CCGT windfall went to **Naturgy +7.1pp share, not IB (-2pp)**. F19/F20 show the aFRR balancing windfall went to **Endesa, not IB**. The simple cross-firm vertical-integration moral-hazard story has no empirical support.

3. **"The blackout created the post-2025 anomalies"** — REJECTED. F8 blackout-split shows IB hydro Q4 share is regime-invariant pre/post-blackout. F7 blackout-split shows IB rent persists at DA15/ID15 (post-blackout) at +12.3% relative markup. S6 blackout-split shows the system-cost shift collapses at MTU15-DA EVEN THOUGH operación reforzada is still in effect. The blackout amplified some absolute-€ readings; the underlying mechanisms are reform-driven, not blackout-driven.

   **Operación reforzada as a regime overlay.** The post-2025-04-28 reinforced-operation regime is the dominant *operational* fact in the post-blackout panel — it forces ~20–30 CCGTs + nuclear daily via PO-3.2 RRTT, enlarges the secondary band (PO-1.5 / PO-7.2), introduces a new zonal reactive market (PO-7.4, BOE-A-2025-13076) and adds €666M direct cost (~€3.77B in RRTT total in 2025, +49% YoY). It is **active continuously from May 2025 onward** and therefore co-exists with both DA60/ID15 and DA15/ID15 — it is **not** a transient that fades away. Methodologically, **D_MTU15 and D_reforzada are non-collinear** in the 2025-03-19 → 2025-04-27 sub-window (clean post-MTU15-IDA / pre-reforzada); cross-regime claims in this thesis already use blackout-split decompositions and the principle is formalised in `docs/notes/SPANISH_MARKET_STRUCTURE.md` §11. Empirical signatures of reforzada are visible in our data (B9 q₂_RT2 surge +13.6 GWh/firm-day in DA15/ID15; F15 CCGT windfall to GN; F16 IB CCGT supply-curve break; F19/F20 GE aFRR capture; S9 solar capture-price collapse; S7 conv-RZ €210–300/MWh) — the regime overlay is real and identifiable.

The thesis claim above survives all three rejections.

---

## Methodology and identification

### Modelling scope (decision 2026-05-02)

The thesis combines three modelling layers, deliberately stopping short of a full structural model:

1. **Reduced-form empirics as the primary evidence.** All headline numbers (S6 €1.1B; B6 R² trajectory; F7 €820M; F10 pivotality shares) are reduced-form estimates with cluster-robust SE, regime FE, cal-month FE, day FE, VRE controls, blackout-splits, and same-cal-month robustness. The X1–X14 dead-DiD record documents the project's attempts at causal identification and why they didn't survive — this is an asset, not a gap.

2. **A simplified IO theory model to illustrate the asymmetric-granularity friction mechanism.** Analytical, not calibrated; closed-form comparative statics under stylised assumptions (representative BRP, symmetric clearing, dual-pricing imbalance settlement). Generates the predictions tested by S6/B6/B9. The friction primitive φ(M) = K σ √(M/J) is the core; Block 3 calibration shows it predicts the correct order of magnitude across regimes.

3. **Light structural exercises using firm first-order conditions** to test specific mechanism predictions. Examples already run: the Allaz–Vila §2 commitment-slope test (rejected as mechanical identity), the HP-sophistication implied-Cournot-vs-realized-markup test (rejected as conduct evidence). These are FOC-based mechanism tests, not full estimation.

**No heavy multi-firm dynamic structural model is built.** The thesis explicitly stops at the partial-equilibrium / reduced-form level. The IO contribution is the regime-invariance-of-market-power result + the three-mechanism-design observations of the reform sequence + the methodological identification design under reforzada + the **within-day flat-vs-critical-hours DiD design** that identifies the firm-level strategic-conduct response to within-market granularity (described in the next subsection).

### Identification at the firm layer — within-day flat-vs-critical-hours DiD (added 2026-05-05)

The granularity reform (MTU15-IDA, MTU15-DA) does not change market rules uniformly across hours of the day. It changes them differentially — the within-market granularity has economic content where the within-hour residual production profile is steep (morning ramps, evening peaks) and is mechanically irrelevant where the profile is flat (overnight plateaus). This asymmetry is the basis for a within-day treatment-control design that absorbs daily-level confounders by construction.

- **Treated** = critical hours $h \in \{7, 8, 16, 17, 18\}$ (top-5 by $\sigma^2_{\text{within}}$ of conventional production).
- **Control** = flat hours $h \in \{3, 4, 5\}$ (bottom of the same ranking; pre-dawn).
- **Pre** = pre-DA15-reform baseline (2018-01 → 2024-06-13).
- **Post** = DA15/ID15 (2025-10-01 onwards).
- **Specification**: $q_{fdh} = \alpha + \beta_1 \, \text{crit}_h + \beta_2 \, \text{post}_d + \beta_3 \, (\text{crit}_h \times \text{post}_d) + \gamma_f + \delta_{\text{DOW}(d)} + \varepsilon_{fdh}$, cluster-robust SE by date.

The DiD coefficient $\beta_3 = +58.6$ MWh per firm-hour (SE 12.18, $p = 1.5 \times 10^{-6}$, N = 78,232) is the project's primary causal claim. Robustness: same-calendar-month restriction survives at 96% magnitude; per-hour decomposition reveals two clusters with opposite signs (ramps + evening peaks positive; midday negative); the within-DA15/ID15 cross-sectional fact ($\beta_1 + \beta_3 = +99.8$ MWh) is constant across baseline-window choices. Conditional parallel trends with renewable + demand controls does NOT hold (only $\sim 13\%$ of the pre-trend absorbed) — the reform-attributable component is a range, not a tight point estimate.

**Supporting evidence at the unit level.** Post-MTU15-DA, dominant-firm CCGT in critical hours bid +8% more tranches per quarter, +13% steeper ladder slopes, and lower qty-weighted average prices than in flat hours — operators use the within-hour granularity to insure the cheap end and speculate on the expensive tail. Wind, solar, and nuclear show no engagement (mechanical-repeat across quarters). The dispatchable / non-dispatchable distinction is exactly what makes the within-day design plausible.

**Fringe-firm placebo.** The same DiD specification on top non-dominant firms (EV, DET, EGL, EHN, REP) yields $\beta_3 = -24.3$ MWh per firm-hour ($p = 2.7 \times 10^{-3}$) — opposite-signed. Granularity exploitation requires pivotal market power, and fringe firms by definition lack it.

**The theoretical-model gap.** The design assumes that, in equilibrium, within-market granularity has no economic content for flat hours. This is what justifies their use as control. A small stylised IO model is needed to defend this formally — anchored in the within-market granularity mechanism, not directly in Allaz–Vila or Ito–Reguant. Out of scope: a fully estimated structural auction model. Stub: `notebooks/memos/_within_market_granularity_model.md`.

### Identification under operación reforzada — primary strategy

Operación reforzada (post-2025-04-28, continuous to date) is the dominant confound for any cross-regime claim that touches data after May 2025. The thesis exploits **two structural features of reforzada** to identify reform effects cleanly:

**1. PDBC vs PDVD chain placement (the most important identification feature).**
Operación reforzada operates via PO-3.2 RRTT *after* OMIE clears PDBC. Therefore:

| Outcome family | Reforzada channel | Confound severity |
|---|---|---|
| PDBC-based: cleared volumes, marginalpdbc clearing prices, **F7 synthetic-firm transfer (computed at PDBC clearing prices)**, F1/F2/F3 implied Lerner (legacy), B9 q₁ DA-cleared, F13 marginal-density measure | Indirect — only via bidder expectations (firms anticipate post-clearing RRTT and bid accordingly) | **Lighter** |
| PDVD / PHF / P48-based: B9 q₂_RT2 (= PHF − PIBCA), RZ-61 activations (S8), F15 CCGT mix (A73-based), F16 IB CCGT slope (A73-based), F17/F18 plant-level dispatch | Direct mechanical (RRTT redispatch literally constructs these objects) | Heavy |

This split changes the calculus for several alive claims. **F7's €820M IB DA-clearing transfer is computed at PDBC clearing prices**, so reforzada hits it only through expectations — a much lighter confound than would apply if F7 were computed downstream. The blackout-split decompositions of F7/F8/F10/F11 may *over-state* the reforzada confound on these PDBC-side outcomes.

**2. Reforzada-held-constant pairings.**
The 6-week DA60/ID15-PRE-blackout window (2025-03-19 → 2025-04-27) is the only post-MTU15-IDA panel that is also pre-reforzada. This gives two natural-experiment comparisons that hold reforzada at a fixed level:

| Reform to identify | Comparison (reforzada held constant) | Window length |
|---|---|---|
| **MTU15-IDA effect** (intraday clock to 15-min) | ISP15-win → DA60/ID15-PRE-blackout (both pre-reforzada) | ~4 mo + ~6 wks |
| **MTU15-DA effect** (DA clock to 15-min, restoring symmetry) | DA60/ID15-POST-blackout → DA15/ID15 (both under reforzada) | ~5 mo + ~7 mo |

The S6 collapse at MTU15-DA (€91M/mo → €7.4M/mo) is the canonical demonstration: the granularity friction collapses *while reforzada is still active*, separating lever 1 (clock-symmetry, resolved) from lever 3 (operational missing-market, open).

**Caveats**: (a) the MTU15-IDA-effect window is short (40 days) — power-limited but informative. (b) reforzada itself evolves within DA60/ID15-POST → DA15/ID15 (PO-7.4 went live June 2025; emergency PO modifications layered Oct 2025; permanent extension Jan 2026); "reforzada held constant" is approximate.

### Other identification-discipline practices (already in use)

- **OVB protocol**: every claim's coefficient compared across sparse-FE and exogenous-augmented specs.
- **Same-calendar-month robustness**: every cross-regime claim restricted to matching cal-months as a minimum acceptable test.
- **Cross-country placebo**: B7 (France DA flat across Spanish reform dates) anchors the Spain-specificity of effects.
- **Bad-control discipline**: F11 (and others) flag where p_actual / IB-share / similar are jointly determined with the outcome and excluded.

---

## Structure: four parts + appendix

### Part I — System-layer reform impact: three mechanism-design observations

**Story**: The reform-impact section organises around three distinct mechanism-design observations, each with its own empirical anchor and its own policy lever:

**(1) Asymmetric-clock friction (the headline).** The 10-month asymmetric window (Dec 2024 — Sep 2025) during which DA cleared at 60-min while ID/imbalance settled at 15-min generated a ~€90–95M/month BRP→TSO settlement transfer (S6, ~€1.1B cumulative). Microfoundation: B6 forecast-error → imbalance pass-through R² = 0.171→0.365→0.028 across regimes. Cross-country placebo: B7 (France DA flat across Spanish reform dates). **Lever 1 = clock-symmetry; implemented at MTU15-DA, transfer collapses 12×.**

**(2) Non-Pigouvian segment incidence (the open question).** Wind + LIB free-market retailers consistently pay 60–65% of imbalance settlement € in EVERY post-ISP15 regime, including post-MTU15-DA — clock-symmetry shrinks the *scale* but not the *structure*. Anchored by S7 (per-segment marginal cost: conv-RZ €210–300/MWh vs LIB ≤€37/MWh, an order-of-magnitude misalignment). **Lever 2 = settlement-rule redesign (segment-conditional pricing); open** — implementation faces real challenges (per-segment MC measurement, BRP forecast-accuracy incentives).

**(3) Operational-regime overlay (the post-blackout reality).** Operación reforzada — REE's tightening of programming and security criteria from 2025-05-01 onward — runs continuously alongside the market layer (~€666M direct cost, €3.77B RRTT 2025, +49% YoY, 2.34% of total system costs). It forces ~20–30 CCGTs + nuclear daily via PO-3.2 RRTT, enlarges the secondary band (PO-1.5/7.2), and introduces the new PO-7.4 zonal reactive power market (BOE-A-2025-13076). It is a **missing-market patch**: voltage stability is a public good the wholesale-market layer cannot price, so the operator pays for it out-of-market (PO-3.2 RRTT) or via a newly-designed ancillary market (PO-7.4). Empirical signatures in our data: B9 q₂_RT2 surge (+13.6 GWh per firm-day in DA15/ID15), F15 CCGT mix shift (Naturgy +7.1pp, IB −2pp), F16 IB CCGT supply-curve break (4.8× more price-responsive post-blackout), F19/F20 GE aFRR capture, S9 solar capture-price collapse. **Lever 3 = reduce structural reliance on out-of-market RRTT via voltage-control investment + storage + grid reinforcement** — RD 997/2025's stated intent. Methodological discipline: D_MTU15 ⊥ D_reforzada non-collinearity in the 2025-03-19→2025-04-27 sub-window — the cleanest separator we have between reform mechanics and regime overlay; the S6 collapse at MTU15-DA *while reforzada is still active* is the canonical demonstration.

**Anchor findings**: S1, S2, S3, S4, **S5** (joint null rejection), **S6** (€1.1B BRP→TSO settlement transfer over 10 mo, blackout-decomposed), S7 (Pigouvian segment heterogeneity), **S8** (RZ activations rose post-IDA at daily disaggregation; same-cal-month robustness 2026-04-30 — 3/4 post-IDA regimes hold positive significant; DA60/ID15 specifically does not survive the same-Apr-Sep restriction. Cite with the caveat that DA60/ID15 is partly Apr-Sep seasonal/blackout-confounded), S9 (renewable cannibalisation — capacity-growth-driven, cite descriptively only), **B6** (forecast-error → imbalance pass-through R² 0.171→0.028), **B7** (France placebo), B3, B4, B5.

**Economic relevance**: This is a **regulatory friction** in the Pigouvian sense — segment-heterogeneous marginal-cost contributions to imbalance with a uniform settlement rule (S7). The framing is "asymmetric-granularity friction" not "deadweight loss" because the BRP→TSO transfer is regulatorily redistributed (the TSO recycles surplus to consumers via tariff with a 1-yr lag); the welfare interpretation requires a counterfactual on tariff pass-through that we do NOT estimate. Cite as a settlement-redistribution finding, not a DWL number.

**Methodology lineage**: Pigou (1920) Article 9.6 of EU GL EB imbalance-settlement methodology; Borenstein–Bushnell (2015 RAND) on settlement-rule design. **Lever 3 (operational-regime overlay) connects to the missing-markets / public-goods literature** (Cramton, Joskow on capacity payments and reserve markets) — voltage stability is a security externality that wholesale markets cannot price directly; PO-7.4's June 2025 reform is a textbook missing-market design response.

### Part II — Firm-level structural market power (regime-invariant, IB-canonical)

**Story**: IB's day-ahead market-power rent (~€820M post-MTU15-IDA, F7) is structural, regime-invariant, and flows through dispatchable hydro Cournot dispatch (Bushnell 2003-style). The mechanism predates the reform and survives the blackout and post-MTU15-DA. Behavioural support: B8 (within-unit bid complexification IB-specific) and B9 (Big-4 DA under-commitment with caveats).

**Anchor findings (post 2026-05-02 kill pass)**: **F7** (synthetic-firm Ciarreta–Espinosa method, IB ~98% of Big-4 transfer, hydro 64% / CCGT 36%, blackout-decomposed; re-sectioned to ALIVE 2026-05-02), **F10** (IB pivotality structural not scarcity-driven), **F13** (IB price-setting power varies with competitive thinness at margin), **F11** (cross-border coupling does NOT discipline), **B8** (IB-specific bid complexification), **B9** (Big-4 progressive q₂ collapse). Wounded supplements: **F8** (IB hydro Q4 dispatch +17pp regime-invariant — descriptive only; Bushnell water-value mechanism rejected), **F6** (Cournot tercile fit — IB cleanly, GE partial, GN/HC opposite), **F12** (pumped-storage arb — reform attribution wounded, solar-trend driven). Killed and excluded: **F1/F2/F3** (HP-sophistication test 2026-05-02 rejects the strategic-conduct interpretation of the implied Cournot Lerner — formula is mechanical, not realized markup); **F5** (Allaz–Vila slope is a mechanical accounting identity).

**Why IB and not the others**: F7 per-firm decomposition (IB ~98% of joint Big-4 DA-clearing transfer, GE/GN/HC ~zero) is the headline. D5 (cross-firm net-seller positions, GE > IB by 1.9–2.5×) rules out vertical-integration explanations. B8 within-unit bid complexification is IB-specific. The modelling-track §0 cross-firm consistency table now rests on **F7/F10/F13/B8** as the surviving alive evidence (F6 wounded; F1/F2/F3 killed by HP-sophistication 2026-05-02; F5 killed 2026-04-29 as mechanical identity).

**Economic relevance**: Bushnell (2003 AER) hydro-thermal Cournot mechanism. The Ciarreta–Espinosa (2010 J Regul Econ) synthetic-firm method we apply was originally developed for the 2002–2005 Spanish pool — we extend it to the 2018–2026 panel and find the same firm-cross-section asymmetry persisting across very different regulatory regimes. The thesis novelty is the **regime-invariance robustness**: the same mechanism that the literature documented for the early-2000s Spanish pool is still present after MTU15-IDA, MTU15-DA, the blackout, and operación reforzada.

**Methodology lineage**: Cournot; Bushnell (2003 AER); Ciarreta–Espinosa (2010 J Regul Econ); Crampes–Moreaux on water-value reasoning. **Hortaçsu–Puller (2008 RAND)** is cited as a methodological influence (the implied-Lerner-from-bid-structure method) but the project's HP-sophistication test (2026-05-02, `f1_f2_f3_hp_sophistication.py`) showed the implied Cournot Lerner does NOT match realized marginal-bid markup at firm-hour grain — the cross-firm ranking matches HP qualitatively, but the levels are formula-mechanical rather than realized-conduct. Cite HP as methodological influence, not as direct evidence of "sophisticated bidding" by Big-4.

### Part III — Cross-market firm specialisation (DA vs aFRR vs CCGT vs mFRR)

**Story**: Post-MTU15-IDA, the four large Spanish firms occupy distinct niches in distinct markets. The reforms did not homogenise market power — they sharpened firm specialisation.

**Anchor findings (organised by market layer)**:
- **DA market**: IB-dominant. F7 IB ~98% of Big-4 transfer; F8 hydro Q4.
- **aFRR balancing**: GE-dominant post-MTU15-IDA. **F19** (GE 34.28% of post-blackout aFRR up-volume), **F20** (GE €13.8M vs IB €9.1M aFRR up-revenue in DA15/ID15, 7-mo window — GE 52% > IB), F9 (system-level aFRR competition increased at MTU15-IDA), **B1** (GE bid-shading evolution).
- **CCGT generation post-blackout**: Naturgy-dominant. **F15** (Naturgy +7.1pp share gain).
- **mFRR balancing**: more competitive (system-level), no single-firm dominance.

**Cross-market consistency**: F9 cross-market check explicitly rules out generic "IB dominates everywhere" — IB is DA-specific.

**Economic relevance**: This is a **portfolio-allocation game** — each firm chooses which market layer to invest its strategic effort in given its plant fleet composition. IB has the largest hydro+nuclear baseload → DA-side Cournot. GE has the largest CCGT+nuclear pair → balancing-side activation. Naturgy has 18 of 50 Spanish CCGT plants → restrictions/operación reforzada. The reform sequence did NOT change the portfolios; it changed the relative payoff to each market layer (S6 BRP→TSO transfer → balancing markets compete; aFRR depth +25% volume / -40% price post-MTU15-IDA per F9).

**Methodology lineage**: Bunn-Day on portfolio bidding; Reguant on Spanish balancing markets; Allaz–Vila in extended sequential-market form.

### Part IV — Post-blackout enforcement and modern firm conduct (CNMC SBO3 lens)

**Story**: The 2025-04-28 blackout triggered CNMC investigations and a batch of ~50 expedientes (SNC/DE/021–050+). We have the full historical CNMC enforcement record (€25M IB hydro 2015, €25.3M Naturgy+Endesa CCGT 2019, €41.5M Naturgy SBO3 2023, plus 65.27 availability cases). Reading the SBO3 resolution gives a rigorous methodology — the three-situation pivotality test. Replicating that test on 2024-2026 data shows the conduct is widespread (F21) and persists post-sanction (F22). Naturgy in particular shows fleet-wide bid-price wedge (11-35% above own DA bid in pivotal hours, 7 of 9 plants, including SBO3 itself which still shows +14% wedge after the 2023 sanction). The within-firm pair-substitution pattern (F17/F18) is the modern manifestation: firms idle plant A to engineer pivotality for plant B in the same fleet.

**Anchor findings**: **F14** (nuclear unaccounted reduction system-wide — kills the simple cross-firm moral hazard reading), **F15** (Naturgy not IB captured CCGT windfall), F16 (IB CCGT supply-curve slope strategic-posture break at blackout — IB BROKE under operación reforzada, not exploited), **F17** (within-firm pair substitution — BES3→BES5, ARCOS3→ARCOS1, SROQ1→SROQ2, CTN4→CTN3), **F18** (sanctioned plants ALL lost share post-blackout), **F21** (three-situation replication — pattern widespread), **F22** (bid-price wedge — Naturgy fleet-wide 11-35%, SBO3 still +14% post-sanction).

**Plus the regulatory archive**: 6 full CNMC resolution PDFs in `docs/regulation/cnmc_resolutions/` + the 2026 incoaciones in `docs/regulation/cnmc_blackout_expedientes_2026.{md,csv}`.

**Economic relevance**: This is a **strategic availability** game (Crampes–Moreaux on capacity allocation; Joskow & Kahn 2002 on California capacity withholding) generalised to within-firm fleet management. The CNMC's SBO3 case treated cross-firm pivotality (Naturgy SBO3 exploiting Endesa PGR5's unavailability); we extend the framework to within-firm fleet pivotality (Naturgy idling SROQ1 to make SROQ2 pivotal). The post-2023 enforcement environment made the explicit RTT-price-wedge expensive; F22 documents the adaptation: Naturgy now bids the OMIE €1000 cap on a fraction of DA tranches as a blocking strategy, accepting modest restriction calls on the residual.

**Methodology lineage**: Crampes–Moreaux water-value / capacity allocation; Joskow–Kahn (2002 EJ) on California; CNMC's own three-situation framework as a directly-replicable benchmark; the 13-year recurring-firms record (F22 connection).

### Appendix — Identification provenance and descriptive context

This is genuine appendix-grade material — not a "Part V" that competes with the four economic parts above.

**Identification appendix.** X1–X14 (dead claims kept as record) and the full `_identification_target.md` provenance frozen post-Week-1. Cited only as "attempted but failed" in the body chapters where relevant; no positive results drawn from this material.

**Descriptive context appendix.** D1 (within-month dispersion), D2 (80–99% identical bids — most plants do NOT exploit MTU15), D3 (HHI shift 2023, pre-reform), D4 (Fringe exit 6.8%), D5 (cross-firm net-seller positions). Establishes baseline market structure; not load-bearing for any of the four parts' main claims.

**Behavioural-finding home assignment.** All B-series findings now live in the part they directly support: B1→Part III, B3/B4/B5/B6/B7→Part I, B8/B9→Part II. No behavioural finding sits in the appendix.

---

## How the parts integrate (the central economic argument)

The thesis builds a **layered story** about Spanish electricity markets under reform:

> **Layer 1 (Part I, system).** Three mechanism-design observations from the reform sequence: (1) asymmetric-clock friction generated €1.1B BRP→TSO transfer in 10 months, RESOLVED at MTU15-DA (lever 1); (2) non-Pigouvian segment incidence — renewables pay 60–65% in every regime regardless of clock symmetry (lever 2, OPEN); (3) operational-regime overlay — operación reforzada is a continuous, missing-market patch with ~€670M direct cost and €3.77B RRTT/yr, requiring lever 3 (reduce structural reliance via voltage-control investment + storage + grid reinforcement; RD 997/2025's intent).
>
> **Layer 2 (Part II, firm).** Underlying that friction, IB's structural day-ahead market power persisted unchanged across all reform regimes, the blackout, and operación reforzada. IB extracts ~€820M of DA-clearing rent through hydro Cournot dispatch — a mechanism the academic literature documented for the 2002-2005 Spanish pool that is still fully operative in 2024-2026. The reform changed IB's absolute € rent (via price levels) but not the relative markup.
>
> **Layer 3 (Part III, cross-market).** The Big-4 firms specialised in different markets. While IB dominated DA, Endesa captured the post-MTU15-IDA aFRR up-revenue position (€27M/yr) and Naturgy captured the post-blackout CCGT generation surge. Each firm picked its niche given its portfolio composition; the reform did not homogenise firm market power, it sharpened the niche map.
>
> **Layer 4 (Part IV, conduct).** The CNMC's 2015/2019/2023 enforcement record documents 13 years of recurring conduct by these same firms in the technical-restrictions market. The 2023 SBO3 sanction (€41.5M against Naturgy) reduced but did not eliminate the conduct — the SBO3 plant itself still bids 14% higher in zone-pivotal hours than competitive hours; Naturgy's broader fleet shows the SBO3 pattern at 11-35% across 7 of 9 plants. The post-2025 blackout-batch expedientes are the regulatory response to a continuation of the pattern. The within-firm fleet substitution we document (F17/F18) is the modern manifestation: idling plant A to engineer pivotality for plant B in the same firm's fleet, shifting the conduct from explicit RTT bid-price-wedges to implicit DA blocking.

**Total welfare impact** (order-of-magnitude): €1.1B settlement transfer (Part I, 10 months) + ~€820M IB DA rent + ~€60M/yr GE aFRR + Naturgy CCGT windfall + restrictions premium = **on the order of €1-2 billion per year of consumer-paid rents during the post-MTU15 period**. The CNMC enforcement framework is partially effective but firms adapt.

**Why this matters for IO and policy**:
- Reform design must consider **firm-portfolio reallocation effects**, not just direct system-cost shifts. MTU15-IDA achieved its system-design goals but did not address firm-level rent extraction.
- **Within-firm fleet substitution** is a previously-undocumented form of pivotality engineering that the CNMC's existing SBO3-style framework (currently targeting cross-firm zone pivotality) does not capture.
- **Cross-market specialisation** suggests that single-market enforcement (e.g. CNMC focused on RTT bid prices) is insufficient when firms can relocate strategic effort across DA / RTT / aFRR / mFRR layers.

---

## Practical thesis-writing recommendations

**Eight-week timeline (deadline ~2026-06-20)**:

- **Weeks 1-2**: Finalise the Part-I system-layer chapter using S5/S6/S7. The €1.1B headline number is the strongest single empirical statement — lead with it. Cross-country placebo (B7) goes here.
- **Weeks 3-4**: Part-II IB-canonical chapter. **F7 + F10 + F13 form the core** post-kill-pass; F11 is the falsification check (cross-border coupling does NOT discipline); F8 wounded but provides hydro-Q4 descriptive evidence; F6 wounded as supplementary Cournot-tercile fit; B8 + B9 behavioural support. **F1/F2/F3 retired** (HP-sophistication 2026-05-02 rejected the conduct interpretation of the implied Cournot Lerner). The Bushnell + Ciarreta–Espinosa methodology gives clean theoretical grounding.
- **Weeks 4-5**: Part-III cross-market specialisation. F19 + F20 (aFRR/GE) and F15 (CCGT/GN) anchor the cross-market case. F9 system-level aFRR competition is the methodological backbone.
- **Weeks 6-7**: Part-IV CNMC SBO3 and modern conduct. F14 (rejection of simple moral hazard) + F17/F18 (substitution pattern) + F21/F22 (replication of CNMC framework). The 6-PDF resolution archive in `docs/regulation/cnmc_resolutions/` is the regulatory anchor.
- **Week 8**: Appendix (identification provenance from `_identification_target.md`, descriptive context); figure polish; viva-defensive read of dead claims.

**Stop-rule check**: this proposal is consistent with `CLAUDE.md` § "Claim-status discipline" — every cited finding traces to an alive ledger row. Wounded F12 cited only with caveat as descriptive. Dead claims (X1-X14) appear only in the identification appendix as "attempted but failed".

---

## What this proposal is NOT

- **Not a definitive plan.** It is a synthesis of what 36 alive findings naturally support (post 2026-05-02 kill pass); many alternative organisations are possible.
- **Not new analysis.** Every claim above maps to existing ledger entries; this document organises and integrates rather than producing new evidence.
- **Not a welfare estimate.** The €1-2B/yr figure is an order-of-magnitude sum of separate findings; rigorous welfare requires a counterfactual general-equilibrium analysis that is out of scope.
- **Not committing to specific modelling.** The modelling-track sections in `_modelling_track.md` (Cournot, Allaz–Vila, Pigouvian, asymmetric-granularity) remain active candidates; the thesis can pick the subset that matches the four-part structure above.
