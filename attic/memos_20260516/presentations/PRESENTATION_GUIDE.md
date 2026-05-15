# May 2026 CEMFI presentation — final prep guide

**Date**: ~2026-05-05. **18–20-min talk** (advisor format guidance: state-of-work, not results-led). IO faculty audience.

**Advisor format guidance (2026-05-03)**: brief reminder of thesis objective + research question + minimum context, then **what I have done · state of the work · difficulties encountered · what is left to do**. NOT a results-leading talk. The detailed results-arc below (slides 5–11) is preserved as audit-trail but the actual May deck follows the advisor's progress-report frame.

**Title (suggested)**: *Spain's MTU15 reform: progress on an empirical assessment*. Continuation of the Feb-2026 deck *"Rewiring the market clock in Spanish electricity markets"*.

**Counts as of 2026-05-02 attack pass**: 41 alive · 7 wounded · 20 dead · 68 active rows.

**Continuity statement (slide 1 or 2)**: The Feb-deck Ito–Reguant (2016) extension predicted (i) DA15 is the key reform that smooths imbalances, (ii) dispersion risk concentrates in DA60/ID15, (iii) finer granularity creates winners and losers across heterogeneous portfolios. **All three are now empirically confirmed** at €1.1B order of magnitude — with a sharper microfoundation than the original Allaz–Vila scaffold, which is now rejected at every granularity (**F5 killed 2026-04-29 as a mechanical accounting identity**).

**Updates after the 2026-05-02 kill pass** (relevant to slide 12 framing only — slides 1–11 unaffected):
- F1, F2, F3 (Cournot-FOC implied Lerner) **killed**: HP-sophistication test rejects the strategic-conduct interpretation. The +0.318 / +0.135 regime-difference coefficients are formula-mechanical, not realized-conduct. Do not cite as strategic markup.
- F5 (Allaz–Vila commitment slope) **killed**: mechanical accounting identity at every granularity.
- F7 (€820M IB DA-clearing transfer) **alive (re-sectioned)** — the surviving headline IB-market-power claim.
- F14–F22 **alive (re-sectioned)** — IB nuclear unaccounted reduction, post-blackout Naturgy CCGT windfall, within-firm plant substitution, CNMC SBO3 replication, per-firm aFRR maps. All were previously misclassified as Dead.
- S8 (RZ activation) **wounded further (2026-04-30)**: DA60/ID15 specifically does not survive same-cal-month restriction; the other 3 post-IDA regimes hold.
- Dual-pricing predictability channel (`dual_pricing_opposite_share.py`, 2026-04-30) **rejected**: the user-proposed dual-pricing imbalance-settlement option-value mechanism does not explain B9 progressive collapse. Original Ito-Reguant market-power reading of B9 stands.

---

## The IO claim in one paragraph

> Spain's 2024–2025 reform sequence revealed **three distinct mechanism-design observations**, each requiring a separate policy lever. (1) **Asymmetric clock scales** caused a €1.1B BRP→TSO settlement transfer in the 10-month asymmetric-granularity window (Dec 2024 – Sep 2025). Lever 1: clock-symmetry at MTU15-DA. *Implemented* — the transfer collapsed 12× (€91M/mo → €7.4M/mo) when DA aligned to 15-min. (2) **Non-Pigouvian uniform-rate allocation across heterogeneous-marginal-cost segments** redistributes burden from dispatchable plants to renewable-portfolio firms. Lever 2: settlement-rule redesign. *Open* — wind + LIB free-market retailers consistently pay 60–65% of imbalance settlement € in EVERY post-ISP15 regime, including post-MTU15-DA. Clock-symmetry shrinks the *scale* of the redistribution but does NOT fix its *structure*. (3) **Operational-regime overlay** — *operación reforzada*, the post-2025-04-28 reinforced-operation regime, runs continuously alongside the market layer (~€666M direct cost; €3.77B RRTT 2025, +49% YoY) and is a missing-market patch: voltage stability is a public good the wholesale layer cannot price, so the operator pays for it out-of-market via PO-3.2 RRTT. Lever 3: reduce structural reliance on RRTT via voltage-control investment + storage + grid reinforcement (RD 997/2025's intent). The new PO-7.4 zonal reactive market (BOE-A-2025-13076) is the first market-design response. **The S6 transfer collapses at MTU15-DA *while reforzada is still active* — the cleanest empirical separator we have between reform mechanics (lever 1) and regime overlay (lever 3).**

This is the system-layer reform impact. Firm-level market power (IB DA Cournot rent ~€820M; cross-market firm specialisation: GE→aFRR, Naturgy→CCGT post-blackout; CNMC SBO3 conduct adaptation) is regime-invariant background, covered in Parts II–IV of the thesis but **off-arc for this preliminary-results talk**.

---

## Slide-by-slide arc (13 slides)

| # | Slide | Figure | Headline | Talking point |
|---|---|---|---|---|
| 1 | Title + author | — | "Asymmetric-granularity friction: empirical evidence on Spain's MTU15 reform sequence" | Reuse Feb-deck title styling |
| 2 | Recap: reform sequence | (Feb-deck slide 4 reuse) | Three reform stages: ISP15 (Dec 2024) + ID15 (Mar 2025) + DA15 (Oct 2025) | "10-month asymmetric window where DA was 60-min while ISP/ID had moved to 15-min" |
| 3 | Recap: Feb-deck theory + IO question | (Feb-deck slide 6 reuse) | Ito–Reguant prediction: dispersion concentrated in DA60/ID15; DA15 smooths | "Today: that prediction is empirically confirmed at €1.1B — but the surviving microfoundation is settlement-clock mismatch + non-Pigouvian incidence, not Allaz–Vila commitment value (which OVB-cleaning rejected)" |
| 4 | Data + empirical strategy | — | 6 yrs OMIE + ENTSO-E + ESIOS + AEMET; same-calendar-month pre-IDA baseline + bootstrap; cross-country placebo | "No clean DiD counterfactual; rich before/after with controls" |
| 5 | **Fig 1 (S5)** | fig01 | Four ENTSO-E system metrics jump concordantly at ISP15, moderate at MTU15-DA | "Joint null is rejected — this isn't one outlier metric. A87, A86, A85, A84 all move together" |
| 6 | **Fig 2 (S6) — HEADLINE** | fig02 | **+€1,094.9M** asymmetric-window cumulative excess; bootstrap CI [-90, +73]M; ≈15× upper bound; collapses to **€7.4M/mo** at MTU15-DA | **The single number to remember.** Settlement REDISTRIBUTION (BRP → TSO), not deadweight loss; recycled to consumers via tariff with 1-yr lag |
| 7 | **Fig 3 (B6) — mechanism** | fig03 | Forecast-error → imbalance VOLUME pass-through R²: 0.171 (clean reform PRE-blackout) → 0.365 (POST-blackout) → **0.028** at DA15/ID15 | "Microfoundation. The post-MTU15-DA collapse is the cleanest signature; blackout amplifies but doesn't create the mechanism" |
| 8 | **Fig 6 (S7) — IO bite** | fig06 | DA60/ID15 direct dual-pricing: LIB retailers paid €108M, wind €77M of €294M reconstructed (37% + 26% = 63% combined) | "Renewables paid 60-65% of imbalance € — but a Pigouvian counterfactual would charge conv-RZ + COR €215M of that. Cross-segment redistribution is the IO bite" |
| 9 | **Fig 7 (regime invariance)** | fig07 | Wind + LIB free-market retailers consistently pay 60–65% of imbalance settlement € in EVERY post-ISP15 regime including post-MTU15-DA | "Clock-symmetry fixed the SCALE but not the STRUCTURE. Two distinct policy levers needed" |
| 10 | **Fig 4 (B7) — placebo** | fig04 | Spain DA volatility responds 2–3× more than France across reform dates | "Cross-country control the Feb proposal said wasn't yet available" |
| 11 | **Fig 5 (S6 blackout split) — robustness** | fig05 | DA15/ID15 collapse to €7.4M/mo holds DESPITE operación reforzada in effect | Defensive figure — friction is reform-driven, not blackout-driven |
| 12 | **Lever 3 — operación reforzada as missing-market patch** | (no figure, narrative slide) | Continuous regime overlay May 2025–present; ~€666M direct + €3.77B RRTT/yr; PO-7.4 zonal reactive market new (June 2025) is the first market-design response. **S6 collapses at MTU15-DA *while reforzada is still active*** — separates lever 1 (granularity friction, resolved) from lever 3 (operational missing-market, open). | Methodological discipline: D_MTU15 ⊥ D_reforzada non-collinearity. **Off-arc but acknowledged**: F7 €820M IB DA rent, F8 hydro Q4 dispatch, F22 CNMC SBO3 — regime-invariant firm-level market power, covered in thesis Parts II–IV. |
| 13 | Next steps | — | Structural model extension; per-BRP verification (ESIOS server permitting); thesis proposal already drafted | Reuse Feb-slide-12 with update + thank you |

---

## Headline numbers to memorize

| Quantity | Number | Source |
|---|---|---|
| Asymmetric-window BRP→TSO cumulative excess | **€1,094.9M** | S6, `s6_monthly_decomposition.csv` |
| Bootstrap null CI for that excess | **[−90, +73] M€** | observed ≈15× upper bound |
| Asymmetric-window monthly mean | **€91M/mo (DA60/ID15), €137M/mo (ISP15-win)** | S6 monthly decomposition |
| Post-MTU15-DA monthly mean | **€7.4M/mo** | 8% of DA60/ID15 level |
| Clean April-2025 PRE-blackout | **+€75.7M** | shows effect exists before blackout |
| B6 R² jump (clean reform) | **0.023 → 0.171 (7×)** | pre-IDA-late vs DA60/ID15 PRE-blackout |
| B6 R² peak (reform + blackout) | **0.171 → 0.365** | DA60/ID15 POST-blackout |
| B6 R² post-MTU15-DA | **0.028** | DA15/ID15 — the cleanest collapse signal |
| Wind + LIB combined burden share | **60% / 63% / 65%** | regime-invariant across ISP15-win / DA60/ID15 / DA15/ID15 |
| Pigouvian-counterfactual redistribution magnitude (DA60/ID15) | **~€141M overpaid by LIB+wind / ~€159M underpaid by conv-RZ+COR+hydro RE** | Figure 6 right panel |
| Up/down dual-pricing spread | **€26–33/MWh** | mean prdvbaqh − prdvsuqh per regime |
| LIB retailer correlation with system imbalance | **0.74–0.81** | vs 0.08-0.35 for thermal/hydro/conv-NRZ — the structural reason for renewable burden invariance |
| Operación reforzada direct cost | **~€666M** (May 2025 – Mar 2026) | REE estimate; 2.34% of total system costs (Nov 2025) |
| RRTT cost 2024 → 2025 | **€2,523M → €3,770M** (+49% YoY) | REE; daily RRTT in early 2026 regularly exceeds OMIE wholesale price |
| q₂_RT2 surge in DA15/ID15 | **+13,639 MWh per firm-day** | empirical signature of operación reforzada in our B9 q₂ definitions audit |

---

## Q&A defenses (10 anticipated questions)

**Q1. "Is €1.1B a deadweight loss?"**
A: No, settlement REDISTRIBUTION. BRPs pay TSO; TSO recycles to consumers via tariff with 1-yr lag. We cite as transfer, not DWL. Welfare interpretation requires counterfactual on tariff pass-through (out of scope). The slide deck consistently uses "BRP→TSO settlement transfer" or "regulatory redistribution"; never "welfare cost".

**Q2. "Why is France a valid placebo?"**
A: France didn't undergo MTU15 in our window (their MTU15 is later). DA prices flat across our reform dates after controlling for fundamentals. Spain responds 2–3× more than France across the reform sequence, controlling for common European shocks. Caveat: post-blackout shock (April 2025) is also Spain-specific, so the placebo is conservative for the post-blackout window.

**Q3. "How do you separate ISP15 from the post-2022 gas crisis trend?"**
A: Same-calendar-month pre-IDA baseline (compare DA60/ID15 in Apr–Sep 2025 to pre-IDA Apr–Sep 2018–2023). Bootstrap CI under the null. Baseline-sensitivity test (`s6_baseline_sensitivity.py`): excluding 2022+2023 from baseline shifts point estimate by only 3-4% (€1,094.9M FULL → €1,061.6M EXCL_CRISIS → €1,048.2M PRE-2022). Bootstrap CI actually narrows under cleaner baseline.

**Q4. "How does the 2025-04-28 blackout confound your asymmetric-window finding?"**
A: Blackout-split robustness (Fig 5): clean April 2025 PRE-blackout single month is +€75.7M; post-blackout DA60/ID15 averages €93.5M/mo (only +24% above clean April — modest amplification, not source); and crucially **the post-MTU15-DA collapse holds at €7.4M/mo despite operación reforzada still in effect**. The collapse decisively separates granularity friction from blackout effects. **Operational detail**: operación reforzada is a tightening of REE's programming and security criteria via PO-3.2 RRTT redispatch + PO-1.5 enlarged secondary band + PO-7.4 zonal reactive market (June 2025); ~€666M direct cost May 2025-Mar 2026; full cascade in `docs/notes/SPANISH_MARKET_STRUCTURE.md` §11. Methodologically: D_reforzada and D_MTU15 are non-collinear in 2025-03-19→2025-04-27 (the clean post-MTU15-IDA pre-reforzada window), so blackout-split decomposition is the correct approach (which we already use).

**Q5. "How does B6's pass-through R² change interpretation if MTU15-DA only reduces VOLUMES not the per-MWh PASS-THROUGH RATE?"**
A: That's a sharp question. The volume R² collapses cleanly (0.365 → 0.028). On the €-side (extension we ran), the relationship is more complex — daily €-side regression shows R² stays around 0.20 in DA15/ID15, but with negative slope reflecting the duck-curve dynamic. The volume collapse is the cleanest reform signature; the €-side relationship has additional renewable-driven structure that is worth flagging but not the lead.

**Q6. "Does the 60-65% renewable burden mean the rule is broken?"**
A: Two interpretations. (a) The rule is non-Pigouvian — it allocates burden by volume share rather than marginal cost. (b) The renewable-segment correlation with system imbalance is 0.74-0.81 (LIB retailers) and 0.5-0.6 (wind), so under dual pricing they systematically face the penalty side. Both contribute. A simple Pigouvian fix (charge each segment its β/MC regardless of system direction) would face implementation challenges: real-time MC measurement is hard, and it would weaken BRP forecast-accuracy incentives. So lever 2 is non-trivial; the talk acknowledges it as an open mechanism-design problem rather than a clean policy intervention.

**Q6b. "What about lever 3 — can voltage-control investment really replace operación reforzada?"**
A: That's the bet of RD 997/2025 (Nov 2025) and PO-7.4's June 2025 reform. The new PO-7.4 zonal reactive market introduces a price for voltage support (basic mandatory + dynamic paid; renewables now eligible for the dynamic tier). The economic logic is clean: when voltage stability is priced as an ancillary service, providers compete on cost, and the operator stops needing to dispatch out-of-market via PO-3.2 RRTT. The empirical question for the thesis is whether RRTT volumes and the €3.77B RRTT cost actually fall as PO-7.4 matures. We don't have a clean answer yet — the regime is too young (PO-7.4 went live June 2025; emergency PO modifications still in effect until January 2027). Worth flagging as a forward-looking research question.

**Q7. "Why is your IO content thinner than a typical IO talk?"**
A: We're showing the system-layer reform-driven slice. The thesis as a whole has firm-level market-power content (Part II IB Cournot-pivotality, Part III cross-market specialisation, Part IV CNMC SBO3 conduct adaptation). Those findings are regime-invariant background market structure — they pre-date and survive the MTU15 reform — so they don't fit the "reform impact" frame of this talk. Two-policy-levers framing IS substantive IO content though: mechanism design + Pigouvian incidence + welfare interpretation + identification.

**Q8. "Where's the structural identification?"**
A: We're explicit that this is reduced-form throughout, deliberately. The identification spine has three pieces: (a) **PDBC vs PDVD chain placement** — operación reforzada acts via PO-3.2 RRTT *after* OMIE clears PDBC, so PDBC-based outcomes (F7, marginalpdbc clearing prices, B9 q₁) face reforzada only through bidder expectations while PDVD/PHF-based outcomes face it as direct mechanical effect. (b) **Reforzada-held-constant pairings**: ISP15-win → DA60/ID15-PRE-blackout for the MTU15-IDA effect (both pre-reforzada); DA60/ID15-POST → DA15/ID15 for the MTU15-DA effect (both under reforzada). The S6 collapse at MTU15-DA *while reforzada is still active* is the canonical separator. (c) Same-calendar-month robustness + cross-country placebo (B7) + bootstrap CIs throughout. The X1-X14 dead-DiD record (parallel-trends fails, randomization p=0.43, treatment-date sweep) shows we tried causal designs and rejected them honestly — that's an asset for credibility. **No heavy structural model**; the thesis stops at partial-equilibrium reduced-form. Structural exercises are FOC-based mechanism tests (Allaz-Vila §2, HP-sophistication §1) that we ran and that turned out to reject the conduct interpretation cleanly.

**Q9. "What does the data NOT support?"**
A: We rejected three alternatives during OVB-cleaning (F5 Allaz–Vila firm-level mechanism, F8 Bushnell water-value, F10 H1/H2/H3 scarcity-pivotality). The 2026-04-27 sweep wounded S8 (RZ activation escalation) when renewable-capacity-growth control flipped the sign. We also corrected the F8 reform-amplification claim (+14pp baseline gap was structurally already there pre-MTU15-IDA; reform didn't widen it as much as we initially claimed). Honest disclosure of these is in the thesis proposal.

**Q10. "What's the relationship between the €1.1B (system) and the €820M (IB DA rent)?"**
A: Different markets. €1.1B is BRP→TSO imbalance settlement transfer (an A87 NET measure, ENTSO-E aggregate). €820M is IB's clearing-price-difference rent in the DA market (a synthetic-firm Ciarreta–Espinosa replication). The two channels involve the same generators in different roles (BRP-side imbalance + generator-side DA cleared-price markup), so they are NON-ADDITIVE — we cite each channel separately and do NOT sum to "€2B reform impact". The audit doc explicitly flags this as triple-counting risk.

**Q11. "Why is q₂ defined as IDA-only, not IDA + continuous market?"**
A: Three reasons. (1) IR's two-period model has a forward auction at p₁ and a spot auction at p₂ with **single clearing prices** — IDA auctions map cleanly (`marginalpibc` per session), the continuous market does NOT (pay-as-bid order book, heterogeneous trade prices, no single p₂). (2) PIBCIE gives clean per-firm signed quantity changes per ISP; aggregating PIBCIC trades to a single q₂ object requires more pricing decisions. (3) Empirically, Big-4 continuous-market activity captures only 6–19% of the IDA q₂ compression (`b9_continuous_market_substitution.py`). **Conservative caveat**: we did test q^total = q₂_IDA + q^CI and got a **sharper** friction signal (Wald F = 1,497 vs 477 for IDA-only, p = 0.83 for DA15/ID15 boundary recovery). The IDA-only headline is therefore conservative; q^total strengthens the case. We lead with IDA-only because the IR mapping is cleaner and easier to defend.

---

## What's off-arc (transparently flagged in slide 12)

These findings are real and substantive but **regime-invariant** (not reform-driven), so they don't belong in this talk. Each has a thesis-chapter home:

- **F7** — IB ~€820M DA cleared-price-difference rent post-MTU15-IDA, regime-invariant. *Thesis Part II.*
- **F8** — IB hydro Q4-dispatch concentration +17pp gap vs Fringe, regime-invariant 2018–2026. *Part II.*
- **F10** — IB strongly/extremely pivotal at 63% of post-MTU15-IDA hours, capturing 92% of cross-firm transfer. *Part II.*
- **F9** — IB-dominance is DA-specific; aFRR market is structurally more competitive. *Part III.*
- **F19/F20** — GE captured aFRR up-revenue post-MTU15-IDA (€13.8M vs IB €9.1M in DA15/ID15). *Part III.*
- **F15** — Naturgy +7.1pp CCGT generation share post-blackout; IB −2.0pp. *Part III + IV.*
- **F17/F18** — within-firm CCGT plant substitution (BES3→BES5, ARCOS3→ARCOS1, SROQ1→SROQ2, CTN4→CTN3); CNMC-sanctioned plants ALL lost share post-blackout. *Part IV.*
- **F21/F22** — CNMC SBO3 three-situation pivotality test replication: Naturgy fleet-wide +11–35% bid-price wedge in zone-pivotal hours; SBO3 itself still +14% post-2023 sanction. *Part IV.*

Honest one-line for the audience: *"The thesis maps three additional IO channels — firm-level structural market power, cross-market specialisation, post-CNMC strategic-availability conduct — covered in Parts II–IV. Today's talk is the system-layer reform impact slice."*

---

## Open follow-ups (post-presentation)

**Data:**
- Re-run individual mFRR `BalancingEnergyBids_12.3.B` sync after May (HTTPError on older daily files; rate-limit or older folder structure). Currently have only ~9 days of bid-level data.
- Write parser for ENTSO-E TP CSV format `AggregatedBalancingEnergyBids_12.3.E_r3` (16 monthly files now downloaded but not in `balancing_bids_all.parquet`). ~½ day.
- ESIOS archive 34 (per-BRP costs) — return after their server outage clears.

**Analysis:**
- Lever-2 (Pigouvian rule redesign) implementation challenges are open research territory — could be a thesis chapter contribution.
- Direction-correlation mechanism (renewables CAUSE their own system shorts → systematically face penalty side of dual pricing) — sharper formulation for thesis Part I.
- Per-segment € verification using parsed `liqsegme` family (segment-level settlement file in raw, not yet parsed).

---

## Behind the talk: the 5-part thesis context

The May talk is **Part I only**. Full thesis structure (in [`thesis/proposal.md`](../../proposal.md)) covers 41 alive findings across:

| Part | Theory anchor | Lead findings | Status |
|---|---|---|---|
| **I — System asymmetric-granularity friction** | §4 asymmetric-granularity + §3 Pigouvian | S5, **S6**, S7, B6, B7 | **THIS TALK** |
| **II — Firm structural market power (IB-canonical)** | §1 Cournot-pivotality (alive); §2 Allaz–Vila (rejected); F1/F2/F3 implied-Cournot killed by HP-sophistication 2026-05-02 | **F7** (alive headline), F10, F11, F13 alive; F6, F8, F12 wounded; F1/F2/F3/F5 dead | thesis Part II |
| **III — Cross-market firm specialisation** | Part III addendum in modelling track | F9, F15, F19, F20 | thesis Part III |
| **IV — Post-CNMC strategic-availability conduct** | §6 strategic availability under within-firm fleet substitution | F14, F15, F17, F18, F21, F22 | thesis Part IV |
| **V — Behavioural + identification appendix** | §5 bid complexification; identification target | B1, B2, B3, B4, B5, B6, B7, B8, B9; X1-X14 | thesis Part V |

**Mechanism story (post-2026-05-02 kill pass):** the only direct-test-surviving firm-layer mechanism is **Cournot-pivotality** (F10) + the synthetic-firm decomposition (F7) anchoring it. Allaz–Vila (F5) is killed as a mechanical identity. Bushnell water-value (F8 mechanism reading) is rejected. F1/F2/F3 implied-Cournot Lerner are killed as conduct evidence by the HP-sophistication test (formula ≠ realized markup). The thesis modelling chapter leads with **F7 + F10** + four direct rejections of alternatives (Allaz-Vila, Bushnell, dual-pricing predictability, HP-Cournot-formula-as-conduct).

**Two-decade pattern persistence:** Ciarreta–Espinosa (2010) documented IB > Endesa cross-firm asymmetry for 2002-2005 (under different mechanism — CTC stranded-cost regulation). We document the same direction under a different mechanism (hydro Cournot-pivotality + asymmetric-granularity) for 2024-2026. Method-replication across periods, NOT independent confirmation.

---

## Workflow reminders

**Building the slide deck (Beamer):** the figures live at [`figures/thesis/fig01..fig07.{pdf,png}`](../figures/) — 7 PDFs ready for direct `\includegraphics{...}` in Beamer. The IO framing in this guide can be directly transcribed into slide markdown. Mirror the file naming convention from `presentation1/` (`Paramio_Pablo_slides_may2026.tex` and `.pdf`).

**Dimensions sanity-cap:** PNG files are ~1870 px wide max (under the 2000-px session-restart cap). Vector PDFs are unaffected.

**Re-running figures**: if data updates, re-execute the notebook:
```bash
uv run python thesis/presentations/workshop_may_2026/build_figures.py
uv run jupyter nbconvert --to notebook --execute --inplace thesis/presentations/workshop_may_2026/figures.ipynb
```

**Re-running per-segment € numbers**: the F3 decomposition runs inline in the notebook (no `data/derived/` dependency). Updating `liquicomun_all.parquet` automatically updates Figure 6 + 7 on next execution.

---

## Final pre-talk checklist (week of)

- [ ] Read this guide once
- [ ] Open figures.ipynb, page through the 7 figures, internalise the talking points
- [ ] Memorise the 12 headline numbers
- [ ] Skim the 10 Q&A defenses; rehearse 2-3 of the trickiest
- [ ] Verify the slide-arc IO-claim paragraph is clear in the deck
- [ ] Check that the "what we don't show" slide 12 framing is honest and bounded
- [ ] Practice the title→thesis claim transition (slide 1 → slide 3 sets up the IO question)
- [ ] Get a colleague to ask Q4 (blackout) and Q6 (renewable burden) and time the answer

You're presenting clean empirical work with honest scoping. The IO content is genuinely novel (clock-symmetry + Pigouvian incidence as TWO distinct levers) and rests on direct settlement evidence (F3) plus reduced-form identification (S5 + B7). This is a defensible preliminary-results talk for an IO faculty audience.

Good luck.
