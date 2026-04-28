# Economic-modelling track

Working note. Replaces `_modelable_patterns.md` (now archived).

Goal: organise alive empirical claims around economic models. Each section is a candidate model that (a) rationalises ≥2 alive claims in `CLAIMS_LEDGER.md` and (b) makes one or more sharp predictions that current empirical work either does or could test. Sections end with a "Priority empirical refinement" line: the single focused analysis that would most advance the model. Refinements are *candidates* for Phase 2 of the project plan, not commitments. Each runs only on user approval and only after passing the stop-rule in `CLAUDE.md`.

References: `CLAIMS_LEDGER.md` (claim status, evidence pointers) | `_identification_target.md` (identification provenance, frozen post-Week 1) | `RESEARCH_LOG.md` (chronological diary) | `thesis/drafts/master_thesis_proposal.md` (5-part synthesis written 2026-04-28).

## Map to thesis-proposal Parts

The thesis proposal organises 37 alive findings into 5 Parts. This file's §-numbering is historical (sections were added in run-order). The mapping:

| Thesis Part | Modelling-track sections | Lead alive claims | Status |
|---|---|---|---|
| **I — System asymmetric-granularity friction** | §3 Pigouvian + §4 asymmetric-granularity welfare | S5, **S6 (€1.1B)**, S7, B7 | alive — Part I theory anchor is §4 |
| **II — Firm structural market power (IB-canonical)** | §0 cross-firm synthesis + §1 Cournot-pivotality | F7, F8, F10, F11, F2, F6 | alive — surviving mechanism is Cournot-structural-pivotality (§1) |
| **III — Cross-market firm specialisation** | §III below (cross-market reading folded into §0; no standalone section) | F9 (aFRR), F15 (CCGT), F19, F20 | alive — §III paragraph below summarises |
| **IV — Post-blackout enforcement and modern firm conduct** | §6 strategic availability under within-firm fleet substitution | F14, F15, F17, F18, F21, F22 | alive — §6 added 2026-04-28 |
| **V — Behavioural + identification appendix** | §5 bid complexification + §2 Allaz–Vila (rejected, appendix-grade) | B1, B3, B4, B5, **B6**, B7, B8, B9 | §5 alive; §2 rejected 2026-04-27 |

**Mechanism-side surviving theory after the 2026-04-27 OVB sweep**: §1 (Cournot-pivotality) and §4 (asymmetric-granularity friction) at the firm and system layers respectively; §3 (Pigouvian) as a static welfare framing for S7; §5 (bid complexification) as the behavioural mirror; §6 (strategic availability) as the Part IV anchor. **§2 (Allaz–Vila) is rejected** at both DA-spot aggregate and IDA-repositioning levels and should not be cited as a surviving mechanism.

---

## §0 — Cross-firm consistency: IB as the canonical strategic firm

Three independent Phase 2 tests on different mechanism predictions converge on the same firm-level heterogeneity pattern: **IB is the single Big-4 firm whose post-MTU15-IDA behaviour fits IO mechanism predictions cleanly across the board.**

| Test | Mechanism | IB | GE | GN | HC |
|---|---|---|---|---|---|
| **F6** Cournot tercile (§1) | Lerner concentrates in steep-slope cells | ✓ monotone decline T1→T3 (+0.126 → +0.044) | partial (T1>T2 but T3 highest) | ✗ opposite | ✗ opposite |
| **F5** Allaz–Vila peak/off-peak (§2) | Commitment slope flips at MTU15-DA in CCGT-margin hours | ✗ IB sign-flip dies under OVB-cleaning (2026-04-27): Δβ_peak collapses from +0.049 to ≈0 with hour FE within peak partition | weak (peak +0.004, off-peak −0.019; direction holds, magnitude too small) | ✗ no fit | ambiguous |
| **B8** Bid complexification (§5 below) | Strategic firms invest in finer price ladders under finer ISP | ✓ within-unit tpp 5.49 → 8.73 (1.59×) | flat at low absolute level (~2 tpp) | ✗ simplifies (6.46 → 3.22) | stable (5.93 → 5.83) |

**Reading.** The thesis can present IB as the canonical strategic-firm case study and use GE/GN/HC as cross-firm placebos. The three tests probe distinct mechanism dimensions (residual-demand elasticity, commitment-value evolution, bid-structure response) and all point to IB. This is internally consistent with IB's portfolio composition: IB holds the largest CCGT fleet on the marginal supply step among Big-4 plus substantial hydro flexibility — the exact portfolio profile the IO models assume.

**Why GE doesn't fit.** GE is large but its DA bidding style is structurally different — only ~2 tranches per ISP both pre and post (an order of magnitude simpler than IB's ~6–9). GE's market-power signal in F1 is real but does not arise from CCGT-margin Cournot or Allaz–Vila quantity-setting in the standard sense. GE's post-reform Lerner peak appears to come from composition (76–90% nuclear in cleared volume per W1), not from strategic CCGT behaviour. F1 is a market-power *outcome*; the *mechanism* is not the textbook-Cournot story.

**Why GN/HC don't fit.** Per F4 (alive): GN/HC market-power shifts at MTU15-IDA are dominated by Rule 28.8 bilateral-contract reallocation (regulatory shock), not strategic conduct. Their portfolios (hydro-heavy GN, small mixed HC) place them away from the strategic-marginal-CCGT setting where IO models predict the Cournot/Allaz–Vila mechanisms operate.

**Implication for the structural-firm chapter.** Lead with IB as the case study; present GE/GN/HC as the cross-firm placebos that *don't* show the IB-style mechanism response, supporting the strategic interpretation. This is a stronger claim than "Big-4 firms exhibit market-power elevation" because the cross-firm heterogeneity has a structural reading: only firms with strategic-marginal-CCGT capacity respond to finer granularity in the predicted ways.

**F7 per-firm decomposition (2026-04-27): IB is the price-setter; GE/GN/HC are price-receivers.** The synthetic-firm method (Ciarreta–Espinosa style) was extended to attribute the joint Big-4 €833M transfer per firm. Result: **IB carries +€8.80/MWh (+12.4%, ~€820M); GE −€0.24/MWh (~−€23M); GN +€0.15/MWh (~€14M); HC +€0.76/MWh (~€70M).** Replacing GE's plants with same-tech Fringe substitutes barely changes the clearing price; replacing IB's plants drops the price by ~€8.80/MWh. **F1's GE Lerner elevation therefore reads as GE benefiting from the high prices IB sets, not as GE setting them.** This is a sharper structural reading than the matched-price Spec 3 can give. The IB-canonical tests now read: F2 (matched-price Lerner, IB benefits) + F6 (IB cleanly fits Cournot) + F7 per-firm (IB is the actual price-setter accounting for ~98% of joint Big-4 transfer). [F5 was previously listed here; retracted 2026-04-27 after OVB-cleaning showed the IB peak-hour sign-flip does not survive controls.] **The thesis claim crystallises: IB is the marginal price-setter in the Spanish DA market post-MTU15-IDA. The joint Big-4 ~€833M transfer is essentially IB's market-power rent.**

**F7 per-IB-unit decomposition (2026-04-27): IB's price-setting is HYDRO-DOMINATED, not CCGT.** Drilling further into IB: replacing each IB plant's offers individually and re-clearing yields **hydro 64% (~€530M, led by TAMEGA +€203M, SIL +€103M, DUER +€92M, TAJO +€90M); CCGT 36% (~€294M, led by CTJON2 / ARCOS2 / STC4 ~€89M each).** Critically, the named B8 "complex-bidder" CCGT units (TAPOWER, ARCOS1, CTN3, CTN4) have **near-zero independent price impact** (~€8M total) despite their 1.59× bid complexification. **Mechanism re-interpretation**: IB's market power flows through dispatchable HYDRO market power (Bushnell 2003-style hydro-thermal competition; Crampes–Moreaux water-value reasoning), not through CCGT-on-the-margin Cournot quantity-setting. The B8 bid-complexification finding is real as a bid-structure observation but **does not translate to price-setting** for the named units. The Allaz–Vila peak-signal (F5) and Cournot tercile fit (F6) for IB are still consistent with this re-interpretation — both apply to dispatchable strategic capacity broadly, not specifically CCGT. Thesis-relevant lineage now includes the hydro-market-power literature (Bushnell 2003 on hydro-thermal Western US, Reguant on Spanish balancing markets, Crampes–Moreaux on water values).

**Caveat for the per-IB-unit finding.** Hydro plant-pair matching is harder than CCGT — Spanish hydro plants vary widely in storage capacity, ramp, and reservoir physics. The magnitude (~€530M IB hydro alone) is too large to be entirely matching noise. **Sensitivity test (2026-04-27)**: re-ran with stricter matching (split reservoir vs run-of-river subtype + capacity ratio in [1/3, 3]); per-unit results identical to baseline within €0.001M. The capacity-band matching-artefact concern (audit attack A1) is therefore data-defended. **Operational-vs-strategic framing footnote (audit attack A2)**: the synthetic-firm method substitutes IB plants with Fringe matches and reads the price difference as IB's "markup". But matched Fringe plants (mostly Portuguese EDP large reservoir hydro: ADOURO/ALIMA/ACAVADO + EHN Acciona ACC2EBR) are NOT pure-fringe price-takers — they have their own operational constraints (water-availability cycles, cross-border interconnection limits, Portuguese MIBEL-side priorities) that may produce DIFFERENT bidding from "strategic Cournot maximizer with same fleet." The €530M is therefore best read as "the rent IB extracts that comparable-size large-reservoir European Fringe plants do not extract" rather than "pure strategic markup against a perfectly competitive benchmark." The thesis-claim direction (IB extracts more rent than its operationally-comparable counterparts) survives this framing; the magnitude carries an upper-bound interpretation.

**F8 direct strategic-dispatch test (2026-04-27): Bushnell signature confirmed; PARTIAL RETRACTION on amplification.** Independent test of the hydro-mechanism interpretation: per-firm distribution of hydro DA cleared MWh across within-month price quartiles. The cross-firm asymmetry is robust and persistent — **mean IB Q4 hydro share 57.2% (range 44–63% by year), Fringe 39.6% (range 36–42%), gap +17.6 pp average across 2018–2024 with year-by-year range [+6.4, +23.3]**. 2025 (full post-MTU15-IDA year): IB 58.7%, Fringe 41.6%, gap +17.1 pp — basically identical to the pre-reform mean. **The original F8 framing "the reform widened the gap from +14pp to +21pp" was a windowing artifact**: the script's "pre" window was only 2024-01→2025-03-18 (the IDA-reform-sequence sub-period, not long-history pre-reform), and the "post" window included a partial-Jan-2026 anomaly (IB Q4 84.5% with only 229 GWh hydro). At year resolution, the post-MTU15-IDA gap is NOT meaningfully larger than the pre-reform mean. This is a **direct, plant-matching-free test** of the Bushnell (2003) strategic-hydro-dispatch hypothesis: IB systematically dispatches its hydro fleet into high-price hours far more than Fringe does, while facing the same hourly prices. **F8 added as alive structural-firm claim.** The mechanism story for the IB-canonical pattern is now triangulated by three converging tests: F1/F2 (IB matched-price Lerner), F7 per-IB-unit (IB hydro 64% of transfer), F8 (IB hydro Q4 dispatch concentration +21pp vs Fringe).

**Note on firm sizes.** Iberdrola is the **larger** of the two dominant Big-4 firms by installed capacity in our 2024–2026 sample (~18.5 GW vs ~13.4 GW for Endesa: IB has CCGT 5.0 + Hydro 6.5 + Nuclear 7.1 GW; GE has CCGT 3.2 + Hydro 3.8 + Nuclear 6.4 GW). Endesa clears more DA volume (~2,316 vs ~958 GWh/month net seller) only because its nuclear baseload runs near-continuously (high capacity factor), while Iberdrola's hydro and CCGT are dispatchable (lower capacity factor, more on-the-margin). **The IB > GE market-power finding is therefore consistent with textbook Cournot** (larger firm by capacity has more market power). Endesa's larger cleared volume reflects baseload composition, not strategic market presence; the marginal capacity that matters for price-setting concentrates in Iberdrola's CCGT + dispatchable hydro fleet.

**Vertical-integration ruled out as the explanation (D5, 2026-04-27).** A natural alternative reading would be: IB is more aggressive in spot bidding because IB is more net-seller than GE — net sellers have higher incentive to push spot prices up, while net buyers (with retail arms) internalise the cost of high spot prices. The data rejects this: post-Rule-28.8 mean monthly net-seller position is **GE +2,316 GWh, IB +958 GWh** — GE is 2.4× more net-seller than IB, exactly the opposite direction. So the IB-canonical pattern cannot be a vertical-integration / net-seller-position effect. **This parallels Ciarreta–Espinosa (2010 Fig 5)** who reached the same conclusion for the 2002–2005 Spanish pool (vertical integration didn't explain EN > IB then). Two-decade pattern persistence — note this is a PATTERN parallel (different data, different mechanism in each period), not independent confirmation of the same causal relationship; the value is that the cross-firm asymmetry survives across very different regulatory regimes. The remaining mechanism candidates are portfolio composition (IB's CCGT fleet on the marginal supply step), strategic conduct, or operational complexity — but NOT downstream-retail incentive alignment.

**Blackout-confound decomposition (2026-04-27): two channels separate by regime.** The 2025-04-28 Iberian blackout triggered REE "operación reforzada" (forced increased CCGT/nuclear commitment via P.O. 3.2) for the rest of the DA60/ID15 window, raising the worry that the asymmetric-granularity findings (F7, F8, S6) are blackout-driven rather than reform-driven. `scripts/analysis/synthetic/blackout_split.py` separates DA60/ID15 PRE-blackout (clean reform window: 2025-03-19 → 2025-04-27, ~6 weeks), DA60/ID15 POST-blackout (operación reforzada: 2025-04-28 → 2025-09-30, ~5 months), and DA15/ID15 (post-MTU15-DA, also post-blackout: 2025-10-01 → 2025-12-15, ~3.5 months). Three findings:

1. **F8 robust to blackout (the underlying mechanism is regime-invariant).** IB hydro Q4 share by era: PRE-blackout 63.1% (gap +20.4pp vs Fringe), POST-blackout 63.6% (+21.8pp), post-MTU15-DA 67.2% (+22.4pp). The Bushnell-style strategic-dispatch concentration is structural and present in every regime, including the clean ~6-week PRE-blackout window. **The reform did not create IB's strategic dispatch and the blackout did not amplify it.**

2. **F7 reframed: IB rent persists at DA15/ID15; the asymmetric window did NOT generate it.** Per-IB transfer by era: DA60/ID15 PRE-blackout ~€38M total at +48% relative markup on €12.51 mean prices; DA60/ID15 POST-blackout ~€184M at +11% rel markup on €68.42; **DA15/ID15 ~€598M at +12.3% rel markup on €77.90 — DA15/ID15 alone accounts for 73% of total IB transfer.** The clean PRE-blackout window has the highest *relative* markup (48% — the cleanest expression of IB's price-setting power per unit price level) but the lowest absolute €. The €820M is dominated by post-MTU15-DA where prices are highest. **Reframing of the §0 thesis claim**: IB is the marginal price-setter in *every* post-MTU15-IDA regime, with relative-markup peaks in the clean asymmetric window. The €820M absolute total is a regime-weighted average that is heavily DA15/ID15-tilted; it is NOT a pure asymmetric-granularity rent.

3. **S6 robust to blackout AND collapses at MTU15-DA — the granularity-friction story for the SYSTEM-LEVEL channel is intact.** A87 NET fiscal surplus excess vs same-calendar pre-IDA baseline by era: April 2025 (clean PRE-blackout) +€75.7M for one month; May–Sep 2025 (post-blackout DA60/ID15) +€467.6M total / €93.5M mean per month (only +24% above the clean April figure — operación reforzada is a modest amplifier, not the source); **Oct–Dec 2025 (DA15/ID15, post-MTU15-DA, still post-blackout) excess collapses to +€22.2M total / €7.4M mean per month — only 8% of the DA60/ID15 level, with December even slightly negative**. The post-MTU15-DA collapse is the key signature: when granularity asymmetry is removed, the system-cost shift evaporates **even though the blackout/operación-reforzada is still in effect**. This decisively separates the granularity-friction effect from the blackout effect at the system layer.

**Two-channel synthesis.** The Spanish reform produced two distinct, separable effects: (i) at the **system layer (S6)**, asymmetric granularity (DA60/ID15 window) generated a **BRP→TSO settlement transfer** of ~€90–95M/month that attenuated to ~€7M/month once DA also moved to 15-min — a granularity-friction signature (note: settlement transfer, not deadweight loss; welfare interpretation requires counterfactual on TSO surplus recycling); (ii) at the **firm layer (F7/F8)**, IB's market-power rent is structural (regime-invariant Q4 hydro concentration), realised in absolute € terms most strongly under high-price post-MTU15-DA conditions. The reform did not *create* IB's market power, but the asymmetric-granularity window did create a measurable system-layer financial signature that attenuates once the asymmetry resolves. **This two-channel reading is the central thesis claim post-blackout-confound check.** **Non-additive note**: S6 (BRP→TSO transfer) and the firm-layer rent (F7) are NOT independently summable because the same generators participate in both — F7's cleared-price-difference rent is partially what BRPs pay for, which drives the S6 transfer. Cite the two channels with separate framings; do not aggregate.

**Cross-market check (F9, 2026-04-27): IB-dominance is DA-specific.** ESIOS `liquicierre`/`liquicierresrs` (publicly accessible per-BSP aFRR settlement, ~23 BSPs, 2015-now panel at PT15M resolution) lets us decompose secondary-regulation provision per firm. Under the LIBERAL mapping (IB = {IMA, IGR, IGN} where IMA carries 128 GWh/post-MTU15-DA-day — the dominant Iberdrola portfolio BSP), IB's aFRR share is **31.8% pre-IDA → 39.1% peak in 3-sess → 32.9% ISP15-win → 27.1% DA60/ID15 → 26.7% DA15/ID15**. **IB's aFRR share has FALLEN 12pp since the 2024-06 IDA reform peak**, while Fringe (TTE/EnergyaVM/Acciona/Axpo/Alpiq + others) has *risen* from 10.9% to 26.7% — the aFRR market is becoming MORE competitive over time. **This is the OPPOSITE direction from F7 in the DA market** (where IB ≈ 98% of Big-4 transfer, regime-invariant). The two together yield a sharper claim: **F7 IB-dominance is DA-market-specific, not a generic firm-level structural fact**. The thesis's IB-canonical reading does not over-claim system-wide IB dominance; aFRR is structurally more competitive than DA. F9 strengthens F7 by ruling out broad-firm-dominance interpretations. **Caveat**: BSP↔firm mapping is not authoritatively published — the 3-letter REE BSP codes (IMA/IGR/IGN/END/GN/HC/EV) don't appear in ESIOS sujetos-del-mercado exports. The LIBERAL mapping is magnitude-suggestive (IGN exact-matches OMIE IGNU = Iberdrola Nuclear; IMA/IGR pattern-fit Iberdrola family with no plausible non-IB owners of ~128 GWh/day aFRR). The CONSERVATIVE mapping (IB = {IGN} only) gives IB 0.5–2.4% across all regimes — clearly dominated by IMA/IGR (which lump into Fringe under conservative) — confirming that under either reading IB's aFRR share is qualitatively non-dominant.

**F9 supply-curve corroboration (2026-04-27):** ESIOS `Curvas_Ofertas_aFRR` (system-aggregate offer curves at PT15M resolution, 2024-11-20 → present, 7.5M tranches) shows the same competitive-aFRR narrative from a different angle. Offer-curve depth and price-level by regime (script `afrr_offer_depth.py`):

| Regime | aFRR-up MW/ISP | aFRR-up median €/MW | aFRR-down MW/ISP | aFRR-down median €/MW | Down tranches/ISP |
|---|---:|---:|---:|---:|---:|
| 3-sess (partial, 11 days) | 1948 | 10.29 | 1990 | 6.21 | 68 |
| ISP15 win | 1825 | 10.07 | 1981 | 6.33 | 68 |
| **DA60/ID15** | **2274** | **6.35** | **2549** | **4.89** | **87** |
| DA15/ID15 | 2215 | 6.22 | 2494 | 5.22 | 84 |

**Three coherent shifts at MTU15-IDA**: (i) **+25% offer volume** (up 1825→2274 MW/ISP; down 1981→2549) — more capacity available; (ii) **−40% median price** (€10→€6 up; €6→€5 down) — competitive pressure; (iii) **+25% tranche granularity on down-side** (68→87 tranches/ISP) — finer price discovery, while up-side tranche count stays flat (~68). All three shifts point the same direction as F9's per-BSP finding: the aFRR market deepened and got more competitive at MTU15-IDA, exactly when DA was undergoing the asymmetric-granularity friction (S6) and the IB-canonical rent extraction (F7) intensified. **The two markets diverged at the same reform date**: DA toward concentration, aFRR toward competition. This is a clean cross-market contrast supporting the §0 reading that F7 is DA-specific.

**Tertiary regulation (mFRR) corroboration (2026-04-27):** The same competitive-balancing narrative appears in the system-aggregate mFRR offer curve (`REE_BalancingEnerBids` panel, 2022-05 → 2024-12-10, 20.2M tranches at PT15M). At the **2024-06 IDA reform** (6 → 3 session reduction):

| Metric | Up direction | Down direction |
|---|---|---|
| Tranches per ISP | +15% (119 → 137) | +27% (110 → 139) — finer offer granularity |
| Median price | -9% (€123 → €112) | **−€30 shift** (€22 → **−€8**) — went negative |
| Q90-Q10 spread | **-42%** (€149 → €86) | +4% (€119 → €123) |

Two notable signatures: (a) the **mFRR up-side spread collapsed -42%** post-IDA-reform — the offer curve became dramatically flatter, consistent with easier entry / homogenisation of bidder behaviour; (b) the **mFRR down-side median price went NEGATIVE** (€22 → −€8) — BSPs paying to be allowed to provide down-regulation, characteristic of renewable-saturation periods where curtailment-avoidance is valuable. Both findings reinforce the aFRR result: **balancing markets (both secondary aFRR and tertiary mFRR) became more competitive across the post-IDA reform window**, exactly the opposite of the DA-market F7 concentration trajectory.

**Synthesis (DA vs balancing reform-direction divergence):**
- **DA market**: concentration ↑ (F7 IB ~98% of Big-4 transfer; ~€820M post-IDA rent; F8 hydro Q4 dispatch +21pp gap stable)
- **aFRR market**: competition ↑ (F9 IB share fell 12pp; supply curve +25% volume, -40% price, +25% granularity)
- **mFRR market**: competition ↑ (offer curve +15-27% granularity, -42% up-side spread, down-side price went negative)

Both balancing markets diverged from DA at the same reform date. This is decisive evidence that F7 IB-dominance is DA-market-specific and not a generic firm-level structural fact.

**F10 pivotality decomposition (2026-04-27): IB pivotality is structural, time-of-day-driven, NOT scarcity-driven.** Direct decomposition of F7's hourly mp_IB transfer (`f7_pivotality_decomposition.py`, n=13,058 post-MTU15-IDA ISPs). Pivotality buckets:

| Bucket | Threshold | Share of hours | Share of transfer | Concentration |
|---|---|---:|---:|---:|
| non-pivotal | $\|\text{mp}_{IB}\| < $€0.5 | 10.1% | -0.8% | -0.08× |
| mildly pivotal | €0.5 ≤ mp_IB < €5 | 27.0% | 8.9% | 0.33× |
| **strongly pivotal** | €5 ≤ mp_IB < €20 | **49.5%** | **54.2%** | 1.10× |
| **extremely pivotal** | mp_IB ≥ €20 | **13.4%** | **37.6%** | 2.80× |

**Headline**: IB is strongly or extremely pivotal at **62.9% of post-MTU15-IDA ISPs, capturing 91.9% of transfer**. The 13.4% of "extremely pivotal" hours alone capture 37.6% of total transfer (concentration ratio 2.8×) — these are the structural-scarcity-pricing hours. **In US electricity markets, firms are typically considered pivotal at 3-5% of hours; IB being pivotal at the majority of post-MTU15-IDA hours is a major structural-firm-level finding.**

**Time-of-day shape**: pivotality peaks at h17-h24 (71-74% strongly-pivotal share) and dips slightly at h11-h16 (~60% — solar saturation lowers prices), but no full mid-day collapse. **Seasonal**: winter (Dec-Feb) higher than summer (78-84% vs 53-58%).

**Mechanism diagnostic (testing user-stated hypotheses):**
- **H1 drought × evening peak** (drought = reservoir D1-D3, evening = h19-22): 76.7% strongly pivotal vs 62.8% for drought × off-evening — but the F7 panel is **reservoir-truncated to dry conditions** (no wet observations across the 10-month window), so wet × evening counterfactual cannot be tested.
- **H2 cold mornings**: winter mornings 78.9% pivotal, but winter NON-mornings are HIGHER (84.1%) — mechanism is winter evenings, not winter mornings.
- **H3 wind-doldrum × evening**: VRE Q1-low × evening 71.5% pivotal vs Q1-low × off-evening 52.8% — but the same pivotality shows up at evening across all VRE quintiles (driven by hour-of-day, not VRE level).

**Key reading.** IB pivotality is dominantly **time-of-day-driven** (evening peak across all weather/water/VRE conditions), NOT scarcity-state-driven. The classical Bushnell water-value mechanism is rejected (F8 wounded refinement); the wind-doldrum scarcity mechanism is rejected (H3); the cold-mornings demand-spike mechanism is rejected (H2). **The Allaz–Vila forward-position-driven dispatch interpretation was previously offered here as the surviving mechanism, with F5's peak-hour signal as corroborating evidence; both are RETRACTED 2026-04-27 after F5 OVB-cleaning showed the IB peak-hour signal does not survive proper controls.** The surviving reading is structural-compositional: IB owns the dispatchable units that the Spanish supply curve sits on at evening peaks (CCGT + reservoir hydro), and IB's pivotality reflects this fixed portfolio composition rather than any granularity-mediated commitment-value or water-value optimization.

**Implication for the structural-firm chapter.** F10 sharpens the §0 IB-canonical claim: IB is the marginal price-setter in the Spanish DA market not just at extreme scarcity events but **structurally, across the diurnal and seasonal cycle**, with intensification at evening peaks. The €820M F7 transfer is broadly distributed (62.9% strongly-pivotal × 91.9% of transfer) rather than tail-driven. The thesis can lead with F10 as the simplest one-paragraph headline of IB market power: *"IB is strongly or extremely pivotal at 63% of post-MTU15-IDA hours, capturing 92% of cross-firm market-power transfer."*

### Mechanism synthesis (2026-04-27): IB's market power is Cournot-structural, NOT Bushnell or Allaz–Vila

After running direct tests of three mechanism candidates (Bushnell water-value F8, Allaz–Vila granularity asymmetry, scarcity-driven pivotality), only Cournot quantity-setting under inelastic residual demand survives clean direct testing. The thesis modelling chapter should lead with this reading.

**Direct test results, mechanism by mechanism:**

| Mechanism | Direct test | Result | Surviving role |
|---|---|---|---|
| **Cournot (§1, F6, F10)** | (a) Lerner concentrates in steep-supply tercile (F6 IB +0.126/+0.087/+0.044, T1→T3 monotone); (b) IB pivotality 62.9% × 92% of transfer (F10) | **PASS** for IB cleanly; partial for GE; rejected for GN/HC | **PRIMARY** mechanism for IB market power |
| **Allaz–Vila granularity-asymmetry (§2, F5)** | DA60/ID15 vs DA15/ID15 IB rel markup: **12.67% vs 12.29%** — flat (aggregate); F5 IB peak-hour Δβ_peak collapses from +0.049 to ≈0 under OVB-cleaning with exogenous-only controls (2026-04-27 update) | **REJECT** at BOTH the DA-spot aggregate level AND the IDA-repositioning level | Allaz–Vila does not survive any direct test in the project. F5's previously-claimed peak-hour signal was OVB-driven by hour-of-day variation within the peak partition. Only weak GE within-firm direction-only result remains, magnitude too small to be load-bearing. |
| **Bushnell water-value (§6, F8)** | ρ(IB Q4 hydro share, reservoir) = $-0.169$; β = $-0.487$ pp/TWh, p=0.868; Fringe placebo $-0.380$ STRONGER than IB | **REJECT** | Bushnell mechanism rejected as the source of IB Q4 concentration |
| **Scarcity-state pivotality (F10 H1/H2/H3)** | Drought × evening, cold mornings, wind doldrums all show same time-of-day shape — pivotality is hour-driven not scarcity-driven | **REJECT** all three scarcity hypotheses | Pivotality is structural-compositional |
| **Vertical integration / net-position (D5)** | GE 2.4× more net-seller than IB but GE has no F7 rent | **REJECT** for IB > GE pattern | Doesn't explain cross-firm asymmetry |

**The surviving mechanism story.** IB exercises Cournot-style markup on residual demand made inelastic by IB's structural ownership of the marginal dispatchable capacity (CCGT + reservoir hydro on the Spanish supply margin). The pattern is dominantly **compositional**: IB happens to own the units that the supply curve sits on, and IB's Lerner reflects the inverse residual-demand elasticity at the cleared price. The pattern is **not driven by reform-induced behaviour change** — IB's per-MWh rel markup is regime-invariant at ~12% across DA60/ID15 and DA15/ID15.

**What the reform DID change at the IB level.** The reform did not change IB's per-MWh markup. It changed the absolute price level (DA60/ID15 €57 → DA15/ID15 €78 — driven by post-blackout operación reforzada and load growth, not by IB's strategic conduct), so the absolute € rent rises in DA15/ID15 (€598M of the €820M sits there). **An IDA-repositioning Allaz–Vila signal at the intraday level was previously claimed via F5's peak-hour sign-flip but did NOT survive OVB-cleaning under exogenous-only controls (2026-04-27 retraction)** — Allaz–Vila is now rejected at both the DA-spot aggregate level AND the IDA-repositioning level.

**What the reform DID create — at the system level (S6).** The €1.1B BRP→TSO settlement transfer (S6) is the granularity-friction signature, separate from IB's Cournot rent. It collapsed at MTU15-DA when the asymmetry resolved. **The two channels are non-additive across the financial system** (same generators participate in both): the thesis should cite S6 and F7 as separate channels of reform impact, not as a single "reform cost."

**Triangulation across alive claims for the IB-canonical case study:**

| Test | Mechanism dimension | IB result |
|---|---|---|
| F1/F2 matched-price Lerner | Cournot: Spec 3 contrast | IB +0.135 vs pre-IDA (significant) |
| F6 Cournot tercile | Cournot: inverse-slope test | IB monotone T1→T3 (+0.126→+0.044) |
| F7 synthetic-firm | Aggregate market-power transfer | IB ~98% of joint Big-4 (€820M) |
| F7 per-IB-unit | Mechanism within IB | Hydro 64%, CCGT 36%, named CCGT complex-bidders ~zero |
| F8 Q4 hydro share | Strategic dispatch concentration | IB +17pp vs Fringe (regime-invariant) |
| F10 pivotality | Cournot via inelastic residual demand | IB pivotal at 62.9% of hours (capturing 92% of transfer) |

All six tests converge on Cournot-structural-pivotality. The thesis structural-firm chapter should lead with this as a tightly-triangulated empirical claim, citing IB as the canonical strategic firm and using GE/GN/HC as cross-firm placebos that don't show the Cournot pattern (per F4 Rule 28.8 caveat for GN/HC; per W1 nuclear-baseload composition for GE).

**What the data DOES NOT support** (honest caveats for the thesis):
- IB's market power is NOT "caused by" the MTU15 reform. It is structurally pre-existing; the reform amplified the absolute rent through price-level effects but not the Cournot wedge.
- IB's Q4 hydro concentration is NOT a Bushnell water-value signature. It is structural and regime-invariant; mechanism is most plausibly fixed operational dispatch protocol (water-management software), not reservoir shadow-pricing or forward-commitment (F5 Allaz–Vila signal also retracted 2026-04-27).
- IB's pivotality is NOT scarcity-state-driven. It is dominantly time-of-day-driven (evening peak across all weather/water/VRE conditions), supporting a structural-compositional reading.

This is a clean, defensible mechanism story that combines positive structural findings (Cournot-pivotality alive) with explicit rejection of three alternative mechanisms (Bushnell, Allaz–Vila aggregate, scarcity-pivotality). The committee can be told the project ran direct tests of all three alternatives and rejected each — sharper than just citing supportive evidence for the preferred mechanism.

---

## Part III addendum — Cross-market firm specialisation

**Sketch.** The four large Spanish firms occupy distinct niches across distinct markets after MTU15-IDA. The reform did not homogenise market power — it sharpened firm specialisation. The thesis-proposal Part III makes this an organising claim; the modelling track previously folded the evidence into §0 ("F9 cross-market check"). This addendum collects that material under one heading.

**Anchor findings.**
- **F7 + F8** — DA market: IB-dominant; ~98% of joint Big-4 transfer; hydro-dispatch concentration +17pp gap vs Fringe.
- **F9** — aFRR balancing market: IB share fell 12pp from 2024-06 reform peak (39.1% → 26.7%) while Fringe rose; aFRR market deepened (+25% volume, −40% price, +25% granularity).
- **F19, F20** — aFRR up-activation: GE captured 34.28% of post-blackout aFRR up-volume and €13.8M revenue in DA15/ID15 vs IB €9.1M (GE 52% above IB). The post-blackout balancing windfall went to Endesa, not Iberdrola.
- **F15** — Post-blackout CCGT generation: Naturgy +7.1pp share gain; IB −2pp. Operación reforzada targeted Naturgy's CCGT-heavy peaker fleet.
- mFRR offer-curve corroboration (in §0 above): tertiary-regulation tranche granularity +15-27%, up-side spread −42%, down-side prices went negative — both balancing markets diverged from DA at the same 2024-06 reform date.

**Reading.** Each firm picks its niche given its portfolio composition. IB has the largest dispatchable hydro + CCGT-on-margin → DA-side Cournot-pivotality (Part II §0/§1). GE has a large CCGT + nuclear baseload → aFRR-side activation (F19, F20). Naturgy holds 18 of 50 Spanish CCGT plants (peaker-heavy) → post-blackout generation surge + technical-restrictions conduct (F15 + Part IV §6). The reform did NOT change portfolios; it changed the relative payoff to each market layer.

**Rule-out**: F9 strengthens F7 by ruling out a generic "IB dominates everywhere" reading. The IB-canonical pattern in §0/§1 is DA-market-specific, not a project-wide claim about market power.

**Methodology lineage** (per thesis proposal Part III):
- Bunn–Day on portfolio bidding across multiple markets.
- Reguant on Spanish balancing-market participation.
- Allaz–Vila in extended sequential-market form (note: the granularity-asymmetry version of Allaz–Vila in §2 is rejected; the extended-sequential-market form for cross-market portfolio choice is a different application and does not depend on the §2 finding).

**Status**: alive as a descriptive empirical pattern with mechanism interpretation (portfolio-driven niche choice). No standalone refinement script — the evidence comes from §0's cross-market subsections plus the F19/F20 aFRR per-firm decomposition. Not a separately-modellable mechanism in the §1–§6 sense; it is the *consequence* of firms with heterogeneous portfolios responding to a multi-market environment, not a single-mechanism model.

---

## §1 — Cournot-Nash with quasi-inelastic demand

**Sketch.** Big firms compete in quantity; demand is approximately inelastic over the relevant range; supply slope from the rest-of-market is a first-order determinant of each firm's residual-demand slope. Firm $i$'s implied Lerner (under the Cournot FOC, scaled by share):

$$L_i \;=\; \frac{p^* - MC_i}{p^*} \;\approx\; \frac{q_i}{p^*\,(1 - s_i)\,|\partial S/\partial p|}.$$

The reform changes the granularity at which firms commit quantities and at which the residual-demand slope is realised. Asymmetric granularity (DA60/ID15 window) generates a wedge between the firm's effective DA-quantity commitment and the realised intra-hour residual-demand slopes.

**Alive claims this rationalises.**
- F1, F2 — GE/IB matched-price Lerner elevation at DA60/ID15 conditional on price level.
- F3 — partial reversal at MTU15-DA when granularity asymmetry closes.
- F4 — GN/HC dominated by Rule 28.8 reallocation (compositional, not strategic) — a *negative* implication of the model: firms whose post-reform share collapses by regulatory shock should not show the Cournot wedge.

**Sharp prediction not yet tested cleanly.** The Cournot wedge should depend on $|\partial S/\partial p|$. If we sort observations by ex-ante supply-slope tercile (using the pre-DA60/ID15 distribution), the F1/F2 elevation should be *largest* in the flattest-slope tercile. Already partially controlled in Spec 3 via price-bin FE, but slope-tercile sorting is a different cut and would test the model's core mechanism.

**Priority empirical refinement.** Re-estimate F1/F2 contrasts conditional on supply-slope tercile (using `build_supply_slope_panel.py` output). Expected runtime: ~½ day.

### Run 2026-04-25 — Cournot mechanism: heterogeneous support across firms

(Originally written 2026-04-25 with the Cournot direction inverted; corrected 2026-04-26 — see correction note below. Hour FE added.)

Script: `scripts/analysis/lerner/cournot_slope_tercile.py`. Tercile cuts on the pre-IDA slope distribution (q33 = 253 MW/EUR, q66 = 643 MW/EUR; tercile labels: T1 steep / T2 medium / T3 flat). Spec 3 matched-price Lerner contrasts vs pre-IDA, run separately within each (firm × tercile) subsample, with calendar-month + hour-of-day + price-bin FE; HC3 SE.

**Cournot prediction (corrected).** With supply slope in MW/EUR, large value = elastic supply (flat curve in $q$-vs-$p$). Cournot Lerner $L \propto 1/|\partial S/\partial p|$. So Cournot predicts:

- T1 steep tercile (low MW/EUR, **inelastic** supply) → **HIGH** Lerner
- T3 flat tercile (high MW/EUR, elastic supply) → **LOW** Lerner

If the matched-price Lerner elevation in DA60/ID15 (alive claims F1, F2) is Cournot-mediated and proportionally amplifies the baseline pattern, the contrast should **decrease** monotonically T1 → T3.

**DA60/ID15 contrast by tercile (with hour FE):**

| Firm | T1 steep | T2 medium | T3 flat | Cournot? |
|---|---:|---:|---:|---|
| **IB** | $+0.126$ | $+0.087$ | $+0.044$ | **Yes — strict monotone decline** |
| **GE** | $+0.289$ | $+0.161$ | $+0.398$ | Mixed: T1 > T2 (right) but T3 highest (wrong) |
| **GN** | $-0.045$ | $-0.036$ | $+0.011$ | No — monotone rise (opposite) |
| **HC** | $-0.029$ | $-0.024$ | $+0.006$ | No — monotone rise (opposite) |

**Reading.**

- *IB*: clean Cournot pattern. Excess Lerner concentrates in steep-supply (inelastic-residual-demand) hours, exactly as the model predicts when the reform amplifies an underlying Cournot mechanism. **F2 (IB matched-price Lerner +0.135) gains a structural mechanism anchor: IB's reform-window market-power increase is consistent with Cournot quantity-setting under inelastic residual demand.**
- *GE*: T1 > T2 is consistent (steep > medium); T3 highest contradicts. Possible interpretations: (a) GE's flat-tercile cells are structurally non-Cournot — concentrated in low-demand hours where CCGT is on the marginal step but with thin volume, so a small absolute MW move maps to a large fractional Lerner; (b) the matched-price spec doesn't fully neutralise within-tercile heterogeneity. **GE's F1 elevation is partially Cournot-consistent but not cleanly attributable to the mechanism.**
- *GN, HC*: opposite of Cournot. For GN (hydro-heavy), this fits the W3 / F4 portfolio caveat: GN's market-power profile is dominated by hydro resale and bilateral-contract exposure, not by CCGT quantity-setting. For HC (small portfolio), the magnitudes are tiny and the cross-tercile contrast is within noise.

**Tautology check.** A direct log-log regression of $\log(L)$ on $\log(|\partial S/\partial p|)$ within (firm, regime) returns $\gamma = -1$ with $R^2 = 1$, mechanical because $L$ is constructed from the formula. Documents that the formula was applied correctly; not a structural test. The tercile sort above is the real test.

**Status of §1 model.** Partially alive — supports Cournot for IB (a Tier-2 mechanism finding), partial for GE, contradicted for GN/HC by portfolio composition. The cross-firm pattern is itself the empirical content: Cournot describes the largest mixed-portfolio firm in our sample (IB) but does not generalise to hydro-heavy or small-portfolio firms.

**F6 ledger row revised (was: wounded).** Status remains wounded *as a uniform Cournot story* but is upgraded to **partial alive** for IB specifically: IB's matched-price Lerner contrast obeys the Cournot inverse-slope relationship cleanly, anchoring F2 in a structural mechanism. GE/GN/HC require alternative explanations.

**Implications for the thesis.**

- Cite **F1 (GE)** as a Spec-3 matched-price contrast with mixed Cournot support (don't claim a Cournot mechanism for GE).
- Cite **F2 (IB)** as a Spec-3 matched-price contrast that *additionally* respects the Cournot tercile prediction — the mechanism story is cleanest for IB.
- Cite **F4 (GN/HC)** as the portfolio-composition story: the F1/F2-style elevation does not extend to GN/HC because their market-power profile is dominated by bilateral-contract reallocation (Rule 28.8) rather than Cournot quantity-setting.

This three-firm partition — Cournot (IB), partial (GE), bilateral-contracts (GN/HC) — is itself an IO finding about how reform-window market power decomposes by firm portfolio.

---

## §2 — Allaz–Vila two-period strategic forward sales

**Sketch.** When firms can sell forward (DA) and then re-trade in a sequential intraday market (ID), forward sales act as commitment devices that soften IDA competition. Each firm's optimal forward position depends on the perceived *information value* of forward commitment vs IDA flexibility. The reform sequence shifts that trade-off:

- IDA reform (2024-06-14): from 6 local sessions to 3 European sessions (less granular IDA → forward commitment more valuable).
- ISP15 (2024-12-01): settlement granularity finer → cost of holding an imbalance position rises → forward commitment less attractive at the margin.
- MTU15-IDA (2025-03-19): IDA granularity finer → forward commitment as commitment device weakens.
- MTU15-DA (2025-10-01): DA granularity finer → DA-IDA symmetry restored.

**Alive claims this rationalises.**
- B1 — bid-shading peaks at 3-sess and ISP15 (elevated commitment value when IDA is coarser/costlier).
- B2 — IDA-offer collapse at MTU15-DA (commitment value falls when granularity symmetric).
- B6 — forecast-error→imbalance pass-through R² jump in DA60/ID15 (asymmetric-granularity window: DA commitment can no longer absorb intra-hour shocks; pass-through to imbalance is mechanically tighter).
- D1 — within-month dispersion rises post-MTU15-DA (more forward arbitrage on intra-hour granularity).

**Sharp prediction.** The DA-cleared share should predict subsequent IDA repositioning under the model, and the predictive relationship should *flip sign* at MTU15-IDA (when forward commitment becomes less valuable). A regression of $\Delta Q_{i,d}^{\text{IDA}}$ on $q^{\text{DA}}_{i,d}$ by regime, with firm FE, would test this.

**Priority empirical refinement.** Allaz–Vila commitment-value test using `da_ida_wedge_structure.py` panel + `build_firm_bid_revenue.py` revenue panel. Expected runtime: ~1 day.

### Run 2026-04-25 — partial support, mixed signs across firms

Script: `scripts/analysis/modelling/allaz_vila_commitment_test.py`. Spec: per-(firm, date, hour) panel of $q_{\text{DA}}$ from pdbce + signed $\Delta Q_{\text{IDA}}$ from pibcie (sell − buy across all IDA sessions). OLS by (firm, regime), date-clustered SE, firm FE absorbed. n = 280,811 obs, ~10k per (firm × non-pre-IDA regime).

| Firm | pre-IDA β | 3-sess β | ISP15 β | DA60/ID15 β | DA15/ID15 β | Pattern |
|---|---:|---:|---:|---:|---:|---|
| **GE** | $-0.039^{***}$ | $-0.076^{***}$ | $-0.050^{***}$ | $-0.053^{***}$ | $-0.031$ | Deepens at 3-sess, recovers toward pre-IDA at MTU15-DA. **Consistent with Allaz–Vila.** |
| **IB** | $-0.026^{***}$ | $-0.011$ | $-0.027^{***}$ | $-0.009^{*}$ | $+0.012$ | Sign-flip at MTU15-DA but magnitude small ($\|\beta\|<0.03$). Weak support. |
| **GN** | $+0.077^{***}$ | $+0.143^{***}$ | $+0.028$ | $+0.200^{***}$ | $+0.054^{**}$ | **Positive throughout** — opposite of textbook prediction. Hydro-heavy resale dynamic, not commitment-deterrence. |
| **HC** | $-0.083^{***}$ | $-0.092^{*}$ | $-0.128^{*}$ | $-0.063$ | $+0.168^{**}$ | **Clean sign-flip at MTU15-DA.** Strongest single-firm support. |

R² small everywhere (1–10%); $q_{\text{DA}}$ explains a modest share of the IDA-delta variation, as expected.

**Reading.** 2 of 4 firms (GE, HC) show regime-dependent commitment-slope evolution consistent with the Allaz–Vila prediction: the slope deepens or stays negative through the asymmetric-granularity window (3-sess + ISP15) and attenuates or flips sign once DA-IDA granularity matches at MTU15-DA. IB is in the same direction but too small to call. GN goes the opposite way — likely reflecting hydro-heavy resale dynamics rather than CCGT-style commitment behaviour, and consistent with GN's portfolio-composition wound (W3, F4 in `CLAIMS_LEDGER.md`).

**Status of §2 model.** Wounded but not killed: the granularity-mediated commitment-value channel finds support in CCGT-heavy firms (GE) and the smallest firm (HC), but does not generalise across all Big-4. New ledger row: **F5** (wounded).

**Caveats.**
- The MTU15-DA slope changes coincide with Rule 28.8 elimination (2025-03-19), the same date as MTU15-IDA. Rule 28.8 reallocates bilateral-contract DA exposure across firms (B5, F4); some of the slope shift could be mechanical rather than strategic.
- No identification — these are regression coefficients, not ATTs.
- Per-firm subsamples in the post-reform regimes are small (~2.5–4.7k obs), so individual β estimates are noisy.

**Not promoted to a new alive claim.** Stays as wounded F5; if a future refinement (per-tech, or controlling for Rule 28.8 share) cleans up the signal, status can be revised.

### Refinement 2026-04-26 — peak/off-peak portfolio split: Allaz–Vila supports IB and GE in peak hours

Script: `scripts/analysis/modelling/allaz_vila_portfolio_split.py`. Hypothesis: the Allaz–Vila commitment-value mechanism applies when firms have strategic-marginal capacity, which in Spain is CCGT in peak demand hours. Hydro and nuclear are infra-marginal. So if Allaz–Vila is the right reading, the slope evolution should be **stronger in peak hours (h11–22)**, where CCGT is on the supply margin.

Pooled regimes (for power): pre-MTU15-IDA = pre-IDA + 3-sess + ISP15-win; then DA60/ID15; then DA15/ID15.

**Δβ from pre-MTU15-IDA to DA15/ID15, by peak-vs-off-peak partition** (positive = slope attenuated toward zero or flipped sign):

| Firm | Δβ peak (CCGT margin) | Δβ off-peak | Reading |
|---|---:|---:|---|
| **IB** | $+0.0494^{*}$ (sign flip $-0.025 \to +0.024$) | $+0.020$ | **Clean Allaz–Vila in peak; weak off-peak.** |
| **GE** | $+0.0284$ (slope attenuates $-0.034 \to -0.005$) | $-0.020$ (slope **deepens** $-0.040 \to -0.060$) | **Allaz–Vila in peak; off-peak goes the *opposite* way.** Diagnostic of CCGT-vs-other-tech mechanism. |
| GN | $-0.047$ (slope decreases but stays positive) | $-0.032$ | Doesn't fit Allaz–Vila in either partition. |
| HC | $+0.200^{*}$ | $+0.304^{***}$ | Both partitions show sign-flip; **off-peak signal larger** — opposite of Allaz–Vila peak prediction. |

**Reading.**

- **IB and GE both show the Allaz–Vila pattern in peak (CCGT-margin) hours.** GE's off-peak slope going the opposite way (deepening rather than attenuating) is itself a *positive* mechanism diagnostic: it means the slope evolution isn't a generic regime shift but is tied to CCGT-margin hours specifically.
- **GN doesn't fit Allaz–Vila in either partition.** Slope is positive throughout, consistent with hydro-resale dynamics dominating GN's behaviour (per F4 Rule 28.8 caveat). The model doesn't apply to hydro-heavy firms.
- **HC's sign-flip is concentrated in off-peak hours**, opposite of the CCGT-margin prediction. HC is a small firm; the off-peak signal could reflect different marginal-capacity dynamics for small mixed portfolios. Doesn't refute Allaz–Vila but doesn't cleanly support it either.

The clearing-price-quartile cut (CUT 2 in the script output) gives noisier results — small per-cell N and explosive Δβ for GN — but the peak/off-peak cut is the cleaner portfolio decomposition.

**F5 status update (was: wounded).** Status: **partial alive** with explicit portfolio scope. The Allaz–Vila granularity-commitment mechanism finds support in peak (CCGT-margin) hours for the two large mixed-portfolio firms (GE, IB). The mechanism does not extend to hydro-heavy GN, and HC's pattern is ambiguous. **The cross-firm decomposition is itself the empirical content**: Allaz–Vila describes large CCGT-portfolio firms specifically.

**Implications for the thesis.**

- F5 can now be cited in the structural chapter as: *"For peak demand hours, the slope of GE's and IB's IDA repositioning on DA cleared quantity attenuates or flips sign across the reform, consistent with the Allaz–Vila prediction that finer IDA granularity reduces the strategic commitment value of forward sales. The pattern does not extend to off-peak hours, where the marginal-capacity tech is non-CCGT, providing a within-firm placebo for the mechanism."*
- The within-firm placebo (peak vs off-peak for GE going opposite ways) is the strongest single-firm Allaz–Vila evidence in the project — closer to identification than F5 looked at first pass.

### Refinement 2026-04-27 — Bushnell water-value test FAILS for IB; F8 mechanism is plausibly Allaz–Vila not Bushnell

Script: `scripts/analysis/lerner/f8_bushnell_water_value.py`. Direct test of the classical Bushnell (2003) hydro-thermal model against IB's Q4 dispatch concentration. Bushnell predicts: low reservoir → high shadow price of stored water → tighter Q4 concentration. Data: ENTSO-E A72 weekly Spanish reservoir-filling indicator 2018-2026 (n=92 monthly observations, range 4.85–15.36 TWh stored hydro energy), merged with IB and Fringe monthly Q4 hydro shares from `hydro_strategic_dispatch.py`.

**Pearson correlations (predicted ρ < 0):**

| Series | ρ vs reservoir | Reading |
|---|---:|---|
| IB Q4 share | $-0.169$ | Weakly negative — predicted direction, tiny magnitude |
| IB-Fringe gap | $-0.014$ | Essentially zero — gap is reservoir-invariant |
| **Fringe Q4 share** | $\mathbf{-0.380}$ | **Placebo STRONGER than treatment** — non-strategic firms track scarcity more than IB |
| IB hydro GWh | $+0.409$ | Sanity check passes (more reservoir → more IB hydro generation) |

**OLS regression with cal-month + year FE** (n=92, R²=0.373):
- IB Q4 share: $\beta_{\text{reservoir}} = -0.487$ pp/TWh, SE 2.92, **p=0.868** (null)
- Gap: $\beta_{\text{reservoir}} = +1.450$ pp/TWh, SE 2.53, **p=0.567** (wrong sign, null)

**Decile cuts** (IB Q4 share by reservoir decile):

| Decile | Mean reservoir TWh | IB Q4 % | Fringe Q4 % | Gap pp |
|---:|---:|---:|---:|---:|
| D1 (driest) | 5.94 | 65.1 | 48.3 | 16.8 |
| D5 | 9.34 | 57.7 | 40.2 | 17.5 |
| D10 (wettest) | 13.82 | 58.9 | 38.2 | 20.7 |

No monotonic gradient. The +17pp gap is structurally constant across reservoir conditions.

**Reading.** The classical Bushnell water-value mechanism — low reservoir raises the shadow price of stored water, prompting tighter strategic concentration in top-price hours — **is rejected for IB Spanish hydro**. The Fringe placebo failure (-0.380 vs IB's -0.169) is the sharpest single signal: if anything, *non-strategic* run-of-river plants concentrate in top-price hours more strongly when reservoirs are low (because they have less scheduling flexibility, not more strategic intent). IB's Q4 concentration is reservoir-invariant.

**Implications for §1 (Cournot) and §2 (Allaz–Vila).** With Bushnell rejected, the most plausible remaining mechanisms for the F8 +17pp Q4-concentration gap are:

1. **Allaz–Vila forward-position-driven dispatch (§2)**. IB's contractual coverage profile (vertical integration, retail load, OTC forwards) may determine optimal Q4 timing regardless of water availability. If IB's expected residual exposure is concentrated in evening peak hours, then Q4 dispatch is the optimal hedging response — invariant to reservoir state. F5's peak-hour result for IB and GE ($\Delta\beta_{\text{peak}} = +0.049^{*}$ for IB, sign-flip from -0.025 to +0.024 across MTU15-DA) is consistent with this reading.

2. **Operational-protocol fixed dispatch heuristic.** IB's hydro-management software may routinely dispatch at evening peak based on a fixed price-prediction protocol, not reservoir-conditional optimization.

3. **Ramping/cycling constraints** that mechanically favour evening peak ramping for large-reservoir multi-cascade systems (TAMEGA, SIL, DUER cascades require coordinated peak-ramp dispatch).

**Diagnostic implication for the modelling chapter.** The modelling chapter cannot lead with a Bushnell story for IB hydro — the direct test fails. Allaz–Vila remains the best surviving mechanism candidate, with Cournot inverse-slope (§1, F6) as a complementary structural-firm-level corroborator. The F8 row in the ledger has been retitled from "Bushnell signature" to "Q4-concentrated dispatch with non-Bushnell mechanism."

**No new claim row.** The Bushnell-rejection is a refinement of F8 wounded; the surviving claim is the +17pp structural gap, with mechanism reframed.

---

## §3 — Pigouvian imbalance-settlement framing

**Sketch.** The ESIOS settlement rule allocates total imbalance cost across BRPs in proportion to each BRP's signed deviation, without conditioning on the *marginal* per-MWh cost that segment imposes on the system. If different segments (wind, hydro, retailers, conv-RZ, etc.) have heterogeneous per-MWh marginal costs at imbalance, the current allocation is non-Pigouvian by construction: the firms producing high-marginal-cost imbalances do not face the marginal price; they face the average-rule price.

A clean OLS of $|\text{imp\_eur}|_{\text{ISP}}$ on segment $|\text{MWh}|$ shares per regime estimates per-segment marginal contribution. Preliminary results in `marginal_imbalance_cost.py` (uncommitted) suggest free-market retailers contribute 35–42% of imbalance volume but cause only €26/MWh of marginal cost; conv-RZ contributes 14% of volume but €312/MWh marginal cost. Order-of-magnitude misalignment.

**Alive claims this rationalises.**
- S1 — A87 net income jumps at ISP15: total cost rises when settlement granularity tightens, and the rule's misalignment with marginal costs becomes welfare-relevant at higher absolute amounts.
- B6 — pass-through jump: when settlement is on 15-min granularity, segments with high marginal costs (conv-RZ) cannot net intra-hour, so imbalance volumes propagate to settlement near-mechanically.

**Sharp prediction.** Under a counterfactual Pigouvian allocation (each segment pays its marginal cost), conv-RZ payments would rise sharply, free-market retailer payments would fall sharply, and total cost recovery should be unchanged. The welfare loss from the current rule = covariance of (segment volume, marginal cost) summed across segments × time.

**Priority empirical refinement.** Clean version of `marginal_imbalance_cost.py` with month-of-year FE + price-bin FE + same-calendar checks (current version has no seasonality control); compare per-segment marginal coefficient to A87 segment shares from `esios_a87_cross.py`. Expected runtime: ~1 day.

### Run 2026-04-25 — Pigouvian misalignment confirmed; survives month + hour FE

Script: `scripts/analysis/modelling/pigouvian_clean_regression.py`. Multivariate OLS per regime (post-ISP15 only, since ISP-resolution settlement requires ISP15 onwards):
$$|\text{imp\_eur}|_t = \text{const} + \sum_{\text{seg}} \beta_{\text{seg}} \cdot |\text{MWh}_{\text{seg}}|_t + \alpha_{\text{cal-month}} + \alpha_{\text{hour}} + \varepsilon_t$$
HC3 SE. Per-segment volume share computed within each regime sample.

**Per-segment marginal-cost coefficients (€/MWh):**

| Segment | ISP15 win | DA60/ID15 | DA15/ID15 | Avg volume share |
|---|---:|---:|---:|---:|
| conv-RZ (big plants in regulation zones) | $+300^{***}$ | $+220^{***}$ | $+210^{***}$ | ~13% |
| conv-NRZ | $-52^{***}$ | $-2$ | $-16^{*}$ | ~10% |
| RE wind | $+90^{***}$ | $+43^{***}$ | $+94^{***}$ | ~30% |
| RE hydro | $+184^{*}$ | $+527^{***}$ | $+19$ | ~2% |
| RE thermal | $-133^{***}$ | $+135^{***}$ | $+81^{*}$ | ~4% |
| **COR retailers (regulated)** | $\mathbf{+784^{***}}$ | $\mathbf{+496^{***}}$ | $-9$ | ~4% |
| **LIB retailers (free market)** | $-22^{**}$ | $+8$ | $+37^{***}$ | **~38%** |
| Export units | (noisy, n→0) | | | ~0% |
| Import units | (noisy, n→0) | | | ~0% |

R² = 0.35–0.38 across regimes; n = 10–20k ISPs per regime.

**Pigouvian misalignment.** The settlement rule charges all segments uniformly per MWh, but the regression-implied marginal contributions differ by **5–15×**. Sharpest contrast:

- **LIB free-market retailers** drive ~38% of total imbalance volume with marginal-cost coefficient ≤€37/MWh — near zero.
- **conv-RZ** drives ~13% of volume with marginal-cost €210–300/MWh — an order of magnitude higher per MWh.

If the rule were Pigouvian (each segment paying its own marginal contribution), conv-RZ payments would rise sharply and LIB payments would fall sharply.

**Regime evolution.**

- *Conv-RZ* stable at €210–300/MWh across all three post-ISP15 regimes — granularity tightening doesn't normalise this.
- *COR retailers* collapse from €784/MWh (ISP15 win) to −€9/MWh (DA15/ID15) — a 90× drop. Could be (a) genuine behavioural recalibration as MTU15 reform completes, (b) sample-size noise (COR is 4% of volume), or (c) rule-side change in COR-specific allocation. Worth flagging as a sub-finding.
- *LIB retailers* shift from −€22 (ISP15 win) to +€37 (DA15/ID15) — modest evolution toward Pigouvian alignment but still small per MWh.

**New ledger row: S7 (alive).** Per-segment marginal imbalance cost is order-of-magnitude heterogeneous; the current uniform-allocation rule is non-Pigouvian. Survives month-of-year + hour-of-day FE.

**Caveats.**

- The multivariate β is the per-segment *marginal contribution to total imbalance amount*, not the welfare-theoretic *marginal cost*. They coincide only if segments contribute independently. Empirically segments correlate (common wind / load shocks), so β is somewhat overdetermined.
- The €784/MWh COR coefficient is unstable across regimes (drops to −€9 in DA15/ID15). The headline misalignment claim does not depend on this coefficient.
- Negative coefficients on conv-NRZ and thermal-RE in ISP15 win suggest the multivariate spec is picking up correlations going opposite to the system imbalance — interpret with care.
- The order-of-magnitude contrast LIB vs conv-RZ is the robust kernel of the finding.

**Status of §3 model.** Alive. The Pigouvian-misalignment framing is empirically supported.

### Refinement 2026-04-28 — Direct per-segment € decomposition: actual vs Pigouvian counterfactual

The β regression measures correlations; for the May talk we want a direct €-attribution. Built `pigouvian_burden_shares.csv` from S6 cumulative-excess totals × per-segment volume shares (actual rule) and × β-weighted volume shares (Pigouvian counterfactual).

**Asymmetric-window decomposition (DA60/ID15, €545M cumulative excess vs same-cal pre-IDA baseline)**:

| Segment | Actual paid (uniform) | Pigouvian counterfactual | Δ (over-/under-paid) |
|---|---:|---:|---:|
| LIB free-market retailers | €226M (41.5%) | €22M (4.0%) | **+€204M overpaid** |
| Wind RE | €140M (25.7%) | €77M (14.2%) | +€63M overpaid |
| Conv-NRZ | €51M (9.3%) | €0M (0%) | +€51M overpaid |
| Conv-RZ (regulation zone) | €69M (12.6%) | €195M (35.7%) | **−€126M underpaid** |
| COR retailers (regulated) | €20M (3.7%) | €130M (23.8%) | **−€110M underpaid** |
| Hydro RE | €11M (1.9%) | €72M (13.2%) | −€61M underpaid |
| Thermal RE | €29M (5.3%) | €50M (9.1%) | −€21M underpaid |

**~58% of the €545M cumulative excess is structurally misallocated** under the uniform allocation rule. The cross-segment redistribution favours dispatchable-portfolio segments (conv-RZ + COR + hydro RE + thermal RE) at the expense of inflexible-portfolio segments (LIB retailers + wind + conv-NRZ).

**Direct answer to "did renewables pay more in liquidaciones?"**: YES. LIB free-market retailers (renewable-heavy retail load) + wind RE together paid **€366M** of the €545M asymmetric-window cumulative excess under the actual rule, vs **€99M** under the Pigouvian counterfactual — a **€267M overpayment** driven by the rule's structural unfairness, not by these segments' marginal contribution to system stress.

**Caveats:**
- The Pigouvian counterfactual uses β estimates that have R²≈0.36, so the share normalisation is approximate (we renormalise positive contributions to 1.0).
- Per-segment volumes are aggregate ESIOS `endXXqh` series (verified to be imbalance volumes, not gross flows: mean 244 MWh abs per ISP).
- We do NOT have direct per-BRP settlement amounts (gated). The decomposition is a reduced-form attribution under the assumption that segments map cleanly to BRP types.
- The €545M is the cumulative excess vs same-cal pre-IDA baseline (the S6 quantity), not the total imbalance settlement amount in the regime. We are decomposing the EXCESS, i.e. the rents created by the asymmetric-granularity window.

**Status:** §3 sharpened. The Pigouvian-incidence claim now has direct €-attribution evidence anchored on S6 totals.

### Unparsed-family inventory (2026-04-28, thesis follow-up)

Audit of `data/raw/esios/liquidaciones/*/extracted/` reveals **234 unique ESIOS settlement family names extracted, but only 19 parsed into `liquicomun_all.parquet`**. The user has 215 unparsed families on disk. Several are highly relevant for per-BRP / per-segment work for the thesis chapter:

- **`endesvlb`** (11 files) — "energía neta de desvíos LIB" = free-market retailer imbalance volumes. Could decompose LIB's €204M overpayment.
- **`grdesvio`** (28 files) — "grupos de desvío" = imbalance groups, likely per-BRP-group.
- **`ccbbrp` / `ccbrpbs3` / `ccbrprad3`** — per-BRP cost components.
- **`costedsv` / `prdsvcos` / `prexcdsv`** — imbalance cost decompositions.
- **`liqsegme`** (28 files) — settlement by segment ("liquidación segmentada"!) — most promising candidate for direct per-segment € verification.
- **`dsvcontr`** — imbalance contributions.
- **`tipoliqu`** — settlement-type classifier.

**Action for thesis chapter** (post-May presentation): write a parser extension to `src/mtu/parsing/esios_liquicomun.py` to add these families to `liquicomun_all.parquet`, with priority to `liqsegme` (segment-level settlement) and `endesvlb` (LIB-specific volumes). This converts the May talk's reduced-form Pigouvian claim into a direct empirical anchor.

---

## §4 — Asymmetric-granularity friction (welfare)

**Sketch.** A welfare-theoretic reading of the three-layer reform sequence. Define $f_{\text{DA}}, f_{\text{ID}}, f_{\text{ISP}}$ as the granularity (frequency in Hz, or 1/period) of each market. The reform sequence trajectory was:

| Regime | $f_{\text{DA}}$ | $f_{\text{ID}}$ | $f_{\text{ISP}}$ |
|---|---|---|---|
| pre-IDA | 60-min | 60-min × 6 sess | 60-min |
| 3-sess (post-IDA-reform) | 60-min | 60-min × 3 sess | 60-min |
| ISP15 window | 60-min | 60-min × 3 sess | 15-min |
| **DA60/ID15** | **60-min** | **15-min** | **15-min** |
| DA15/ID15 (post-MTU15-DA) | 15-min | 15-min | 15-min |

The DA60/ID15 row is the *asymmetric* row: DA and ISP at different granularities, ID matched to ISP only. The other rows are symmetric (matched-frequency markets). A friction proxy is the |DA-frequency − ISP-frequency| × volume term; this peaks in DA60/ID15.

**Alive claims this rationalises.**
- S5 — four-way ENTSO-E concordance peaks at ISP15 and moderates at MTU15-DA.
- F1, F2 — Lerner elevation peaks at DA60/ID15 (the asymmetric row) and reverses at MTU15-DA.
- B1, B4 — bid-shading and XBID trade-price σ peak at DA60/ID15.

The peak-friction-at-DA60/ID15 pattern is consistent across all three layers (system, structural, behavioural), which is the headline of D14–D16 in `_identification_target.md`. The model gives the pattern a name and a structural interpretation.

**Sharp prediction.** A87 net income deviation from same-calendar pre-asymmetry baseline should scale with a function of $|f_{\text{DA}} - f_{\text{ISP}}|$. A back-of-envelope welfare-loss estimate in the asymmetric window: $\Delta W = \int_{T_{\text{ISP15}}}^{T_{\text{MTU15-DA}}} \big( A87_t - A87_t^{\text{baseline}} \big) \,dt$ where the baseline is the pre-2024 same-calendar mean.

**Priority empirical refinement.** A87 deviation from same-calendar pre-asymmetry baseline, integrated over the DA60/ID15 window, with bootstrap CIs from monthly residuals. Cross-validate with A86, A85, A84 deviations to triangulate the welfare proxy. Expected runtime: ~1.5 days.

### Run 2026-04-25 — A87 welfare proxy is large and significant; A85 confirms; A86/A84 noisy at monthly aggregation

Script: `scripts/analysis/modelling/asymmetric_granularity_welfare.py`. For each system-level outcome, monthly OLS:
$$y_m = \alpha_{\text{cal-month}} + \sum_r \beta_r \cdot \mathbf 1\{m \in r\} + \varepsilon_m$$
with pre-IDA as the dropped baseline. $\beta_r$ is the monthly excess of regime $r$ over the same-calendar pre-IDA baseline. Welfare integral = $\beta_{\text{ISP15 win}} \cdot 4 + \beta_{\text{DA60/ID15}} \cdot 6$ (the 10-month asymmetric window). Bootstrap 95% CI under the null = resample pre-IDA residuals, refit, recompute integral; 1000 reps.

| Outcome | Asymmetric window cumulative excess | Bootstrap CI under null | Verdict |
|---|---:|---|---|
| **A87 net income (EUR-millions/month)** | **+€995.8M** | $[-213, +217]$ | **Significant. Large.** |
| A85 imbalance-price σ (EUR/MWh-months) | $+216.9$ | $[-139, +149]$ | **Significant.** |
| A86 daily mean &#124;V_imb&#124; (MWh-months) | $+17{,}021$ | $[-29{,}735, +34{,}557]$ | Insignificant at monthly aggregation; consistent with daily-level S2 alive claim from nb11. |
| A84 mean activation price (EUR/MWh-months) | $-173.1$ | $[-311, +414]$ | Insignificant. Possibly wrong outcome (the up–down *spread* would be more relevant than the level). |

**Headline.** Across the 10-month asymmetric-granularity window (4 months ISP15 win + 6 months DA60/ID15, 2024-12-01 to 2025-10-01), the Spanish system collected approximately **€996 million in additional BRP-to-TSO net imbalance settlement** above what the same-calendar 2018–2024 baseline would predict. The point estimate is ~4.6× the upper edge of the bootstrap null distribution.

The price-dispersion measure (A85 σ) corroborates: imbalance-price σ is elevated +€217 above null in the asymmetric window. Per-month: +€20–24/MWh σ depending on sub-regime.

A86 (volumes) and A84 (activation price level) are noisy at monthly aggregation. nb11's daily-level S2 finding (+5.1 GWh/d at ISP15) is the right power for volumes; my monthly point estimate (+3.7 GWh/d in ISP15 win) is consistent with it but underpowered.

**Reading.** The asymmetric-granularity friction model gets *strong* support from A87 — a single welfare-relevant number that the thesis can quote. ~€1 billion in 10 months (€100M/month average) is large in absolute terms; relative to pre-IDA baseline (~€40M/month), it's a 2.5× elevation. The number falls back to ~€23M/month at MTU15-DA, which the model predicts (granularity re-symmetrises).

**New ledger row: S6 (alive).** "A87 cumulative excess in asymmetric-granularity window vs pre-IDA same-calendar baseline = +€996M, highly significant (bootstrap null CI [-213, +217])."

**Caveats.**
- The €996M is BRP→TSO transfer, not deadweight loss. It represents redistribution from market participants to the system, partially returned via reserve-procurement spending (A87 expenses, A01 direction). The net version is below.
- The pre-IDA baseline implicitly includes the energy-crisis years 2022–2023, when imbalance prices spiked. Calendar-month FE handles seasonality but not regime-shifting volatility. A more conservative baseline would restrict to 2018–2021 (or "calm" months only); not done here.
- The +€996M figure is consistent with the descriptive S1 alive claim (€38 → €160 → €72M/mo) but quantifies the cumulative deviation. S6 is a sharper aggregation of S1.

### Refinement 2026-04-27 — Month-by-month decomposition of the +€1.1B

The headline regression integral averages over 10 monthly observations within the asymmetric window. The actual month-by-month profile is:

| Sub-period | Months | Mean excess (€M/mo) | Cumulative (€M) |
|---|---:|---:|---:|
| Pre-IDA (Jan-May 2024) | 5 | ±10 (noise floor) | ±10 |
| 3-sess (Jul-Nov 2024) | 5 | +7.4 | +38 |
| **ISP15-win (Dec 2024 – Feb 2025)** | **3** | **+136.3** | **+408.9** |
| MTU15-IDA mid (Mar 2025) | 1 | +137.9 | +138 |
| **DA60/ID15 stable (Apr-Sep 2025)** | **6** | **+90.9** | **+545.4** |
| ↳ excluding June 2025 op.reforzada outlier | 5 | +80.6 | +403 |
| **Post-MTU15-DA (Oct-Dec 2025)** | **3** | **+14.6** | **+43.9** |

**Three observations sharpen the §4 mechanism story:**

1. **The dominant leg is ISP15-win + MTU15-IDA-mid (Dec 2024 – Mar 2025): +€546.8M in 4 months at +€137M/mo** — that's 50% of the headline in 40% of the window-time. The 15-min imbalance settlement rule (effective 2024-12-01) is the **primary** driver. Mechanically: settling 15-min positions separately quadruples the gross settlement base relative to hourly netting; the immediate post-ISP15 surge is consistent with the BRP-mechanical-exposure jump.

2. **DA60/ID15 (Apr-Sep 2025) sustains the transfer at a lower level** (+€91M/mo, ~33% lower than ISP15-win). Plausible reading: MTU15-IDA introduced 15-min intraday products that gave BRPs new tools to self-correct positions before real-time, partially closing the BRP-exposure gap even though the DA-vs-imbalance asymmetry persisted. So MTU15-IDA didn't structurally change the asymmetry but did give participants a new behavioural channel to dampen its consequences.

3. **MTU15-DA closure (Oct 2025) collapses the excess by ~6×** — this is the cleanest piece of policy evidence in the project. When DA aligned to 15-min and the settlement clocks matched, the transfer evaporated to near pre-reform levels, even with the post-blackout operación-reforzada period still in effect.

**The June 2025 outlier (+€142.5M)** sits inside the DA60/ID15 sub-period but is driven by operación-reforzada: A01 reserve activation cost doubled to €51M (2× neighboring months) and A02 BRP-paid spiked to €172M. Removing this single month drops the DA60/ID15-window total from €545M to €403M and the mean from €91M to €80M/mo — still meaningful elevation, but ~26% of that sub-period's cumulative excess sits in one anomalous month.

**Revised mechanism reading for the §4 model**: the asymmetric-granularity friction operates primarily through **the DA-vs-imbalance settlement-clock mismatch** (active from Dec 1, 2024 to Sep 30, 2025), with MTU15-IDA introducing a behavioural dampening channel (15-min intraday self-correction). The "asymmetric-granularity window" is correctly defined; the original §4 framing implicitly conflated the imbalance-clock change with the IDA-clock change, but the data shows the imbalance-clock change is doing most of the work. The §4 model should be interpreted with the imbalance-vs-DA granularity gap as the primary friction, not the DA-vs-IDA gap.

### Refinement 2026-04-25 — A87 NET fiscal balance (A02 − A01)

A02 alone could rise mechanically if more reserves are activated (because more BRPs pay imbalance settlement, but the TSO also pays BSPs more for those reserves). Subtracting A01 expenses isolates the **system rent** — the fiscal surplus the TSO collects from the imbalance settlement above what it pays out for reserves.

| Outcome | Asymmetric window cumulative excess | Bootstrap null CI | Notes |
|---|---:|---|---|
| A87 net income (A02 alone) | $+€995.8\text{M}$ | $[-213, +217]$ | gross |
| A87 expenses (A01 alone) | $-€99.2\text{M}$ | $[-255, +283]$ | not significant; reserve costs flat |
| **A87 NET (A02 − A01)** | $+€\mathbf{1{,}094.9\text{M}}$ | $\mathbf{[-90, +73]}$ | **~15× upper null bound; sharpest** |

The per-month net coefficients are even sharper: ISP15 win +€137M/mo (p<0.001), DA60/ID15 +€91M/mo (p<0.001), DA15/ID15 +€15M/mo (significant but small).

**Reading.** A02 rose by €120/€86M/month in the asymmetric window. A01 stayed essentially flat (point estimates close to zero, none significant). The net — what BRPs paid the TSO above what the TSO paid BSPs — rose by ~€137/€91M/month, accumulating to **+€1.095 billion across the 10-month asymmetric window**. The bootstrap null CI on the net measure is ±€80M, so the observed cumulative is ~15× the upper null bound — extremely significant.

This is a cleaner welfare-relevant statement than A02 alone. The rule's friction generates rent that is not offset by additional system spending; it accumulates as a fiscal surplus.

**S6 row updated** in `CLAIMS_LEDGER.md` to cite the NET figure (€1,094.9M) as the canonical number, with the gross A02 (€995.8M) and A01 (≈0) decomposed inline.

**Status of §4 model.** Alive, sharpened. The asymmetric-granularity prediction now anchors a cleaner welfare-relevant number — net fiscal surplus, not gross transfer.

### Run 2026-04-27 — S8: persistent redispatch escalation across all post-IDA regimes (LATER WOUNDED)

**⚠️ S8 demoted to wounded 2026-04-27 (later same day).** The renewable-control regression (`s8_renewable_control.py`) showed that ~80% of the post-IDA RZ elevation is statistically explained by Spanish renewable-share growth (+80% wind+solar generation in pre-IDA window alone). Only the ISP15-window-specific elevation (4 months Dec 2024 – Mar 2025, +156 GWh/mo p=0.022) survives the renewable control; DA60/ID15 and DA15/ID15 regime effects collapse to zero (p≈0.5). The "persistence post-MTU15-DA" feature was the key signature of the original claim, and it doesn't survive. The original analysis is preserved below for record; the WOUND is documented in `CLAIMS_LEDGER.md` S8 row and in the §4 synthesis below.


A second system-layer effect not captured by §4's granularity-friction model. Per-month RZ system-security activations (TipoRedespacho 61 in `totalrp48preccierre`, 2015 → 2026-04 panel) vs same-calendar pre-IDA baseline:

| Regime | Months | Total RZ (GWh/mo) | Excess vs baseline | Excess % |
|---|---|---:|---:|---:|
| pre-IDA | 114 | 269.5 | (baseline) | 0% |
| 3-sess | 6 | 485.3 | +215 | **+82%** |
| ISP15 win | 4 | 502.4 | +220 | **+80%** |
| DA60/ID15 | 7 | 427.8 | +159 | **+60%** |
| **DA15/ID15** | 3 | 414.2 | +157 | **+61%** |

Bootstrap null CI [-93, +110] GWh/mo. All post-IDA regimes 1.4–2.0× above the upper bound — significant.

**The DA15/ID15 (post-MTU15-DA) elevation is the decisive feature.** S6's granularity-friction effect collapsed at MTU15-DA (€94M/mo → €7M/mo). S8's RZ-redispatch effect did NOT — it stays elevated at +60% post-MTU15-DA. The granularity-friction model in §4 therefore explains S6 but does NOT explain S8.

**Mechanism candidate (untested)**: the 6→3 IDA-session reduction at 2024-06 may have broken a prior matching of imbalance-settlement granularity to DA dispatch, increasing the residual that REE must redispatch via RZ. This is a market-design effect that's structurally different from the asymmetric-granularity friction §4 models — it operates at a different layer (operational / network-security rather than balance-fiscal) and with a different reform trigger (IDA session reduction rather than ISP15-vs-DA60 mismatch).

**Direct cost**: ~€10–14M/month above pre-IDA baseline at regime-mean RZ closure prices €60–90/MWh × +157 GWh/mo excess; ~€200–280M cumulative across the 20-month post-IDA window. This is direct REE→generator transfer cost, not deadweight loss; the welfare interpretation requires a counterfactual that isn't currently available.

**Implications for §4.** The §4 model cleanly explains S6 (collapses at MTU15-DA) but does NOT explain S8. A two-channel system-layer welfare analysis is more accurate:
- **Channel 1 (S6, asymmetric-granularity friction)**: closed by MTU15-DA reform.
- **Channel 2 (S8, IDA-session-reduction redispatch escalation)**: NOT closed by MTU15-DA; persists, suggesting a separate structural mechanism originating in the 6→3 IDA session reduction.

The §4 framework should therefore be cited as the right model for the S6 number, but with an appended footnote that the system-layer welfare reading is incomplete without S8 (the redispatch channel). A future modelling extension could formalise the IDA-session-reduction → redispatch-residual link as a separate operational-design friction.

**Status of §4 + S8 (revised 2026-04-27 PM after C1 audit attack).** §4 model alive and clean. S8 demoted to wounded after `s8_renewable_control.py` showed that controlling for monthly Spanish wind+solar generation collapses the DA60/ID15 and DA15/ID15 regime coefficients (p≈0.5); only the ISP15-window 4-month effect (+156 GWh/mo, p=0.022) survives. The two-channel reading remains the right system-layer synthesis: **S6** (granularity-friction fiscal cost shift, robust) + **S5** (four-way ENTSO-E concordance). S8 is no longer a clean third channel; it remains as a wounded narrower claim about the ISP15-window adjustment period. The thesis system-layer chapter should lead with S5 + S6 and treat S8 as a footnote about adjustment-period dynamics, not a separate "redispatch-escalation channel." The two phenomena (renewable expansion + IDA reform sequence) are partially co-temporal in Spain 2018–2025, making clean causal separation infeasible at thesis-scale data.

---

## §5 — Bid complexification under finer ISP (B8)

**Sketch.** A textbook market-design prediction: as the market-clearing time slot becomes finer (60-min hour → 15-min ISP), strategic firms with marginal-cost variation across the slot have incentive to submit a finer price ladder (more tranches per slot) to express their willingness to clear at different price points. Non-strategic firms (infra-marginal nuclear, hydro-baseload, fringe-RE) face a flatter marginal-cost curve and don't gain from finer bid structure. So bid complexity should respond heterogeneously to MTU15-IDA based on each firm's strategic-marginal capacity.

**Predictions.**
- Strategic firms with CCGT on the supply margin: bid-complexity per ISP rises post-MTU15-IDA.
- Infra-marginal / inflexible firms: bid-complexity per ISP is flat or falls (less of a need to differentiate within an already-finer time slot since each individual ISP is now "smaller" relative to the unit's commitment).
- Mechanical / format-driven response would predict uniform behaviour across firms.

**Alive claims this rationalises.**
- B8 (alive 2026-04-26): IB within-unit tranches-per-period 5.49 → 8.73 (1.59×), confirmed on three named CCGT units (TAPOWER, ARCOS1, CTN4). HC stable. GE flat at ~2 tpp throughout. GN simplifies (6.46 → 3.22). Fringe-survivors simplify (5.69 → 3.07).
- The IB-only complexification + GN/Fringe simplification together rule out the mechanical/format story (which predicts uniform behaviour).

**Why this matters.** B8 is the third test that picks out IB as the canonical strategic firm (alongside F6 Cournot tercile and F5 Allaz–Vila peak hours). Three different mechanism predictions, same firm-level heterogeneity, same answer: IB is the firm whose behaviour traces the textbook IO mechanism response. The other Big-4 firms either have non-strategic-CCGT portfolio composition (GE), or different operational responses (GN simplifies — possibly hydro-block bidding), or are too small to be diagnostic (HC).

**Implications for the thesis.** Cite B8 as a behavioural complement to F1/F2 (Lerner) and F5 (Allaz–Vila). The cross-firm contrast is the IO content: bid-complexity response to finer ISP is *not* mechanical — only firms with strategic-marginal CCGT capacity invest in finer price ladders.

**Priority empirical refinement (not run).** Within IB, regress within-unit Lerner on within-unit tranches-per-period, with month + price-bin FE. If IB's bid complexification co-moves with IB's market power, it strengthens the structural reading: complexification IS the strategic instrument. ~½ day if pursued.

---

## §6 — Strategic availability under within-firm fleet substitution

**Sketch.** The classical Joskow–Kahn (2002 EJ) capacity-withholding framework was developed for California 2000–2001: firms strategically reduce plant availability to engineer supply scarcity and price spikes. Crampes–Moreaux extend the static withholding intuition to capacity-allocation problems where firms control multiple plants in the same regulatory zone. The CNMC's 2023 SBO3 case (€41.5M sanction against Naturgy) operationalised this for the Spanish technical-restrictions market via a **three-situation pivotality test**: classify each (plant × ISP) cell as zone-pivotal vs zone-non-pivotal vs zone-irrelevant, then test whether the firm's RTT bid price differs systematically across the three situations. The CNMC framework treated cross-firm pivotality (Naturgy SBO3 exploiting Endesa PGR5's unavailability in zone-Z); the project extends this to **within-firm fleet pivotality** — firm idles plant A inside its own fleet to engineer pivotality for plant B.

**Why this section is needed.** The post-2025-04-28 blackout produced ~50 CNMC expedientes (Article 64.37 + 65.34) against IB/GE/GN/REP. The conduct pattern documented in those expedientes — and replicated in OMIE data via the F17/F18/F21/F22 cluster — is not rationalised by any of §1–§5. Cournot quantity-setting (§1) is about price-taking residual demand; Allaz–Vila (§2, rejected) was about granularity-mediated forward commitment; Pigouvian misalignment (§3) is about settlement-rule design; asymmetric-granularity friction (§4) is system-layer; bid complexification (§5) is about within-unit ladders. The strategic-availability mechanism is distinct: it operates on plant-level binary availability and within-firm cross-plant substitution, not on quantity offers or bid structure. Part IV of the thesis proposal points at this gap.

**Alive claims this rationalises.**
- **F17** — within-firm pair substitution: BES3→BES5 (Endesa Besós); ARCOS3→ARCOS1 (Naturgy Arcos); SROQ1→SROQ2 (Iberdrola San Roque); CTN4→CTN3 (Iberdrola Cartagena). The four documented pairs each show one plant losing share while a same-fleet substitute gains.
- **F18** — sanctioned plants ALL lost share post-blackout. Direct evidence the within-firm reshuffle is enforcement-responsive, not random.
- **F21** — three-situation pivotality test replication is widespread across Big-4 CCGT, not isolated to SBO3.
- **F22** — Naturgy fleet-wide bid-price wedge 11–35% in pivotal vs competitive hours, with SBO3 still showing +14% wedge after the 2023 sanction. The conduct adapted but did not stop.
- **F14** (negative implication) — the simple cross-firm moral-hazard reading (one firm undersupplies voltage control to capture a CCGT windfall at another firm's expense) is rejected: nuclear unaccounted reduction is system-wide 22-38%, not firm-specific.
- **F15** (mechanism diagnostic) — post-blackout CCGT windfall went to Naturgy (+7.1pp), not IB (-2pp). The strategic-availability rent flows to the firm with the largest matching plant fleet (Naturgy 18 of 50 Spanish CCGT plants), not the firm with the largest overall portfolio.
- **F16** (regime-shift signature) — IB CCGT supply-curve slope (FE-controlled) jumped from 0.95 MW/€ pre-blackout to 4.55 MW/€ post (4.8×). The strategic-CCGT posture *broke* under operación reforzada — IB no longer found it optimal to engineer fine-grained pivotality once REE was forcibly committing CCGT capacity. Mechanism diagnostic: when the regulator removes the firm's availability decision, the strategic instrument disappears.

**Sharp predictions (testable; mostly tested).**
1. Sanctioned plants face higher detection costs, so post-2023 SBO3 conduct should migrate from explicit RTT bid-price-wedges (which CNMC measures directly) to implicit DA blocking (extreme-high tranches at the OMIE €1000 cap on a fraction of supply). **F22 confirms**: explicit wedge persists at smaller magnitude; the implicit DA blocking is documented separately in the F22 evidence.
2. Within-firm fleet substitution should be observable as paired share movements within a regulatory zone. **F17 confirms** for four explicit pairs.
3. The strategic-availability rent should flow to the firm with the largest matching fleet in the binding constraint. **F15 confirms**: Naturgy CCGT-fleet captured the post-blackout windfall.
4. Once REE forces availability via P.O. 3.2 (operación reforzada), the strategic instrument should disappear in the affected technology. **F16 confirms**: IB CCGT supply-curve flattened 4.8× post-blackout.
5. (Untested) The cross-firm pivotality framework that the CNMC applied to SBO3 should generalise to other Spanish regulatory zones and other plant pairs. F21 begins this; full panel replication for all CNMC zones × 2024–2026 is a candidate refinement.

**Methodology lineage.**
- Joskow & Kahn (2002 EJ) — capacity-withholding diagnostics from California 2000–2001; methodology for inferring strategic withholding from availability data.
- Crampes & Moreaux — capacity-allocation in multi-plant firms; water-value reasoning extended to capacity choice.
- CNMC Resolución SBO3 (2023) — the three-situation pivotality test, directly replicable. See `docs/regulation/cnmc_resolutions/README_economic_methodology.md`.
- 13-year CNMC enforcement record — `docs/regulation/cnmc_historical_sanctions_2013-2026.md`. €25M IB hydro 2015 (Article 65.27) → €25.3M Naturgy + Endesa 2019 CCGT availability cases → €41.5M Naturgy SBO3 2023 → ~50 post-blackout expedientes 2026.

**Status of §6 model.** Alive — supports the strategic-availability mechanism with within-firm fleet substitution as the modern adaptation post-2023 enforcement. Six alive claims (F14 negative, F15, F16, F17, F18, F21, F22) rationalise consistently. The mechanism is distinct from §1–§5 and complements them: §1 explains DA-market price-setting (IB-canonical); §6 explains technical-restrictions and post-blackout CCGT/balancing conduct (Naturgy-canonical, with GE on the aFRR side per F19/F20).

**Priority empirical refinement (not run).** Cross-zone replication of F17 within-firm pair substitution: identify all CNMC regulatory zones with ≥2 plants from the same firm, classify (plant × ISP) cells as pivotal/non-pivotal/irrelevant per the SBO3 three-situation framework, regress availability and bid-price-wedge on situation interacted with firm. Expected runtime: ~1.5 days. Would convert F17 from descriptive ("4 pairs documented") to systematic ("X out of N within-firm pairs in CNMC zones show substitution conduct").

**Connection back to the thesis proposal.** Part IV anchor — see `thesis/drafts/master_thesis_proposal.md` § Part IV. The within-firm fleet-substitution framing is the thesis's structural extension of the CNMC's cross-firm SBO3 framework.

---

## How sections evolve

When a refinement is run (and only after passing the stop-rule):

1. The section receives a "Run" subsection with a date and the result.
2. If the result tightens the model's empirical anchoring, the affected `CLAIMS_LEDGER.md` rows get a pointer back to this file in the `Evidence notebook` column.
3. If the result undermines the model, mark the section "wounded" or "discarded" inline; don't delete.
4. If the result generates a new alive claim, add a row to the ledger and reference it from the section.

Sections may be added or removed as exploration proceeds. The criterion is: **does this model organise ≥2 alive claims?** If yes, keep the section. If no, archive it.

## Sections considered but not yet drafted

- *Hortaçsu–Puller structural bid functions* — already partially used in `build_firm_lerner_panel.py`. Could be promoted to its own section if a future refinement (e.g. firm-level bid-function recovery vs the Cournot reduced form) would be informative.
- *Wolak (2003) production-cost recovery* — a different structural identification of markups that could complement §1. Not yet evaluated.
- *Allcott (2012) imperfect competition with carbon costs* — analogous to §3 but with carbon as the externality. Useful if the thesis wants a comparison case.

These are noted for completeness and not pursued unless the four primary sections above mature.
