# Red-team audit of the alive ledger (2026-04-27)

Adversarial review of the 28 alive claims. Goal: surface the weakest links before a viva committee does. Each section ranks attacks by severity (★★★ = potentially load-bearing, ★★ = serious caveat, ★ = needs framing).

For attacks I think the project can defend, I add a "Defense" line. For attacks I cannot defend cleanly, I add a "Mitigation needed" line — these are the ones that should change either an analysis or the ledger row.

---

## §1 — F7/F8 (the IB-canonical headline) — most exposed

The whole structural-firm chapter rests on F7 (IB ≈ 98% of Big-4 cleared-price-difference rent) and F8 (IB hydro Q4 dispatch 63% vs Fringe 42%). Six attack vectors.

### ✅ A1. (DEFENDED 2026-04-27) Hydro plant-pair matching survives stricter criteria 100%

**Audit attack** (preserved below for record):

F7's per-IB-unit decomposition attributes ~€530M to IB hydro (TAMEGA €203M, SIL €103M, DUER €92M, TAJO €90M). The method substitutes each IB hydro plant with a same-tech, same-capacity Fringe plant. **Spanish hydro plants are NOT exchangeable along this match dimension**:

- Storage class varies enormously (large reservoir vs pondage vs run-of-river)
- Ramp speed differs by an order of magnitude
- Marginal water value (Crampes–Moreaux) depends on reservoir level, snow inflow, hydrologic year — none of which are matched
- Geographic location (north Spain Cantabrian basin vs Duero vs Tajo vs Galicia) means different marginal demand exposure

A TAMEGA reservoir-hydro plant matched against a Galician run-of-river Fringe plant compares apples to bananas. A committee will ask: **"How much of the €530M IB-hydro attribution survives stricter matching (same storage class, same hydrologic basin)?"** The answer in the project is currently *unknown*.

The F7 ledger row acknowledges this in a caveat. The caveat is honest but the magnitude of the artifact is not bounded.

**Mitigation needed**: re-run `synthetic_firm_per_unit_ib.py` under restrictive matching (require same `storage_class` + same `hydro_basin` from `lista_unidades.csv` if those fields exist; otherwise require capacity within ±10% AND in same `zone`). Report what fraction of the €530M survives. ~½ day.

**Result of the sensitivity test (`f7_hydro_strict_sensitivity.py`, 2026-04-27)**: **100.0% survival.** Strict criteria applied: (i) split hydro into Reservoir (Hidráulica Generación + Hidráulica de Bombeo Puro) vs Run-of-river (RE Mercado Hidráulica + RE Tar. CUR Hidráulica), matching only within subtype; (ii) K-ratio (capacity_L/capacity_S) ∈ [1/3, 3]. All 7 IB hydro plants matched under strict criteria; per-unit attributions identical to baseline within €0.001M (rounding noise). Reason: IB hydro plants are large (132–1651 MW), and the BASELINE closest-capacity rule already implicitly selected only large-reservoir Fringe matches — small run-of-river Fringe plants (median 2.2 MW) were never the closest-capacity option. The hypothetical 1000-MW-vs-30-MW match concern of A1 was theoretical, not actual. **Status: DEFENDED with data.** The complementary A2 attack (operational-vs-strategic conflation — large-reservoir EDP/Acciona Fringe matches may themselves bid non-competitively) remains independent and unaffected by this test.

### ⚠ A2. (FRAMED 2026-04-27) Operational-vs-strategic conflation — addressed by framing footnote

**Audit attack** (preserved below for record):

The Ciarreta–Espinosa method asks: "what if IB bid like Fringe?" But Fringe hydro plants bid the way they do partly because they HAVE TO — small reservoirs, run-of-river spillage avoidance, no firm storage rights. Fringe hydro bidding €0 may be necessity, not competitive behavior.

So replacing IB hydro with Fringe doesn't isolate "IB strategic markup" — it isolates "IB strategic markup + the difference between IB's operational flexibility and Fringe's operational constraints." These are different counterfactuals. The €820M number is an upper bound on the strategic component, not a point estimate.

**Defense (partial)**: this is a generic feature of the synthetic-firm method, not a bug introduced by us. Ciarreta–Espinosa (2010) cite the same caveat. The two-decade replication of the IB > Endesa pattern strengthens the "strategic" interpretation against pure-portfolio-composition explanations because portfolios differ across periods.

**Mitigation needed**: add an explicit "operational-vs-strategic" footnote to the F7 row. Frame the €820M as "the rent that IB extracts under its operational structure that Fringe could not extract under their operational structure" — which is still economically meaningful, just not "pure strategic markup."

**Result of mitigation (2026-04-27)**: framing footnote added in three places: (i) `CLAIMS_LEDGER.md` F7 row caveat 3 (alongside the A1 sensitivity result); (ii) `_modelling_track.md` §0 caveat for the per-IB-unit finding; (iii) `_coherence_audit.md` new dedicated section "Operational-vs-strategic framing for F7/F8". Standard framing: read the €530M as "rent IB extracts that operationally-comparable large-reservoir European peers do not" (matched Fringes are mostly EDP Portugal large hydros + EHN Acciona — themselves portfolio operators, not pure-fringe benchmarks). Cross-firm direction (IB > matched peers) survives; magnitude has upper-bound interpretation. **Status: addressed by framing.** Cannot be data-defended (no perfectly-competitive benchmark exists); the framing is the right answer.

### ✅ A3. (DEFENDED 2026-04-27) 73% of F8 gap survives the endogeneity correction (French quartile)

**Audit attack** (preserved below for record):

"IB hydro concentrates 63% in Q4 hours" — but Q4 hours are defined using the within-month price distribution that IB partly sets. The F8 row + coherence audit defends this with "cross-firm comparison is robust because IB and Fringe face the same prices." That's true at the price-formation level but not at the dispatch-OPPORTUNITY level: IB has reservoir storage that lets it CHOOSE to dispatch at Q4 hours; Fringe (especially run-of-river) often physically cannot.

So F8's +21pp gap reflects two confounded things:
- IB's strategic decision to dispatch when prices are high (the Bushnell signature we want)
- IB's larger reservoir flexibility making such dispatch possible at all

A non-Bushnell interpretation (operational-asymmetry) generates the same gap. The F8 finding cannot distinguish between them.

**Defense**: the cross-firm gap PRE-MTU15-IDA was already +14pp, then widened to +21pp post-reform. The widening (+7pp) is plausibly the strategic-amplification piece, since operational asymmetries shouldn't widen at a market-design reform.

**Mitigation needed**: re-frame the F8 contribution as "the +7pp INTENSIFICATION post-reform is the cleanest strategic-dispatch evidence; the level (+14 → +21pp) is partly operational-asymmetry."

**Result of mitigation (2026-04-27)**: ran `f8_endogeneity_sensitivity.py` which redefines within-month price quartiles using **French DA price** instead of Spanish — France is climate-correlated with Spain but exogenous to IB (IB doesn't bid in France). Two findings:

| | IB Q4 share | Fringe Q4 share | Gap |
|---|---|---|---|
| Spanish DA quartile (baseline) | 62.4% | 41.7% | **+20.7 pp** |
| French DA quartile (exogenous) | 51.9% | 36.8% | **+15.1 pp** |
| **Survival ratio** | | | **73%** |

Concordance: 60% of Spain Q4 hours coincide with France Q4 hours; 85% are France Q3 or Q4. Spain Q4 is mostly "high European demand hours" (exogenous to IB). Pre→post widening under both references is +5-6pp (cannot be a baseline-level artifact). Reading: ~73% of F8 gap is real strategic dispatch; ~27% reflects IB's own contribution to Spanish Q4 prices. **Even fully corrected, IB shows +15pp gap — clean Bushnell signature.** Status: DEFENDED with data.

### ★★★ A4. F9 mapping ambiguity is severe

F9's headline "IB aFRR share fell 39% → 27%" depends on the LIBERAL mapping IB = {IMA, IGR, IGN}. Under CONSERVATIVE (IB = {IGN} only), IB's share is 0.5–2.4% across all regimes. The ledger and modelling track present this as "qualitatively the same conclusion (IB non-dominant)" — but a finding that ranges from 0.5% to 39% based on a mapping assumption is **not really one finding**.

Worse, the user just confirmed (today's session) that the public ESIOS taxonomy files (Comercializadores, Distribuidores, sujetos-del-mercado) do NOT include the 3-letter BSP codes. So the LIBERAL mapping is not just unverified — it's **unverifiable** without REE settlement-system documentation we don't have.

A committee will ask: "If IMA is not Iberdrola, what is your F9 finding?" The honest answer is "qualitatively the same direction (Fringe rose, Big-4 fell) but the IB-specific story collapses."

**Mitigation needed**: rewrite F9 to lead with the regime-invariant finding ("**Big-4 aFRR share fell from ~70% to ~46%; Fringe rose from 11% to 27%**"), which holds under either mapping. Present the IB-specific number as a sensitivity ("under LIBERAL mapping IB carries 27%; under CONSERVATIVE, 2%; either way the structural fact is that aFRR competitive entry rose post-reform"). This is honest and committee-defensible.

### ★★ A5. No causal identification anywhere in the structural-firm layer

All IB findings (F1–F9) are correlations, comparative statics, or descriptive. The F7 "€820M IB transfer" is a counterfactual calculation under one assumption set, not a causal estimate. There is no instrument, RD, DiD, or natural experiment isolating IB's strategic response to a specific reform.

**Defense**: the project explicitly retired its identification ambitions in `_identification_target.md` D11–D13. The thesis claims are honest about being structural-comparative, not causal. The system-layer claims (S1–S5) are causal-by-design (control-area aggregates with cross-country placebo).

**Framing**: thesis prose should consistently use "consistent with" / "structural reading" / "implied" rather than "caused by" / "identifies" / "establishes that". Already largely done but worth a global pass.

### ★ A6. Vertical-integration ruling out (D5) has a confounder

D5: GE +2,316 GWh net seller; IB +958 GWh; therefore vertical-integration doesn't explain IB > GE market power. But "net seller" position post-Rule-28.8 is itself an outcome of bilateral-contract reallocation — it's not exogenous to the reform package. The 2.4× ratio is post-March-2025, after Rule 28.8 elimination.

**Defense**: Ciarreta–Espinosa (2010) found the same negative result for 2002–2005, when Rule 28.8 was different. Two-decade replication of "vertical-integration doesn't explain" strengthens the conclusion. But the LEVEL of GE's net-seller position in our data is post-reform.

**Framing**: cite the two-decade pattern as the load-bearing evidence; treat the 2.4× ratio as illustrative of one period.

---

## §2 — S6 (€1.1B fiscal cost shift) — second-most exposed

### ★★★ B1. Transfer vs deadweight loss

S6's "+€1,094.9M asymmetric-window net fiscal balance" is BRP → TSO settlement transfer, NOT deadweight loss. Whether it represents welfare cost depends on what TSO does with the surplus:

- If TSO surplus → tariff reduction next year → it's just timing redistribution
- If TSO surplus → reserve-procurement subsidy → it's distortionary
- If TSO surplus → general-budget transfer → it's a tax

The thesis prose calls it "fiscal cost shift" but the welfare interpretation requires a counterfactual we don't have. The S6 ledger row's "fiscal-surplus generator" language is honest but a committee will press: "what's the welfare loss?"

**Defense**: the +€1.1B BRP-to-TSO flow is itself an interesting policy-relevant number — it documents a magnitude of regulatory-induced redistribution that policymakers should care about even if welfare is ambiguous. We can quote this as "BRP → TSO transfer of €X" without claiming it's welfare loss.

**Mitigation needed**: in thesis prose, replace "welfare cost" / "fiscal cost shift" with "BRP-to-TSO settlement transfer" or "regulatory redistribution." Sharper, defensible, no overclaim.

### ✅ B2. (DEFENDED 2026-04-27) Crisis-excluded baseline shifts point estimate <4%; CI narrows

**Audit attack** (preserved below for record):

Calendar-month fixed effects don't account for regime-shifting volatility. The 2022–2023 imbalance prices were anomalously high (gas crisis, Ukraine war). If you include those in the baseline, the post-reform "excess" appears smaller than it would against a "calm-period" baseline; if you exclude them, larger. The current spec includes them implicitly.

**Mitigation needed**: re-run `asymmetric_granularity_welfare.py` excluding 2022-Q1 to 2023-Q4 from the baseline. Report sensitivity. ~30 min.

**Result of the sensitivity test (`s6_baseline_sensitivity.py`, 2026-04-27)**:

| Baseline window | Asymmetric-window excess | 95% CI null | Observed/upper |
|---|---:|---|---:|
| FULL (default, 2018-2024-05) | +€1,094.9 M | [−90, +73] | ~15× |
| EXCL_CRISIS (drop 2022+2023) | +€1,061.6 M | [−47, +38] | ~28× |
| PRE-2022 (2018-2021 only) | +€1,048.2 M | [−33, +30] | ~35× |

Point estimate shifts only 3–4% across specifications. Bootstrap CI **narrows** under cleaner baselines (less heteroskedastic residuals when crisis years are removed), making the observed/null ratio *stronger*. **Status: DEFENDED with data.** S6 conclusion gets sharper under the cleaner baseline, not weaker. The +€1,048–1,095M range survives under any defensible reference window choice.

### ★★ B3. Post-MTU15-DA "collapse" rests on n=3 months (Oct–Dec 2025)

S6 fiscal surplus excess collapses to €7.4M/mo at MTU15-DA. But this is from 3 monthly observations only (Dec even shows −€14.5M). The "collapse" could be:

- Real reversal (granularity-friction story confirms)
- Sample noise (n=3, October-December seasonal anomaly)
- Late-2025 specific shock (e.g. mild weather, low gas prices)

The bootstrap CI is constructed from pre-IDA residuals; with n=3 post-MTU15-DA, the post-MTU15-DA point estimate is itself imprecise.

**Mitigation needed**: in S6 row, add explicit "n=3 months" caveat. Re-evaluate after Q1 2026 data lands. Don't stress the "collapse to €7M/mo" without that caveat.

### ★ B4. Bootstrap null is mis-specified for regime breaks

Bootstrapping pre-IDA residuals to construct the null distribution assumes the underlying DGP is stationary across the pre-IDA period. With the 2022–2023 anomaly inside, residuals are heteroskedastic, and bootstrap CIs likely understate the null distribution. The "15× upper bound" significance is overstated.

**Defense**: even with conservative resampling (e.g. block-bootstrap respecting autocorrelation), the +€1B excess is too large to plausibly be pre-IDA noise.

**Mitigation needed**: report block-bootstrap robustness alongside the i.i.d. bootstrap. ~½ day.

---

## §3 — S8 (RZ activation escalation) — third-most exposed

### ⚠️ C1. (PARTIAL RETRACTION 2026-04-27) S8 demoted to wounded — renewable growth statistically explains most of the elevation

**Audit attack** (preserved below for record):

The S8 ledger row says "Mechanism interpretation (candidate, untested): 6→3 IDA session reduction broke matching of imbalance-settlement granularity to DA dispatch." But the +60-82% RZ activation could be:

- Renewable-share growth (uncontrolled — Spanish solar capacity grew ~12 GW between 2023 and 2025)
- New generation mix (more variable, more redispatch needed structurally)
- REE policy change (post-blackout conservative dispatch heuristics)
- Network expansion delays creating local congestion
- Operación reforzada residue (REE may have permanently raised CCGT commitment thresholds)

The "structural reform-induced" claim is one of many candidates. With no IV or RD design, S8 cannot identify which.

**Mitigation needed**: weaken the S8 ledger language from "structural reform-induced redispatch escalation" to "RZ activation level shifts ~+60% above pre-IDA baseline; mechanism uncertain (candidates: IDA-session reduction, renewable-share growth, post-blackout policy)." Frame it as a fact-without-causal-attribution.

**Result of mitigation (2026-04-27)**: ran `s8_renewable_control.py` regressing monthly RZ activations on regime dummies + cal-month FE + monthly average wind+solar generation. Spanish renewable generation grew **+80% during the pre-IDA window alone** (8 GW Jan-2018 → 14 GW Jun-2024), so the renewable-growth alternative is empirically substantial and not co-incidental.

| Specification | DA60/ID15 β (GWh/mo) | DA15/ID15 β (GWh/mo) | What survives? |
|---|---:|---:|---|
| Spec 1 (regime only) | +125 *** | +96 *** | (baseline) |
| Spec 2 (+ cal-month FE) | +120 *** | +96 *** | matches existing S8 |
| Spec 3 (+ renew_mw control) | **−27 (p=0.61)** | **−43 (p=0.42)** | ISP15-win only (+156, p=0.02) |

**Renewable coefficient is +0.033 GWh-RZ per MW-renew (p<0.001).** The +6 GW of renewable expansion alone implies ~+200 GWh/mo additional RZ — statistically explaining most of the post-IDA elevation we observed. The DA60/ID15 and DA15/ID15 regime effects are not robust to the renewable control.

**S8 demoted from alive to wounded (2026-04-27).** The "post-MTU15-DA persistence" feature (the key signature of the original claim) does NOT survive the renewable control. What survives is a narrower 4-month ISP15-window-specific elevation (+156 GWh/mo, p=0.022) that is not explained by renewable growth alone — possibly reflecting the adjustment period to the new ISP15 settlement clock before MTU15-IDA introduced matching trading granularity. Status: **PARTIAL RETRACTION**. Alive count 28 → 27; wounded count 4 → 5.

### ★★ C2. Cost calc uses published price, but S7 just established that's the wrong number

The S8 row computes "~€10–14M/mo at €60–90/MWh × 157 GWh/mo excess." But S7's anchor cross-check explicitly distinguished published price (~€75/MWh = REE→generator transfer) from social cost (S7 structural figure €210–300/MWh). Using the published price for S8's cost calc is internally INCONSISTENT with the S7 framing.

**Mitigation needed**: either:
- (a) compute S8's cost at S7's social-cost level (€210–300 × 157 GWh = €33–47M/mo, much larger), OR
- (b) frame S8's cost as "redispatch transfers totaling ~€10–14M/mo" without claiming it's welfare cost.
Pick one. Currently the row has it both ways.

### ★ C3. Post-MTU15-DA persistence rests on n=3 months

Same issue as B3 — S8's "key signature" (post-MTU15-DA persistence) has only 3 monthly observations. Could be sample noise.

**Mitigation needed**: same as B3, add n=3 caveat.

---

## §4 — Cross-claim consistency issues

### ★★★ D1. Triple-counting risk in the three-channel synthesis

S6 (BRP → TSO transfer ~€1.1B), S8 (TSO → generator transfer ~€200M cumulative), F7 (generator surplus from market power ~€820M). All three involve money flows in the same financial system across overlapping windows. **Are we counting the same euros multiple times?**

- S6 is BRP-to-TSO at the imbalance-settlement window (per-ISP)
- S8 is TSO-to-generator at the redispatch instruction window (per-ISP)
- F7 is generator-to-buyer at the DA-clearing window (per-hour)

These are different markets with different windows but the SAME underlying generators participate. If a generator's RZ-redispatch revenue (S8) is partly enabled by their DA market power (F7), counting both as "reform impact" double-counts.

**Defense**: the three claims operate at different layers — S6 is BRP-side, S8 is reserve-side, F7 is generator-side rent. Total system impact is genuinely the sum of these three flows in the absence of perfect substitutability. But the thesis prose should be careful not to add them without acknowledgment.

**Mitigation needed**: add an explicit "non-additive across channels" footnote to the three-channel synthesis paragraph in `_coherence_audit.md` and `_modelling_track.md`. Total system impact ≤ sum of channels.

### ★★ D2. The pdbce BRP-routing reallocation contaminates Big-4 categorisation

Per nb12's data caveat: `grupo_empresarial` is the *settlement BRP*, not the physical owner. Rule 28.8 elimination (March 2025) reallocated bilateral-contract settlement flows. So the "GE", "IB" groupings shift across regimes due to contract structure, not just behaviour. F7's per-firm decomposition uses these BRP-level groupings for the post-MTU15-IDA period (post-Rule-28.8) — so "IB carries €820M" reflects IB's **post-Rule-28.8 BRP scope**, not its physical-asset scope.

**Defense**: the F7 method substitutes IB's BIDS with Fringe BIDS at the unit level. So the analysis is bid-level, not BRP-level. The grupo_empresarial categorisation enters only at the per-firm aggregation stage.

**Mitigation needed**: verify in `synthetic_firm_per_firm.py` that the per-firm aggregation uses physical-unit-to-firm mapping, not the BRP-shifting categorisation. ~30 min check.

### ★ D3. F7 vs B8 internal tension

B8 says IB CCGT units complexify bids (5.49 → 8.73 tranches/period). F7 per-IB-unit says those exact units (TAPOWER, ARCOS1, CTN3, CTN4) have ~€8M total price impact. The coherence audit reconciles this as "bid-structure investment ≠ price-setting power." OK — but then what IS B8 evidence of?

If complexification doesn't translate to price-setting, B8 is a curious behavioural artifact without a clear strategic interpretation. A committee will ask: "Why is IB investing in finer bid ladders that don't move prices?"

**Mitigation needed**: either (a) demote B8 to "behavioural curiosity, mechanism unclear" — honest; or (b) propose a mechanism (e.g. "IB hedges against future-clearing-price uncertainty by spreading offers across many price points; the median tranche doesn't clear, but the optionality has value") — speculative.

---

## §5 — Methodology / general

### ⚠ E1. (FRAMED 2026-04-27) Replaced with "method-replication across periods, pattern persistence" framing

**Audit attack** (preserved below for record):

The thesis cites Ciarreta–Espinosa 2010 finding (IB > Endesa, 2002–2005, with vertical-integration ruled out) as supporting evidence for our 2024–2026 finding. But we use the SAME synthetic-firm method they used. So the "two-decade replication" is the same method applied to a different period — not an independent confirmation. If the method has biases (e.g. operational-vs-strategic conflation per A2), both periods carry the same bias.

**Mitigation needed**: in thesis prose, frame the Ciarreta–Espinosa parallel as "method validation across periods" rather than "independent confirmation." More modest, more accurate.

**Result of mitigation (2026-04-27)**: framing updated in CLAIMS_LEDGER F7 row, `_modelling_track.md` §0, and `_coherence_audit.md` (two paragraphs). Standard wording: "two-decade pattern persistence" or "method-replication across periods" — explicit that the same estimator family produces the same direction in different data, NOT independent confirmation. The cross-firm direction (IB > Endesa) is what survives; the mechanism story differs (CTC stranded-cost regulation in 2002–2005 vs hydro-portfolio + asymmetric-granularity in 2024–2026). **Status: FRAMED.**

### ★ E2. aFRR market design vs DA market design are not symmetric

F9's "DA toward concentration, aFRR toward competition" cross-market contrast presents the divergence as IB-specific behaviour. But aFRR is a capacity-procurement auction with explicit floors and TSO-managed competitive entry; DA is a uniform-price two-sided clearing market. The two markets respond to ANY shock differently because they have different market designs. A committee will ask: "Is the DA-vs-aFRR divergence a finding about IB or a finding about market mechanism?"

**Defense**: the divergence happened at the same reform date (2024-06 IDA) which affected both markets identically. If it were a market-mechanism story, both should respond similarly to the same shock; they didn't. So the divergence IS IB-specific in the sense that it appeared at a reform that didn't differentially favor one market design.

**Framing**: be explicit about this defense in F9's ledger row and in the thesis cross-market section.

### ★ E3. 7-month asymmetric-granularity window is short

S6 is detected in only ~7 months of DA60/ID15 + ~4 months of ISP15-win. Statistical power is limited. Bootstrap CIs help but don't resolve the underlying short-window concern.

**Defense**: the regime windows are exogenous (set by REE/CNMC) and short by design — we can't get more data without waiting. The point estimates are large and consistent across multiple outcomes (S1–S5 four-way concordance).

**Framing**: explicit "short-window" disclaimer in S6 prose.

---

## §6 — Priority remediation list (constructive)

In order of expected committee-defense value:

1. **Re-frame F9 around Big-4-share-fell** (independent of IMA mapping). ~30 min text change. **Highest ROI.**
2. **Re-frame S8 cost calc** to be consistent with S7 social-vs-published distinction. Pick one framing. ~30 min.
3. **Re-frame S6 prose** to use "BRP→TSO transfer" instead of "welfare cost" / "fiscal cost shift". ~30 min.
4. **Add n=3 caveats** to both S6 and S8 post-MTU15-DA findings. ~10 min.
5. **Add non-additive footnote** to three-channel synthesis. ~10 min.
6. **Verify** F7 per-firm aggregation uses physical-unit mapping, not Rule-28.8-affected BRP categorisation. ~30 min.
7. **Sensitivity-test F7 hydro attribution** under stricter matching (basin + storage class). ~½ day.
8. **Sensitivity-test S6 baseline** excluding 2022–2023 energy-crisis years. ~30 min.
9. **Frame Ciarreta–Espinosa parallel** as "method validation" not "independent replication". ~10 min.
10. **Add operational-vs-strategic footnote** to F7. ~15 min.

**Items 1–6 are ledger / framing fixes**, low time, high committee-defense value. 
**Items 7–8 are sensitivity analyses**, slightly more effort, would materially strengthen the project against the strongest attacks.
**Items 9–10 are framing tweaks** in the thesis prose.

---

## §7 — Claims that mostly survive the red-team

For balance, the following alive claims are robust to the attacks I can construct:

- **S5** (four-way ENTSO-E concordance) — system-aggregate, no firm assumption; cross-country placebo (B7) rules out a generic cross-country shock.
- **B6** (forecast-error → imbalance pass-through R² jump) — direct measurement, no IB attribution.
- **B7** (France placebo) — placebo by construction; the strongest identification piece in the project.
- **D1, D2** (within-month price dispersion; non-strategic 15-min bid replication) — descriptive, well-defined.
- **D4** (Fringe extensive-margin exit at MTU15-IDA) — direct count of exiting units; unambiguous.

These don't need defensive prose. The thesis can lead with them as the cleanest pieces of evidence and use F7/F8/F9 as the structural-firm interpretive layer with appropriate caveats.

---

## What this audit is NOT

- Not a recommendation to drop any alive claim. None of these attacks is fatal; all are addressable through framing or sensitivity.
- Not exhaustive — these are the attacks I can construct in one pass. A committee may have others.
- Not a pre-thesis-writeup checklist — it's a prep document for thesis prose decisions.

The biggest single risk is **F9's mapping ambiguity (A4)** because it's the cleanest cross-market piece in the thesis and a hostile committee will press hardest there. The §6 mitigation (re-frame around Big-4-share-fell) defangs this attack with no loss of substance.

The biggest single methodological risk is **F7 hydro plant-pair matching (A1)** because the €530M IB-hydro attribution is currently unbounded against matching artifact. The §6 sensitivity test would convert that to a defensible interval.
