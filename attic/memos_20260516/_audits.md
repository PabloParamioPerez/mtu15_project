# Audits — coherence + red-team (2026-04-27)

Two complementary audit passes over the alive ledger, both run 2026-04-27. Combined here for navigation; originally lived as `_coherence_audit.md` and `_red_team_audit.md`.

- **Part A — Coherence audit**: do the ~26–28 alive claims tell ONE consistent story? Identifies tensions across system / structural-firm / behavioural / descriptive layers and proposes resolutions. Working note for thesis-drafting; not a frozen reference.
- **Part B — Red-team audit**: adversarial attacks on each alive claim, ranked by severity (★★★ = potentially load-bearing, ★★ = serious caveat, ★ = needs framing). Each attack has either a "Defense" line (project can defend) or "Mitigation needed" line (analysis or ledger needs to change).

The two views are complementary. Coherence asks "do the findings agree with each other?"; red-team asks "does each finding survive a hostile reader?"

---

# Part A — Coherence audit of alive claims

26 alive claims spread across system / structural-firm / behavioural / descriptive layers. This note checks they tell ONE consistent story, identifies tensions, and proposes resolutions. Working note for thesis-drafting; not a frozen reference document.

---

## Layer-by-layer reading

### System layer (S1–S7) — uncontested, mutually reinforcing

- **S1–S5** are the four-way ENTSO-E concordance (A87 net income, A86 volumes, A85 prices σ, A84 reserve spread). All jump at ISP15, moderate at MTU15-DA. S5 is the joint statement.
- **S6** integrates S1 over time: cumulative excess +€1.095B in the 10-month asymmetric window (ISP15-win + DA60/ID15) net of A01 reserve costs. Bootstrap CI [-90, +73]M; observed ≈15× upper bound.
- **S7** is structurally about the *settlement rule*, not the reform: per-segment marginal imbalance cost is 5–15× heterogeneous (conv-RZ €210–300/MWh vs LIB free retailers ≤€37/MWh). Survives FE controls.

**Coherence:** clean. S1–S6 are nested; S7 is independent (rule misalignment). No tensions.

### Structural-firm layer (F1–F8) — coherent after one reframing

- **F1 (GE +0.318), F2 (IB +0.135), F3 (partial reversal at MTU15-DA), F4 (GN/HC dominated by Rule 28.8)** — all from the same Spec 3 matched-price Lerner regression.
- **F5** — Allaz–Vila slope, peak vs off-peak split, IB sign-flip / GE within-firm placebo.
- **F6** — Cournot tercile sort, IB cleanly fits, GE mixed.
- **F7** — Synthetic-firm method (Ciarreta–Espinosa style), joint Big-4 ~€833M; per-firm: IB ALONE carries ~€820M; per-IB-unit: hydro 64% / CCGT 36%; named B8 CCGT complex-bidders carry ~0.
- **F8** — Bushnell-signature direct test: IB hydro Q4-concentrated structurally more than Fringe; gap +17pp (avg 2018-2025), persistent across years. Original "intensifies at MTU15-IDA (56→63%)" framing partially retracted 2026-04-27 — was a windowing artifact; year-by-year analysis shows the gap is structurally stable, NOT reform-amplified.

**Apparent tension #1: F1 says GE Lerner +0.318; F7 per-firm says GE has near-zero independent price impact.**

Resolution: these measure *different* objects.
- **F1** measures GE's *implied Cournot Lerner* given the market clearing price (= q_GE / (p* × (1−s_GE) × |∂S/∂p|)). When the market clearing price is high, GE's Lerner index is mechanically high, regardless of who set the price.
- **F7** measures GE's *independent contribution to setting the price* (replacing GE's offers with Fringe matches barely changes clearing).

So **F1 is "GE benefits from high prices"; F7 is "GE doesn't set them"**. Together they say: *the high prices in DA60/ID15 are real and GE collects rents on them, but GE's own bidding is not driving the elevation*. This is structurally important and must be stated explicitly in the thesis.

**Apparent tension #2: F5 was framed as Allaz–Vila / CCGT-margin; F7 says IB's mechanism is hydro-dominated.**

Resolution: F5's "peak hours (h11–22)" is a CCGT-OR-hydro-margin proxy, since both run at peak demand. Allaz–Vila applies to any dispatchable forward sales — CCGT, hydro, mixed. The original F5 framing privileged CCGT because that was the canonical Allaz–Vila context. With the F7/F8 hydro pivot, **F5's mechanism interpretation should read "dispatchable-marginal-capacity commitment slope" rather than "CCGT-margin Allaz–Vila"**. The empirical finding (IB peak sign-flip; GE peak-vs-off-peak going opposite directions) is unchanged; only the tech attribution shifts.

**Apparent tension #3: F6 (IB cleanly fits Cournot tercile) + F7 hydro pivot — does Cournot still apply?**

Resolution: yes. The Cournot tercile test sorts on the *aggregate market supply slope* (not on tech). It tests whether IB's matched-price Lerner is concentrated in steep-supply hours, regardless of whether IB's marginal capacity is CCGT or hydro. **F6 confirms IB fits the Cournot prediction; F7/F8 specify that the underlying marginal capacity is hydro.** Cournot is a *quantity-setting* prediction, not a CCGT-specific one. The two findings are complementary, not contradictory.

**Apparent tension #4: D5 says GE is 2.4× more net-seller than IB, yet IB has more market power.**

Resolution: net-seller volume ≠ marginal capacity. GE clears more MWh because of nuclear baseload (price-takers). IB has more *installed* capacity (18.5 vs 13.4 GW) and especially more *dispatchable* capacity (hydro 6.5 vs 3.8 GW). Cournot market power flows from marginal capacity, not from total cleared volume. **F8 confirms: IB hydro is concentrated in high-price hours (strategic dispatch), GE hydro is spread evenly (run-of-river-like).** Internally consistent with the IB-larger-by-marginal-capacity interpretation.

### Behavioural layer (B1–B8) — coherent with one explicit caveat

- **B1, B2, B3, B4, B5, B7** — descriptive bid-shading, IDA collapse, XBID liquidity, France placebo, Rule 28.8 revenue reallocation. Standalone observations.
- **B6** — Forecast-error → imbalance pass-through R² jumps in DA60/ID15. Anchors the asymmetric-granularity story (§4 of modelling track).
- **B8** — IB CCGT bid complexification: tranches-per-period 5.49 → 8.73. *Specifically* on TAPOWER / ARCOS1 / CTN3 / CTN4 + IB CCGT units in aggregate.

**Apparent tension #5: B8 says IB CCGT complexifies bids; F7 per-IB-unit says those exact units have ~0 independent price impact.**

Resolution: bid *complexity* (number of price-quantity points per ISP) and price-*setting power* (how far above the synthetic-Fringe baseline the firm pushes the clearing price) are **different objects**. A unit can submit a fine 9-tranche price ladder that all sits well above the marginal price step → high B8 complexity score, ~zero F7 price impact. Both findings are true. **The thesis should clearly state**: B8 documents bid-structure investment (real, IB-specific); F7 per-IB-unit shows where price-setting power flows (mostly hydro, not the CCGT complex-bidders). They are complementary observations about IB's strategic toolkit, not a contradiction.

### Descriptive layer (D1–D5) — clean, no tensions

- **D1, D2** are descriptive within-month dispersion / non-strategic bid replication.
- **D3** — HHI rises 0.28 → 0.42+ post-reform.
- **D4** — Fringe exit at MTU15-IDA (~5 GW of small Fringe units exit DA).
- **D5** — net-seller positions; GE > IB; vertical integration ruled out.

All consistent. D5 + F8 jointly tell the IB-larger-by-marginal-capacity story.

---

## Cross-layer coherence checks

### S6 vs F7: are these the same money?

No. **Different markets, different rents.**

- **S6 = €1.095B** is the BRP→TSO net imbalance settlement transfer (A87 NET = A02 income − A01 expenses) over the 10-month asymmetric window. This is the *imbalance market*: BRPs collectively pay more for their imbalances because (a) imbalance volumes are mechanically larger and (b) imbalance prices are more dispersed.
- **F7 = €833M** is the DA *auction* cleared-price-difference: Big-4 sellers extract above-Fringe-benchmark rents from electricity buyers via DA market power.

These coexist without double-counting. Total Big-4 + asymmetric-granularity rent in the post-IDA window ≈ €1.9B = **~€1.65B/year** annualized. Spanish wholesale revenue is ~€20–30B/year, so this is **5–8% of wholesale revenue**. Plausible by international comparison (Wolfram 1999 found 25% UK markup; Borenstein–Bushnell–Wolak 2002 found 59% of California crisis-period price increase from market power).

### F2 (IB Lerner +0.135) vs F7 IB transfer +€820M: do magnitudes line up?

Approximate check. F7 IB transfer: +€8.80/MWh average over post-MTU15-IDA. Mean post-MTU15-IDA price is ~€57–78/MWh. So IB's price-setting power = ~12–15% of price.

F2: IB Lerner +0.135 above pre-IDA (where pre-IDA was ~0.05). Post-reform IB Lerner ≈ 0.18, conditional on price level.

These aren't measuring the same thing (F2 is conditional Lerner index controlling for price; F7 is unconditional cleared-price difference vs Fringe baseline). But **both numbers are in the 10–15% range**, broadly consistent. No magnitude inconsistency.

### F5 + F8 + F7: does the mechanism story hold?

The IB-canonical mechanism story:

1. IB has the largest dispatchable-marginal capacity (hydro + CCGT) among Big-4.
2. IB strategically concentrates hydro dispatch in high-price hours (F8: 63% Q4 vs 42% Fringe; intensifies post-MTU15-IDA).
3. This raises the clearing price ~€8.80/MWh on average (F7 per-firm, IB-only attribution).
4. The Allaz–Vila commitment-slope evolution (F5) is consistent with finer IDA granularity reducing forward-commitment value, increasing intra-day arbitrage incentive (which IB exploits via its dispatchable hydro + CCGT).
5. F1 (GE Lerner +0.318) is therefore *not* GE setting prices; it's GE collecting rents on prices IB sets.
6. The €820M IB transfer = ~€820M / 14 months ≈ €58M/month rent — matched in magnitude by Bushnell 2003 / Reguant 2014 / Wolfram 1999 international comparators.

This is internally consistent and matches the cited literature. **The thesis claim is defensible.**

---

## What a thesis committee would flag

1. **F2 magnitude (+0.135 IB Lerner) vs F7 IB attribution share (98% of joint Big-4)** — F2's "IB elevation" is much smaller than GE's (+0.318), but F7 says IB carries 98% of price-setting. **The thesis must explain why**: F2 measures *implied* market power conditional on price level (high for GE because GE's nuclear share is large); F7 measures *contribution* to setting price (low for GE because GE's plants are mostly inframarginal). This is structurally important and a likely viva question.

2. **No independent identification of IB's strategic intent** — all our IB findings are correlations (high market power, strategic dispatch concentration, complex bidding, sign-flip slope). We don't have an instrument or a cost-shock that would let us identify IB's *response* to a specific incentive. The thesis should be honest about this — the findings are *consistent with* strategic conduct but don't *identify* it causally.

3. **Hydro plant-pair matching imperfections** — F7's hydro attributions (TAMEGA €203M, etc.) rest on hydro plant-matching that's harder than CCGT (storage / reservoir / ramp differ across plants). F8 is matching-free; F7's per-unit hydro numbers should be cited with the matching caveat. This is documented in the F7 row but easy to forget in writeup.

4. **The "structural" label for F1/F2** — Hortaçsu–Puller call this a *reduced-form* approach (no marginal cost data). Calling F1/F2 "structural Lerner" is over-claiming. The thesis should use "implied Lerner" or "Hortaçsu–Puller-style Lerner".

5. **F8 within-month price quartiles are endogenous** — IB's own dispatch decisions partly *set* the prices that we then quartile-rank. The cross-firm comparison (IB 63% vs Fringe 42%) is robust to this because both face the same prices, but a viva committee will ask. The defense: the comparison is between firms at the *same* hourly prices, so the cross-firm gap is firm-specific behavior, not a price-level artifact.

---

## Suggested thesis claim (final, post-audit + 2026-04-27 question on pre-reform)

**Important framing correction.** Earlier drafts of the thesis claim said IB is the structural price-setter "post-MTU15-IDA." This wording can be read as "the reform created IB's market power." The data does not support that — F8 pre-MTU15-IDA shows IB hydro was already 56% Q4-concentrated vs Fringe 42% (a +14 pp gap). The reform pushed the gap to +21 pp. **IB was always the price-setter; the reform intensified the strategic value of dispatch concentration.** This is the correct framing.

Cross-decade replication: Ciarreta–Espinosa 2010 found IB > EN in market power for 2002–2005 (their explanation: CTC regulation). We find IB > GE in 2024–2026 (our explanation: portfolio composition + strategic hydro dispatch). Different mechanisms across periods, same cross-firm pattern.

> Iberdrola — the larger of the two Big-4 dominant firms by installed capacity (~18.5 GW vs ~13.4 GW for Endesa) — has been the dominant marginal price-setter in the Spanish day-ahead market across at least two decades. Ciarreta–Espinosa (2010) document the same cross-firm pattern (IB > Endesa in implied market power) for 2002–2005 under CTC-regulation incentives. We document the pattern persisting in 2024–2026 under a different mechanism: dispatchable-marginal-capacity portfolio (hydro + CCGT) and strategic dispatch concentration in high-price hours (Bushnell-style; +14 percentage-point Q4-share gap vs Fringe hydro pre-reform, intensifying to +21 percentage-point gap post-MTU15-IDA — a 50% relative intensification of the strategic-dispatch signature). Iberdrola's total cleared-price-difference transfer post-MTU15-IDA is approximately €820M over 14 months. The MTU15-IDA reform does NOT create Iberdrola's price-setting power; the +14 pp baseline gap shows it pre-existed. The reform amplifies the strategic value of dispatch concentration in the asymmetric-granularity window, intensifying the rent extraction. The reform-attributable increment to the €820M transfer cannot be measured directly with the synthetic-firm method (pre-reform bid data is parser-artefact-padded), but the F8 evidence suggests the increment is a fraction of the total — perhaps on the order of a few hundred million, not the full €820M. Endesa's larger cleared volume reflects nuclear baseload (price-taking), not strategic-marginal presence; its elevated Hortaçsu–Puller-style implied Lerner reads as rent collection on prices Iberdrola sets, not as price-setting itself. Vertical integration is ruled out as the explanation for cross-firm market-power heterogeneity (Endesa is 2.4× more net-seller than Iberdrola), replicating the same negative result Ciarreta–Espinosa found for 2002–2005. The system-level reform impact, separately, is approximately €1.1 billion in fiscal-surplus net imbalance settlement collected by the TSO from BRPs in the asymmetric-granularity window — a separate rent in the imbalance market on top of the day-ahead-market rent. The mechanism story for the day-ahead-market rent triangulates across three independent IO methodologies: Hortaçsu–Puller-style implied Lerner (F1/F2 with F6 Cournot tercile validation), Allaz–Vila commitment-value slope evolution (F5 with within-firm placebo on Endesa), and Ciarreta–Espinosa synthetic-firm price-substitution (F7 per-IB-unit) — combined with a direct Bushnell-style strategic-dispatch test (F8). The CCGT bid-complexification finding (B8) is a real bid-structure observation specific to Iberdrola but does not translate to clearing-price impact for the named complex-bidder units.

This is publishable-Master's-quality. Identifications have honest caveats; the IO mechanism is tied to specific literature (Bushnell, Hortaçsu–Puller, Ciarreta–Espinosa, Allaz–Vila); the magnitude numbers are plausible by international comparison; the cross-firm story has both a positive (IB carries it) and a negative (vertical integration doesn't) result. Two-decade persistence of the IB > Endesa cross-firm pattern across different regulatory regimes (CTC stranded-cost period 2002–2005 vs MTU15 reform sequence 2024–2026) is itself a publishable historical-IO contribution. **Framing note (2026-04-27)**: this is pattern-persistence across periods, not independent causal replication — the F1/F2/F7 estimators we use are the same ones Ciarreta–Espinosa used (Hortaçsu–Puller-style implied Lerner, Ciarreta–Espinosa synthetic-firm). What survives across two decades is the DIRECTION of cross-firm asymmetry; the mechanism story differs (their CTC explanation vs our hydro-portfolio explanation).

---

## Clean-ups to do before thesis writing

1. Update F5 row in CLAIMS_LEDGER.md: reframe interpretation from "CCGT-margin commitment" to "dispatchable-marginal-capacity commitment" given F7/F8 hydro pivot.
2. Update B8 row: add explicit caveat that the named CCGT units' bid complexification does not translate to clearing-price-setting per F7.
3. Update F1/F2 wording: replace "structural Lerner" with "implied Lerner" or "Hortaçsu–Puller-style Lerner" to avoid overclaiming.
4. Decide whether F1 should be reframed as "rent-collection" rather than "market-power" given the F7 GE near-zero finding.

These are micro-edits. The substantive coherence is solid.

---

## Operational-vs-strategic framing for F7/F8 (audit attack A2, added 2026-04-27)

A natural committee question on F7's IB-hydro €530M attribution: "the synthetic-firm method substitutes IB plants with Fringe matches and reads the price difference as IB's strategic markup. But are the matched Fringe plants pure price-takers?" The honest answer is **no** — the matched plants are mostly large Portuguese EDP reservoir hydros (ADOURO, ALIMA, ACAVADO) plus EHN Acciona's large reservoir ACC2EBR. These are themselves portfolio operators with their own operational constraints (water-availability cycles, Portuguese MIBEL-side dispatch priorities, cross-border interconnection limits). Their bids reflect a mix of operational necessity and possibly some strategic optimisation of their own — they are not "perfectly competitive benchmarks."

**Implication for the F7 €530M IB-hydro number**: it should be read as **"the rent IB extracts that operationally-comparable large-reservoir European peers do not"** rather than **"pure strategic markup against a perfectly competitive benchmark"**. The cross-firm direction (IB > matched Fringe) survives this framing — comparable-size, comparable-storage-class peers do not push prices the way IB does. The magnitude carries an upper-bound flavour: pure strategic Cournot benchmark would likely give a different (probably smaller) number.

**Implication for F8 Q4-dispatch concentration (+21pp gap)**: similarly, the +14 pp pre-reform gap and +7 pp post-MTU15-IDA intensification reflect IB's strategic dispatch given its operational flexibility, not the choice between "perfectly competitive Fringe" and "strategic monopolist." The +7 pp post-reform intensification is the cleanest piece of evidence for strategic-amplification (see audit A3 defense above).

This framing should be reflected in any thesis-prose write-up of F7 and F8: cite the cross-firm asymmetry direction as the load-bearing finding; treat magnitudes as upper-bound estimates of pure strategic markup.

---

## Blackout-confound check (added 2026-04-27)

User asked: "in the asymmetric-granularity window are you considering the blackout? It is key." The 2025-04-28 Iberian blackout triggered REE "operación reforzada" (forced increased CCGT/nuclear commitment), which is a confound for any DA60/ID15-window claim because ~5 of the 6 DA60/ID15 months are post-blackout. `scripts/analysis/synthetic/blackout_split.py` partitions F7/F8/S6 across PRE-blackout DA60/ID15 (~6 weeks), POST-blackout DA60/ID15 (~5 months), and DA15/ID15 (post-MTU15-DA, ~3.5 months).

### Findings

**F8 fully robust.** IB hydro Q4 share by era: PRE-blackout 63.1% (gap +20.4pp), POST-blackout 63.6% (+21.8pp), post-MTU15-DA 67.2% (+22.4pp). Bushnell-style strategic dispatch is regime-invariant. Neither the reform nor the blackout creates the Q4 concentration; both leave it essentially unchanged.

**F7 reframed.** Per-IB transfer by era: PRE-blackout ~€38M total at **+48% relative markup on €12.51 mean prices** (highest rel-markup of any era — cleanest expression of price-setting power per unit price); POST-blackout DA60/ID15 ~€184M at +11% rel markup on €68.42 (rent inflated by high prices, not by widened markup); **DA15/ID15 ~€598M at +12.3% rel markup on €77.90 — DA15/ID15 alone accounts for 73% of total IB transfer**. Original §0 framing — "the asymmetric-granularity window generates the rent" — is **partially retracted**: in absolute € terms, DA15/ID15 dominates; in *relative-markup* terms, the clean PRE-blackout window peaks. The €820M is a regime-weighted absolute total dominated by post-MTU15-DA price levels, NOT a pure asymmetric-granularity rent.

**S6 robust + reframed positive.** A87 NET fiscal surplus excess vs same-calendar baseline: April 2025 (clean PRE-blackout) +€75.7M for one month; May–Sep (post-blackout DA60/ID15) +€467.6M total / €93.5M mean per month (+24% above clean April — modest blackout amplification, not source); **Oct–Dec (DA15/ID15, post-MTU15-DA, still post-blackout) +€22.2M total / €7.4M mean per month — only 8% of the DA60/ID15 level**. The post-MTU15-DA collapse is the key signature: when granularity asymmetry is removed, the system-cost shift evaporates **even though the blackout/operación-reforzada is still in effect**. This is a clean separation: at the system layer (S6), the granularity-friction effect dominates; the blackout effect is small and modulating.

### Revised two-channel thesis claim

The Spanish reform produced two distinct, separable effects that the thesis must keep distinct:

1. **System-layer fiscal cost shift (S6).** Asymmetric granularity (DA60/ID15) generated ~€90–95M/month in BRP→TSO net imbalance settlement transfer above the same-calendar pre-IDA baseline. The clean April-2025 (PRE-blackout) figure (€75.7M) demonstrates the effect is real even before operación reforzada. Once granularity re-symmetrises at MTU15-DA, the excess collapses to €7.4M/month (8% of asymmetric-window level) **even though the blackout is still in effect**. This is the cleanest welfare-relevant granularity-friction signature in the project.

2. **Firm-layer market-power rent (F7/F8).** IB's strategic-dispatch market power is **regime-invariant**: F8 Q4 concentration is essentially identical pre-blackout, post-blackout DA60/ID15, and post-MTU15-DA. The reform did not create IB's market power, and the blackout did not amplify it. The €820M cleared-price-difference transfer is a regime-weighted absolute total dominated by DA15/ID15 (73%) where prices are highest — **the bulk of IB's rent is realised post-MTU15-DA, in absolute € terms, while the clean DA60/ID15 PRE-blackout window has the highest *relative* markup (48%)**. Reading: IB is the structural price-setter across all post-MTU15-IDA regimes; the asymmetric-granularity window is associated with the highest *relative* markup but not the highest *absolute* rent.

The original "the asymmetric-granularity window creates ~€2 billion in rent" reading is **superseded** by this two-channel view:
- ~€1.1B at the system layer in the asymmetric window (S6, clean granularity-friction signal)
- ~€820M at the firm layer (F7) — but distributed across regimes, NOT concentrated in the asymmetric window in absolute terms

### Updated suggested thesis claim

The §0 / final thesis-claim paragraph above (lines 116–122) should be revised to:

> Iberdrola — the larger of the two Big-4 dominant firms by installed capacity (~18.5 GW vs ~13.4 GW for Endesa) — has been the dominant marginal price-setter in the Spanish day-ahead market across at least two decades, with **regime-invariant** strategic-dispatch concentration of its hydro fleet (Bushnell-style, F8: ~63% top-quartile dispatch vs ~42% Fringe across PRE-blackout, POST-blackout, and post-MTU15-DA). The MTU15-IDA reform did NOT create Iberdrola's price-setting power; the +14 pp pre-reform Q4-gap shows it pre-existed (Ciarreta–Espinosa 2010 documented the same IB > Endesa cross-firm pattern for 2002–2005). The reform's effect partitions into two channels: (i) at the **system layer**, asymmetric granularity (DA60/ID15) generated ~€90–95M/month in BRP→TSO net imbalance fiscal cost shift, with the post-MTU15-DA collapse to ~€7M/month (8% of the asymmetric-window level) demonstrating clean granularity-friction welfare loss; (ii) at the **firm layer**, Iberdrola's cleared-price-difference rent realises in absolute terms most strongly under high-price post-MTU15-DA conditions (~73% of the €820M post-IDA total), while the *relative* markup peaks in the clean DA60/ID15 PRE-blackout window (+48% on low €12.51 prices vs +12.3% on €77.90 post-MTU15-DA). The 2025-04-28 Iberian blackout and subsequent "operación reforzada" period is a modest amplifier (post-blackout DA60/ID15 fiscal-surplus mean is +24% above the clean April-2025 figure) but not the source of either channel. Endesa's larger cleared volume reflects nuclear baseload (price-taking), not strategic-marginal presence; its elevated Hortaçsu–Puller-style implied Lerner reads as rent collection on prices Iberdrola sets, not as price-setting itself. Vertical integration is ruled out (D5: Endesa is 2.4× more net-seller than Iberdrola, the opposite direction). The mechanism story for the day-ahead-market firm-layer rent triangulates across three IO methodologies (Hortaçsu–Puller implied Lerner with F6 Cournot tercile validation; Allaz–Vila commitment-value with F5 within-firm placebo on Endesa; Ciarreta–Espinosa synthetic-firm with per-IB-unit hydro pivot in F7) anchored by a direct Bushnell-style F8 dispatch test. The CCGT bid-complexification finding (B8) is a real bid-structure observation specific to Iberdrola but does not translate to clearing-price impact for the named units.

This revised claim is **defensive against viva**: every regime-attribution number is explicitly decomposed PRE-blackout / POST-blackout / post-MTU15-DA, every channel has a clean separation argument, and the magnitude numbers are not double-counted between system and firm layers.

---

## F9 + S8 additions (2026-04-27)

After publishing the post-blackout claim above, two more findings landed from the ESIOS taxonomy expansion. Both are coherent with — and strengthen — the existing reading.

### F9 (alive structural-firm) — IB-dominance is DA-market-specific

ESIOS `liquicierre`/`liquicierresrs` (2015-now per-BSP aFRR settlement, ~23 BSPs at PT15M) lets us decompose secondary-regulation provision per firm. Under LIBERAL mapping (IB = {IMA, IGR, IGN}, with IMA dominating at ~128 GWh/post-MTU15-DA-day), IB's aFRR share trajectory: **31.8% pre-IDA → 39.1% peak in 3-sess → 32.9% ISP15-win → 27.1% DA60/ID15 → 26.7% DA15/ID15**. Fringe rose 11% → 27% over the same period.

This is the **opposite direction from F7** (where IB ≈ 98% of Big-4 DA-market transfer is regime-invariant). The aFRR market is *becoming more competitive* over the reform period.

**Coherence reading:** F7's IB-dominance claim is **DA-market-specific**, not generic. F9 strengthens F7 by ruling out broad-firm-dominance interpretations: IB is dominant in DA where its hydro+CCGT marginal capacity is the price-setter (per F8 strategic dispatch), but does NOT extend that dominance to aFRR procurement where REE explicitly diversifies and qualified-provider entry has been growing.

**Caveat for viva**: BSP↔firm mapping is not authoritatively published; LIBERAL mapping rests on inference from magnitude + OMIE prefix-matching (IGN ↔ IGNU = Iberdrola Generación Nuclear is exact; IMA / IGR are pattern-fit). CONSERVATIVE mapping (IB = {IGN} only) gives IB 0.5–2.4% across all regimes — qualitatively the same conclusion (aFRR is *not* IB-dominant).

### S8 (alive system-layer) — RZ activation escalation persists post-MTU15-DA

Per-month RZ system-security activations (TipoRedespacho 61 in `totalrp48preccierre`) vs same-calendar pre-IDA baseline:

| Regime | RZ activations (GWh/mo) | Excess vs baseline |
|---|---|---|
| pre-IDA (114 mo) | 269.5 | (baseline) |
| 3-sess (6 mo) | 485.3 | **+82%** |
| ISP15-win (4 mo) | 502.4 | **+80%** |
| DA60/ID15 (7 mo) | 427.8 | **+60%** |
| **DA15/ID15** (3 mo) | **414.2** | **+61%** |

Bootstrap null CI [-93, +110] GWh/mo — all post-IDA regimes 1.4–2.0× above the upper bound.

**The post-MTU15-DA persistence is the decisive feature.** S6 (asymmetric-granularity fiscal cost shift) collapsed at MTU15-DA from €94M/mo to €7M/mo. S8 (RZ redispatch volume) STAYS elevated at +60% in DA15/ID15. Neither granularity friction nor the blackout explains S8: this is a **structural reform-induced redispatch escalation**.

**Direct cost**: at regime-mean RZ closure prices €60–90/MWh × +157 GWh/mo excess, ~€10–14M/month direct redispatch cost above pre-IDA baseline; ~€200–280M cumulative across the 20-month post-IDA window.

**Coherence reading:** S8 adds a third channel to the two-channel synthesis. The Spanish reform produced THREE distinct operational/welfare effects:

1. **System-layer fiscal cost shift (S6)**: ~€90–95M/month in asymmetric-granularity window, **collapses at MTU15-DA**. Granularity-friction signature.
2. **Firm-layer market-power rent (F7/F8)**: structural / regime-invariant; IB ~€820M cleared-price-difference total post-IDA, dominated in absolute € by post-MTU15-DA conditions.
3. **System-layer redispatch escalation (S8)**: ~+157 GWh/mo (+60–80%) of additional RZ activation, **persists at MTU15-DA**. Mechanism untested but candidate is the 6→3 IDA-session reduction creating new operational residuals beyond what the price-setting market handles.

S8 + S6 together give a sharper system-layer reading: granularity-friction welfare cost (S6) is closed by re-symmetrisation, but the IDA-session-reduction operational cost (S8) is NOT. The reform package was thus partially welfare-improving (closing S6 at MTU15-DA) but not fully (S8 still elevated).

### Updated three-channel thesis claim (post-2026-04-27)

**Important non-additive footnote (added during 2026-04-27 red-team audit).** S6, S8, and F7 all involve money flows in the same financial system across overlapping windows (BRP-to-TSO settlement, TSO-to-generator redispatch transfer, generator-to-buyer DA cleared-price-difference). The same physical generators participate in all three layers. The three numbers therefore should NOT be summed naively: a generator's RZ-redispatch revenue (in S8) may be partly enabled by their DA market position (in F7). Total system "reform impact" is **at most** the sum of S6 + S8 + F7 in the absence of perfect substitutability, and likely substantially less. Cite each channel separately with its own measurement framing; do not present an aggregate "€X total reform cost" without explicitly noting the non-additivity.



The Spanish reform package (2024-06 IDA + 2024-12 ISP15 + 2025-03 MTU15-IDA + 2025-10 MTU15-DA) produced two clean separable effects (revised 2026-04-27 PM after S8 demotion): (i) **system-layer asymmetric-granularity BRP→TSO settlement transfer** (S6: monthly excess profile is +€136M/mo Dec 2024-Mar 2025 → +€91M/mo Apr-Sep 2025 → +€15M/mo Oct-Dec 2025 — see month-by-month decomposition in `_modelling_track.md` §4 Refinement 2026-04-27. **Primary mechanism: 15-min imbalance settlement rule effective Dec 1, 2024 created the DA-vs-imbalance settlement-clock asymmetry; MTU15-IDA dampened it by ~33% via 15-min intraday self-correction tools; MTU15-DA closure collapses the transfer 6× when the asymmetry resolves.** Total cumulative excess +€1,094.9M across the 10-month asymmetric window — clean granularity-friction signature that the MTU15-DA reform closes); (ii) ~~system-layer redispatch escalation~~ **S8 demoted to wounded** — RZ activations doubled post-IDA but renewable-control regression shows ~80% of the elevation is explained by Spanish solar+wind capacity growth (+80% renewable generation pre-IDA alone); only the ISP15-window 4-month effect survives the renewable control. The reform-mechanism story for S8 is no longer cleanly supportable; (iii) **firm-layer market-power rent in the DA market** (F7/F8: Iberdrola structural price-setter, regime-invariant ~63% hydro Q4-dispatch vs Fringe 42%, ~€820M cleared-price-difference rent post-IDA, dominated by post-MTU15-DA price levels). The aFRR market does NOT show parallel firm-level concentration (F9: IB share *fell* 12pp 2024-06 → 2025-12), confirming that the F7 IB-dominance claim is DA-specific and that REE's aFRR procurement is structurally more competitive. The blackout/operación-reforzada period is a modest modulator on (i) and (iii) but not the source of any channel; vertical integration is ruled out as the explanation for cross-firm market-power heterogeneity (D5). The mechanism story for the firm-layer rent triangulates across four IO methodologies (Hortaçsu–Puller implied Lerner F1/F2 with Cournot tercile validation F6; Allaz–Vila commitment-value F5 with within-firm placebo on Endesa; Ciarreta–Espinosa synthetic-firm F7 with per-IB-unit hydro pivot; direct Bushnell-style strategic-dispatch F8). Ciarreta–Espinosa (2010) document the same IB > Endesa cross-firm pattern for 2002–2005 under different mechanism (CTC regulation) — two-decade PATTERN PERSISTENCE of cross-firm asymmetry under the same family of estimators (Hortaçsu–Puller implied Lerner, Ciarreta–Espinosa synthetic-firm). Note: this is method-replication across periods (same estimator class, different data), not independent confirmation of the same causal relationship — what's robust is the cross-firm direction, not the magnitude or mechanism.

---

# Part B — Red-team audit of the alive ledger

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

**Result of mitigation (2026-04-27)**: framing footnote added in three places: (i) `CLAIMS_LEDGER.md` F7 row caveat 3 (alongside the A1 sensitivity result); (ii) `_modelling_track.md` §0 caveat for the per-IB-unit finding; (iii) Part A of this file, "Operational-vs-strategic framing for F7/F8" subsection. Standard framing: read the €530M as "rent IB extracts that operationally-comparable large-reservoir European peers do not" (matched Fringes are mostly EDP Portugal large hydros + EHN Acciona — themselves portfolio operators, not pure-fringe benchmarks). Cross-firm direction (IB > matched peers) survives; magnitude has upper-bound interpretation. **Status: addressed by framing.** Cannot be data-defended (no perfectly-competitive benchmark exists); the framing is the right answer.

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

### ⚠ A6. (PARTIALLY DEFENDED 2026-04-27) Direction robust, magnitude reform-amplified

**Audit attack** (preserved below for record):

D5: GE +2,316 GWh net seller; IB +958 GWh; therefore vertical-integration doesn't explain IB > GE market power. But "net seller" position post-Rule-28.8 is itself an outcome of bilateral-contract reallocation — it's not exogenous to the reform package. The 2.4× ratio is post-March-2025, after Rule 28.8 elimination.

**Defense**: Ciarreta–Espinosa (2010) found the same negative result for 2002–2005, when Rule 28.8 was different. Two-decade replication of "vertical-integration doesn't explain" strengthens the conclusion. But the LEVEL of GE's net-seller position in our data is post-reform.

**Framing**: cite the two-decade pattern as the load-bearing evidence; treat the 2.4× ratio as illustrative of one period.

**Result of mitigation (2026-04-27)**: ran `d5_sell_side_long_run.py` to compute annual GE vs IB sell-side cleared volume across 2018–2026 (sell side is unaffected by Rule 28.8 buy-side bilateral-contract bidding artifacts).

| Era | GE / IB sell-side ratio |
|---|---:|
| 2018–2021 (clean pre-reform) | **1.12** (basically tied; in 2020-2021 IB > GE) |
| 2022–2024 (energy crisis incl.) | 1.88 |
| 2025–2026 (post-Rule-28.8) | **2.48** |

The **2.4× magnitude is not a clean structural fact** — it reflects a combination of hydrologic-year variation, Rule 28.8 BRP reallocation, and nuclear-maintenance scheduling. **However**, in NO era is IB clearly more net-seller than GE on average; the 2024–2026 window where our F1–F8 market-power tests run has GE consistently more net-seller (ratio 1.9–2.5×). **The QUALITATIVE direction (vertical integration cannot explain IB > GE) is robust; the QUANTITATIVE 2.4× ratio is reform-amplified.**

D5 row updated to cite "GE more net-seller than IB" as the directional fact (load-bearing) rather than the 2.4× magnitude. Status: **PARTIALLY DEFENDED.** The audit attack's nit on the magnitude is correct; the audit attack's broader point (D5 unfit for vertical-integration ruling-out) does NOT land — the directional ruling-out survives.

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

**Mitigation needed**: add an explicit "non-additive across channels" footnote to the three-channel synthesis paragraph. Total system impact ≤ sum of channels. **Status (2026-04-27)**: footnote added in Part A "Updated three-channel thesis claim" subsection above and in `_modelling_track.md` §0.

### ⚠ D2. (PARTIALLY DEFENDED 2026-04-27) Migration footprint bounded at 1.6% of IB transfer

**Audit attack** (preserved below for record):

Per nb12's data caveat: `grupo_empresarial` is the *settlement BRP*, not the physical owner. Rule 28.8 elimination (March 2025) reallocated bilateral-contract settlement flows. So the "GE", "IB" groupings shift across regimes due to contract structure, not just behaviour. F7's per-firm decomposition uses these BRP-level groupings for the post-MTU15-IDA period (post-Rule-28.8) — so "IB carries €820M" reflects IB's **post-Rule-28.8 BRP scope**, not its physical-asset scope.

**Defense**: the F7 method substitutes IB's BIDS with Fringe BIDS at the unit level. So the analysis is bid-level, not BRP-level. The grupo_empresarial categorisation enters only at the per-firm aggregation stage.

**Mitigation needed**: verify in `synthetic_firm_per_firm.py` that the per-firm aggregation uses physical-unit-to-firm mapping, not the BRP-shifting categorisation. ~30 min check.

**Result of verification (2026-04-27)**:

1. **Matching table internally consistent with F7 panel period**. Of the 62 matched Big-4 CCGT/Hydro plants in `synthetic_plant_match.parquet`, **zero have a mismatch** between `firm_L` (matching-table firm) and the post-MTU15-IDA pdbce dominant `grupo_empresarial`. The matching script aggregated 2024-01+ pdbce data which contained pre-migration EHN-attributed sessions for 3 plants (ACC1EBR, ACC2EBR, IPG), but the matching-table `firm_L='GE'` for these plants matches their post-2025-03-19 BRP. So the per-firm decomposition for the F7 analysis period is consistent.

2. **Three units shifted firm assignment at exactly Rule 28.8 (March 2025)**: ACC1EBR, ACC2EBR, IPG (all EHN-Acciona small hydro plants) migrated from `grupo_empresarial='EHN'` to `'GE'` precisely at 2025-03-01. These are real BRP migrations, IDA-reform-coincident.

3. **Material impact on F7 IB-canonical headline is bounded**. Of these 3 migration units, ACC2EBR is matched-S for IB's EBRA hydro plant. Per `synthetic_firm_per_unit_ib.csv`, EBRA contributes **€12.9M of €823.9M IB transfer = 1.6%**. The substitution at this matched pair replaces EBRA-IB offers with ACC2EBR offers — which post-2025-03-19 are submitted under GE BRP rather than EHN. If GE-style offers are systematically more aggressive than EHN-Acciona's true competitive baseline, the F7 method UNDER-states IB's markup at EBRA → biases mp_IB DOWNWARD. Conversely if the operational owner (Acciona) submits stable offers regardless of BRP attribution, the bias is zero.

4. **GE/GN/HC firm-level totals (-€23M, +€14M, +€70M) include similar matching ambiguities** for TJEG/GDNA/TEES → ACC1EBR/ACC2EBR/IPG (GE), UFBG → ACC2EBR (GN), HCHI → ACC2EBR (HC). Magnitudes are themselves small relative to the IB headline and don't break the IB-canonical reading.

**Status: PARTIALLY DEFENDED.** The internal consistency check (zero mismatches) defends the F7 IB-canonical headline. The 1.6%-of-IB bound on migration-induced bias is small and does not break the IB-canonical reading. The bid-vs-BRP distinction (substitution uses unit_code, not firm) is exactly the right defense — F7 substitutes physical-plant offers, not BRP-attributed offers, so the F7 method is bid-level not BRP-level. The per-firm AGGREGATION (where firm_L is the categorizer) inherits the small migration bias quantified above.

**Recommendation**: cite the IB +€820M as "robust to BRP-migration bias of at most 1.6%". Document the migration units in F7 ledger row. No re-run of the synthetic-firm pipeline needed; the verification result is dispositive.

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

**Result of mitigation (2026-04-27)**: framing updated in CLAIMS_LEDGER F7 row, `_modelling_track.md` §0, and Part A above (two paragraphs). Standard wording: "two-decade pattern persistence" or "method-replication across periods" — explicit that the same estimator family produces the same direction in different data, NOT independent confirmation. The cross-firm direction (IB > Endesa) is what survives; the mechanism story differs (CTC stranded-cost regulation in 2002–2005 vs hydro-portfolio + asymmetric-granularity in 2024–2026). **Status: FRAMED.**

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
