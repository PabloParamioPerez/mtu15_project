# Coherence audit of alive claims (2026-04-27)

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
- **F8** — Bushnell-signature direct test: IB hydro 63% Q4-concentrated vs Fringe 42% (+21pp), intensifies at MTU15-IDA (56% → 63%).

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



The Spanish reform package (2024-06 IDA + 2024-12 ISP15 + 2025-03 MTU15-IDA + 2025-10 MTU15-DA) produced two clean separable effects (revised 2026-04-27 PM after S8 demotion): (i) **system-layer asymmetric-granularity BRP→TSO settlement transfer** (S6: ~€90–95M/mo in DA60/ID15 and ISP15 windows, ~€7M/mo at MTU15-DA — clean granularity-friction signature that the MTU15-DA reform closes); (ii) ~~system-layer redispatch escalation~~ **S8 demoted to wounded** — RZ activations doubled post-IDA but renewable-control regression shows ~80% of the elevation is explained by Spanish solar+wind capacity growth (+80% renewable generation pre-IDA alone); only the ISP15-window 4-month effect survives the renewable control. The reform-mechanism story for S8 is no longer cleanly supportable; (iii) **firm-layer market-power rent in the DA market** (F7/F8: Iberdrola structural price-setter, regime-invariant ~63% hydro Q4-dispatch vs Fringe 42%, ~€820M cleared-price-difference rent post-IDA, dominated by post-MTU15-DA price levels). The aFRR market does NOT show parallel firm-level concentration (F9: IB share *fell* 12pp 2024-06 → 2025-12), confirming that the F7 IB-dominance claim is DA-specific and that REE's aFRR procurement is structurally more competitive. The blackout/operación-reforzada period is a modest modulator on (i) and (iii) but not the source of any channel; vertical integration is ruled out as the explanation for cross-firm market-power heterogeneity (D5). The mechanism story for the firm-layer rent triangulates across four IO methodologies (Hortaçsu–Puller implied Lerner F1/F2 with Cournot tercile validation F6; Allaz–Vila commitment-value F5 with within-firm placebo on Endesa; Ciarreta–Espinosa synthetic-firm F7 with per-IB-unit hydro pivot; direct Bushnell-style strategic-dispatch F8). Ciarreta–Espinosa (2010) document the same IB > Endesa cross-firm pattern for 2002–2005 under different mechanism (CTC regulation) — two-decade PATTERN PERSISTENCE of cross-firm asymmetry under the same family of estimators (Hortaçsu–Puller implied Lerner, Ciarreta–Espinosa synthetic-firm). Note: this is method-replication across periods (same estimator class, different data), not independent confirmation of the same causal relationship — what's robust is the cross-firm direction, not the magnitude or mechanism.
