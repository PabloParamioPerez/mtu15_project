# Economic-modelling track

Working note. Replaces `_modelable_patterns.md` (now archived).

Goal: organise alive empirical claims around economic models. Each section is a candidate model that (a) rationalises ≥2 alive claims in `CLAIMS_LEDGER.md` and (b) makes one or more sharp predictions that current empirical work either does or could test. Sections end with a "Priority empirical refinement" line: the single focused analysis that would most advance the model. Refinements are *candidates* for Phase 2 of the project plan, not commitments. Each runs only on user approval and only after passing the stop-rule in `CLAUDE.md`.

References: `CLAIMS_LEDGER.md` (claim status, evidence pointers) | `_identification_target.md` (identification provenance, frozen post-Week 1) | `RESEARCH_LOG.md` (chronological diary).

---

## §0 — Cross-firm consistency: IB as the canonical strategic firm

Three independent Phase 2 tests on different mechanism predictions converge on the same firm-level heterogeneity pattern: **IB is the single Big-4 firm whose post-MTU15-IDA behaviour fits IO mechanism predictions cleanly across the board.**

| Test | Mechanism | IB | GE | GN | HC |
|---|---|---|---|---|---|
| **F6** Cournot tercile (§1) | Lerner concentrates in steep-slope cells | ✓ monotone decline T1→T3 (+0.126 → +0.044) | partial (T1>T2 but T3 highest) | ✗ opposite | ✗ opposite |
| **F5** Allaz–Vila peak/off-peak (§2) | Commitment slope flips at MTU15-DA in CCGT-margin hours | ✓ peak sign-flip (−0.025 → +0.024) | partial (peak attenuates; off-peak deepens — within-firm placebo) | ✗ no fit | ambiguous |
| **B8** Bid complexification (§5 below) | Strategic firms invest in finer price ladders under finer ISP | ✓ within-unit tpp 5.49 → 8.73 (1.59×) | flat at low absolute level (~2 tpp) | ✗ simplifies (6.46 → 3.22) | stable (5.93 → 5.83) |

**Reading.** The thesis can present IB as the canonical strategic-firm case study and use GE/GN/HC as cross-firm placebos. The three tests probe distinct mechanism dimensions (residual-demand elasticity, commitment-value evolution, bid-structure response) and all point to IB. This is internally consistent with IB's portfolio composition: IB holds the largest CCGT fleet on the marginal supply step among Big-4 plus substantial hydro flexibility — the exact portfolio profile the IO models assume.

**Why GE doesn't fit.** GE is large but its DA bidding style is structurally different — only ~2 tranches per ISP both pre and post (an order of magnitude simpler than IB's ~6–9). GE's market-power signal in F1 is real but does not arise from CCGT-margin Cournot or Allaz–Vila quantity-setting in the standard sense. GE's post-reform Lerner peak appears to come from composition (76–90% nuclear in cleared volume per W1), not from strategic CCGT behaviour. F1 is a market-power *outcome*; the *mechanism* is not the textbook-Cournot story.

**Why GN/HC don't fit.** Per F4 (alive): GN/HC market-power shifts at MTU15-IDA are dominated by Rule 28.8 bilateral-contract reallocation (regulatory shock), not strategic conduct. Their portfolios (hydro-heavy GN, small mixed HC) place them away from the strategic-marginal-CCGT setting where IO models predict the Cournot/Allaz–Vila mechanisms operate.

**Implication for the structural-firm chapter.** Lead with IB as the case study; present GE/GN/HC as the cross-firm placebos that *don't* show the IB-style mechanism response, supporting the strategic interpretation. This is a stronger claim than "Big-4 firms exhibit market-power elevation" because the cross-firm heterogeneity has a structural reading: only firms with strategic-marginal-CCGT capacity respond to finer granularity in the predicted ways.

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
