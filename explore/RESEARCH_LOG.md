# Research Log — MTU15 Project

**Purpose.** Single-file audit trail of what this project has asked, tested, and concluded. If someone asks "did you try X?", this is where you look first.

**Conventions.**
- Status tags: [SUPPORTED] empirically backed; [REJECTED] tested and rejected; [FLAGGED] real but interpretation uncertain; [UNTESTED] known candidate not yet attempted; [WITHDRAWN] previously claimed, retracted after further evidence.
- Notebook references: `nbNN §k` means section $k$ of `explore/NN_*.ipynb`. Commit hashes are 7-char git SHAs.
- The working analytical narrative lives in [`_identification_target.md`](./_identification_target.md). This file is the outward-facing index; that one is the internal argument.

---

## 1. Thesis question

Between June 2024 and October 2025, Spain ran four sequential reforms progressively raising the resolution of both price-setting and imbalance settlement from 60-min to 15-min intervals. Did this reform sequence change dominant-firm (Big-4: Iberdrola [IB], Endesa [GE], Naturgy [GN], HC-Energía [HC]) strategic bidding behaviour in the wholesale electricity market, and if so, through what mechanism?

## 2. Reform calendar

| Abbr. | Date | Scope |
|---|---|---|
| IDA | 2024-06-14 | Intraday auctions: 6 local MIBEL sessions → 3 European IDA sessions |
| ISP15 | 2024-12-01 | **Settlement-side**: imbalance settlement period from 60-min to 15-min |
| MTU15-IDA | 2025-03-19 | **Trading-side (intraday)**: IDA + continuous XBID from 60-min to 15-min |
| MTU15-DA | 2025-10-01 | **Trading-side (day-ahead)**: day-ahead market from 60-min to 15-min |

## 3. Research Questions

- **RQ1.** Does the reform sequence have a descriptive footprint in dominant-firm DA/ID repositioning behaviour (the Ito–Reguant $\Delta Q = Q^{\text{DA}} - Q^{\text{IDA-final}}$ object)? → `nb03`.
- **RQ2.** Can the observed compression in $|\Delta Q|$ be explained by "engineering" (mechanical/physical) alternatives, or does it require a behavioural interpretation? → `nb05`.
- **RQ3.** Can we causally identify the effect of any specific reform on Big-4 strategic bidding? → `nb07` (DiD), `nb08` (wind-IV).
- **RQ4.** Do the reforms show up in system-level balancing-market dynamics (imbalance volumes, activation prices)? → `nb04`.
- **RQ5.** Does the bid-level conduct gap between Big-4 and Fringe attenuate at MTU15-IDA, consistent with the "relief reform" interpretation? → `nb06`.
- **RQ6.** How did Big-4 bid *structure* (number of tranches, granularity, order types) evolve across the reform sequence? → `nb09` (ongoing).

## 4. Hypotheses register

### 4.1 Engineering alternatives for $|\Delta Q|$ compression (nb05)

| ID | Hypothesis | Status | Evidence |
|---|---|---|---|
| **H1** | Profile-matching: firms reposition because MTU15 reveals within-hour demand shape they must match | [REJECTED] | `nb05 §2`. Big-4 OS-settled programs have $\sigma^{\text{shape}} = 0$ across 198,956 post-MTU15-IDA CCGT operating hours; non-dominants show $\sigma$ up to 800 MW. The shape that would flow through intraday does not exist for Big-4 on PHF. |
| **H2** | Ramp / start-up lumpiness: compression driven by start-up / shut-down hours | [REJECTED] | `nb05 §3`. Compression uniform across hour-types (−75% to −80%); shut-down / steady ratio stable at ~3.9× pre and post. |
| **H3** | Reserve-procurement substitution: dominants route capacity from DA-withholding into balancing reserves | [REJECTED] | `nb04 §8`, `nb05 cross-walk`. $|V^{\text{imb}}|$ rises +127% at ISP15 but falls −30% at MTU15-IDA — pattern matches settlement-vs-trading mismatch sequencing, not reserve routing. Direct test requires A81 (not available). |
| **H4** | Storage internalisation: new battery + pumped-hydro capacity absorbs the repositioning | [REJECTED] | `nb05 §4`. B10+B25 capacity grew by only 32 MW 2024→2025 (ceiling ~253 MWh/day, ~2% of aggregate); pumped hydro flat at 3,418 MW since 2022. A68 installed-capacity pipeline in place (commit `14d021e`). |

### 4.2 Identification candidates

| ID | Hypothesis | Status | Evidence |
|---|---|---|---|
| **H5** | TWFE-DiD between Big-4 and Fringe identifies an ATT of ISP15 (or any reform) on $\Delta Q$ | [REJECTED] | `nb07 §3` (event study, pre-trends), `§6` (analytical placebo at 2024-09-01: $\hat\beta=+314$, $p<0.01$), `§11` (randomization inference, both full-window and pre-period-only), `§12` (treatment-date sweep peaks at 2024-07-01, months before ISP15). Commit `7b5f0bb`, `2195b1d`. The +217 coefficient is a conditional association, not an ATT. |
| **H6** | Aggregate wind-IV on Big-4 $\Delta Q$ identifies an ISP15 slope contraction in strategic responsiveness | [WITHDRAWN] | `nb08 §9 H2`. Initially supported (§6c: low-wind $\hat\rho$ from +15.6 in 3-sess to +0.8 in ISP15, commit `a6168f5`). Ex-nuclear robustness check: excluding 8 nuclear units reduces $\Delta(3\text{-sess} - \text{ISP15})$ from +14.79 to +2.88 and every regime slope loses significance. Nuclear $|\Delta Q|$ variance is ~6× CCGT's; aggregate was nuclear-variance-weighted. |
| **H7** | GE × CCGT signed flip (low-wind, 5 units) is an Ito–Reguant strategic-withholding signature localised to ISP15 | [REJECTED] | `nb08 §10`. Placebo sweep restricted to the 3-sess + ISP15 combined window (22 candidate boundaries, weekly) produces $|\Delta|$ at or above the real $\Delta = +18.42$ for 6 of 22 fake dates. Empirical $p = 0.273$; real Δ at the 83rd percentile. The signed flip is real descriptively but not localised to the reform boundary. Commit `ceec5bc`. |
| **H8** | Spanish nuclear $\Delta Q$ has a large wind-IV slope that collapses at ISP15 | [FLAGGED] | `nb08 §9 H1`. Real pattern, reproducible across ANAV (Ascó/Vandellós, Endesa-majority) Δ=+45.8 and CNAT (Almaraz, Iberdrola-majority) Δ=+59.8. Rules out Endesa-operator-coordination reading. Mechanism uncertain (load-following / outage scheduling / REE redispatch / $\Delta Q$ scaling artefact). **Not claimed as strategic-bidding evidence.** |
| **H9** | GN × ResHydro wind-IV slope reverses across ISP15 | [FLAGGED] | `nb08 §8`. 3 units, Δ(3-sess − ISP15) = −29.8. Sample too small for mechanistic interpretation. Drives GN's aggregate opposite-direction effect in `§7`. |

### 4.3 Bid-structure hypotheses (nb09, ongoing)

| ID | Hypothesis | Status | Evidence |
|---|---|---|---|
| **H10** | Big-4 CCGT DA bid granularity (tranches per offer-period) declines at MTU15-IDA (2025-03-19) | [PARTIALLY WITHDRAWN — mostly composition; one within-unit case] | `nb09 §1/§2`, §12 audit. Aggregate: 5-7 → 1-2. Unit audit: most units have IDENTICAL median tranches pre and post (ACE3: 8→8, ARCOS1-2: 8→8, BES3/5/PGR5/SROQ2: 2→2, CTN3: 12→13, ESC6: 12→12). Aggregate drop is driven by complex-bidder units EXITING (TAPOWER 11-tranche → 0 activity, SRI4R/5R 6-tranche → 98% drop, ARCOS1 8-tranche → 87% drop, CTN4/CTJON2 12-13-tranche → 91-98% drop). One within-unit case: PALOS3 (Naturgy) 13 → 1 tranches (genuine). The "behavioral shift" interpretation is partially unsupported. |
| **H11** | Big-4 hydro DA bid granularity *increases* across the reform sequence (opposite of CCGT) | [FLAGGED — descriptive] | `nb09 §1/§2`. Reservoir hydro: 2 → 3. Pumped hydro: 2 → 3 at MTU15-DA. |
| **H12** | Big-4 CCGT shifts bid-structure complexity from DA to IDA once MTU15-IDA provides matching 15-min intraday tools | **[WITHDRAWN — pure composition artefact]** | `nb09 §3/§4`, `§12` audit. Aggregate IDA tranches 1 → 5 at MTU15-IDA is real descriptively. Unit audit: **NOT A SINGLE Big-4 CCGT unit changes its median IDA tranche count across MTU15-IDA**. Every unit has identical pre/post medians (ACE4: 5→5, ARCOS1: 1→1, CTGN1-3: 5→5, PALOS1-3: 5→5, SAGU1-3: 5→5, SBO3: 5→5, BES3: 1→1). The aggregate "1 → 5" rise comes entirely from participation-weight shifts: Naturgy's always-5-tranche units (PALOS, SAGU, CAMGI10) became more active; Iberdrola's always-1-tranche units (ARCOS, STC4) declined relatively. Not a within-unit behavioral shift. |
| **H13** | The new Big-4 CCGT IDA tranches are strategic-spread bids (wide price range) rather than quantity-precision subdivisions (narrow price range) | [REJECTED] | `nb09 §5`. Median IDA tranche price range per offer-period stays in 0-3 EUR/MWh across all regimes for Big-4 CCGT, despite tranche count rising from 1 to 5. P75 range is 4-6 EUR/MWh, P90 is 7-12 (pre-reform 6-sess P90=34 is the exception, contracted dramatically at IDA reform). On clearing prices of ~90-140 EUR/MWh, the per-offer range is <5%. The complexity shift is quantity-precision-increasing, not price-spread-increasing. Implication: the H12 pattern is **not** a classic Ito–Reguant strategic-withholding signature. |
| **H14** | Big-4 CCGT IDA supply function slope (Chang 2026-style elasticity measure) shifts across the reform sequence | [PARTIALLY WITHDRAWN — mostly composition; some within-unit flattening at Naturgy CCGTs] | `nb09 §6`, `§12` audit. Aggregate slope 4.24 → 13.28 across regimes. Unit audit: within-unit changes mixed. Real within-unit flattenings at PBCN1 (GN, 3.8 → 38.8), CAMGI10 (GN, 10.1 → 22.8), MALA1 (GN, 3.9 → 13.6), PALOS3 (GN, 2.7 → 12.9). But: BES5 (GE) 102 → 13 and PGR5 (GE) 68.0 → 20.3 STEEPENED (opposite direction). Aggregate flattening is mostly composition (steep-curve low-slope units exit or scale down, flat-curve high-slope Naturgy units become more active). The "3× slope flattening" claim overstates within-unit change. |
| **H15** | Firms use MTU15-IDA / MTU15-DA to submit *sub-hourly differentiated* bids (intra-hour strategic heterogeneity) | [REJECTED for most tech × market cells] | `nb09 §7`. For each offer-hour post-reform, count distinct bid signatures across the 4 intra-hour 15-min ISPs. Rate of all-4-identical: Big-4 CCGT 85% (IDA), 92% (DA); Big-4 Pumped Hydro 93% / 99%; Big-4 Nuclear 88% / 82%. Only Big-4 Reservoir Hydro in DA post-MTU15-DA (68% identical, i.e. 32% differentiated) and Fringe Pumped Hydro in IDA (60%) use the granularity meaningfully. **For 80-99% of offer-hours, firms submit identical bids across the 4 intra-hour ISPs — the 15-min granularity is not strategically exploited.** Refines H12/H14 interpretation: bid-structure changes happen at the hourly level, then get replicated across the 4 ISPs. |
| **H16** | Battery-storage / hybrid-renewable-storage units bid strategically, exploiting 15-min granularity more than conventional generation | [FLAGGED — activity jumped 300× post-reform, but no iceberg use] | `nb09 §8b`. Storage XBID activity: ~50-150 orders/regime pre-reform → 25,000-50,000 orders/regime post-MTU15-IDA; active units 5 → 9-12. Storage is a NEW entrant into intraday that the 15-min reforms enabled (or their commissioning timing coincided). BUT: storage iceberg rate stays at 0-20%, not strategic information-hiding. Contrast pumped-hydro iceberg at 99%. Storage operates transparently; the real strategic storage-like actor remains pumped hydro (H18). |
| **H18** | Big-4 Pumped Hydro adopted iceberg strategies at MTU15-DA | **[WITHDRAWN — composition artefact, not behavioral shift]** | `nb09 §11`. Unit-level timeline reveals: La Muela (MUEL) iceberg rate was already 99% by mid-2021 and has stayed there — predates all 2024-25 reforms. UFBG (Naturgy Bugalleira) withdrew from XBID at MTU15-DA (3,019 Sep-2025 orders → 110 Oct-2025 orders), with pre-reform low iceberg rate (0-4%). The aggregate "surge" is arithmetic: losing UFBG's high-volume low-iceberg contribution while MUEL's always-high activity continues leaves an aggregate dominated by MUEL ≈ 100%. §9 placebo correctly detects a step; its interpretation is compositional, not behavioral. Original claim of "pumped-hydro adopted iceberg at MTU15-DA" is not supported. Replaces with H19 below. |
| **H19** | Naturgy's Bugalleira (UFBG) withdrew from XBID at MTU15-DA while maintaining DA/IDA auction participation | [FLAGGED — real, unexplained] | `nb09 §11`. UFBG XBID sell-side monthly orders: 3,019 (Sep 2025) → 110 (Oct 2025) → 548 → 612 → 143. Near-96% reduction pinned to reform date. MUEL (Iberdrola, larger plant) did not withdraw. Candidate explanations (none tested): (a) scale-based economic selection — smaller PH units can't justify active XBID with 96 DA periods; (b) trading-desk reorganization at Naturgy; (c) algorithmic platform change. Worth a separate investigation. |
| **H20** | La Muela (MUEL) has used near-total iceberg strategy in XBID since ~2021 | [SUPPORTED — persistent unit-level pattern, not reform-triggered] | `nb09 §11`. Full 2018-2026 timeline: 0% (Jun 2018, XBID launch) → ramp-up through 2019-2020 → 99-100% saturation from Jul 2021 onwards. Behavior pre-dates IDA reform (2024-06-14), ISP15 (2024-12-01), MTU15-IDA (2025-03-19), MTU15-DA (2025-10-01). Plausible (untested) explanation: Iberdrola's largest pumped-storage asset is managed with a dedicated trading desk using iceberg as default configuration. Descriptive finding about firm-specific XBID sophistication; not a reform-effect claim. |
| **H21** | Several Big-4 CCGT complex-bidder units exited or dramatically reduced DA activity at MTU15-IDA | [FLAGGED — real, unexplained, supersedes earlier H10 aggregate framing] | `nb09 §12` audit. Units that exited or scaled down 80%+: TAPOWER (IB Tarragona Power, median 11 tranches → 0 offers), SRI4R+SRI5R (HC Soto de Ribera, median 6 → 98% drop), ARCOS1 (IB, median 8 → 87% drop), CTN4 (IB, median 13 → 98% drop), CTJON2 (IB, median 7 → 91% drop), COL4 (GE, exit), CTGN2 (GN, exit). The common pattern: these were the complex-bidder units. Simpler-bidder units largely stayed. Analogous to H19 (UFBG) but for CCGT. Question: what determines which plants Iberdrola/HC chose to exit from DA at MTU15-IDA? |
| **H22** | Several Naturgy CCGT units modestly flattened their IDA supply slopes within-unit at MTU15-IDA | [SUPPORTED — small within-unit effect] | `nb09 §12` audit. Units showing genuine within-unit slope increases (curve flattening) pre/post MTU15-IDA: PBCN1 (3.8 → 38.8), MALA1 (3.9 → 13.6), CAMGI10 (10.1 → 22.8), PALOS3 (2.7 → 12.9). All Naturgy. Opposite direction at GE: BES5 (102 → 13, steepened), PGR5 (68 → 20, steepened). Net: a firm-specific pattern (Naturgy flattens IDA curves, Endesa steepens them). Much smaller effect than the aggregate "3× flattening" claim implied. |
| **H17** | Demand-side offers (retailers, direct consumers, last-resort retailers) exhibit strategic bidding patterns analogous to generator withholding | [PARTIALLY ADDRESSED — pre-March-2025 DA data is Rule-28.8-contaminated; requires alternative proxies] | `nb10 §1-§4`. Initial demand-side EDA reveals the March 2025 "shift" in DA buy-offer volumes (70 → 20 TWh/month, flat rounded pre-reform quantities → variable realistic volumes) is the **elimination of Rule 28.8** (bilateral-contract opportunity-cost buy-back obligation) per CNMC Resolución 28-Feb-2025, Section F. NOT a behavioral shift in response to MTU15-IDA and NOT a format artefact — a regulatory change. The reform coincided in date but did not cause the shift. Pre-reform DA buy data is artificial (programmatic opportunity-cost bids); only post-March-2025 DA buy data represents real retailer demand. To study actual demand-side behavior: (1) use cleared DA demand (pdbc/pdbce) rather than offers; (2) use IDA buy-side; (3) use post-March-2025 DA only. H17 remains OPEN for these paths. |

### 4.4 Theoretical rationalisation

| ID | Proposition | Status |
|---|---|---|
| **H_theory** | Two-channel model ($\alpha_r$ imbalance-gaming + $\Phi(\lambda,\beta,b_{21})$ ramp-thinness) rationalises the descriptive pattern | **[DEPRECATED 2026-04-24]** archived to `attic/theory_granularity_extension/`. Predicted intensive-margin responses that aren't supported by the data (see nb09 §11-§12 unit-level audit). Thesis narrative pivoted to extensive-margin vs intensive-margin firm responses to reform sequence, for which this model is orthogonal. Historical references in nb03-09 synthesis cells left intact as point-in-time context. |

## 5. Methods attempted

| # | Method | Where | Outcome |
|---|---|---|---|
| 1 | Descriptive regime means & time-series | `nb03`, `nb06` | Standard footprints; nb03 is the descriptive core |
| 2 | Matched-wind tercile analysis | `nb03 §3e, §3f`; `nb08 §6c` | Low-wind subsample is the cleanest strategic-binding margin |
| 3 | Technology decomposition | `nb05 §2-§4`; `nb07 §5b`; `nb08 §8` | CCGT carries most strategic signal; nuclear anomaly flagged |
| 4 | Per-firm decomposition (grupo_empresarial) | `nb03 §4`; `nb07 §10`; `nb08 §7` | Effect concentrated in GE (Endesa); IB null, GN opposite, HC small |
| 5 | Saturated multi-reform DiD | `nb07 §5a` | Mass on ISP15 interaction (+217) — not an ATT |
| 6 | Event study (relative-time dummies) | `nb07 §3, §8b` | Pre-trends visible; no sharp break at any reform date |
| 7 | Analytical placebo at fake dates | `nb07 §6` | 2/3 fake dates produce significant coefficients; fails no-anticipation |
| 8 | Randomization inference (200 full-window draws) | `nb07 §11` (original) | $p=0.43$; not conclusive |
| 9 | Randomization inference (121 pre-period draws) | `nb07 §11` (revised, commit `7b5f0bb`) | Pre-period structurally non-stationary for Big-4 vs Fringe differential; neither RI variant meaningful |
| 10 | Treatment-date sweep | `nb07 §12` | $\hat\beta$ peaks at 2024-07-01, not ISP15; declines monotonically |
| 11 | Bid-level TWFE on wavg IDA bid | `nb07 §9` | Unit FE absorbs the descriptive conduct-gap collapse; $\hat\beta=-22$ ns |
| 12 | Refined control group (55 dispatchable-conventional Fringe units) | `nb07 §8a` | Sharpens ISP15 coefficient slightly but doesn't fix parallel trends |
| 13 | Wind forecast-error IV (Ito–Reguant style) | `nb08 §3` | First-stage relevant; regime heterogeneity present |
| 14 | Fringe placebo wind-IV | `nb08 §4` | $\|\hat\rho\| \le 0.83$ for all Fringe regimes vs +44 pre-IDA Big-4. >400× ratio |
| 15 | Winsorisation robustness (1%/99%) | `nb08 §6a` | Strengthens point estimate (+44 → +62); not tail-driven |
| 16 | Solar forecast-error as complementary IV | `nb08 §6b` | Pre-IDA $\hat\rho_{\text{solar}} = +72.8$ ($p=0.02$), same pattern |
| 17 | Low-wind subsample IV | `nb08 §6c` | Regime slopes collapse at ISP15: +17.9 → +15.6 → +0.8 |
| 18 | Rolling-window IV (6-month, monthly step) | `nb08 §6d` | Smooth decline, not discrete break |
| 19 | Per-firm × per-regime IV (low-wind) | `nb08 §7` | Only GE shows the aggregate pattern (Δ=+29.7); IB null, GN opposite (Δ=−13.2) |
| 20 | Per-firm × per-tech IV (low-wind) | `nb08 §8` | GE×CCGT: +11.8 → −16.1 (signed flip, Δ=+27.9). GE×Nuclear anomaly (Δ=+53.8). |
| 21 | ANAV vs CNAT nuclear-operator split | `nb08 §9 H1` | Both show the large slope collapse. Rules out operator-specific strategic reading. |
| 22 | Ex-nuclear aggregate robustness | `nb08 §9 H2` | Drops Δ from +14.79 to +2.88. Aggregate was nuclear-variance-weighted. |
| 23 | Sliding-boundary placebo sweep (GE×CCGT) | `nb08 §10` | 6/22 fake boundaries within 3-sess+ISP15 window have \|Δ\| ≥ real. $p=0.273$. |
| 24 | Tranche-count EDA by regime × group × tech | `nb09 §1/§2` | Big-4 CCGT median tranches drops from 5-7 to 1-2 at MTU15-IDA. |
| 24a | IDA tranche-count mirror test (H12) | `nb09 §3/§4` | DA simplification offset by IDA complexification; DA+IDA sum stable 5-9. |
| 24b | IDA price-range per offer-period (H13) | `nb09 §5` | Median range 0-3 EUR/MWh across all regimes for Big-4 CCGT; new tranches are quantity-precision, not strategic spread. |
| 24c | IDA supply function slope per offer-period (H14, Chang 2026) | `nb09 §6` | Big-4 CCGT slope rises 4.24 → 13.28 across reforms (flattening); Big-4/Fringe gap inverts at MTU15-IDA. |
| 24d | Within-hour bid similarity (H15) | `nb09 §7` | 80-99% of Big-4 offer-hours have identical bids across 4 intra-hour ISPs; exceptions are Big-4 DA ResHydro (68% identical) and Fringe IDA PumpedHydro (60%). |
| 24e | XBID iceberg rate by group × tech × regime (H18) | `nb09 §8a` | Big-4 Pumped Hydro iceberg rate 40% → 98.8% across reforms; Fringe CCGT 17-37% → 78-87% at MTU15-IDA. |
| 24f | Battery/storage XBID activity trajectory (H16) | `nb09 §8b` | Storage orders/regime jumped from ~50-150 to ~25k-50k post-MTU15-IDA; iceberg rate stays 0-20%. |
| 24g | Sliding-boundary placebo for H18 | `nb09 §9` | Monthly Δ(iceberg rate). Real MTU15-DA Δ=0.480 vs non-reform placebo range [0.210, 0.327]. Empirical p=0/18=0.000. **Placebo-validated.** |
| 24h | Buy-vs-sell side asymmetry + per-unit decomposition for H18 | `nb09 §10` | Sell-side surges to 98.8%, buy-side only 67.5% (sell−buy = +0.313). Muela (Iberdrola, 630 MW) produces 98.3% of post-reform PH sell orders; the aggregate "Big-4 PH" is really "La Muela." Mechanism rationalization across 5 channels. |
| 24i | H18 correction via unit-level timeline | `nb09 §11` | MUEL iceberg 99% since mid-2021 (reform-independent); UFBG withdrew from XBID at MTU15-DA. Aggregate "surge" is composition, not behavior. H18 withdrawn. |
| 24j | Systematic unit-level audit of H10/H12/H14 | `nb09 §12` | H12 pure composition (zero within-unit change); H10 mostly composition + one within-unit case (PALOS3); H14 mostly composition + Naturgy CCGTs flatten within-unit while Endesa CCGTs steepen. |
| 24k | Complex-conditions check (DA MAV, IDA block orders) | `nb09 §13` | DA MAV: 100% pre → 1% post is a format artefact (OMIE file format changed at MTU15-IDA), not behavioral. IDA blocks: small magnitudes, no clean pattern. Saved as ref memory to prevent future redoing. |
| 24l | nb09 synthesis + reframing | `nb09 §14` | Six of six aggregate findings fail/narrow under unit audit. Reframed contribution: participation-shift documentation, not within-unit behavioral shift. |
| 25 | Descriptive OS-intervention decomposition (CCGT vs coal) | `nb03 §3g` | OS-intervention on CCGT is positive (amplifying, not attenuating, strategic position); coal gets curtailment |
| 26 | Robustness sensitivity to blackout exclusion | `nb03 §5b` | Excluding 2025-04-26 to 2025-04-30 moves wedge by −0.17 EUR/MWh and ΔQ by 1.4% |
| 27 | Installed-capacity ENTSO-E A68 pipeline | commit `14d021e` | H4 storage-ceiling quantification |
| 28 | ENTSO-E A75 actual-generation pipeline | commit `7df2320` | Enables wind forecast error = A75 − A69 |

### 5.1 Methods NOT attempted (candidates)

| Method | Rationale | Priority |
|---|---|---|
| Narrow-window RD around ISP15 (±60 days) | Different identification-assumption set; cross-validates wind-IV | Medium |
| Synthetic control for Big-4 ΔQ | Abadie-Diamond-Hainmueller style | Low (pre-period trend would contaminate weights) |
| Cohort-DiD with heterogeneous effects (Callaway–Sant'Anna) | Not applicable: no staggered adoption | N/A |
| Bid-level wind-IV (different outcome) | Could corroborate §8 GE×CCGT finding | Medium |
| Within-hour bid similarity post-MTU15-DA | Directly tests whether firms use 15-min granularity or replicate hourly curves 4× | High (nb09 §3+) |
| XBID iceberg / order-lifetime analysis | `reduced_qty_mw`, `submitted_at` columns never used | High (nb09 §5+) |
| Complex-conditions / block-order usage | `min_acceptable_volume_mw`, `exclusive_group`, block fields never used | Medium (nb09 §4+) |
| Partial identification (Manski bounds) | Could bracket the treatment effect without point identification | Low (explicitly deferred in plan) |
| Structural BSTS (Bayesian time-series) | Deferred in plan; high implementation cost | Low |

## 6. Data sources

### 6.1 Used

**OMIE families** (see `CLAUDE.md` for full catalogue; pipeline at `scripts/pipelines/omie/`):

| Family | Purpose | Used in |
|---|---|---|
| `pdbc`, `pdbce`, `phf`, `phfc` | DA and OS-settled final programs → $Q^{\text{DA}}$ | All notebooks |
| `pibci`, `pibcie`, `pibcic`, `pibcice`, `pibcac` | IDA and continuous intraday programs → $Q^{\text{IDA-final}}$, $\Delta Q$ | All notebooks |
| `precios_pibcic`, `marginalpdbc`, `marginalpibc` | Clearing prices | `nb03`, `nb06` |
| `curva_pbc`, `curva_pibc` | Aggregate supply/demand curves | `nb06 §4` within-hour dispersion |
| `cab`, `det` | DA offer headers/details | `nb07 §9`, `nb09` |
| `icab`, `idet` | IDA offer headers/details | `nb07 §9` |
| `orders`, `trades` | XBID continuous-intraday order book & trades | `nb06 §2` (partial) |
| `lista_unidades` (external ref) | Unit master → firm (`grupo_empresarial`) attribution | All notebooks |

**ENTSO-E Transparency** (pipeline at `scripts/pipelines/entsoe/`):

| Table | Purpose | Used in |
|---|---|---|
| A68 | Installed generation capacity by type | `nb05 §4` (H4 ceiling) |
| A69 | DA forecast wind + solar | `nb08 §1` |
| A74 | Intraday forecast revisions | `nb04 §7` |
| A75 | Actual wind + solar | `nb08 §1` (enables forecast error) |
| A84 | Activated balancing energy prices | `nb04 §4` |
| A85 | Imbalance settlement prices | `nb04 §2` |
| A86 | Imbalance volumes | `nb04 §3, §8` |

### 6.2 Attempted / pending

| Source | Status | Needed for |
|---|---|---|
| ENTSO-E A81 (contracted reserves per BSP) | Availability for Spain uncertain | H3 direct firm-level test |
| ESIOS (REE) balancing per-BSP | Token not yet obtained | Firm-level imbalance-cost regression |
| Firm-level storage commissioning (REE) | Not on transparency platform | H4 firm-level test |

### 6.3 Never collected

Qualitative operator interviews, CNMC complaint records, firm-level strategic disclosures. Out of scope.

## 7. Notebook index

Live notebooks in `explore/`; older exploratory nb01 + nb02 moved to `explore/archive/` (commit `544f5c6`).

| Notebook | Cells | Role | Key finding(s) |
|---|---:|---|---|
| [archive/01_market_statistics.ipynb](archive/01_market_statistics.ipynb) | — | Archived. Early structural statistics exploration. | Superseded. |
| [archive/02_bidding_behaviour.ipynb](archive/02_bidding_behaviour.ipynb) | — | Archived. Early bidding exploration. | Superseded by nb06 and (now) nb09. |
| [03_reform_narrative.ipynb](03_reform_narrative.ipynb) | 39 | Descriptive footprints: DA-IDA wedge, within-hour dispersion, ΔQ time-series, matched-wind placebo, OS-settled decomposition, cross-regime summary. | Big-4 low-wind $\|\Delta Q\|$ compresses from −271 to −78 MWh/unit-day at MTU15-IDA (descriptive). |
| [archive/04_imbalance_balancing.ipynb](archive/04_imbalance_balancing.ipynb) | 28 | (archived 2026-04-25) ENTSO-E balancing descriptive EDA. A85/A86/A84/A69 now formally event-studied in nb11; unique §7 forecast-revision analysis and inconclusive §8 H3 test preserved in archive. | $\|V^{\text{imb}}\|$ jumps +127% at ISP15, falls −30% at MTU15-IDA — system-level echo (formally re-estimated in nb11). |
| [05_engineering_decomposition.ipynb](05_engineering_decomposition.ipynb) | 12 | Tests engineering alternatives H1–H4. | All four alternatives rejected → behavioural residual. |
| [archive/06_attenuation_dashboard.ipynb](archive/06_attenuation_dashboard.ipynb) | 15 | (archived 2026-04-25) Bid-level conduct gap, Ito-Reguant $\hat\beta$, within-hour DA dispersion. Central CCGT-gap finding absorbed by unit FE in nb07 §9; bid-level behaviour covered more cleanly by nb13 §1. | CCGT conduct gap (Big-4 − Fringe) collapses 128-146 → 10 EUR/MWh at MTU15-IDA descriptively; is cross-sectional composition, not behavioural shift. |
| [07_main_regression.ipynb](07_main_regression.ipynb) | 33 | Formal DiD (§4 flagship, §5a saturated, §5b tech, §6 placebos, §8a refined control, §8b within-Big-4, §9 bid-level, §10 per-firm, §11 RI, §12 treatment-date sweep, §13 identification standards). | Saturated ISP15 coefficient +217 ($p<0.01$) robust to control refinement, but **not** an ATT under modern-DiD rigor. |
| [08_wind_iv.ipynb](08_wind_iv.ipynb) | 35 | Wind-IV (§3), Fringe placebo (§4), §6 robustness (a-d), §7 per-firm, §8 per-firm×tech, §9 nuclear-robustness, §10 placebo sweep. | GE × CCGT descriptive signed flip +11.8 → −16.1; not localised to ISP15 per §10 placebo. |
| [09_bid_shape_eda.ipynb](09_bid_shape_eda.ipynb) | 43 | Bid-structure EDA. §1-§10 build initial aggregate findings; §11 overturns H18 via unit-level MUEL/UFBG decomposition; §12 systematic unit-level audit of H10/H12/H14; §13 complex-conditions (MAV format artefact documented); §14 final synthesis. | Final state: six aggregate findings fail or narrow under unit audit. What survives: H15 (widespread non-use of 15-min granularity), H19 (UFBG withdrawal at MTU15-DA), H20 (La Muela persistent iceberg), H21 (complex-bidder CCGT DA exits at MTU15-IDA), H22 (Naturgy CCGT IDA slope flattening). Contribution reframed as documentation of firm-level **participation shifts** around reforms rather than within-unit behavioral changes. |
| [10_demand_side_eda.ipynb](10_demand_side_eda.ipynb) | 10 | Demand-side EDA (initial cut). Market structure, firm-group monthly trajectory, per-unit format-artefact check, regulatory-change diagnosis. | DA buy-offer "shift" at March 2025 diagnosed as **Rule 28.8 elimination** (CNMC 28-Feb-2025), not behavioral. Pre-reform DA buy data is artificial opportunity-cost bids. Future demand-side analysis must use cleared demand (pdbc/pdbce), IDA buy-side, or post-March-2025 DA only. Documentation-first methodology saved the project from another false-positive aggregate finding. |
| [11_outcome_audit.ipynb](11_outcome_audit.ipynb) | 18 | Systematic outcome audit: event-study four ENTSO-E balancing outcomes (A87 monthly financial balance — newly synced in this notebook; A86 |V_imb|; A85 price σ; A84 aFRR spread) across all four reform regimes. | **Four-way concordance at ISP15.** A87 net income (BRPs→TSO) jumps €38M→€160M/mo at ISP15, moderates to €72M post-MTU15-DA. A86 |V_imb| +5.1 GWh/d at ISP15 ($p<0.001$). A85 σ +40% at ISP15. A84 aFRR spread +35% at ISP15. All four independent outcomes show the same sharp-jump-then-moderation pattern predicted by the reform design. Unlike the firm-level claims, system-level claims require no comparable-control-group assumption. |
| [12_structural_markup.ipynb](12_structural_markup.ipynb) | 11 | Hortaçsu-Puller / Cournot-Nash structural Lerner indices per Big-4 firm × regime. Uses residual-demand elasticity derived from local supply-curve slope at clearing (`curva_pbc`, finite-difference ±€10/MWh). Share-held-fixed decomposition to isolate slope-driven vs compositional Lerner changes. | **Big-4 Lerner indices rise sharply across reforms, peak at DA60/ID15.** GE median Lerner 5.2% (pre-IDA) → **35%** (DA60/ID15) → 10% (DA15/ID15); even after share-held-fixed decomposition, GE Lerner goes 5.2% → 24.5% → 7.0%. IB shows smaller but directionally similar pattern; GN/HC patterns dominated by bilateral-contract reallocation (composition shift). Peak friction is the asymmetric-granularity window (15-min IDA + 60-min DA), closes at MTU15-DA. No identification assumption required — computed from Cournot FOC. |
| [13_bid_liquidity_revenue.ipynb](13_bid_liquidity_revenue.ipynb) | 12 | Three-section outcome expansion: P2 IDA sell-side bid-weighted offer prices (by firm × regime), P4 XBID liquidity (orders/trades/fill rate/price SD across regimes), P5 firm revenue (DA+IDA daily €M by firm × regime). | GE IDA wavg offer price €103→**€348** at IDA reform (+238%), moderates to €83 at MTU15-DA. XBID orders/hour ×15 from pre-IDA (921) to DA15/ID15 (13,868); fill rate drops 5.2%→2.7%. GE DA revenue nearly doubles (€3.2→€6.2M/day) via bilateral-contract intermediation; GN/HC revenue collapses (-65-75%) from Rule 28.8 elimination composition shift. Three independent outcomes (bid prices, liquidity, revenue) all peak at DA60/ID15. |

### 7.1 Supporting documents

- [`_identification_target.md`](_identification_target.md) — working identification narrative (Phase A1–A5 articulation, Phase B audit of nb07, Phase C decision, Phase D1–D13 wind-IV closure and revisions).
- `attic/theory_granularity_extension/granularity_extension.tex` — two-channel theoretical model. **DEPRECATED 2026-04-24**. Intensive-margin predictions not empirically supported; not load-bearing.
- `CLAUDE.md` — project-root coding conventions and data-family catalogue.

## 8. What this project has and has not established

### 8.1 Empirically established

1. **Reform-window descriptive footprints are real** and appear in multiple outcomes: DA-IDA wedge, within-hour price dispersion, $|\Delta Q|$ compression, bid-level conduct-gap collapse, system-level imbalance volumes. (`nb03`, `nb04`, `nb06`.)
2. **Four engineering alternatives are rejected.** The observed compression cannot be explained by profile-matching, ramp-lumpiness, reserve substitution, or storage internalisation. (`nb05`.)
3. **A new descriptive bid-structure finding** (nb09): Big-4 CCGT DA-offer granularity drops sharply at MTU15-IDA from 5-7 tranches to 1-2. Not yet stress-tested.
4. **Nuclear-dispatch wind-sensitivity pattern** (nb08 §9): Spanish nuclear $\Delta Q$ has a large wind-IV slope that collapses at ISP15, reproducible across ANAV and CNAT. Real but not a strategic-bidding mechanism.
5. **System-level reform signature** (nb11): four independent ENTSO-E balancing outcomes (A87 monthly financial balance newly synced; A86 imbalance volume; A85 imbalance-price σ; A84 aFRR up-down spread) all show sharp increases at ISP15 (2024-12-01) and gradual moderation through MTU15-IDA (2025-03-19) and MTU15-DA (2025-10-01). No comparable-control-group assumption needed — these are control-area aggregates. The joint concordance constitutes a system-level null rejection. See `_identification_target.md` Phase D14.
6. **Structural market-power rise peaks at DA60/ID15** (nb12, nb13). GE's implied Lerner rises from 5% pre-reform to 35% (peak) at the asymmetric-granularity window (MTU15-IDA pre MTU15-DA), moderating to 10% once MTU15-DA closes the reform sequence. GE's IDA offer price rises €103 → €348 → €83 across the same window. XBID order activity rises 15× pre-IDA to DA15/ID15 with fill rate falling. All three layers (structural markup, bid prices, liquidity) show the same DA60/ID15 peak signature, consistent with the reform sequence's asymmetric-granularity window creating strategic-friction opportunities that were closed at MTU15-DA.

### 8.2 Empirically *not* established (identification claims withdrawn or rejected)

1. **No causal identification of any specific reform's ATT on firm-level quantity outcomes.** TWFE-DiD fails parallel trends, anticipation, placebo (nb07); aggregate wind-IV fails ex-nuclear robustness (nb08 §9); GE×CCGT signed flip fails placebo localisation (nb08 §10).
2. **Ito–Reguant strategic-withholding mechanism is not cleanly pinned to any reform at the quantity level.** The theoretical framework is consistent with the patterns but neither channel has a cleanly identified empirical counterpart in $\Delta Q$ outcomes.

### 8.2.1 Withdrawn placebo claim (nb09 §11 correction)

What appeared to be a "placebo-validated iceberg surge" at MTU15-DA is a sample-composition artefact. Unit-level decomposition (§11) shows:
- **La Muela (MUEL) has been at 99% iceberg since mid-2021**, predating all 2024-25 reforms.
- **Naturgy's UFBG (Bugalleira) withdrew from XBID at MTU15-DA** (3,019 → 110 orders/month), removing its high-volume low-iceberg contribution from the aggregate.
- The aggregate "99%" post-reform iceberg rate is mechanical: only MUEL remains active; MUEL is always at 99%.

The §9 placebo test is still mathematically valid — it correctly detected a step pinned to MTU15-DA. The detected step is a **composition change (who trades in XBID)**, not a **behavioral change (how they trade)**. H18 as originally stated is withdrawn. The new open question is H19 (why did UFBG withdraw).

### 8.2.2 Lesson about aggregate-level findings

Every aggregate claim in the project that has been unit-decomposed has dissolved or been severely narrowed under decomposition: H5 (DiD pre-trends), H6 (wind-IV nuclear weighting), H7 (GE×CCGT placebo), H18 (MUEL/UFBG composition), now H12 (no within-unit change) and H10/H14 (mostly composition). **Five out of five aggregate findings have failed unit-level audit** (to varying degrees of severity).

### 8.2.3 Post-audit state of the thesis (after §12)

**Claims that survive audit or were never aggregate-level:**
- nb05 engineering-alternatives rejection (H1-H4). Unit-level by construction.
- H15 (within-hour bid similarity 80-99% identical). Measured per offer-hour, so unit-level by construction.
- H20 (La Muela persistent iceberg strategy since 2021). Unit-level observation.
- Descriptive documentation in nb03-nb04 (prices, imbalance volumes, wedge). Aggregate but not interpreted as behavioral shift.

**Claims that survive only at the unit level (narrowed from original aggregate framing):**
- H10 for PALOS3 specifically (Naturgy Palos 3: 13 → 1 tranches, genuine within-unit DA simplification).
- H22 (several Naturgy CCGTs modestly flattened IDA slopes within-unit: PBCN1, MALA1, CAMGI10, PALOS3). Smaller effect than aggregate claimed.
- H19 (UFBG withdrew from XBID at MTU15-DA). Real, pinned to reform.
- H21 (several IB/HC CCGT complex-bidder units exited DA activity at MTU15-IDA). Real, pinned to reform, firm-specific pattern.

**Claims withdrawn:**
- H12 (IDA tranche complexification): no within-unit shift exists. Aggregate is pure composition.
- H18 (PH iceberg surge): sample composition; MUEL always 99%, UFBG withdrew.
- H5, H6, H7: earlier identification candidates.

**What the thesis can say honestly:**

> Across the Spanish 15-min reform sequence of 2024-25, we document (1) engineering alternatives (profile-matching, ramp-lumpiness, reserve substitution, storage internalisation) cannot explain the observed $|\Delta Q|$ compression; (2) standard causal-identification strategies (TWFE-DiD, wind-IV, Ito–Reguant-style placebo localisation) do not pin a specific firm-level behavioral shift to any specific reform on quantity outcomes; (3) bid-structure indicators (tranche counts, slopes, iceberg use) aggregated at group × tech × regime level show reform-correlated shifts, but these shifts are dominated by *participation composition* (which units are active in which markets pre vs post) rather than by within-unit behavioral change; (4) 80-99% of Big-4 offer-hours replicate the same bid across all 4 intra-hour 15-min ISPs, suggesting firms did not strategically exploit the new 15-min granularity within hours; (5) two plant-level/firm-level patterns are reform-pinned: Naturgy's Bugalleira withdrew from XBID at MTU15-DA, and several Iberdrola/HC complex-bidder CCGTs exited DA at MTU15-IDA.

Items (1) and (4) are positive findings. Items (2), (3), (5) are honest documentations of the empirical limits: no clean behavioral ATT, mostly composition, and some firm-specific participation decisions around the reforms.

This is a modest but defensible set of claims for a master's thesis, grounded in unit-level audited evidence.

### 8.3 Known limitations

- Sample is short relative to DiD needs: panel starts 2023-12, reforms span 2024-06 to 2025-10.
- Per-firm × per-tech cells are small (GE×CCGT: 5 units; GN×ResHydro: 3 units).
- OMIE data is clearing-state; strategic *intent* requires modelling assumptions.
- Key external data not obtained: ESIOS per-BSP balancing, A81 reserves, firm-level storage.
- Randomization-inference construction did not rescue identification (pre-period non-stationarity).

## 9. Current state and next steps

**Current thesis framing.** Two-layer: (1) system-level reform signature from four concordant ENTSO-E outcomes (nb11, Phase D14), no firm-level identification required. (2) Firm-level narrow claims from nb07/nb08, with documented identification limits. See `_identification_target.md` Phase D14 for the refined statement and D11–D13 for what the firm-level analysis does and does not identify.

**Active direction (as of most recent commit):** `nb09` bid-structure EDA. The Big-4 CCGT tranche-count simplification at MTU15-IDA is a new, promising descriptive channel. Next cuts in nb09:

- §3 Price-tranche distribution: where along the price axis do firms place their tranches?
- §4 Within-hour bid similarity post-MTU15-DA: do firms submit 4 identical offer curves per hour or 4 different ones?
- §5 IDA tranche counts (does the DA simplification mirror an IDA complexification? — direct test of H12)
- §6 XBID iceberg orders and order-lifetimes
- §7 Complex-conditions / block-orders usage

**Outstanding open questions** (not commitments):

- Why does GN move oppositely to GE in the firm decomposition? (H9 sub-question)
- Is the nuclear anomaly (H8) a load-following artefact, a ΔQ scaling issue, or something else?
- Does the bid-structure simplification mirror complexification in IDA (H12)?

---

*Last updated: 2026-04-24 with nb11 outcome audit + A87 pipeline. Update this file as new hypotheses are tested, rejected, or raised.*

---

## 2026-04-25

- **Project restructure (Phase 1 of plan).** Built `/CLAIMS_LEDGER.md` (19 alive / 4 wounded / 13 dead, 36 rows). Added STATUS headers to 27 scripts in `scripts/analysis/` and STATUS cells to 8 active notebooks in `explore/`. Appended "Claim-status discipline" section to `CLAUDE.md`. Created `_modelling_track.md` (4 economic-model sections: Cournot, Allaz–Vila, Pigouvian, asymmetric-granularity friction). Rewrote `explore/README.md` to 30 lines.
- **Batch B (notebook moves).** `git mv` `07_main_regression.ipynb` and `08_wind_iv.ipynb` to `explore/archive/` with retraction cells. Both are dead per the Phase B audit (`_identification_target.md`).
- **Batch C (doc archives).** `git mv` `_modelable_patterns.md`, `_open_robustness_queue.md`, `_robustness_summary.md` to `explore/archive/` after content migration into the ledger.
- **Narrative cleanup.** Inserted canonical-headline cells in nb12 (lead with Spec 3 matched-price contrasts; demote raw 35% peak; flag CCGT-only-null caveat) and nb13 (excise +238% IDA jump; lead with B2 MTU15-DA collapse).
- **Identification target frozen.** `_identification_target.md` now thesis-appendix-grade; no further rewrites, only append-only D-additions if status changes warrant.
- **Phase 2 #1 — Allaz–Vila commitment-value test.** New script `allaz_vila_commitment_test.py`. Per-(firm, date, hour) panel: 280k obs Big-4 only. OLS of $\Delta Q_{\text{IDA}}$ on $q_{\text{DA}}$ by (firm, regime). **GE and HC show regime-dependent commitment-slope evolution consistent with Allaz–Vila** (slopes deepen at 3-sess, attenuate or flip toward zero at MTU15-DA). IB weakly directional. GN opposite — hydro-resale dynamic, not commitment-deterrence. R² small. **Recorded as new wounded claim F5**; Allaz–Vila modelling-track §2 status: wounded but not killed. Confound: MTU15-DA coincides with Rule 28.8 elimination (B5/F4).
- **Phase 2 #2 — Cournot supply-slope-tercile sort.** New script `cournot_slope_tercile.py`. Spec 3 matched-price Lerner contrasts conditional on pre-IDA slope tercile. Cournot prediction (elevation concentrates in flat-slope tercile): **non-monotonic for GE, opposite for IB, tiny-magnitude monotonic for GN/HC.** F1/F2 alive claims survive at the aggregate, but the *Cournot mechanism interpretation* doesn't extend cleanly to the slope cut for IB. **Recorded as new wounded claim F6**; modelling-track §1 status: wounded.
- **Phase 2 #3 — Asymmetric-granularity welfare proxy.** New script `asymmetric_granularity_welfare.py`. Monthly OLS of A87 / A86 / A85 / A84 with calendar-month FE and pre-IDA reference; cumulative excess across the 10-month asymmetric-granularity window (4 months ISP15 win + 6 months DA60/ID15); bootstrap null CI from pre-IDA residuals. **A87 cumulative excess = +€995.8M** vs same-calendar pre-IDA baseline; bootstrap null CI $[-213, +217]$M (observed ≈ 4.6× upper bound). A85 σ corroborates (+217 EUR/MWh-months, also significant). A86 underpowered at monthly aggregation (consistent with daily-level S2 from nb11). A84 (activation price level) insignificant — likely the wrong outcome (up–down spread would be more relevant). **Recorded as alive claim S6**; modelling-track §4 status: alive.
- **Phase 2 #4 — Pigouvian clean regression.** New script `pigouvian_clean_regression.py`. Multivariate OLS of $|\text{imp\_eur}|$ on $|\text{MWh}_{\text{seg}}|$ across 9 segments per regime, with month-of-year + hour-of-day FE; HC3 SE. **Per-segment marginal cost is order-of-magnitude heterogeneous and survives FE controls.** conv-RZ at €210–300/MWh across all post-ISP15 regimes; LIB free-market retailers ≤€37/MWh despite driving ~38% of imbalance volume. **Recorded as new alive claim S7**; modelling-track §3 status: alive.
- **Phase 2 #5 — Welfare-proxy refinement (A87 NET = A02 − A01).** Extended `asymmetric_granularity_welfare.py` with A01 expenses panel and net fiscal balance. **A87 NET cumulative excess = +€1,094.9M** in the 10-month asymmetric window vs same-calendar pre-IDA baseline; bootstrap null CI $[-90, +73]$M (observed ≈ 15× upper bound). A01 expenses cumulative excess is essentially flat (−€99M, not significant). The asymmetric window generates a fiscal *surplus* — not just gross transfer. **S6 ledger row updated** to cite the NET figure as canonical (€1.095B) with the A02/A01 decomposition.

## 2026-04-26

- **Salvage F6 (Cournot tercile).** Caught a Cournot-direction error in the original 2026-04-25 §1 write-up: Cournot predicts higher Lerner under *steep* supply (low MW/EUR), not flat. Corrected reading: **IB cleanly supports Cournot** (matched-price contrast falls monotonically T1 +0.126 → T3 +0.044 across slope terciles). GE partial (T1>T2 but T3 highest). GN/HC opposite. **F6 upgraded from wounded to partial-alive for IB**, anchoring F2 in a structural mechanism. Added hour FE to the tercile spec (results unchanged) and a tautology log-log check (γ = −1 by formula construction, as expected — confirms formula was applied right but is not a structural test).
- **Salvage F5 (Allaz–Vila portfolio split).** New script `allaz_vila_portfolio_split.py`. Hypothesis: Allaz–Vila applies in CCGT-margin hours (peak demand). Result: **GE and IB show the predicted slope evolution in peak hours; GE's off-peak slope goes the *opposite* way (slope deepens rather than attenuates)** — within-firm placebo for the mechanism. GN doesn't fit either partition; HC's sign-flip is bigger off-peak (opposite of CCGT-margin prediction). **F5 upgraded to partial-alive with explicit portfolio scope** — Allaz–Vila describes large mixed-portfolio firms in peak hours, not hydro-heavy or small firms.
- **Salvage W3 attempt — extensive-margin exit audit.** New script `ccgt_extensive_margin_exit.py`. **The named complex-bidder CCGT units (TAPOWER, SRI4R, ARCOS1, CTN4) DID NOT EXIT at MTU15-IDA** — all four remain active post-reform with comparable or higher cleared volume. This contradicts the W3 wound's "~95% composition (exit of complex-bidder units)" rationale. **W3 wound rationale revised to 'unverified — pending direct per-unit tranche-count comparison'** — the within-unit simplification might be more substantial than nb09 §12 indicated. Separately, a real Fringe-driven exit pattern was identified: 23 small Fringe units + 3 HC units exit DA, ~6.8% of pre-reform volume (~5 GW). Big-4 zero exits. **Recorded as new alive descriptive claim D4.**
- **W3 verification — within-unit tranche count.** New script `ccgt_within_unit_tranche_count.py`. Direct comparison on the four named units: tranches-per-period **stable or increased** (TAPOWER 5.80→7.13, SRI4R 5.94→5.24, ARCOS1 5.49→6.74, CTN4 6.55→11.28). None simplified bids; three complexified. Combined with the no-exit finding, **W3 is killed**: there is no behavioural mechanism (within-unit *or* extensive-margin) that explains the aggregate "5-7 → 1-2 tranches" drop. The aggregate is almost certainly a MAV-format-change parser artefact. **W3 status: dead (renamed X14).**
- **New finding from the same data — B8 (alive behavioural).** The verification produced an *opposite-direction* finding worth recording: Big-4 CCGT strategic bidders **complexify** their bids per ISP at MTU15-IDA (1.23×–1.72× rise in tranches-per-period for 3 of 4 units). Total bid rows per day rise 1.9–3.7× from combined effect of more periods × more tranches per period. IO-interesting as a behavioural response to finer market-clearing granularity: strategic firms invest in finer-grained price ladders, not coarser ones. **nb09 STATUS cell updated** to reflect the W3 retraction and B8 addition.
- **B8 robustness — firm-aggregate + unit-level disambiguation.** Two new scripts (`bid_complexity_panel.py` for firm-aggregate, `bid_complexity_unit_level.py` for within-unit) extend the named-units check to all Big-4 + Fringe-survivor units. Firm-aggregate: IB +52%, Fringe-surv +29%, GE −33%, GN −51%, HC −42%. Unit-level (correct measure of within-unit bid complexity): **IB strongly complexifies** (5.49 → 8.73, ratio 1.59×); HC stable (5.93 → 5.83); GE flat at low absolute ~2 tpp; **GN simplifies** (6.46 → 3.22); Fringe-survivors simplify (5.69 → 3.07). **B8 reframed: IB-specific, not generic Big-4.** Likely reflects IB's specific strategic position (largest CCGT fleet on marginal supply step among Big-4). The cross-firm heterogeneity is itself the IO content: finer granularity does not mechanically drive complexification; only firms with strategic-marginal CCGT capacity invest in finer price ladders.
- **Synthetic-firm Lerner method (Ciarreta-Espinosa 2010 replication).** New three-script pipeline (`synthetic_firm_matching.py`, `synthetic_firm_clearing.py`, `synthetic_firm_aggregate.py`). Plant-pair matching: 62/106 Big-4 plants matched to same-tech same-capacity Fringe plants (CCGT 36/36, Hydro 26/26; Nuclear 44/44 unmatched, kept actual per Ciarreta-Espinosa convention). Synthetic supply built by replacing each L's offer with K_L/K_S × S's offer; auction re-cleared in DuckDB per ISP for 22 months (June 2024 – April 2026). **Result: post-MTU15-IDA Big-4 market-power index = 13.79% at DA60/ID15, 12.17% at DA15/ID15** (mean +€7.86 / +€9.48 per MWh respectively). Total transfer ≈ €833M across the 14-month post-IDA window. **Two caveats** flagged in the F7 ledger row: pre-2025-03-19 bid data is 0-padded so the method is only interpretable post-MTU15-IDA; complex offers (block orders, interconnection rationing) are excluded so absolute price levels differ from published OMEL by ~€23 — the DIFFERENCE between actual and synthetic is unbiased (Ciarreta-Espinosa's own caveat). **Two-decade replication**: 2002-2005 Ciarreta-Espinosa found ~21% Big-4 market power; we find ~13% in 2025-2026 — consistent with tighter regulation since 2007. **F7 added as alive structural-firm claim** providing an independent measurement of F1/F2 (matched-price Lerner) — does not depend on the Hortaçsu-Puller formula.

## 2026-04-27

- **Vertical-integration test (Ciarreta–Espinosa Fig 5 replication for 2024–2026).** New script `vertical_integration_net_position.py`. Per-firm monthly net seller position = sell-side cleared MWh − buy-side cleared MWh from pdbce. Post-Rule-28.8 (2025-03 onwards): **GE +2,316 GWh/mo, IB +958 GWh/mo, GN +169, HC +69.** GE is **2.4× more net seller than IB**, the *opposite direction* required for vertical integration to explain why IB > GE in market-power tests (F1/F2/F5/F6/B8/F7). **Vertical integration is therefore NOT the mechanism behind the IB-canonical pattern.** Replicates Ciarreta–Espinosa's 2002–2005 finding (vertical integration did not explain EN > IB then either) — two-decade replication of the same negative result. **D5 added** as alive descriptive claim. The remaining mechanism candidates for the IB-canonical pattern are portfolio composition (IB's CCGT fleet on the marginal supply step), strategic conduct, or operational complexity. Updated `_modelling_track.md` §0 with this rule-out paragraph.
- **Synthetic-firm per-firm decomposition (Ciarreta–Espinosa Tables 3-4 style).** New script `synthetic_firm_per_firm.py`. Builds per-firm synthetic supply (replace ONLY firm F's plants with same-tech Fringe matches; keep other firms actual) and re-clears, attributing the F7 joint €833M transfer to each Big-4 firm. **Result: IB carries ~€820M (+€8.80/MWh, +12.4%); GE −€0.24/MWh (−€23M, near-zero); GN +€0.15/MWh; HC +€0.76/MWh.** IB alone accounts for ~98% of the joint Big-4 market-power transfer. **GE has near-zero independent price impact** — replacing GE's plants with synthetic Fringe barely shifts the clearing price. **Sharper structural-IO reading: IB is the marginal price-setter; GE/GN/HC are price-receivers benefiting from the high prices IB sets.** This is the 4th independent IO test pointing at IB (after F1/F2 Lerner, F5 Allaz–Vila, F6 Cournot, B8 bid complexification). The thesis claim crystallises: **the post-MTU15-IDA joint Big-4 €833M transfer is essentially IB's market-power rent.** F7 ledger row updated with the per-firm decomposition; §0 updated with the price-setter-vs-price-receiver framing.
- **Bid-curve concentration test (mechanism check on F7 per-firm).** New script `marginal_price_step_concentration.py`. Per-firm CCGT tranche price-gap distribution from clearing price, post-MTU15-IDA: **IB median |gap| = €75; GE median |gap| = €100. IB mean |gap| = €96; GE mean |gap| = €1,308**. GE submits many absurdly-high block tranches that never clear (capacity-show signals); IB's tranches sit systematically closer to actual market clearing. **Mixed-direction signal**: IB has lower % MWh within ±€5 of clearing (4.4% vs 7.2%), but that narrow-band metric is biased by tranche count (IB has 7× more CCGT tranches). The median/mean |gap| is the cleaner cross-firm statistic and supports IB being structurally closer to the margin. **Documented as supporting evidence in F7 row; no new alive claim.** F7 per-firm remains the cleanest mechanism evidence.
- **Hour-of-day decomposition of IB price-setting (F7).** New script `synthetic_firm_hour_of_day.py`. Result: **IB's market-power markup is remarkably persistent across all hours of the day** — €7–11/MWh almost everywhere. Peak vs off-peak ratio ≈ 0.98×, basically equal. Hour-by-hour: h7 morning ramp (p=€145) has only €7.6 IB markup despite high price; h13–16 midday solar peak (p=€28) has €7.5 IB markup; h19–24 evening peak (p=€66–78) has highest IB markup at €10.3. The Spanish 2025 duck curve has shifted when "peak" is — solar dominates midday, evening h19–22 is the new CCGT-margin peak. **Interpretation: IB's price-setting power is structural / persistent, not opportunistic on demand peaks.** It activates whenever IB's CCGT is on the margin (which is most hours). Solar dampens slightly but doesn't eliminate the markup. **No clean alive ledger row** — the simple "peak-hour Cournot" story doesn't fit. Hour-of-day CSV preserved at `data/derived/results/synthetic_firm_hour_of_day.csv` for thesis-writing reference.
- **Per-IB-unit decomposition (major mechanism pivot).** New script `synthetic_firm_per_unit_ib.py`. Replace each of IB's 17 matched plants individually (CCGT 10 + Hydro 7) with its synthetic Fringe match; re-clear; attribute price impact per unit. **Result: IB's ~€820M price-setting is HYDRO-DOMINATED — Hydro 64% (~€530M, led by TAMEGA +€203M, SIL +€103M, DUER +€92M, TAJO +€90M); CCGT 36% (~€294M, led by CTJON2 / ARCOS2 / STC4 ~€89M each). Named B8 complex-bidder CCGT units (TAPOWER, ARCOS1, CTN3, CTN4) have near-zero independent price impact (~€8M total) despite 1.59× bid complexification.** **Mechanism re-interpretation**: IB's market power flows through dispatchable hydro market power, not CCGT Cournot quantity-setting. The B8 bid-complexification finding is real as a bid-structure observation but does not translate to price-setting for the named CCGT units. **Thesis-relevant lineage broadens** to include the hydro market-power literature (Bushnell 2003 on hydro-thermal competition, Reguant on Spanish balancing, Crampes–Moreaux on water values). **Caveat**: hydro plant-pair matching is harder than CCGT (storage / reservoir / ramp differ across plants), so some of the hydro-attributed power could be matching artefact; magnitude (~€530M IB hydro alone) is too large to be entirely noise. **F7 ledger row updated**, modelling-track §0 updated with hydro-dominated mechanism story. **Strategic chapter pivot**: structural-firm story is now "Iberdrola dispatchable-hydro market power" rather than "Iberdrola CCGT Cournot".
- **Hydro strategic-dispatch test (Bushnell signature, plant-matching-free).** New script `hydro_strategic_dispatch.py`. Direct test of the Bushnell-style strategic-hydro-dispatch hypothesis: per-firm, what fraction of hydro DA cleared MWh is dispatched in within-month top-price-quartile (Q4) hours? **Result: IB hydro Q4 share = 63.1% post-MTU15-IDA, Fringe hydro Q4 share = 42.0% — a +21pp gap.** GE hydro Q4 share is only 27.0% (non-strategic run-of-river). IB Q4 concentration **intensified at MTU15-IDA** (pre-reform 56.3% → post 63.1%), consistent with the F7 finding that IB's market-power transfer accumulates in the post-reform window. **This test is plant-matching-free** — both IB and Fringe face the same hourly prices, so the cross-firm gap directly identifies IB's strategic dispatch behaviour, not a matching artefact. **F8 added as alive structural-firm claim.** The mechanism story for the IB-canonical pattern is now triangulated by three converging tests: F1/F2 (IB matched-price Lerner), F7 per-IB-unit (IB hydro 64% of transfer), F8 (IB hydro Q4 concentration +21pp vs Fringe). Anchors the Bushnell 2003 / Crampes–Moreaux water-value lineage in the thesis.
- **Coherence audit of all 26 alive claims.** New working note `_coherence_audit.md` checks the alive claims tell ONE consistent story. Five apparent tensions identified (F1 GE Lerner +0.318 vs F7 GE near-zero; F5 CCGT-Allaz–Vila vs F7 hydro pivot; F6 Cournot vs F7 hydro mechanism; D5 GE > IB net seller vs IB > GE market power; B8 IB CCGT complexify vs F7 zero impact for those units), all resolvable with careful framing — F1 measures rent-collection; F7 measures price-setting; both true. Two-decade Ciarreta–Espinosa replication of IB > GE cross-firm pattern under different mechanism explanations across periods. Drafted publishable thesis-claim paragraph; flagged five committee-likely viva questions and prepared defenses.
- **Blackout-confound check.** User flagged that the 2025-04-28 Iberian blackout (REE "operación reforzada" — forced increased CCGT/nuclear commitment under P.O. 3.2) is a confound for any DA60/ID15-window claim because ~5 of the 6 DA60/ID15 months are post-blackout. New script `blackout_split.py` partitions F7 (synthetic-firm IB transfer), F8 (IB hydro Q4 concentration), and S6 (A87 NET fiscal surplus excess) across PRE-blackout DA60/ID15 (~6 wks), POST-blackout DA60/ID15 (~5 mo), and DA15/ID15 (post-MTU15-DA, ~3.5 mo). **Three findings**: (1) **F8 fully robust** — IB Q4 share remarkably stable at 63.1% / 63.6% / 67.2% (gap +20.4 / +21.8 / +22.4pp). The Bushnell strategic-dispatch concentration is structural / regime-invariant; neither reform nor blackout creates it. (2) **F7 reframed** — DA60/ID15 PRE-blackout has the highest *relative* markup (+48% on €12.51 mean prices, ~€38M total), POST-blackout DA60/ID15 +11% on €68.42 (~€184M), **DA15/ID15 +12.3% on €77.90 (~€598M = 73% of total IB transfer)**. The €820M absolute is a regime-weighted total dominated by post-MTU15-DA price levels, NOT a pure asymmetric-granularity rent. The original §0 framing "the asymmetric-granularity window generates the rent" is **partially retracted** in absolute terms (still holds for relative markup). (3) **S6 robust + clean granularity-friction signature** — clean April-2025 PRE-blackout +€75.7M for one month; May-Sep POST-blackout DA60/ID15 €93.5M/mo (+24% above clean April — modest blackout amplification, not source); **Oct-Dec DA15/ID15 collapses to €7.4M/mo (8% of DA60/ID15 level)** even though the blackout is still in effect. The post-MTU15-DA collapse decisively separates granularity friction from blackout effects. **Two-channel synthesis**: at the system layer (S6), asymmetric granularity creates the fiscal cost shift that disappears on re-symmetrisation; at the firm layer (F7/F8), IB market-power rent is structural / regime-invariant. F7, F8, S6 ledger rows updated; modelling-track §0 expanded with the two-channel reading; coherence audit revised with the post-blackout thesis-claim paragraph.
- **ESIOS taxonomy expansion + per-BSP aFRR cross-market check (F9).** Five new ESIOS public archives integrated: liquicierre (id=17, 2015→2024-12), liquicierresrs (id=203, 2024-11→now), totalrp48preccierre (id=28, 2015→now), ree_balancing_bids (id=181, mFRR bid-level), curvas_ofertas_afrr (id=234, aFRR offer curves). Generic `sync_archive_loop` helper in `esios_common.py`; per-archive sync scripts now thin configurations. **Per-BSP verification** (was thought gated per project memory): liquicierre/liquicierresrs serve per-BSP aFRR settlement publicly at PT15M resolution. ~23 BSPs initially, expanded to 34 over time. Field renamed B1 → BSP at 2024-11-22 reform. Memory note `project_esios_pending.md` corrected. **BSP↔firm mapping problem**: the 3-letter REE BSP codes (IGN, IMA, IGR, END, GN, HC, EV) don't appear in the ESIOS sujetos-del-mercado export the user added (data/external/esios/, 4 reference files: 753 SMs, 3745 UPs, 6461 UFs, 31 explicit-auction participants). IGN exact-prefix-matches OMIE IGNU=Iberdrola Generación Nuclear; IMA/IGR pattern-fit Iberdrola family with no plausible non-IB owners of ~128 GWh/post-MTU15-DA-day aFRR provision. Adopted dual-mapping reading: LIBERAL (IB={IMA,IGR,IGN}) + CONSERVATIVE (IB={IGN}). **F9 result (LIBERAL)**: IB aFRR share = 31.8% pre-IDA → 39.1% 3-sess (peak) → 32.9% ISP15-win → 27.1% DA60/ID15 → 26.7% DA15/ID15. **OPPOSITE direction from F7 in DA**: IB aFRR share has fallen 12pp since the 2024-06 reform peak; Fringe rose from 11% to 27%. **Reading**: F7 IB-dominance is DA-market-specific; aFRR is structurally more competitive and becoming more so. F9 strengthens F7 by ruling out generic-firm-dominance interpretations. **F9 added as alive structural-firm claim.** Modelling track §0 updated with cross-market contrast.
- **S7 anchor cross-check.** New script `s7_rz_anchor_validation.py` validates S7 (per-segment marginal imbalance cost) against directly-published RZ closure prices in `totalrp48preccierre` (TipoRedespacho 61 = system-security redispatch under P.O. 3.2). Regime-weighted means: pre-IDA €75.8/MWh, ISP15-win €87.8 (peak), DA60/ID15 €33.0 (depressed by post-blackout high-volume forced commitment), DA15/ID15 €71.6. **The directly-published price (~€75/MWh) is far below the S7 structural figure (€210–300/MWh)** — but they measure DIFFERENT concepts: published price is the REE→generator transfer for redispatch instructions, structural S7 is the per-MWh social cost imposed on the system under the current allocation rule. No contradiction; S7 row footnoted with the distinction. **Side-finding**: RZ activations roughly *doubled* post-IDA (269 GWh/mo pre-IDA → 414–502 GWh/mo across all post-IDA regimes including post-MTU15-DA, suggesting structural reform-induced redispatch escalation independent of blackout response).
