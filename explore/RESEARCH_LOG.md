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

---

## Diary

Dated diary entries (2026-04-25 through current) moved to [`RESEARCH_DIARY.md`](RESEARCH_DIARY.md) on 2026-04-28 to keep this file focused on the structured hypotheses register and notebook map. New analyses are appended to the diary, not here. This file is updated only when a new hypothesis is added to the register (§4) or methods/data sources change (§5/§6).
