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
| **H10** | Big-4 CCGT DA bid granularity (tranches per offer-period) declines at MTU15-IDA (2025-03-19) | [FLAGGED — new descriptive finding, not stress-tested] | `nb09 §1/§2`. Median tranches drops from 5-7 (2024) to 1-2 (Mar 2025 onward). March 2025 has 106 offer-periods; sample adequate. Monthly series shows a step, not a gradual drift. Commit `c51c756`. Caveats: Spring 2025 has low CCGT activity overall; whether this is bid-structure shift or unit-composition shift is unresolved. |
| **H11** | Big-4 hydro DA bid granularity *increases* across the reform sequence (opposite of CCGT) | [FLAGGED — descriptive] | `nb09 §1/§2`. Reservoir hydro: 2 → 3. Pumped hydro: 2 → 3 at MTU15-DA. |
| **H12** | Big-4 shifts bid-structure complexity from DA to IDA once MTU15-IDA provides matching 15-min intraday tools | [SUPPORTED — descriptive, refined by H13] | `nb09 §3/§4`. Big-4 CCGT IDA median tranches go 1 → 2 → 4 → 3 → 5 across regimes, mirror-imaging the DA decline (7 → 4 → 4 → 2 → 2). DA+IDA sum stays in the 5-9 range across all regimes. Monthly time series: at MTU15-IDA (2025-03) DA drops 5→1 and IDA jumps 1→5 simultaneously. Specific to Big-4 CCGT — not hydro, not nuclear, not Fringe. Descriptive only; no causal identification attempted. |
| **H13** | The new Big-4 CCGT IDA tranches are strategic-spread bids (wide price range) rather than quantity-precision subdivisions (narrow price range) | [REJECTED] | `nb09 §5`. Median IDA tranche price range per offer-period stays in 0-3 EUR/MWh across all regimes for Big-4 CCGT, despite tranche count rising from 1 to 5. P75 range is 4-6 EUR/MWh, P90 is 7-12 (pre-reform 6-sess P90=34 is the exception, contracted dramatically at IDA reform). On clearing prices of ~90-140 EUR/MWh, the per-offer range is <5%. The complexity shift is quantity-precision-increasing, not price-spread-increasing. Implication: the H12 pattern is **not** a classic Ito–Reguant strategic-withholding signature. |
| **H14** | Big-4 CCGT IDA supply function slope (Chang 2026-style elasticity measure) shifts across the reform sequence | [SUPPORTED — descriptive] | `nb09 §6`. Median slope = (total offered qty) / (price range) per offer-period. Big-4 CCGT: 4.24 (6-sess) → 9.27 (3-sess) → 7.77 (ISP15) → **13.16 (DA60/ID15)** → 13.28 (DA15/ID15). A 3× flattening across the reforms. Fringe CCGT stays at 10-17 throughout. Big-4/Fringe slope ratio inverts at MTU15-IDA (0.38 pre-reform → 1.25 post), i.e. Big-4 curves become *flatter* than Fringe. Direction **opposite** to Chang's Australian finding (3.79 → 2.45) but reforms are structurally different. Plausible Spanish mechanism: 15-min granularity reduces economic weight per offer-period, making elastic curves less costly; strategic-rent advantage of steep curves dissipates. |
| **H15** | Firms use MTU15-IDA / MTU15-DA to submit *sub-hourly differentiated* bids (intra-hour strategic heterogeneity) | [REJECTED for most tech × market cells] | `nb09 §7`. For each offer-hour post-reform, count distinct bid signatures across the 4 intra-hour 15-min ISPs. Rate of all-4-identical: Big-4 CCGT 85% (IDA), 92% (DA); Big-4 Pumped Hydro 93% / 99%; Big-4 Nuclear 88% / 82%. Only Big-4 Reservoir Hydro in DA post-MTU15-DA (68% identical, i.e. 32% differentiated) and Fringe Pumped Hydro in IDA (60%) use the granularity meaningfully. **For 80-99% of offer-hours, firms submit identical bids across the 4 intra-hour ISPs — the 15-min granularity is not strategically exploited.** Refines H12/H14 interpretation: bid-structure changes happen at the hourly level, then get replicated across the 4 ISPs. |

### 4.4 Theoretical rationalisation

| ID | Proposition | Status |
|---|---|---|
| **H_theory** | Two-channel model ($\alpha_r$ imbalance-gaming + $\Phi(\lambda,\beta,b_{21})$ ramp-thinness) rationalises the descriptive pattern | [CONSISTENT, not separately identified] `theory/granularity_extension.tex`. Structurally coherent; neither channel has a cleanly identified empirical counterpart after nb07/nb08 rigor checks. |

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
| [04_imbalance_balancing.ipynb](04_imbalance_balancing.ipynb) | 28 | ENTSO-E balancing dynamics. A85 prices, A86 volumes, A84 activated, A69 forecast, A74 revisions, §8 H3 test. | $\|V^{\text{imb}}\|$ jumps +127% at ISP15, falls −30% at MTU15-IDA — system-level echo. |
| [05_engineering_decomposition.ipynb](05_engineering_decomposition.ipynb) | 12 | Tests engineering alternatives H1–H4. | All four alternatives rejected → behavioural residual. |
| [06_attenuation_dashboard.ipynb](06_attenuation_dashboard.ipynb) | 15 | Bid-level conduct gap, Ito–Reguant $\hat\beta$, within-hour DA dispersion post-MTU15-DA. | CCGT conduct gap (Big-4 − Fringe) collapses 128-146 → 10 EUR/MWh at MTU15-IDA descriptively; absorbed by unit FE in nb07 §9. |
| [07_main_regression.ipynb](07_main_regression.ipynb) | 33 | Formal DiD (§4 flagship, §5a saturated, §5b tech, §6 placebos, §8a refined control, §8b within-Big-4, §9 bid-level, §10 per-firm, §11 RI, §12 treatment-date sweep, §13 identification standards). | Saturated ISP15 coefficient +217 ($p<0.01$) robust to control refinement, but **not** an ATT under modern-DiD rigor. |
| [08_wind_iv.ipynb](08_wind_iv.ipynb) | 35 | Wind-IV (§3), Fringe placebo (§4), §6 robustness (a-d), §7 per-firm, §8 per-firm×tech, §9 nuclear-robustness, §10 placebo sweep. | GE × CCGT descriptive signed flip +11.8 → −16.1; not localised to ISP15 per §10 placebo. |
| [09_bid_shape_eda.ipynb](09_bid_shape_eda.ipynb) | 23 | Bid-structure EDA. §1-§2 DA tranche counts and monthly series; §3-§4 IDA tranches + DA+IDA overlay (H12); §5 IDA tranche-price range (H13); §6 IDA supply-function slope (H14, Chang 2026-style); §7 within-hour bid similarity (H15). | H10 Big-4 CCGT DA tranches drops 5-7 → 1-2 at MTU15-IDA. H12 SUPPORTED: DA simplification mirrored by IDA complexification. H13 REJECTED: new IDA tranches are quantity-precision, not strategic spread. H14 SUPPORTED: Big-4 CCGT IDA slope flattens 3×; Big-4/Fringe gap inverts at MTU15-IDA. H15 REJECTED for most cells: 80-99% of Big-4 offer-hours have identical bids across 4 intra-hour ISPs — firms mostly do NOT use 15-min granularity for sub-hourly differentiation. |

### 7.1 Supporting documents

- [`_identification_target.md`](_identification_target.md) — working identification narrative (Phase A1–A5 articulation, Phase B audit of nb07, Phase C decision, Phase D1–D13 wind-IV closure and revisions).
- `theory/granularity_extension.tex` — two-channel theoretical model rationalisation.
- `CLAUDE.md` — project-root coding conventions and data-family catalogue.

## 8. What this project has and has not established

### 8.1 Empirically established

1. **Reform-window descriptive footprints are real** and appear in multiple outcomes: DA-IDA wedge, within-hour price dispersion, $|\Delta Q|$ compression, bid-level conduct-gap collapse, system-level imbalance volumes. (`nb03`, `nb04`, `nb06`.)
2. **Four engineering alternatives are rejected.** The observed compression cannot be explained by profile-matching, ramp-lumpiness, reserve substitution, or storage internalisation. (`nb05`.)
3. **A new descriptive bid-structure finding** (nb09): Big-4 CCGT DA-offer granularity drops sharply at MTU15-IDA from 5-7 tranches to 1-2. Not yet stress-tested.
4. **Nuclear-dispatch wind-sensitivity pattern** (nb08 §9): Spanish nuclear $\Delta Q$ has a large wind-IV slope that collapses at ISP15, reproducible across ANAV and CNAT. Real but not a strategic-bidding mechanism.

### 8.2 Empirically *not* established (identification claims withdrawn or rejected)

1. **No causal identification of any specific reform's ATT.** TWFE-DiD fails parallel trends, anticipation, placebo (nb07); aggregate wind-IV fails ex-nuclear robustness (nb08 §9); GE×CCGT signed flip fails placebo localisation (nb08 §10).
2. **Ito–Reguant strategic-withholding mechanism is not cleanly pinned to any reform.** The theoretical framework is consistent with the patterns but neither channel has a cleanly identified empirical counterpart.
3. **No firm-level strategic behaviour change has been causally attributed to a specific reform date.**

### 8.3 Known limitations

- Sample is short relative to DiD needs: panel starts 2023-12, reforms span 2024-06 to 2025-10.
- Per-firm × per-tech cells are small (GE×CCGT: 5 units; GN×ResHydro: 3 units).
- OMIE data is clearing-state; strategic *intent* requires modelling assumptions.
- Key external data not obtained: ESIOS per-BSP balancing, A81 reserves, firm-level storage.
- Randomization-inference construction did not rescue identification (pre-period non-stationarity).

## 9. Current state and next steps

**Current thesis framing.** Descriptive + negative-identification. See `_identification_target.md` Phase D13 for the detailed statement.

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

*Last updated: 2026-04-24 after commit `c51c756`. Update this file as new hypotheses are tested, rejected, or raised.*
