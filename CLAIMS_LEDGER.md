# Claims Ledger

Single source of truth for empirical claims in this project. One row per claim. When a claim's status changes, update the row in place (do not delete) and follow the discipline cycle in `CLAUDE.md` § "Claim-status discipline".

**Last full audit:** 2026-04-25.

## Conventions

**Status values**

- **alive** — passed all documented robustness checks; safe to cite in thesis prose.
- **wounded** — survives in narrowed form (smaller magnitude, narrower scope, or one robustness check still pending). Cite only with caveat.
- **dead** — retracted or contradicted. Do not cite as positive result; may appear in identification appendix as "attempted but failed".

**Layers**

- **system** — control-area aggregate (ENTSO-E A87/A86/A85/A84). No firm-level identification assumption.
- **structural-firm** — Cournot-Nash Lerner / structural firm-level outcomes.
- **behavioural** — bid-level / liquidity / revenue descriptive at firm or unit level.
- **identification** — TWFE-DiD, wind-IV, RD specifications targeting causal ATTs.
- **descriptive** — narrative-grade pattern, no causal claim attached.

**Evidence pointers**

- *Evidence script* — file in `scripts/analysis/` whose output supports the claim. `(none)` if claim is observational from raw parquet or composite of other claims.
- *Evidence notebook* — `nbXX §Y` or `_robustness_summary.md §Z`. For dead claims that were demoted, the notebook may now live in `explore/archive/`.

---

## Alive claims (18)

| ID | Layer | Claim | Evidence script | Evidence notebook | Date_changed |
|---|---|---|---|---|---|
| **S1** | system | A87 net income jumps €38 → €160 → €72M/mo across regimes (ISP15 spike + MTU15-DA moderation). | `esios_a87_cross.py`, `a87_reserve_decomposition.py` | nb11 §5 | 2026-04-25 |
| **S2** | system | A86 absolute imbalance volume rises +5.1 GWh/d at ISP15 (regime dummies, p<0.001, n=873). | (direct from ENTSO-E A86) | nb11 §5 | 2026-04-25 |
| **S3** | system | A85 imbalance-price volatility (σ) increases +40% at ISP15. | (direct from ENTSO-E A85) | nb11 §5 | 2026-04-25 |
| **S4** | system | A84 aFRR reserve spread rises +35% at ISP15. | (direct from ENTSO-E A84) | nb11 §5 | 2026-04-25 |
| **S5** | system | Four ENTSO-E outcomes show concordant jump at ISP15 + moderation at MTU15-DA (joint null rejection). | (composite of S1–S4) | nb11 §5; `_identification_target.md` D14 | 2026-04-25 |
| **S6** | system | A87 NET fiscal balance (A02 income − A01 expenses) cumulative excess across the 10-month asymmetric-granularity window (ISP15 win + DA60/ID15) is **+€1,094.9M** vs same-calendar pre-IDA baseline; bootstrap null CI [-90, +73]M; observed ≈15× upper bound. Decomposition: A02 income +€995.8M (significant); A01 expenses −€99.2M (not significant) — the asymmetric window is a fiscal-surplus generator, not a "system is just larger" effect. | `asymmetric_granularity_welfare.py` | `_modelling_track.md` §4 (Run + Refinement 2026-04-25) | 2026-04-25 |
| **S7** | system | Per-segment marginal imbalance cost is order-of-magnitude heterogeneous: conv-RZ €210–300/MWh vs LIB free retailers ≤€37/MWh, despite LIB driving ~38% of imbalance volume vs conv-RZ ~13%. Survives month-of-year + hour-of-day FE. The current uniform-allocation rule is non-Pigouvian. | `pigouvian_clean_regression.py` | `_modelling_track.md` §3 (Run 2026-04-25) | 2026-04-25 |
| **F1** | structural-firm | GE Lerner +0.318 above pre-IDA at DA60/ID15, conditional on clearing-price level (Spec 3, price-bin FE). | `seasonal_correction_lerner.py`, `build_firm_lerner_panel.py`, `build_supply_slope_panel.py` | nb12 §7; `archive/_robustness_summary.md` §7 | 2026-04-25 |
| **F2** | structural-firm | IB Lerner +0.135 above pre-IDA at DA60/ID15, conditional on clearing-price level (Spec 3). | `seasonal_correction_lerner.py` | nb12 §7; `archive/_robustness_summary.md` §7 | 2026-04-25 |
| **F3** | structural-firm | GE/IB matched-price Lerner partially reverse at MTU15-DA (GE +0.080, IB −0.028 vs pre-IDA). | `seasonal_correction_lerner.py` | `archive/_robustness_summary.md` §7 | 2026-04-25 |
| **F4** | structural-firm | GN/HC Lerner shifts dominated by Rule 28.8 bilateral-contract reallocation at 2025-03-19, not strategic. | `seasonal_correction_lerner.py` | nb10; `archive/_robustness_summary.md` §7 | 2026-04-25 |
| **B1** | behavioural | GE IDA bid-shading peaks +€250 (3-sess) and +€218 (ISP15) above pre-IDA mean €22; normalises to −€12 at MTU15-DA. | `hhi_withholding_bidshading.py` | nb13; `archive/_robustness_summary.md` §13 | 2026-04-25 |
| **B2** | behavioural | GE IDA sell-side offer prices collapse to 8-year-low at MTU15-DA (€99.6/MWh GE, €108.6/MWh IB; May–Sep 2025). | `bid_function_shape.py`, `build_firm_bid_revenue.py` | nb13 §1 (post-recalibration); `archive/_robustness_summary.md` §8 | 2026-04-25 |
| **B3** | behavioural | XBID liquidity orders/hour rises 15× pre-IDA → DA15/ID15 (921 → 13,868). | `build_xbid_liquidity.py` | nb13 P4 | 2026-04-25 |
| **B4** | behavioural | XBID trade-price σ peaks at DA60/ID15 (€11.3/MWh), moderates to €8.5/MWh at MTU15-DA. | `build_xbid_liquidity.py` | nb13 P4 | 2026-04-25 |
| **B5** | behavioural | Rule 28.8 elimination concentrates DA revenue at 2025-03-19: GE +93%, GN −65%, HC −75%. Mechanical, not strategic. | `build_firm_bid_revenue.py` | nb10; nb13 P5 | 2026-04-25 |
| **B6** | behavioural | Forecast-error → imbalance pass-through R² jumps 0.001–0.06 → 0.305 in DA60/ID15. | `passthrough_forecast_imbalance.py` | `archive/_modelable_patterns.md` P1 (will move to `_modelling_track.md`) | 2026-04-25 |
| **B7** | behavioural | France DA prices flat across Spanish reform dates — Spain-specific cross-country placebo. | `sync_parse_a44_fr.py`, `france_da_placebo.py` | `archive/_robustness_summary.md` §10 | 2026-04-25 |
| **B8** | behavioural | Within-unit bid complexity response to MTU15-IDA is **IB-specific, not generic Big-4**. IB strongly complexifies (within-unit tranches-per-period 5.49 → 8.73, ratio 1.59×; confirmed on named CCGT units TAPOWER/ARCOS1/CTN4). HC stable (5.93 → 5.83). GE roughly flat at low absolute level (~2 tpp throughout). **GN simplifies** (6.46 → 3.22), **Fringe-survivors simplify** (5.69 → 3.07). The complexification is IB-specific strategic response — likely reflecting that IB has the largest CCGT fleet on the marginal supply step among Big-4. | `ccgt_within_unit_tranche_count.py`, `bid_complexity_panel.py`, `bid_complexity_unit_level.py` | data/derived/{ccgt_within_unit_tranche_count, bid_complexity_unit_level}.csv | 2026-04-26 |
| **D1** | descriptive | Within-month price dispersion rises post-MTU15-DA in Spain (and not in France). | `dispersion_15min_check.py` | nb03; `archive/_robustness_summary.md` §10 | 2026-04-25 |
| **D2** | descriptive | 80–99% of Big-4 offer-hours post-MTU15-DA do not strategically exploit 15-min granularity (identical bids across 4 ISPs). | `dispersion_15min_check.py` | nb09 §7 | 2026-04-25 |
| **D3** | descriptive | Market concentration (HHI) rises from 0.283 (pre-IDA) to 0.42+ (post-3-sess), with Big-4 share climbing from 49% to 66%. CCGT-only HHI even higher (0.46 → 0.60+). | `hhi_withholding_bidshading.py` | `archive/_robustness_summary.md` §11 | 2026-04-25 |
| **D4** | descriptive | Extensive-margin Fringe exit at MTU15-IDA: 23 small Fringe units (mostly RE/cogen) and 3 HC units exit DA between pre-MTU15-IDA and post, accounting for ~6.8% of pre-reform DA volume (~5 GW). Big-4 (GE, IB, GN) had zero exits. | `ccgt_extensive_margin_exit.py` | data/derived/ccgt_extensive_margin_exit.csv | 2026-04-26 |

## Wounded claims (4)

| ID | Layer | Claim | Evidence script | Evidence notebook | Date_changed | Wound |
|---|---|---|---|---|---|---|
| **W1** | structural-firm | GE raw (unconditional) Lerner peaks at 35% in DA60/ID15. | `build_firm_lerner_panel.py` | nb12 §5 | 2026-04-25 | (a) 76–90% of post-IDA cleared volume is nuclear (inframarginal); CCGT-only Lerner is null (`archive/_robustness_summary.md` §9). (b) Placebo reform-date sweep: 72.5% of fake dates produce a larger \|Δ\| than MTU15-IDA, so the GE peak is *not localised* to that date (`archive/_robustness_summary.md` §3); IB/GN/HC pass the placebo. Use F1/Spec 3 as canonical headline; cite raw 35% only with composition + non-localisation disclosure. |
| **W2** | identification / behavioural | Spanish nuclear units' wind-IV slope collapses sharply at ISP15 (Δ ≈ +54). | `within_tech_lerner.py` | `archive/08_wind_iv.ipynb` §8–§9 | 2026-04-25 | Real and reproducible across ANAV + CNAT operators, but nuclear is not a strategic-bidding setting. Candidate readings (load-following, outage scheduling, REE redispatch, ΔQ scaling) untested. Phase 2 priority placebo could reframe as demand-side or kill cleanly. |
| **F5** | structural-firm | Allaz–Vila commitment slope $\beta = \partial \Delta Q_{\text{IDA}} / \partial q_{\text{DA}}$: in **peak (h11–22, CCGT-margin) hours**, slope attenuates toward zero (GE) or flips sign (IB) at MTU15-DA — consistent with granularity-mediated commitment-value channel for thermal-portfolio firms. GE off-peak goes opposite (slope deepens) — within-firm placebo for the mechanism. GN doesn't fit; HC's sign-flip is bigger off-peak (opposite of CCGT-margin prediction). | `allaz_vila_commitment_test.py`, `allaz_vila_portfolio_split.py` | `_modelling_track.md` §2 (Run 2026-04-25, Refinement 2026-04-26) | 2026-04-26 | Original aggregate test wounded by GN opposite-sign; 2026-04-26 portfolio split with peak/off-peak partition shows the Allaz–Vila mechanism describes IB and GE peak hours specifically. GE peak-vs-off-peak going opposite directions is the single strongest mechanism diagnostic in the project. R² still small. Regression coefficient, not ATT. |
| **F6** | structural-firm | Cournot mechanism (corrected 2026-04-26): IB's matched-price Lerner elevation respects the inverse-slope prediction (steep→flat monotone decline +0.126 → +0.044); GE partial (T1>T2 but T3 highest); GN/HC opposite (monotone rise — but tiny magnitudes, ≤0.05). | `cournot_slope_tercile.py` | `_modelling_track.md` §1 (Run 2026-04-25, corrected 2026-04-26) | 2026-04-26 | Cournot predicts higher Lerner where supply is *steep* (low MW/EUR, low \|dS/dp\|). Original 2026-04-25 doc had the inequality reversed; corrected reading shows IB cleanly fits Cournot, GE partial, GN/HC opposite (consistent with their hydro/portfolio composition per F4). F2 (IB Lerner) gains a Cournot mechanism anchor. F1 (GE) stays as Spec-3 contrast with mixed mechanism support. |
| **F7** | structural-firm | Ciarreta–Espinosa synthetic-firm market-power index, post-MTU15-IDA: **13.79% at DA60/ID15** (mean +€7.86/MWh), **12.17% at DA15/ID15** (mean +€9.48/MWh) — actual DA prices systematically above the synthetic-Big-4 benchmark. Total cleared-price-difference transfer ≈ €833M across the 14-month post-IDA window. | `synthetic_firm_matching.py`, `synthetic_firm_clearing.py`, `synthetic_firm_aggregate.py` | `data/derived/results/synthetic_firm_regime.csv` | 2026-04-26 | Independent of F1/F2 — uses Ciarreta–Espinosa (J Regul Econ 2010) plant-pair substitution method (no marginal cost data; replaces each Big-4 plant L's offer with a same-tech same-capacity Fringe plant S's offer scaled by K_L/K_S). 62/106 Big-4 plants matched (CCGT 36/36, Hydro 26/26, Nuclear 44/44 unmatched — kept actual). **Caveat 1**: pre-2025-03-19 bid prices are 0-padded in det_all (parser artefact); the synthetic method is therefore only interpretable for post-MTU15-IDA regimes. **Caveat 2**: complex offers (block orders, indivisibility, interconnection-capacity rationing) are excluded from re-clearing; the re-cleared p_actual differs from published OMEL price by ~€23 mean. The DIFFERENCE p_actual − p_synth is unbiased (same exclusion in both), per Ciarreta–Espinosa's own caveat. **Two-decade replication**: this method reproduces the 2002–2005 Ciarreta–Espinosa finding pattern (Big-4 systematically above synthetic-Big-4); 2025-2026 magnitude (~13%) is lower than 2002-2005 (~21%), consistent with tighter regulation. |

## Dead claims (14)

| ID | Layer | Claim | Evidence script | Evidence notebook | Date_changed | Reason killed |
|---|---|---|---|---|---|---|
| **X1** | identification | Big-4 × Post-MTU15-IDA TWFE-DiD coefficient +217 MWh/unit-day identifies a causal ATT. | (none — coefficient demoted) | `archive/07_main_regression.ipynb` §4–§8 | 2026-04-25 | Parallel trends fail (nb07 §3 event-study); analytical placebos fail (§6); randomization p=0.43 (§11); treatment-date sweep peaks 2024-07 not 2024-12-01 (§12). See `_identification_target.md` Phase B. |
| **X2** | identification | Big-4 aggregate wind-IV slope contraction +15 to +18 MWh/unit-day/GWh identifies ISP15 effect on strategic responsiveness. | (none — demoted) | `archive/08_wind_iv.ipynb` §6 | 2026-04-25 | Nuclear-variance-weighted. Ex-nuclear robustness reduces Δ from +14.79 to +2.88; all regime slopes lose significance. `_identification_target.md` D10. |
| **X3** | identification | GE × CCGT wind-IV signed flip (3-sess +11.8 → ISP15 −16.1) uniquely localised to ISP15 calendar date. | (none — demoted) | `archive/08_wind_iv.ipynb` §8, §10 | 2026-04-25 | Placebo sweep across the 3-sess + ISP15 window: 6 of 22 fake boundaries produce comparable flips. Real flip at 83rd percentile of placebo distribution. `_identification_target.md` D12. |
| **X4** | behavioural | +238% IDA-reform sell-side offer-price jump (€103 → €348) is reform-driven. | (none — demoted) | nb13 §1 (pre-recalibration) | 2026-04-25 | Same-calendar-weeks comparison: 2022–23 spring offers already at €250–306/MWh, comparable to supposedly-elevated 3-sess level. 2024 pre-IDA spring (€45) was the anomaly. `archive/_robustness_summary.md` §8. Replaced by alive claim B2. |
| **X5** | identification | Profile-matching explains ΔQ compression (H1). | (engineering decomposition) | nb05 §2 | 2026-04-24 | Rejected in nb05. |
| **X6** | identification | Ramp/start-up lumpiness drives compression (H2). | (engineering decomposition) | nb05 §3 | 2026-04-24 | Rejected in nb05. |
| **X7** | identification | Reserve-procurement substitution explains compression (H3). | (engineering decomposition) | nb05 §8 | 2026-04-24 | Rejected in nb05. |
| **X8** | identification | Storage internalisation drives compression (H4). | (engineering decomposition) | nb05 §4 | 2026-04-24 | Rejected in nb05. |
| **X9** | behavioural | DA-tranche simplification mirrors IDA complexification (DA+IDA sum stable 5–9; H12). | (bid structure audit) | nb09 §3–§4 | 2026-04-24 | Composition shift, not within-unit behaviour. |
| **X10** | behavioural | Big-4 CCGT XBID iceberg rates surge to 98% at MTU15-DA as strategic information-hiding (H18). | (XBID order-book audit) | nb09 §11 | 2026-04-24 | Iceberg rates already at 98% pre-reform; no reform-induced shift. |
| **X11** | behavioural | La Muela (MUEL) uses iceberg strategy specifically at MTU15-DA (H20). | (unit-level XBID audit) | nb09 §11 | 2026-04-24 | MUEL was already at 99% iceberg since 2021. |
| **X12** | identification | DA capacity withholding (cleared/offered ratio) declines significantly post-reform. | `hhi_withholding_bidshading.py` | `archive/_robustness_summary.md` §12 | 2026-04-25 | Effect size insignificant after seasonal correction. |
| **X13** | system | A87 reserve-cost redistribution ≈ €840M/yr at ISP15. | `a87_reserve_decomposition.py` | (intermediate finding, not in any nb synthesis) | 2026-04-24 | Inferred from A87 − impdsvqh residual. After extending the ESIOS parser to include `imresecqh` directly, decline is gradual €71 → €30M/mo (cost saving, not redistribution). |
| **X14** | behavioural | Big-4 CCGT DA bid granularity drops 5–7 → 1–2 tranches at MTU15-IDA (formerly W3). | `ccgt_within_unit_tranche_count.py`, `ccgt_extensive_margin_exit.py` | nb09 §1–§2, §12; data/derived/ccgt_within_unit_tranche_count.csv | 2026-04-26 | Direct within-unit verification on the four named complex-bidders (TAPOWER, SRI4R, ARCOS1, CTN4): tranches-per-period **stable or increased** post-MTU15-IDA (TAPOWER 5.80→7.13, SRI4R 5.94→5.24, ARCOS1 5.49→6.74, CTN4 6.55→11.28). None of the four units simplified bids; three complexified. Combined with `ccgt_extensive_margin_exit.py` finding that none exited DA, there is no within-unit *or* extensive-margin behavioural source for the aggregate "5-7 → 1-2 tranches" claim. The aggregate drop is almost certainly a **MAV-format-change parser artefact** (per memory: pre-MTU15-IDA every det_all row had MAV value; post only ~1% — depending on nb09's tranche-count construction this can yield apparent 5-fold simplification with no behavioural change). |

---

## Counts

- Alive: 24 (7 system, 5 structural-firm, 8 behavioural, 4 descriptive)
- Wounded: 4
- Dead: 14

## Cross-references

- **Identification provenance & history**: `explore/_identification_target.md` (frozen post-2026-05-02). The D-sections D1–D17 narrate how firm-level claims narrowed from D4 aggregate to D11 GE×CCGT to D12 placebo failure to D14 system-layer pivot.
- **Robustness check details**: `explore/archive/_robustness_summary.md` (archived 2026-04-25; per-claim findings now live in this ledger and in nb12/nb13 canonical-headline cells).
- **Economic-modelling track**: `explore/_modelling_track.md` (replaces archived `_modelable_patterns.md`).
- **Discipline cycle**: see `CLAUDE.md` § "Claim-status discipline" for the procedure to follow when a claim's status changes.
