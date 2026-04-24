# Identification Target — Articulating Comparisons, Parameters, Assumptions

Working note. Not thesis prose. Prunes over time as clarity improves.

---

## Context (one paragraph)

The mechanism of interest: **ISP15 (2024-12-01) eliminated intra-hour imbalance netting at settlement. That removed a strategic buffer firms had been using**: before ISP15, a firm could carry a $+50$ MW imbalance in period 1 and a $-50$ MW imbalance in period 4 of the same hour, and they would net to zero at 60-min settlement. After ISP15 each 15-min ISP settles separately, so the buffer is gone. Three months later, **MTU15-IDA (2025-03-19)** gave firms 15-min intraday trading to match the 15-min settlement, restoring finer-grained flexibility. The empirical question: does this mechanism show up in dominant firms' DA/ID repositioning behaviour, and under what conditions?

Notebook stack nb03–nb07 contains the descriptive patterns (strong) and formal regressions (ambiguous). This note articulates the comparisons, parameters, and assumptions *before* running more estimators.

---

## A1 — Comparisons

Four distinct comparisons. Each answers a different empirical question. The thesis is probably going to make claims from more than one; the point is to keep them separate, not collapse.

### (a) Counterfactual-trajectory

**Question:** What would Big-4 dominant $\Delta Q$ have looked like in the post-reform window had the reform sequence not occurred? How does the observed post-reform trajectory differ from that counterfactual?

**How:** Construct a counterfactual Big-4 $\Delta Q$ from (i) pre-reform Big-4 data alone extrapolated forward, or (ii) pre-reform Big-4 data + auxiliary controls (wind, prices, load) whose pre-reform relationship to Big-4 $\Delta Q$ we hold fixed.

**Thesis relevance:** this is the comparison implicit in any "the reform caused X" causal claim. It's the most natural framing for a single-event treatment in time-series settings.

### (b) Between-group pre/post (Big-4 vs Fringe)

**Question:** Did the Big-4 $-$ Fringe gap in $\Delta Q$ change across the reform? Is that change attributable to the reform's differential effect on Big-4 vs Fringe?

**How:** DiD. $(\mathbb{E}[Y^{\text{B4}}_{\text{post}}] - \mathbb{E}[Y^{\text{F}}_{\text{post}}]) - (\mathbb{E}[Y^{\text{B4}}_{\text{pre}}] - \mathbb{E}[Y^{\text{F}}_{\text{pre}}])$.

**Thesis relevance:** this is what nb07 has been doing. The natural comparison if we believe Fringe is a valid control (i.e. would have trended in parallel with Big-4 absent treatment).

### (c) Within-firm across regimes

**Question:** Holding Big-4 firm identity fixed, how does the same firm's $\Delta Q$ differ across reform regimes?

**How:** Within-firm panel regression with regime dummies. No external control group. Unit FE + calendar controls (day-of-week, month-of-year) + continuous controls (wind, load).

**Thesis relevance:** descriptive-leaning, but arguably the cleanest framing given that (a) we don't have a credible counterfactual trajectory and (b) the Fringe control group is fundamentally incomparable. The claim is weaker ("Big-4 behaviour was different in regime $r'$ than in regime $r$") but defensible.

### (d) Conditional-on-wind

**Question:** Any of (a)–(c), restricted to low-wind days where the Ito–Reguant oversell incentive is mechanically largest.

**How:** Add a conditioning event $\{w_d \in \text{low wind}\}$ to the above. Matches nb03 §3e framing.

**Thesis relevance:** sharpens the test of whether the mechanism is operating through strategic behaviour (which is wind-sensitive) rather than mechanical fundamentals (which shouldn't be wind-sensitive).

---

## A2 — Parameters of interest (mathematical)

Let $Y_{i,d} = \Delta Q_{i,d}$ be dominant-unit per-day net DA–IDA repositioning. Let $T_i = 1$ if unit $i$ is Big-4, $T_d^r = 1$ if day $d$ is post reform $r$. Let $Y_{i,d}(1), Y_{i,d}(0)$ be potential outcomes with and without the reform sequence.

- **(a)** $\text{ATT}^{(a)}_r = \mathbb{E}[Y_{i,d}(1) - Y_{i,d}(0) \mid T_i = 1, d \in \text{post reform } r]$. The reform's causal effect on treated units' outcome, averaged over the post-treatment window. The counterfactual $Y_{i,d}(0)$ is unobservable.
- **(b)** $\text{ATT}^{(b)}_r = \big(\mathbb{E}[Y_{i,d} \mid T_i = 1, T_d^r = 1] - \mathbb{E}[Y_{j,d} \mid T_j = 0, T_d^r = 1]\big) - \big(\mathbb{E}[Y_{i,d} \mid T_i = 1, T_d^r = 0] - \mathbb{E}[Y_{j,d} \mid T_j = 0, T_d^r = 0]\big)$. Requires $T_i \neq T_j$ on the same dates.
- **(c)** $\mu_r = \mathbb{E}[Y_{i,d} - \alpha_i - X_{i,d}'\gamma \mid T_i = 1, d \in r]$, comparable across regimes $r$ after absorbing unit FE $\alpha_i$ and calendar/wind controls $X_{i,d}$. Contrasts of interest: $\mu_r - \mu_{r'}$ for reform-pair $(r, r')$.
- **(d)** Add $\mathbf 1\{w_d < w^{33\%}\}$ to the conditioning in (a)–(c).

Under strict potential-outcomes, (a) is the "real" causal parameter. (b), (c), (d) are observable objects that *may* equal (a) under specific assumptions (stated in A3).

---

## A3 — Identifying assumptions (words first, then math)

For each parameter in A2, the assumptions that equate the observable statistic to the causal parameter.

### (a) Counterfactual-trajectory

**Words.**
- *Stable counterfactual DGP.* The process generating Big-4 $\Delta Q$ — as a function of observable controls (wind, prices, calendar) — was stable in the pre-reform period and would have remained stable in the post-reform period if the reform hadn't happened.
- *No anticipation in pre-period.* Big-4 behaviour in the pre-reform window does not already reflect firms' expectations of the coming reform.
- *Controls unaffected by treatment.* The covariates we project forward (wind, prices, load) do not themselves respond to the reform. Wind is exogenous; prices and load are *not* — the reform changes market-clearing, so post-reform observed prices reflect partly the reform's effect.

**Math.** $\mathbb{E}[Y_{i,d}(0) \mid X_{i,d}] = f_{\text{pre}}(X_{i,d})$ for some fixed function $f_{\text{pre}}$ estimated from pre-reform data, and this same function applies post-reform. No anticipation: $Y_{i,d}(1) = Y_{i,d}(0)$ in the pre-period. Exogeneity of controls: $\mathbb{E}[X_{i,d} \mid \text{treated}] = \mathbb{E}[X_{i,d} \mid \text{untreated}]$ at any date (for the subset of $X$ that is genuinely exogenous).

### (b) Between-group DiD

**Words.**
- *Parallel trends.* Absent the reform, Big-4 and Fringe $\Delta Q$ would have evolved with a constant difference over time.
- *No anticipation.* Big-4's pre-reform behaviour is not already adjusting to the coming reform.
- *SUTVA (no spillovers).* Fringe's outcomes are not affected by Big-4's treatment. If Big-4 repositioning moves clearing prices, Fringe responds, contaminating the control.
- *Comparable controls.* Fringe's underlying dispatch physics, cost structure, and strategic exposure are similar enough to Big-4's that parallel trends is plausible. Small RE hydro with zero-marginal-cost dispatch is NOT comparable to Big-4 conventional CCGT with gas-price-driven marginal cost.

**Math.** $\mathbb{E}[Y_{i,d}^{\text{B4}}(0) - Y_{j,d}^{\text{F}}(0) \mid d] = \mathbb{E}[Y_{i,d}^{\text{B4}}(0) - Y_{j,d}^{\text{F}}(0) \mid d']$ for all $d, d'$. $Y_{i,d}^{\text{B4},\text{pre}}(0) = Y_{i,d}^{\text{B4},\text{pre}}(1)$. $Y^{\text{F}}_{j,d}$ does not depend on Big-4 treatment status.

### (c) Within-firm across regimes

**Words.**
- *Conditional mean independence of regime indicator.* Within a firm and after absorbing calendar (day-of-week, month-of-year) and continuous controls (wind, prices), the regime indicator $\mathbf 1\{d \in r\}$ is uncorrelated with unobserved determinants of $Y_{i,d}$.
- *Stable composition within firm across regimes.* The units belonging to firm $i$ are the same in regimes $r$ and $r'$ (no unit retirements / commissionings between the two regimes that would contaminate the within-firm contrast).
- *Linear / separable trend component.* Any secular trend in $Y_{i,d}$ unrelated to the reform is captured by the calendar controls and does not load onto the regime dummy.

**Math.** $Y_{i,d} = \alpha_i + \sum_r \mu_r \mathbf 1\{d \in r\} + X_{i,d}'\gamma + \varepsilon_{i,d}$, with $\mathbb{E}[\varepsilon_{i,d} \mid \mathbf 1\{d \in r\}, X_{i,d}, \alpha_i] = 0$.

### (d) Conditional-on-wind

**Words.** Same as (a)/(b)/(c) plus: *wind tercile is exogenous to treatment.* The conditioning set $\{w_d \in \text{low}\}$ is determined by weather, not by firm strategy or the reform.

**Math.** Add $\mathbf 1\{w_d \in \text{low}\}$ as a conditioning event; the exogeneity of wind means this doesn't bias the remaining assumptions.

---

## A4 — Plausibility assessment per assumption

Citations refer to the sections of nb07 and nb03 that provide evidence.

| Comp. | Assumption | Status | Reason |
|---|---|---|---|
| (a) | Stable counterfactual DGP | **inconclusive** | We don't have an out-of-sample test. The pre-reform Big-4 trajectory itself is *trending* (nb07 §3 event-study), so "stable DGP" would have to mean stable trend, which is non-trivial to defend. |
| (a) | No anticipation in pre-period | **fails** | nb07 §3 event-study: pre-reform Big-4 × event-month coefficients trend upward from $k=-12$ through $k=-1$. Consistent with Spanish regulators having announced ISP15 well before its activation. |
| (a) | Controls unaffected by treatment | **partial** | Wind is exogenous (holds). Prices respond to the reform (fails). If we use only wind as control, we have an exogenous subset but lose demand-side information. |
| (b) | Parallel trends | **fails** | Same event-study evidence as (a). The Big-4 $-$ Fringe gap is widening throughout 2024 before any reform, inconsistent with the constant-gap null. |
| (b) | No anticipation | **fails** | As in (a). |
| (b) | SUTVA | **likely holds** | Fringe is small relative to Big-4 in dispatchable conventional; their response to Big-4 clearing-price changes is second-order. Cannot rule out entirely but unlikely to dominate. |
| (b) | Comparable controls | **fails for raw Fringe, partial for refined** | Raw Fringe is 79% small RE hydro (median 2.5 MW), structurally incomparable. Refined Fringe (nb07 §8a, 55 dispatchable-conventional units) is better but still heterogeneous vs Big-4. |
| (c) | Conditional mean independence of regime indicator | **inconclusive** | Depends on whether calendar controls + unit FE absorb all non-reform-related trend. The within-Big-4 event study in nb07 §8b shows a continuous trend, so the regime dummy will mechanically absorb part of that trend as "reform effect." |
| (c) | Stable composition within firm | **likely holds** | Big-4 unit rosters are stable over 2023-2025 (small retirements, no large entries). |
| (c) | Linear/separable trend | **fails** | The within-Big-4 trend in nb07 §8b is not just a linear secular drift; it has structure (rises through mid-2024, slows, rises again). Linear month-of-year FE won't capture that. |
| (d) | Wind tercile exogenous to treatment | **holds** | Weather is not a function of firm strategy or the reform calendar. |

**Summary of A4.** Comparison (a) fails on no-anticipation and is inconclusive on stable DGP. Comparison (b) fails on parallel trends, no anticipation, and comparable controls. Comparison (c) is inconclusive on conditional mean independence and fails on linear/separable trend. Comparison (d) inherits the problems of whichever base comparison it conditions on.

**No comparison in A1 is cleanly identified with current data and methods.** This is the unvarnished honest assessment. The question becomes: what's the *least-compromised* comparison, and what would be needed to promote it to identified?

---

## A5 — Data needs

Each item is a potential addition to `data/processed/`, listed with the comparison it would most help, the identification problem it would address, and an explicit duplication check against what we already have.

### ENTSO-E A75 — actual wind/solar generation per production type

- **Helps:** (a) and (d). Specifically, enables a wind-forecast-error IV à la Ito–Reguant 2016. We can construct $\epsilon^{\text{wind}}_d = w^{\text{actual}}_d - w^{\text{DA-forecast}}_d$ and use it as an instrument for residual demand (which in turn shifts the Ito–Reguant strategic-bidding incentive exogenously of the reform calendar).
- **Why now:** the wind-IV strategy is the only one we've discussed that could address the parallel-trends failure in (b) and the anticipation problem in (a), because wind shocks are clean of reform anticipation.
- **Duplication check:** we have OMIE `pdbc` aggregated over wind units (nb03 §3e `daily_wind`), which is **DA-committed** wind MWh. A75 is **realised** wind MWh. These are different objects; committed minus realised is precisely the wind forecast error we want. A75 is therefore not duplicative of pdbc; it's the complementary series we need.
- **Cost:** 1–2 days of pipeline work (new `00/10/20` triple in `scripts/pipelines/entsoe/generation/`).

### ENTSO-E A65 — total load

- **Helps:** (a), (b). Useful as a continuous control for demand-side variation that currently isn't captured.
- **Duplication check:** OMIE `pdbc` has demand-side rows (buy orders), but those are *cleared* demand at the wholesale market, not *actual system load* (which includes losses, interconnection exchange, self-consumption, etc.). A65 is the TSO-reported system load — different object, not duplicative.
- **Cost:** 1 day.

### ENTSO-E A81 — contracted reserves per BSP

- **Helps:** firm-level H3 test (reserve substitution).
- **Duplication check:** no OMIE equivalent (reserves are REE's domain, not OMIE's wholesale). Not duplicative.
- **Cost:** unknown. Availability for Spain uncertain.

### Firm-level storage commissioning timeline

- **Helps:** firm-level H4 test (storage internalisation).
- **Duplication check:** ENTSO-E A68 (country-level installed capacity) is in `data/processed/entsoe/generation/installed_capacity_all.parquet`. Firm-level breakdown is not on the transparency platform. REE publishes it irregularly in Spanish.
- **Cost:** unknown, probably requires manual data collection from REE PDFs.

### ESIOS balancing settlement (per-BSP)

- **Helps:** (b) firm-level version if we can get per-BSP imbalance volumes/prices.
- **Duplication check:** **probable partial duplication.** ENTSO-E mirrors many REE series at system level (imbalance prices A85, imbalance volumes A86 are already in `data/processed/entsoe/balancing/`). If ESIOS adds the per-BSP dimension that ENTSO-E lacks, it's not duplicative. If it just re-publishes the same system-level series in Spanish, it is duplicative — do not sync twice.
- **Cost:** access token pending. 1–2 days of pipeline work once access is available.

### Priority ordering

Under the current A4 assessment, the most identification-relevant addition is **A75**, because it's the only one that addresses the anticipation / parallel-trends problem. A65 is useful as a control but doesn't fix identification. A81 / firm-level storage / ESIOS are useful for specific sub-questions but don't move the main comparisons.

Recommendation: if any data addition happens, A75 is the priority. Single pipeline, 1–2 days.

---

## Phase B — Audit of existing regressions against the articulated comparisons

For each section of nb07, what it estimates, which comparison it targets, and whether A4's assumption-plausibility assessment lets us read it as identifying the target parameter.

### nb07 §4 flagship (spec 1–5)

- **Estimates:** Big-4 × Post-MTU15-IDA interaction coefficient under unit FE, date FE, or both.
- **Targets:** comparison (b), with comparison (d) in low-wind specs.
- **A4 read:** **partial/no.** (b)'s parallel-trends and no-anticipation assumptions fail. The coefficient is a conditional correlation, not an ATT. Bias direction: upward (the pre-trend is positive, and TWFE absorbs part of it into the treatment effect).
- **Action:** demote the coefficient from "identified treatment effect" to "regression coefficient under maintained assumptions A3(b) which empirically fail at A4(b)." nb07 §7 synthesis and §13 identification section already do this partially; the downstream nb03 summary/thesis-narrative language needs the same demotion.

### nb07 §5a saturated reforms

- **Estimates:** marginal coefficient on each of the four cumulative Big-4 × Post-$r$ indicators, all entered jointly.
- **Targets:** comparison (b) decomposed by reform.
- **A4 read:** **partial/no.** Same identification issues as §4. The decomposition *is* useful as a descriptive partitioning of the total Big-4 $-$ Fringe shift across reform dates — it shows which reform date the cross-sectional shift concentrates on. But the per-reform coefficients are not ATTs. Bias direction: the Post-ISP15 coefficient absorbs ISP15-anticipation trend (upward); Post-MTU15-IDA is net-zero once ISP15 is controlled (consistent with the relief interpretation, but also consistent with the trend having peaked).
- **Action:** reframe the $+217$ coefficient as "the saturated specification concentrates its cross-sectional mass at the ISP15 interaction" rather than "ISP15 is the identified treatment." Add the pre-trend caveat explicitly.

### nb07 §5b tech split

- **Estimates:** comparison (b) within each tech subsample (CCGT, Hydro, Nuclear).
- **Targets:** comparison (b) with tech heterogeneity.
- **A4 read:** **partial/no.** Same (b) issues, plus small-sample problems for Nuclear (no Fringe) and CCGT (small Fringe). Hydro is the sub-sample with the tightest analytical significance.
- **Action:** report as heterogeneity, not identification. The pattern (Hydro carrying the effect) is informative about which tech is most behaviourally responsive, but the coefficient is not ATT.

### nb07 §6 analytical placebos

- **Estimates:** Big-4 × Post-fake interaction coefficient under two-way FE, at three hand-picked fake dates.
- **Targets:** tests (b)'s no-anticipation assumption.
- **A4 read:** **informative but not decisive.** Two of three placebos have non-zero coefficients, confirming no-anticipation fails. This is a negative diagnostic for (b), correctly flagged.

### nb07 §8a refined-control flagship

- **Estimates:** comparison (b) with dispatchable-conventional Fringe (55 units) instead of raw Fringe (257 units).
- **Targets:** comparison (b) with tighter comparable-control assumption.
- **A4 read:** **partial/no.** Comparable-controls assumption is better-satisfied than raw Fringe (dispatchable conventional is more structurally similar to Big-4 CCGT / reservoir hydro than small RE). But parallel trends still fails in the event study. Bias direction: smaller in magnitude than the raw-control spec, consistent with the refined control picking up less of the Fringe-specific secular trend.
- **Action:** §8a's spec is the "best (b) we can do with the data we have." Its ISP15 coefficient ($+217$) is the regression-level headline, but it is NOT an ATT.

### nb07 §8b within-Big-4 event study

- **Estimates:** within-Big-4 event-time coefficients relative to $k = -1$, no Fringe.
- **Targets:** comparison (c), though not exactly — it's a continuous event-time object rather than a regime-indicator contrast.
- **A4 read:** **partial.** The identification here rests on (c)'s linear/separable-trend assumption, which A4 calls **fails**. The shape of the event-study coefficients (gradual rise from $k=-12$ through $k=+9$ with no break at $k=0$) is itself informative: it tells us the non-reform trend is not linear-separable, and therefore comparison (c)'s regression estimates will conflate trend with reform.
- **Action:** read the event-study as a diagnostic — it's the clearest piece of evidence that no single reform date produces a discrete shift in Big-4 behaviour. It *undermines* the causal attribution story more than it supports it.

### nb07 §9 bid-level regression

- **Estimates:** comparison (b) with bid-level outcome (CCGT IDA wavg bid) instead of $\Delta Q$.
- **Targets:** comparison (b) on bid outcome.
- **A4 read:** **no.** Under unit FE, the coefficient on Big-4 × Post-MTU15-IDA is $-22$, $p = 0.36$. The descriptive conduct-gap collapse in nb06 §2 is entirely absorbed by unit FE. Bias direction: nb06 §2 overstates because it doesn't control for unit-level level differences. nb07 §9 correctly identifies that there's no within-unit bid-level shift.
- **Action:** this result needs to propagate to nb06 §2's narrative. The conduct-gap compression is cross-sectional composition, not behavioural shift at MTU15-IDA.

### nb07 §10 per-firm regression

- **Estimates:** comparison (b) separately for each Big-4 firm.
- **Targets:** comparison (b) with firm-level heterogeneity.
- **A4 read:** **partial/no.** Each per-firm regression inherits the identification problems of (b). Heterogeneity of the coefficients (IB and GE significant, GN imprecise, HC non-estimable) is informative descriptively but not structurally.

### nb07 §11 randomization inference

- **Estimates:** empirical p-value of the real Post-ISP15 coefficient against N=200 fake-reform-date coefficients drawn from uniform dates over 2023-12 to 2026-01 (excluding ±30d around real reforms).
- **Null tested:** "the real $\hat\beta$ at ISP15 is typical of $\hat\beta$'s at arbitrary calendar dates in the full window." Result: empirical p = 0.43.
- **Null we want (for (b) identification):** "the real $\hat\beta$ at ISP15 is typical of $\hat\beta$'s at dates where *by construction* there is no treatment effect" — i.e. fake dates drawn only from a true pre-reform window (e.g. 2023-12 to 2024-06).
- **These differ.** The implemented null includes fake dates inside post-reform regimes, where $\hat\beta$ reflects real treatment variation. The pre-period-only null is tighter and is the right test for (b)'s identification.
- **Action:** re-run §11 with fake dates restricted to a true pre-treatment window. Likely yields a smaller empirical p-value. That tighter p-value is the one that belongs in the synthesis. The current p=0.43 is technically correct for the broader null but should not be read as "the real date is indistinguishable from any date," because that's not the identification-relevant claim.

### nb07 §12 treatment-date sweep

- **Estimates:** $\hat\beta(\text{start-date})$ as the assumed treatment-start date varies monthly from 2024-03 to 2025-07.
- **Targets:** diagnostic for (b)'s no-discrete-break assumption.
- **A4 read:** **damning for (b).** $\hat\beta$ peaks at 2024-07-01 and declines monotonically — inconsistent with a discrete reform-induced break at ISP15. This is independent evidence for the pre-trend / anticipation interpretation.
- **Action:** keep §12 as the lead diagnostic when discussing (b)'s identification failure. It's more informative than §11.

---

## Phase C — Decision

No comparison in A1 is cleanly identified with current data and methods. **Comparison (b) fails on parallel trends, no-anticipation, and control-group comparability. Comparison (a) fails on no-anticipation and is inconclusive on stable DGP. Comparison (c) fails on linear/separable trend.** The honest empirical contribution of nb07 is documentation of a reform-window regime shift conditionally correlated with the ISP15 calendar, robust across specifications, but not an identified ATT under any plausible set of modern-DiD assumptions.

**Most promising single next step:** sync **ENTSO-E A75** (actual wind/solar generation) and construct a wind-forecast-error IV à la Ito–Reguant. This is the only available strategy that addresses the anticipation / parallel-trends problem, because wind shocks are clean of reform anticipation by construction. The IV identifies comparison (a) under the exclusion restriction that wind-forecast errors affect Big-4 $\Delta Q$ only through their effect on residual demand (and hence the reform-sensitive strategic-bidding incentive). This is a non-trivial restriction to defend, but it's more defensible than parallel trends in our sample.

**Secondary next step** (cheap, independent): re-run nb07 §11 with a pre-treatment-only fake-date window, and propagate the tighter p-value into the synthesis cells. This doesn't change identification but it tightens the existing rigour narrative.

**Do not commit to BSTS, RDD, synthetic control, or partial identification** in this plan. Those are step-4/5 decisions and depend on what (a), (b), or (c) we end up targeting. The wind-IV upgrade is the one that changes the choice set; everything else can wait.
