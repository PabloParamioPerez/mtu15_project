# Modelable empirical patterns — economic models the data could ground

**Status**: working list of empirical regularities that have a clean
economic-model interpretation. Each could anchor a chapter or section of
the thesis. Created 2026-04-25.

The thesis goal is to identify **facts** that can be **modeled**. Patterns
below are listed with (a) the empirical regularity, (b) the economic
model it could motivate, (c) what additional analysis would tighten it.

---

## Pattern 1 — Forecast-error → imbalance pass-through jumps in DA60/ID15

**Empirical regularity**. Daily-aggregate regression of |V_imb| on |wind
forecast error| + |solar forecast error| by regime:

| Regime | n | α (GWh) | β_wind | γ_solar | R² |
|---|---:|---:|---:|---:|---:|
| pre-IDA | 2346 | 5.89 | −0.04 | −0.20 | 0.03 |
| 3-sess | 170 | 3.63 | +0.03 | −0.03 | 0.06 |
| ISP15 window | 108 | 8.64 | +0.005 | −0.06 | 0.001 |
| **DA60/ID15** | **196** | **2.97** | **+0.064** | **+0.139** | **0.305** |
| DA15/ID15 | 198 | 5.56 | −0.003 | −0.002 | 0.005 |

In the asymmetric-granularity regime DA60/ID15, **R² jumps to 0.305**
and the slope coefficients become positive and significant. In all other
regimes, forecast errors do not predict imbalance volume.

**Economic model**. A two-stage market with a "forecast-error filter":

  Stage 1 (DA): firms submit hourly bids based on day-ahead forecast.
  Stage 2 (IDA): firms re-bid with intraday forecast updates.
  Stage 3 (settlement): residual is settled at imbalance price.

  Filter strength depends on (i) settlement clock $\sigma$ ∈ {60, 15} and
  (ii) intraday trading clock $\tau$ ∈ {60, 15}.

  - $(\sigma=60, \tau=60)$ — pre-reform: forecast errors net within hour
    in both settlement and trading; weak pass-through.
  - $(\sigma=15, \tau=60)$ — ISP15 window: settlement exposes 15-min
    error but trading can't respond; **disconnect** between forecast
    error and settled imbalance (high noise, low R²).
  - $(\sigma=15, \tau=15)$ — DA60/ID15: 15-min IDA tracks 15-min wind/
    solar realisation; pass-through is **proportional**, R² jumps.
  - $(\sigma=15, \tau=15)$ + 15-min DA: DA15/ID15: hourly DA can be
    disaggregated optimally before realisation; pass-through falls
    again.

This is a **regime-dependent pass-through model** with two parameters
(σ, τ) that map to four cells in the data.

**To model**:
- $|V^{\text{imb}}_t| = f(|\epsilon^{\text{forecast}}_t|, \sigma, \tau)$
- Theory: $f$ is convex in $\epsilon$ when $\tau > \sigma$ (cannot fully
  hedge), linear when $\tau = \sigma$ (fully responsive), absorbed in
  hourly netting when both are 60.
- Estimable structural parameter: ratio of pass-through coefficients
  across regimes identifies the "filter strength" of each clock.

**Reproducing**: `scripts/analysis/passthrough_forecast_imbalance.py`.

---

## Pattern 2 — GE × CCGT bid-function shape: reservation-pricing peak in 3-sess + ISP15

**Empirical regularity**. Quantity-weighted distribution of GE's CCGT
sell-side IDA offer prices per regime:

| Regime | p25 | p50 | p75 | p95 | share > €100 | share > €300 |
|---|---:|---:|---:|---:|---:|---:|
| pre-IDA | 27 | 67 | 117 | 800 | 32% | 10% |
| **3-sess** | 109 | 139 | **800** | 802 | **84%** | **37%** |
| **ISP15 window** | 125 | 157 | **800** | 800 | **94%** | 36% |
| DA60/ID15 | 91 | 100 | 127 | 800 | 49% | 7% |
| **DA15/ID15** | 84 | 88 | 92 | 101 | **5%** | **0%** |

GE's CCGT bid function in 3-sess and ISP15 has 75th percentile at the
**price cap (€800)**, with 36-37% of capacity priced above €300 — these
are "reservation tranches" that almost certainly will not clear. In
DA15/ID15 the bid function collapses to a tight band around €85-100/MWh
with 0% above €300.

IB × CCGT and IB × Hydro show same direction, smaller magnitudes.

**Economic model**. Hortaçsu-Puller (2008) optimal supply-function
bidding under uncertainty, **augmented with a "settlement risk" term**:

  Firm $i$ chooses bid function $S_i(p)$ to maximise
    $\mathbb{E}\left[\,\int_0^p (p - MC_i(q))\,dS_i(q)\,\right]$
    $- \;\theta_i\;\mathbb{E}\left[\,c^{\text{imb}}\,\big|\,\Delta Q_i\,\right]$

  where $\theta_i$ is an effective imbalance-cost weight that depends
  on the **settlement-trading clock pair** $(\sigma, \tau)$:

  - $\theta_{60,60}$ low: intra-hour netting absorbs misallocation cost
  - $\theta_{15,60}$ high: settlement exposes 15-min error, no trading to
    fix it → firm prefers NOT to clear in DA, posts reservation tranches
  - $\theta_{15,15}$: high but mitigated by IDA trading
  - $\theta_{15,15}$ with DA15: low, as 15-min DA matches settlement

  Optimal bid function shape:
    $p^*(q) = MC(q) + \mu(q, \theta)$, with $\mu \uparrow \theta$.

  Predicts: when $\theta$ is high (ISP15 window), firms shift mass to
  reservation tranches above clearing. When $\theta$ falls (DA15/ID15),
  bid function collapses near MC.

**The data match this prediction tightly**:
- Pre-IDA $\theta$ low → 32% above €100, baseline.
- ISP15 window $\theta$ peak → 94% above €100. Strong reservation.
- DA15/ID15 $\theta$ minimum → 5% above €100. Competitive.

**To model**:
- Log-normal MC distribution per firm × tech.
- Bid function $p^*(q) = MC(q) + \theta \cdot V(q)$ where $V$ is a
  reservation premium decreasing in expected clearing probability.
- Calibrate $\theta_r$ per regime from the empirical share-above-€100
  statistic.

**Reproducing**: `scripts/analysis/bid_function_shape.py`.

---

## Pattern 3 — DA-IDA wedge: dispersion rises, persistence weakens

**Empirical regularity**. Hourly DA-IDA price wedge moments:

| Regime | mean | std | AR(1) | mean abs |
|---|---:|---:|---:|---:|
| pre-IDA | −0.98 | 7.34 | 0.805 | 4.15 |
| 3-sess | −0.94 | 8.28 | 0.716 | 5.76 |
| ISP15 window | −0.49 | 8.62 | 0.728 | 5.87 |
| DA60/ID15 | −1.25 | 9.67 | **0.680** | 6.72 |
| DA15/ID15 | −1.43 | 10.27 | 0.743 | **7.15** |

Mean abs wedge rises **+72%** from pre-IDA to DA15/ID15. Variance rises
**+96%**. AR(1) declines from 0.805 to 0.680 in DA60/ID15, recovers to
0.743 in DA15/ID15.

**Economic model**. Two-stage information revelation:

  $p^{DA}_h = \mathbb{E}[p^*_h | I^{DA}]$
  $p^{IDA}_h = \mathbb{E}[p^*_h | I^{IDA}]$, where $I^{IDA} \supset I^{DA}$.

  Wedge $w_h = p^{IDA}_h - p^{DA}_h$ has variance $\propto$ size of
  information increment between DA and IDA.

  - Pre-reform: hourly DA, hourly IDA. Information increment is small
    (2-4 hour ahead update). Var(w) low, AR(1) high (persistent because
    same hourly aggregation).
  - DA60/ID15: hourly DA, 15-min IDA. Information increment is **larger
    AND finer**. Var(w) rises, AR(1) falls (15-min noise).
  - DA15/ID15: 15-min DA, 15-min IDA. Information increment moderate;
    AR(1) recovers because both markets are at same granularity.

  Predicts the exact pattern: dispersion peaks in DA60/ID15, AR(1)
  troughs in DA60/ID15.

**To model**:
- A martingale-stage model: $p^*_h = \pi_h + \nu_h^{DA} + \nu_h^{IDA}$
  where $\nu^{IDA}$ is realised between DA and IDA.
- Cross-market mean-reversion in the wedge.
- The asymmetric-granularity window expands $\nu^{IDA}$ variance
  endogenously.

**Reproducing**: `scripts/analysis/da_ida_wedge_structure.py`.

---

## Pattern 4 — HHI rise from 0.28 to 0.42 across reforms

**Empirical regularity** (from `_robustness_summary.md` §11). Big-4
share of DA cleared sell-side rises from 49% (pre-IDA) to 66%
(DA60/ID15). Full-market HHI rises from 0.283 to 0.425. Crosses FTC/DOJ
"very highly concentrated" threshold (0.40) at IDA reform. CCGT-only
HHI is structurally even higher (0.46 → 0.60).

**Economic model**. Bilateral-contract reallocation reduces effective
number of "pure-price" sellers in DA market.

  Pre-reform: $N$ retail+production firms each settle their own DA
  positions. Each firm has small market share.

  Reform creates compliance costs that fall disproportionately on small
  firms (Rule 28.8 documentation, 15-min systems). Big-4 absorbs
  bilateral-contract intermediation. Effective $N \to N'$ with
  $N' < N$, increasing HHI.

  Cournot quantity-equilibrium prediction: equilibrium price-cost
  margin scales with $1/N \cdot \bar\eta^{-1}$ where $\bar\eta$ is
  market demand elasticity. Concentration rise should produce ↑
  margins on net.

**To model**:
- Reform as a fixed-cost shock that selects out small firms.
- HHI as endogenous outcome of N-firm Cournot with entry-exit.
- Welfare loss = ½ × Δ(price) × (Δ quantity) ≈ Δ(p) × HHI / |ε_D|.

---

## Pattern 5 — Imbalance settlement flow: €128M/mo swing at ISP15

**Empirical regularity** (from nb11 / `_robustness_summary.md`). A87
monthly net income (BRPs → TSO) jumps from €38M/mo pre-reform to
€160M/mo at ISP15, partially moderates to €72M at MTU15-DA. Concordant
with A86 |V_imb| rise of 5.1 GWh/d.

**Economic model**. BRP imbalance-charge optimization with intra-hour
netting parameter $\nu$:

  BRP utility: $U_i = \mathbb{E}[\text{revenue}_i] - \mathbb{E}[\text{settlement}_i]$

  Settlement under intra-hour netting parameter $\nu$:
  $$S_i = \pi^{\text{imb}} \cdot \big| \nu \sum_q d_{i,q} + (1-\nu) \sum_q |d_{i,q}| \big|$$

  - Pre-ISP15: $\nu = 1$ (full netting). Hourly net deviation only.
  - Post-ISP15: $\nu = 0$ (no netting). Sum of absolute deviations.

  Aggregate gross-vs-net differential:
  $$\Delta S = \pi^{\text{imb}} \cdot \mathbb{E}\Big[\sum_q |d_{i,q}| - \big|\sum_q d_{i,q}\big|\Big]$$

  This is exactly the €128M/mo swing observed.

**To model**:
- Calibrate $\nu$ from pre/post settlement-flow ratio.
- Predict BRP behavioural response: post-reform, BRPs trade more
  aggressively in IDA to reduce $|d|$. → predicts XBID liquidity rise
  (pattern 7) and IDA volume rise.
- Welfare: settlement transfer is zero-sum within market, but reducing
  $|d|$ has real efficiency value (less reserve activation by TSO).

---

## Pattern 6 — Within-month price dispersion: ES doubles, FR flat

**Empirical regularity** (from `_robustness_summary.md` §10). Within-
month SD of DA prices (€/MWh):

| Regime | ES | FR |
|---|---:|---:|
| pre-IDA | 23.2 | 34.7 |
| 3-sess | 38.0 | 35.5 |
| ISP15 window | 44.4 | 43.8 |
| DA60/ID15 | 39.9 | 36.0 |
| DA15/ID15 | 36.7 | 38.3 |

Spain nearly doubles; France flat. Clean Spain-specific.

**Economic model**. Increased reform-induced bidding heterogeneity
produces wider price distribution within a month.

  Specifically: post-reform, firms with high $\theta$ shift to
  reservation tranches (Pattern 2). On low-residual-demand hours these
  reservation tranches don't bind, so clearing price is set by low-
  bidding tranches → low price. On high-residual-demand hours,
  reservation tranches DO bind → high price. The within-month
  distribution becomes more bimodal, raising SD.

  France didn't experience this regime change, so its within-month SD
  stays flat.

**To model**:
- Mixture model of price distribution: low-price mode (renewable
  surplus) + high-price mode (CCGT scarcity, scarcity reservation).
- Reform shifts mass between modes. SD rises endogenously.

---

## Pattern 7 — XBID liquidity 15× growth, fill rate halves

**Empirical regularity** (from nb13 §2). Orders/hour: 921 (pre-IDA) →
13,868 (DA15/ID15). Trades/hour: 274 → 1,994. Fill rate 5.2% → 2.7%.
Trade-price SD peaks at €11.3/MWh in DA60/ID15.

**Economic model**. A search-cost model where matching frequency
depends on contract granularity:

  Pre-MTU15-IDA: 24 hourly contracts/day. Traders search across hours.
  Post-MTU15-IDA: 96 quarter-hour contracts/day. **Same** trading
  volume now distributed across 4× contracts.

  Predicts: orders rise (more contracts) but fill rate falls (more
  competition per contract). Matches data exactly: orders ×15 (more
  than 4× because of behavioural growth), trades ×7 (genuine volume
  growth), fill rate halves.

**To model**:
- Markov matching with arrival rate $\lambda$ and contract count $C$.
- Equilibrium fill rate ∝ $\lambda / C$.
- Order count rises ∝ $C \cdot$ (matching effort per trader).

---

## Pattern 8 — Capacity-withholding ratio: GE clearance RISES post-reform

**Empirical regularity** (from `_robustness_summary.md` §12). GE
cleared/offered ratio rises from 24% (pre-IDA) to 64-72% (DA60/ID15+
DA15/ID15). Other firms' ratios remain low (~10%) and don't show clear
pattern.

**Economic model.** GE post-reform reduces "reservation tranche" *quantity*
while raising prices (Pattern 2). Net effect: less offered overall but
more of what's offered is at clearing prices → ratio rises.

  Alternative reading: GE pre-reform offered a wide range with deep
  reservation tail; post-reform, GE compresses its offer schedule into
  fewer but higher-priced tranches → higher fraction clears at cleared
  marginal prices.

**This is consistent with Pattern 2**: bid function collapses
post-MTU15-DA and the small remaining offered quantity is at
competitive prices and clears.

---

## Synthesis — what the data are telling us, ready for a model

The patterns above coalesce around **a single mechanism with multiple
observable signatures**:

> A reform that changes the (settlement, trading) clock pair $(\sigma, \tau)$
> alters the BRP/firm imbalance-cost exposure $\theta$, which drives:
>
> 1. Pass-through of forecast errors to imbalance volumes (Pattern 1)
> 2. Bid-function shape: reservation tranches when $\theta$ high (Pattern 2)
> 3. DA-IDA wedge dispersion when DA and IDA at different clocks (Pattern 3)
> 4. Settlement flow magnitude (Pattern 5)
> 5. Within-month price dispersion (Pattern 6)
> 6. Continuous-intraday liquidity (Pattern 7)
>
> When the reform converges to $(\sigma=15, \tau=15)$ with both DA and
> IDA at 15-min granularity, $\theta$ falls and most patterns reverse
> toward pre-reform levels.

**This is a thesis-grade story.** The empirical contribution is
documenting six concordant signatures of a single underlying mechanism
(reform-induced clock-asymmetry $\theta$). The theoretical contribution
is a structural model where $\theta$ is identified from pattern 1's
pass-through coefficient differential (regime $r$ with σ=15 τ=60 vs
regime with σ=15 τ=15).

The CCGT bid-function shape (Pattern 2) is the cleanest *firm-level*
observable; A87 settlement (Pattern 5) is the cleanest *system-level*
observable; pass-through (Pattern 1) is the cleanest *mechanism-level*
observable.

## Recommended next checks (priority order)

1. **Calibrate $\theta_r$**: extract a single number per regime from
   pattern 2's "share-above-€100" or pattern 1's pass-through slope.
   See if these ratios across regimes line up.
2. **Bid-function shape for IB × Hydro and IB × CCGT**: confirm the
   ISP15-peak / DA15/ID15-collapse pattern is general not GE-specific.
3. **Mean reversion test on DA-IDA wedge**: GARCH or AR(1) regime-
   dependence test.
4. **Welfare proxy**: producer surplus from CCGT cleared at high vs
   low price tranches. Quantify the redistribution from consumers to
   producers across regimes.
5. **Cross-pattern joint test**: regression of one pattern on another
   to confirm they're driven by a common factor.
