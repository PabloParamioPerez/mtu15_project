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

---

## (1) + (2) executed 2026-04-25 afternoon — calibration of (θ, ρ) model

### Extended bid-function shape across all Big-4 × techs

CCGT reservation share (>€100/MWh) per (firm, regime):

| Firm | pre-IDA | 3-sess | ISP15 win | DA60/ID15 | DA15/ID15 |
|---|---:|---:|---:|---:|---:|
| GE | 32% | **84%** | **94%** | 49% | **5%** |
| IB | 24% | 81% | 87% | **91%** | 26% |
| GN | 14% | 50% | **88%** | 20% | 14% |
| HC | 33% | 36% | 63% | 24% | 23% |

**The reservation pattern is general across Big-4 CCGT, not GE-specific.**
GE shows the cleanest peak-and-collapse; IB stays elevated through
DA60/ID15; GN spikes only at ISP15; HC is flatter.

Hydro (separate but related pattern):
- **GN × Hydro stays at 91-97% reservation** in every post-IDA regime —
  GN's hydro is structurally always at high prices, similar to the
  La Muela iceberg pattern from nb09. Possibly portfolio-strategic
  rather than reform-driven.
- **IB × Hydro**: 31% → 82% → 93% → 63% → 75%. Same ISP15 peak.
- **GE × Hydro**: 29% → 61% → 55% → 42% → 46%. Lower magnitude.
- **GN × PumpHydro: spikes to 100% in DA15/ID15** — hydro
  arbitrage-pricing during the new 15-min DA market. New finding.

### θ × ρ structural calibration

Theory: $β_r = β_0 \cdot θ_r \cdot ρ_r$ (pass-through nonzero only when
both settlement-risk AND IDA-responsiveness conditions hold).

| Regime | θ | ρ | s (Big-4 CCGT) | β_wind | β_solar | R² |
|---|:-:|:-:|---:|---:|---:|---:|
| pre-IDA | 0 | 0 | 20% | −0.04 | −0.20 | 0.03 |
| 3-sess | 0 | 0 | **68%** ⚠ | +0.03 | −0.03 | 0.06 |
| ISP15 window | 1 | 0 | 88% | +0.005 | −0.06 | **0.001** |
| DA60/ID15 | 1 | 1 | 34% | **+0.064** | **+0.139** | **0.305** |
| DA15/ID15 | 0 | 1 | 16% | −0.003 | −0.002 | 0.005 |

**Theory verdict:**

1. ✓ **β fits the θ × ρ interaction model perfectly.** Pass-through is
   nonzero (R²=0.305) only in DA60/ID15 — the unique cell with both
   θ=1 AND ρ=1. Other regimes have R² < 0.06.

2. ⚠ **Reservation share s has a 3-sess anomaly.** Pure θ predicts low
   reservation in 3-sess (still hourly settlement). Observed 68%.
   This is anticipation effect — ISP15 was announced/known before its
   activation, so firms started reservation pricing in 3-sess. Adding
   an "anticipation" indicator $A_r = 1$ for 3-sess would make $s_r =
   f(θ_r + A_r)$ fit cleanly.

3. ✓ **The DA60/ID15 reservation moderation** (down to 34% from ISP15's
   88%) makes sense in the model: when ρ=1 (IDA can respond), firms
   substitute IDA trading for reservation tranches. Bid function moves
   back closer to MC because firms can correct positions in IDA.

4. ✓ **DA15/ID15 collapse** (s=16%) is consistent with θ=0: settlement
   matches DA clock, no need for reservation pricing.

### Refined model statement

> The Spanish reform sequence is parametrised by a clock pair (σ, τ)
> determining settlement and intraday-trading granularity. A
> theoretical mechanism with two parameters — settlement-risk exposure
> $θ = 1[σ < δ_{DA}]$ and IDA-responsiveness $ρ = 1[τ ≤ σ]$ — predicts
> the regime ordering of two distinct empirical patterns:
>
> 1. **Reservation tranche share** s_r: $s_r = f(θ_r + A_r)$ where $A_r$
>    is reform-anticipation (=1 in 3-sess due to known ISP15 schedule).
>    Predicts pattern: low → high → high → moderate → low across
>    {pre-IDA, 3-sess, ISP15, DA60/ID15, DA15/ID15}. Observed.
>
> 2. **Forecast-error pass-through** β_r: $β_r = β_0 \cdot θ_r \cdot ρ_r$.
>    Predicts pattern: 0 except in DA60/ID15 where both conditions
>    hold. Observed: R²=0.305 only in DA60/ID15, < 0.06 elsewhere.
>
> The two parameters $(θ, ρ)$ are not separately identified from one
> pattern alone, but they ARE identified from the joint pattern across
> regimes. This is the structural-economic content of the dataset.

This is a clean structural model anchored in the data. Reproducing:
`scripts/analysis/theta_calibration.py`.

### What still needs work

- 3-sess anomaly: the reservation pricing in 3-sess (when settlement is
  still hourly) needs a behavioural explanation. Likely candidates:
  anticipation (firms expecting ISP15) or IDA-reform-induced
  (6→3 session structure change altering strategic incentive).
- Per-firm heterogeneity: GN/HC don't follow GE/IB pattern as cleanly.
  Possibly because their CCGT cleared volumes are too small post-reform
  to dominate the bid distribution.

---

## Pattern 9 — Welfare proxy: 63% of ISP15-window cleared CCGT MW came from > €100 bids (executed 2026-04-25)

The reservation-pricing pattern (Pattern 2) is **not** all "tranches that don't clear" — a substantial fraction **does** clear, setting marginal prices. Welfare-proxy analysis on Big-4 CCGT IDA cleared MW:

| Regime | Cleared GWh | Rev M€ | **%MW from bids > €100** | %Rev from bids > €100 |
|---|---:|---:|---:|---:|
| pre-IDA | 277,043 | 28,323 | 9.9% | 19.1% |
| 3-sess | 1,324 | 131.7 | **29.1%** | 35.6% |
| **ISP15 window** | 1,303 | 161.1 | **63.4%** | **68.0%** |
| DA60/ID15 | 4,952 | 405.6 | 8.5% | 11.4% |
| DA15/ID15 | 6,318 | 571.4 | **3.1%** | 3.7% |

Per-firm CCGT extreme contrast (% of cleared MW from > €100 bids):
- GE: 12% → 26% → **56%** → 6% → **0.27%**
- IB: 18% → 19% → 52% → 15% → 9%
- GN: 6% → 38% → **74%** → 9% → 3%
- HC: 30% → 15% → 45% → 5% → 3%

**Welfare interpretation:**

- In ISP15 window, 63% of Big-4 CCGT IDA cleared MWh came from bids
  originally priced above €100/MWh. These bids were not "pure
  reservation" — they were marginal in actual market clearing.
- Each MWh from a > €100 bid earned the clearing price (which itself
  was elevated). The strategic effect compounds: high bids both INCLUDE
  themselves at the margin (raising the firm's revenue) AND raise the
  clearing price (transferring rents from consumers to all generators).
- DA15/ID15 collapse to 3% means **the reform sequence ultimately
  eliminated the high-price-clearing channel**. Post-MTU15-DA, Big-4
  CCGT IDA clears almost entirely from competitive (< €100) bids.

**Strategic markup proxy** (cleared high-bid MW × (p* − €50)):

| Regime | days | shaded rev (M€/day) |
|---|---:|---:|
| pre-IDA | 651 | 6.23 |
| 3-sess | 40 | 0.69 |
| ISP15 window | 61 | 1.12 |
| DA60/ID15 | 15 | 1.70 |
| DA15/ID15 | 12 | 0.95 |

Per-day shaded-revenue magnitudes are smaller post-reform because
**total cleared CCGT MW collapsed** post-renewable-substitution. The
proportional metric (% MW from > €100) is the cleaner reform-shift
indicator. The absolute markup matters for welfare aggregation but
needs to be weighted by total CCGT clearing.

This is the **euros-of-rent** anchor for the structural model. The
$\theta$ parameter from the model maps to:

$$\theta_r \times \text{(MW clearing)}_r \times \text{(price uplift from reservation)} = \text{strategic transfer}_r$$

Reproducing: `scripts/analysis/welfare_proxy.py`. Output panel saved
to `data/derived/welfare_proxy_panel.parquet`.

---

## Pattern 10 — Anticipation: heterogeneous firm response to ISP15 announcement (executed 2026-04-25)

The 3-sess anomaly (Pattern 2) is **anticipation-driven**, but with
**heterogeneous timing across firms**. ISP15 was announced via CNMC
resolution on **2024-10-03**, taking effect 2024-12-01. Splitting the
3-sess regime around the announcement reveals two firm types:

| Firm | reservation share PRE-announce (2024-06-14 → 2024-10-02) | POST-announce (2024-10-04 → 2024-11-30) | Δ |
|---|---:|---:|---:|
| **GE** | 80.7% | 87.8% | **+7pp** |
| **IB** | 80.9% | 81.6% | +1pp |
| GN | 34.9% | 65.5% | **+31pp** |
| HC | 29.5% | 44.1% | +15pp |

**GE and IB pre-anticipated** — they were already at 80%+ reservation
the moment the IDA reform took effect (2024-06-14), four months before
the formal CNMC announcement. **GN and HC reacted to the announcement**,
not the IDA reform.

Monthly time series (% of CCGT capacity priced > €100/MWh):

| Month | GE | IB | GN | HC |
|---|---:|---:|---:|---:|
| 2024-04 | 61% | 16% | 3% | 0% |
| 2024-05 | 54% | 28% | 8% | 0% |
| 2024-06 | 53% | **68%** | 23% | **69%** |
| 2024-07 | 78% | 84% | 20% | 23% |
| 2024-08 | 86% | 82% | **60%** | 26% |
| 2024-09 | 81% | 76% | 40% | 38% |
| 2024-10 | 87% | 85% | 44% | 22% |
| 2024-11 | 89% | 78% | **82%** | **51%** |
| 2024-12 | 91% | 83% | 89% | 65% |
| 2025-01 | 94% | 88% | 95% | 69% |
| 2025-02 | 95% | 87% | 92% | 70% |
| 2025-03 | 95% | 92% | 68% | 29% |
| 2025-04 | 60% | **96%** | 12% | 10% |
| 2025-05 | 55% | 92% | 14% | 10% |
| 2025-09 | 11% | 84% | 11% | 14% |
| 2025-10 | **9%** | **34%** | 16% | 23% |
| 2025-11 | **3%** | **35%** | 14% | 16% |
| 2025-12 | **5%** | **12%** | 9% | 30% |

**Strong economic content**:

1. **GE and IB**: respond at IDA reform (June 2024). Sophisticated
   strategic adjustment; firms anticipate that the 6→3 session
   structure makes reservation pricing more attractive even before
   ISP15 settlement is enforced.
2. **GN and HC**: respond at ISP15 announcement (Oct 2024). Slower
   adaptation; firms react to announced rules rather than to
   market-structure changes.
3. **Reform-completion collapse timing differs across firms**:
   - GE: bid-function collapse begins April 2025 (post-MTU15-IDA),
     reaches near-zero by November 2025 (post-MTU15-DA).
   - IB: stays at 90%+ until October 2025 (post-MTU15-DA), then
     gradually falls to 12% by December 2025. **IB is the last firm
     to normalize**.
   - GN and HC: collapse already in April 2025 with GE.

### Modelable structure

The reservation-pricing dynamics fit a **two-stage anticipation +
adjustment model**:

  At time $t$, firm $i$'s reservation share $s_{i,t}$ depends on:
    - $\theta_t$ : current settlement-risk exposure (= 1 in ISP15 + DA60/ID15)
    - $A_{i,t}$ : firm $i$'s anticipation of future $\theta$
    - $\rho_t$ : current IDA responsiveness (= 1 from MTU15-IDA on)

  Firm types:
    Sophisticated (GE, IB): $A_{i,t}$ updates at IDA reform (June 2024)
    Adaptive (GN, HC): $A_{i,t}$ updates at ISP15 announcement (Oct 2024)

  Adjustment after MTU15:
    GE, GN, HC: $s_{i,t}$ falls when $\rho = 1$ + $\theta$ stays low
    IB: $s_{i,t}$ persists into late 2025; possibly higher hedging cost
        or different strategic-position constraints

This is a heterogeneous-agent extension of the pattern-2 model,
with the rational-expectations / adaptive-expectations distinction
identified directly from the announcement-date discontinuity.

Reproducing: `scripts/analysis/anticipation_test.py`. Monthly panel
saved to `data/derived/anticipation_test_panel.parquet`.

---

## Pattern 11 — Portfolio flexibility explains firm-specific collapse timing (executed 2026-04-25)

After Pattern 10 surfaced different post-reform-collapse timing across
firms, this pattern asks WHY. Test of the **portfolio-flexibility
hypothesis**: firms with more flexible technology (hydro, pumphydro)
can sustain reservation-pricing strategies longer because flexible
units fill gaps when reservation tranches don't clear.

### Firm portfolio composition (DA cleared MW share, post-IDA average):

| Firm | CCGT | Hydro | Nuclear | PumpHydro | Other |
|---|---:|---:|---:|---:|---:|
| GE | 4-5% | 7-18% | 76-89% | **2-4%** | 0% |
| **IB** | 4-17% | **16-56%** | 10-64% | **12-16%** | <1% |
| GN | 39-71% | 3-14% | 19-57% | <2% | 0% |
| HC | 0-50% | 1-10% | 12-95% | 0% | 0-31% |

**IB stands alone in PumpHydro (12-16%)** and has the highest combined
flexible-tech share (28% PumpHydro+Hydro on average). GE/HC are
nuclear-heavy, GN is CCGT-heavy.

### Monthly reservation share evolution 2025 — IB vs GE

| Month | GE CCGT | GE Hydro | GE PumpHydro | IB CCGT | IB Hydro | IB PumpHydro |
|---|---:|---:|---:|---:|---:|---:|
| 2025-01 | 94% | 49% | 31% | 88% | **97%** | **75%** |
| 2025-02 | 95% | 65% | 53% | 87% | 94% | 97% |
| 2025-03 (MTU15-IDA) | 95% | 9% | 6% | **93%** | 49% | 15% |
| 2025-04 | **60%** | 11% | 1% | **96%** | 31% | 0% |
| 2025-05 | 55% | 3% | 2% | 92% | 31% | 0% |
| 2025-06 | 62% | 46% | 29% | 93% | **93%** | **78%** |
| 2025-07 | 42% | 80% | 10% | 92% | 88% | 71% |
| 2025-08 | 28% | 76% | 7% | 94% | 83% | 68% |
| 2025-09 | **11%** | 62% | 2% | 84% | 49% | 50% |
| 2025-10 (MTU15-DA) | 9% | 53% | 25% | **34%** | 75% | 76% |
| 2025-11 | **3%** | 48% | 17% | 35% | 79% | 42% |
| 2025-12 | **5%** | 37% | 9% | **12%** | 69% | 63% |
| 2026-01 | 3% | 88% | 12% | 37% | 80% | 81% |

**Key contrasts**:

1. **GE CCGT collapses immediately at MTU15-IDA** (April 2025: 95% →
   60% within one month, drops to 3% by November). GE's hydro and
   pumphydro are too small (~2-3% of cleared MW) to sustain CCGT
   reservation strategy alone.

2. **IB CCGT stays at 88-96% throughout 2025** until MTU15-DA. Drops
   only in October-December 2025 (34% → 35% → 12%). Then briefly
   rebounds to 37% in January 2026 (winter peak).

3. **IB hydro and pumphydro maintain 70%+ reservation pricing** through
   most of 2025, including post-MTU15-IDA. Provides the substitute
   capacity that lets IB's CCGT keep reservation strategy.

4. **CCGT market share**: GE has highest (36-45%), IB moderate (19-29%).
   So IB doesn't have higher market power — its persistence is from
   portfolio flexibility, not dominance.

### Modelable structure

The reservation-pricing duration depends on **substitute-capacity
availability**:

  $\text{collapse-timing}_i = T_{MTU15} - \kappa \cdot \text{flex-share}_i$

where flex-share is the fraction of firm $i$'s cleared MW from hydro +
pumphydro. Firms with high flex-share (IB) can maintain CCGT
reservation longer; firms with low flex-share (GE) collapse
immediately at MTU15-IDA.

This is identifiable from the cross-firm × cross-time monthly
reservation-share dataset. Each firm provides one observation of
flex-share + collapse-timing.

### Three-trigger model of reservation pricing

Combining Patterns 10 + 11, the reservation-pricing dynamics are
driven by three triggers:

1. **Anticipation onset** — IDA reform (sophisticated firms GE, IB) or
   ISP15 announcement (adaptive firms GN, HC).
2. **Activation peak** — ISP15 (Dec 2024); all firms at peak
   reservation by ISP15-window mid (Jan-Feb 2025).
3. **Collapse onset** — MTU15-IDA for low-flexibility firms (GE, GN,
   HC); MTU15-DA for high-flexibility firms (IB).

Three reform dates × two firm-type axes (sophistication, flexibility)
= a 4-cell typology that explains the cross-firm time profiles.

Reproducing: `scripts/analysis/firm_collapse_timing.py`.

---

## ⚠ SKEPTICISM CHECK 2026-04-25 — same-calendar tests, several patterns weakened

User pushback: "we end up failing robustness checks; please check
again." Applied the same-calendar / same-season test that destroyed
the original Lerner finding.

### TEST 1 — Same-calendar reservation share (Pattern 2)

Winter comparison (Dec-Mar) — GE CCGT res > €100:

| Window | reservation share |
|---|---:|
| 2022-23 winter (pre-IDA, gas crisis) | 40% |
| **2023-24 winter (pre-IDA)** | **63%** |
| 2024-25 ISP15 window (post-reform) | 94% |

The pre-IDA 2023-24 winter was **already** at 63% reservation, well
above the pre-IDA full-period mean of 32%. The ISP15 → 94% jump is
+31pp from the most-recent-comparable winter, not the headline +62pp
from the full-period pre-IDA average.

Spring-summer (Apr-Sep) — GE CCGT res > €100:

| Window | reservation share |
|---|---:|
| 2023 Apr-Sep pre-IDA | 46% |
| **2024 Apr-Sep pre-IDA + 3-sess** | **65%** |
| 2025 Apr-Sep DA60/ID15 | **45%** |

**GE's DA60/ID15 reservation share is LOWER than 2024 same-calendar
pre-IDA**, and equal to 2023. The "DA60/ID15 reservation elevation"
for GE is a same-calendar artefact.

For IB the picture differs:

| Window | IB CCGT reservation share |
|---|---:|
| 2023 Apr-Sep | 47% |
| 2024 Apr-Sep | 56% |
| 2025 Apr-Sep DA60/ID15 | **91%** |

**IB DA60/ID15 elevation is real** (91% vs 47-56% same-calendar).

### TEST 2 — Bid distribution shape: GE >€100 mass is at the price cap

|  Window | GE >€100 | GE at cap (€800) |
|---|---:|---:|
| 2022-23 winter pre-IDA | 40% | **18%** |
| 2023-24 winter pre-IDA | 63% | **47%** |
| ISP15 window | 94% | 36% |
| 2025 Apr-Sep DA60/ID15 | 45% | 5% |
| 2025 Oct-Dec DA15/ID15 | 6% | 0% |

GE's reservation share is **dominated by price-cap (€800) tranches**.
Pre-IDA 2023-24 winter was 47% at the cap — already very high. The
ISP15 window has 36% at cap. **GE is doing binary cap pricing**, not
gradient reservation. And the at-cap share trajectory has been
**rising since 2022**, not specifically reform-driven.

IB by contrast has 0% at cap throughout. IB's reservation tranches are
gradient-priced (€100-300). **Different strategic behavior types.**

### TEST 3 — Pass-through R² across years (Apr-Sep same-calendar)

|  Window | n | R² | β_wind | β_solar |
|---|---:|---:|---:|---:|
| 2018 | 183 | 0.087 | +0.73 | +1.89 |
| 2019 | 183 | 0.097 | +0.97 | +1.55 |
| 2020 | 177 | 0.002 | +0.02 | -0.01 |
| 2021 | 183 | 0.018 | +0.02 | -0.00 |
| **2022** (gas crisis) | 180 | **0.215** | +0.085 | +0.077 |
| 2023 | 183 | 0.004 | +0.003 | +0.003 |
| 2024 | 183 | 0.004 | +0.001 | +0.004 |
| **2025 (DA60/ID15)** | 183 | **0.410** | +0.110 | +0.115 |

**The 2022 gas-crisis Apr-Sep already had R²=0.22.** The 2025
DA60/ID15 R²=0.41 is higher but **not unique to reform**. Gas-crisis
years also showed the pass-through pattern. Likely a market-stress /
high-renewable signature rather than a clean reform effect.

### Honest pattern table after the skepticism check

| Pattern | Survives same-calendar test? |
|---|---|
| 1. Pass-through R² jump | **PARTIAL** — 2022 also showed R²=0.22 |
| 2. Bid-function reservation rise | **PARTIAL** — GE DA60/ID15 not elevated; pre-IDA 2023-24 already at 63% |
| 3. DA-IDA wedge structure | UNTESTED but probably similar issues |
| 4. HHI concentration rise | YES — standard metric, not Lerner-formula |
| 5. A87 settlement flow swing | **YES** — pre/post December comparison, no spring-summer mixing |
| 6. Within-month dispersion | YES (FR placebo controlled) |
| 7. XBID liquidity 15× | YES — Spain-specific SIDC product |
| 8. Capacity-withholding ratio | UNCLEAR |
| 9. Welfare proxy 63% > €100 | NEEDS same-calendar check; likely partly seasonal |
| 10. ISP15 anticipation timing | **WEAKENED** — pre-IDA 2023-24 winter was already at 63% reservation, suggesting trend rather than 2024-10-03 announcement effect |
| 11. Portfolio flexibility | PARTIAL — depends on Pattern 2 validity, but the cross-firm difference (GE cap-pricing vs IB gradient-pricing) is itself robust |

### What clearly survives

1. **A87 settlement flow** (€38 → €160 → €72M/mo) — Spain-specific
   construction, no calendar overlap, pre/post December.
2. **HHI rise** from 0.28 to 0.42 — standard metric.
3. **DA15/ID15 reservation collapse for GE** to 6% — genuinely below
   ALL same-calendar pre-IDA windows (which ranged 25-71%).
4. **IB-specific DA60/ID15 reservation elevation** (91% vs 47-56% same-
   calendar). IB's gradient reservation behavior is reform-aligned.
5. **GE/IB reservation-style heterogeneity** — GE does cap pricing,
   IB does gradient pricing. A cross-firm fact, not a regime claim.
6. **Within-month price dispersion** (FR-controlled).
7. **XBID liquidity 15×** (Spain-specific by SIDC product design).

### What's now suspect / weakened

- The DA60/ID15 reservation peak for GE — same-calendar shows no excess.
  GE was already at 65% reservation in Apr-Sep 2024 pre-IDA.
- The ISP15 anticipation timing claim — pre-IDA winter 2023-24 was
  already at 63% reservation for GE, before any ISP15 announcement.
  The "anticipation jump" GE/IB observed at IDA reform might just be
  a continuation of a 2022-2024 secular trend.
- Pass-through R² jump — partly seasonal/stress-driven, not unique.
- "63% of cleared MW from > €100 bids" welfare claim — needs same-
  calendar comparison before being cited.

### Updated thesis stance

The CCGT reservation-pricing **TREND** is real and rising 2022-2024 →
peaking 2024-2025 → collapsing post-MTU15-DA. The trend predates the
reform; the **collapse** is reform-attributable. So the cleanest claim
is:

> Big-4 CCGT reservation pricing has been rising structurally since
> 2022 (gas crisis era), peaks during the asymmetric-granularity
> period (3-sess + ISP15 + DA60/ID15), and **collapses sharply at
> MTU15-DA**. The collapse is the cleanest reform-attributable fact,
> not the rise.

This is a NARROWER but DEFENSIBLE claim. The "asymmetric-granularity
window peaks in market power" framing weakens because the peak was
already happening pre-reform. The post-MTU15-DA NORMALIZATION is the
cleaner finding.

Combined with the surviving system-level (A87) and structural (HHI,
within-month dispersion) findings, the thesis backbone remains:

1. Reform changed system-level settlement flows (A87)
2. Reform increased market concentration (HHI)
3. Reform completion (MTU15-DA) collapsed CCGT reservation pricing
   from previously rising/elevated levels
4. Cross-firm heterogeneity in reservation style (cap vs gradient)
5. IB specifically shows reform-aligned within-window elevation

The "10 patterns of a single (θ, ρ) mechanism" framing OVER-STATED
the case. The reality is messier: secular trend (2022-2024 reservation
rise) + reform completion (MTU15-DA collapse) + firm heterogeneity
(GE cap vs IB gradient).

Reproducing: `scripts/analysis/skepticism_check_patterns.py`.

---

## ⚠ MORE SKEPTICISM — buy-side, forecast bias, curtailment, cross-border (2026-04-25 evening)

User: "continue similar robustness checks and try different ideas".
Applied same-calendar logic to four new angles.

### Buy-side bid pattern (P9b)

Aggregate retailer buy-side %p < €30 (low-reservation buying) by
same-calendar window:

| Window | %p<€30 | %p<€0 |
|---|---:|---:|
| 2022-23 winter pre-IDA | 15% | 3.5% |
| 2023-24 winter pre-IDA | **31%** | 7.5% |
| 2024-25 ISP15 window | 27% | 18% |
| 2023 Apr-Sep pre-IDA | 9% | 3.6% |
| **2024 Apr-Sep pre-IDA + 3-sess** | **50%** | **27%** |
| 2025 Apr-Sep DA60/ID15 | 52% | 28% |
| 2025 Oct-Dec DA15/ID15 | 37% | 18% |

The big buy-side reservation-buying jump happened **2023 → 2024**
(9% → 50% in spring/summer), before any major reform. Continues into
2025. Not reform-driven; trend-driven (renewable surplus).

GE-specific buy-side: bidding heavily below €30 across 2024-2025
(66-75%), then collapsing to **4.3% in DA15/ID15** with wavg bid going
from −€87 to +€87. **GE-specific DA15/ID15 collapse on the buy-side
mirrors the sell-side collapse.** Both reservations retire at MTU15-DA.

### Forecast bias (P12)

Wind forecast bias (signed daily error in GWh, +ve = under-forecast):

| Window | wind_bias | solar_bias |
|---|---:|---:|
| 2018 Apr-Sep | −0.43 | −0.28 |
| 2022 Apr-Sep (gas crisis) | −0.25 | −1.97 |
| 2023 Apr-Sep | −1.11 | −4.02 |
| 2024 Apr-Sep | −1.27 | −2.59 |
| **2025 Apr-Sep** | **−5.22** | +4.32 |
| 2025 Oct-Apr (DA15/ID15) | **−9.02** | −8.68 |

Wind forecast becoming more downward-biased over time (forecasts too
high, actual production lower). Linked to fast renewable installation
that outpaces forecaster calibration. **Forecasting issue, not market
design.** Not reform-attributable.

### Curtailment frequency (P13)

% of hours with DA price ≤ €0 in same-calendar Apr-Sep:

| Year | %p≤€0 | %p≤€10 |
|---|---:|---:|
| 2022 (gas crisis) | 0.0% | 0.7% |
| 2023 | 1.4% | 5.1% |
| **2024** | **13.7%** | **26.4%** |
| 2025 (DA60/ID15) | 15.6% | 29.6% |

The big curtailment-frequency jump was **2023 → 2024** (+12pp), not
2024 → 2025. Renewable-deployment-driven, not reform-driven.

### ES-FR cross-border shift (P14)

Signed (ES − FR) DA spread by Apr-Sep window:

| Year | mean signed | %ES>FR |
|---|---:|---:|
| 2018 | +€12 | 84% |
| 2022 (gas crisis) | **−€163** | 20% |
| 2023 | −€0.2 | 58% |
| **2024** | **+€15** | **65%** |
| 2025 (DA60/ID15) | +€11 | 60% |

Spain shifted from being **net-exporter** (cheaper than FR) in 2022 to
**net-importer-more-often** (65% of hours ES > FR) in 2024-25. The
shift happened **2023 → 2024**, before reform. Tracks the renewable-
deployment trajectory in Spain vs nuclear-heavy France.

## Pattern of secular trends 2022-2025

The Spanish electricity market is going through MASSIVE structural
changes that confound reform attribution:

1. **Gas-crisis aftermath** (2022 was anomaly with €203 average prices).
2. **Accelerating renewable installation** (drives curtailment frequency,
   forecast bias direction, low-price hours).
3. **Bilateral-contract reallocation** (Rule 28.8 elimination March 2025).
4. **Cross-border flow direction shift** (ES exports → ES imports).

Many of the patterns I attributed to the reform sequence are accompanying
these trends, not caused by the reform.

## What clearly survives — refined claim list

After two waves of skepticism testing (same-calendar, France placebo,
within-tech, anticipation, cross-border, forecast, curtailment), the
robust reform-attributable findings are:

1. **A87 settlement flow swing** (€38 → €160 → €72M/mo, ISP15-pivoted)
   — Spain-specific, pre/post December comparison.
2. **Within-month price dispersion**: ES doubles, FR flat. France placebo
   isolates Spain-specific reform effect.
3. **GE DA15/ID15 reservation collapse** on both sell-side (94% → 5%) and
   buy-side (70% → 4%). Below ALL same-calendar pre-reform windows.
   Reform completion (MTU15-DA) closure.
4. **IB-specific DA60/ID15 elevation** (91% vs same-calendar 47-56%). IB
   is the only firm where DA60/ID15 elevation survives.
5. **GE / IB reservation-style heterogeneity**: GE does cap (€800)
   pricing, IB does gradient (€100-300) pricing. Robust cross-firm
   typology.
6. **HHI concentration rise** (0.28 → 0.42) — standard metric.
7. **XBID liquidity 15× growth** — Spain-specific by SIDC product
   design (FR not on Iberian XBID).

The "reform causes 10 patterns" framing was an over-interpretation. The
defensible thesis claim is much narrower:

> The Spanish reform sequence produced changes in (1) aggregate balancing
> settlement flows, (2) within-month price dispersion (Spain-specifically),
> (3) market concentration, (4) the post-MTU15-DA collapse of dominant-
> firm reservation pricing strategies, and (5) cross-firm strategic-style
> heterogeneity (GE cap vs IB gradient). Many other observable patterns
> in 2022-2025 (curtailment, forecast bias, cross-border flows, secular
> reservation-pricing rise) are driven by renewable deployment and other
> non-reform structural changes, NOT by the reform sequence.

This is a **defensible, narrower** thesis. The empirical contribution is
more disciplined — distinguishing reform-attributable from co-occurring
secular trends.

Reproducing: `scripts/analysis/buy_side_skeptical.py` and
`scripts/analysis/forecast_bias_curtailment.py`.

---

## ⚠ THIRD SKEPTICISM WAVE — long-history scrutiny of "robust" survivors (2026-04-25)

User: "continue similar robustness checks". Applied long-history
trend-detection to the patterns I'd marked "robust." Most are secular.

### HHI long history (2018-2026)

| Year | HHI | Big-4 share |
|---|---:|---:|
| 2018 | 0.244 | 51.4% |
| 2019 | 0.217 | 47.9% |
| 2020 | 0.245 | 49.1% |
| 2021 | 0.264 | 51.6% |
| 2022 | 0.289 | 51.1% |
| **2023** | **0.388** | 40.5% |
| 2024 | 0.404 | 56.1% |
| 2025 | 0.414 | 63.3% |
| 2026 | 0.379 | 60.4% |

**HHI was already rising since 2021** (0.264 → 0.289 → 0.388 across
2021-2023). The biggest jump was **2022 → 2023 (+0.10)**, before any
major reform. The 2024-2025 reform-window increases are smaller
(+0.016 and +0.010). **HHI rise is mostly secular trend.**

Big-4 share is U-shaped: dropped to 41% in 2023 (gas-crisis
disaggregation), recovered to 63% in 2025. The 2024-2025 rise is real
but on top of recovery from a 2023 dip.

### XBID liquidity long history

| Year | orders/h | trades/h |
|---|---:|---:|
| 2018 | 216 | 75 |
| 2019 | 447 | 106 |
| 2020 | 831 | 247 |
| 2021 | 1,107 | 305 |
| 2022 | 1,219 | 299 |
| 2023 | 1,180 | 455 |
| 2024 | 2,278 | 497 |
| **2025** | **10,663** | **1,604** |
| 2026 | 13,535 | 1,362 |

**XBID orders/h has grown 100-200% per year since 2018.** From 2018
to 2022 alone (pre-reform period) it grew 6× (216 → 1,219). The
2024 → 2025 jump (4.7×) is the largest single-year jump but on top
of a long-running trajectory. **The "15×" headline was inflated by
pre-reform growth not separated.** Net reform attribution: the 2025
4.7× jump on top of organic growth.

### A87 long history — annual mean monthly net income

| Year | A87 net income (€M/mo) |
|---|---:|
| 2018 | 13 |
| 2019 | 10 |
| 2020 | 12 |
| **2021** | **43** ⚠ |
| **2022** | **81** ⚠ (gas crisis) |
| 2023 | 62 |
| 2024 | 67 |
| **2025** | **116** |

**The biggest jump was 2020 → 2021** (+€31M, +260%), well before any
reform. 2022 gas crisis pushed it further to €81M. Post-2022 the
baseline settled at €60-80M. The 2025 mean of €116M is clearly
elevated but the SHAPE of the increase matters:

Monthly detail 2024-2025:
- 2024 Dec: **€194M** (single highest, ISP15 first month)
- 2025 Jan-Feb-Mar: €150-153M (ISP15 winter peak)
- 2025 Apr-May: €65-79M (DA60/ID15 spring, lower)
- 2025 Jun-Sep: €127-172M (DA60/ID15 summer, high)
- 2025 Oct-Dec: **€66-78M** (DA15/ID15)

**Post-MTU15-DA settles at €66-78M, the same baseline as 2023-2024
(€60-67M annual mean).** The ISP15 winter SPIKE (€144-194M for 4
months) is the reform-pulse; the post-MTU15-DA return to €66-78M
suggests the structural change settles back to the 2023-2024
baseline.

**Net A87 reform-attributable signal:** the 4-month ISP15 winter spike
(Dec 2024 - Mar 2025) at €144-194M, on top of an already-rising
baseline. NOT the headline €38 → €160 → €72 swing I had been quoting,
because the €38 baseline pre-IDA was actually elevated by 2022-2024
secular changes.

### Refined-again surviving findings

After three skepticism waves, the cleanest reform-attributable
findings shrink to:

1. **A87 ISP15-winter spike** (Dec 2024 - Mar 2025 at €144-194M, vs
   2023-2024 baseline €62-67M). 4-month pulse of +€80-130M.
   Post-MTU15-DA returns to baseline.
2. **GE DA15/ID15 reservation collapse**: 94% sell-side → 5% and
   70% buy-side → 4%. Below ALL same-calendar pre-reform windows,
   including 2023-24 winter at 63% sell. **Genuine reform-completion
   collapse.**
3. **Within-month price dispersion**: ES nearly doubles 23→44, FR
   flat 35→44. France placebo isolates the Spain-specific reform
   contribution. ~€10/MWh excess Spain dispersion.
4. **IB-specific DA60/ID15 reservation elevation** (91% vs same-
   calendar 47-56%). IB is the only firm where DA60/ID15 elevation
   survives same-calendar.
5. **GE/IB strategic-style heterogeneity**: GE does cap-pricing
   (€800), IB does gradient pricing. Not a regime claim, a
   cross-firm fact.

What was ALSO weakening on closer look:
- HHI: mostly secular 2021-2023; small reform-window contribution.
- XBID 15×: 6× pre-reform growth dominates; reform adds maybe 4-5×
  on top.
- A87 €38→€160 framing: 2018-2020 baseline was €11M; pre-reform 2024
  was €67M. The "€38" pre-IDA average mixes early years.

### What this all means for the thesis

The empirical contribution is now **carefully limited**:

> The Spanish 15-min reform sequence produced (a) a 4-month settlement-
> flow pulse during the asymmetric-granularity ISP15 window (A87 net
> income peaking at €194M/mo vs €67M/mo 2024 baseline), (b) a sharp
> collapse of dominant-firm CCGT reservation pricing at MTU15-DA (GE
> from 94% to 5%, both buy and sell sides), (c) a Spain-specific
> increase in within-month price dispersion (vs France placebo), and
> (d) sustained reservation-pricing elevation in Iberdrola (IB)
> through the asymmetric-granularity window.

> Many co-occurring patterns in 2022-2025 — rising HHI, growing XBID
> liquidity, increasing curtailment, cross-border flow shifts, and
> the secular CCGT reservation-pricing trend — are driven by
> renewable deployment, gas-crisis aftermath, and bilateral-contract
> reallocation, NOT directly by the reform sequence.

This is a NARROWER thesis than the "10 patterns of a single mechanism"
framing. It's also more honest. The empirical contribution is the
careful disentangling of reform-attributable from co-occurring secular
effects.

Reproducing: `scripts/analysis/survivor_skeptic.py`.

---

## ⚠ FOURTH SKEPTICISM WAVE + new finding (2026-04-25)

### (1) Within-month dispersion year-by-year — Apr-Sep
| Year | ES SD | FR SD | ES − FR |
|---|---:|---:|---:|
| 2018 | 6.8 | 11.7 | −4.9 |
| 2021 | 17.3 | 25.6 | −8.3 |
| 2022 | 41.4 | 111.5 | −70.1 (gas crisis) |
| 2023 | 31.9 | 34.2 | −2.3 |
| 2024 | 34.5 | 34.7 | **−0.2** (already at parity) |
| 2025 | 39.8 | 36.0 | **+3.8** (first ES > FR) |

**The "ES doubles, FR flat" claim is overstated.** 2024 was already at
parity. The Spain-specific 2025 elevation is +3.8 €/MWh vs FR same-
calendar, not the headline +€10/MWh I had been quoting. Modest,
real, but smaller than claimed.

Winter 2024-25 ISP15 window: ES SD ~44, FR SD ~44. Already at
parity in winter too. Spain-specific elevation in winter ISP15 is
also small.

### (2) DA 15-min bid replication post-MTU15-DA (2025-10+) — NEW PATTERN

For each (date, hour, offer_code, version) cell, check whether the
4 quarter-hour bids are identical or differentiated. Big-4 4Q cells:

| Firm | 4Q cells | identical price | identical quantity | both same |
|---|---:|---:|---:|---:|
| **HC** | 9,364 | **8%** | 11% | 8% |
| **IB** | 46,337 | **24%** | 20% | 20% |
| GE | 38,101 | 63% | 59% | 52% |
| GN | 47,467 | 63% | 62% | 61% |

**Striking heterogeneity in use of new granularity:**

- **IB and HC differentiate aggressively** (80% and 92% of cells use
  different prices across quarters). They are USING the new 15-min DA
  market.
- **GE and GN replicate more often** (~50% identical). They use less
  of the granularity.

**This contradicts the prior H15 finding (nb09 memory note) that 80-99%
of Big-4 bids replicate across quarters.** That nb09 finding was on
IDA pre-MTU15-DA (when DA was still hourly). Post-MTU15-DA, the new DA
15-min data shows substantial behavioral differentiation, ESPECIALLY
for IB and HC.

This is a **NEW reform-attributable behavioral pattern**: post-MTU15-DA,
two of four Big-4 firms use the new granularity heavily, two use it
moderately. Heterogeneity in granularity-adoption is itself
modelable as a firm-specific adjustment cost or strategic choice.

### Refined surviving-finding catalog (after 4 skepticism waves)

1. **A87 ISP15-winter spike**: 4-month pulse Dec 2024-Mar 2025 at
   €144-194M vs €67M baseline (2024 mean). Post-MTU15-DA returns to
   €66-78M. **+€80-130M Spain-specific reform pulse.**
2. **GE DA15/ID15 reservation collapse** (sell + buy side, both below
   ALL same-calendar pre-reform). **Genuine reform-completion effect.**
3. **Within-month price dispersion**: ES +3.8 €/MWh excess vs FR same-
   calendar 2025 Apr-Sep. Modest Spain-specific elevation.
4. **IB-specific DA60/ID15 reservation elevation** (91% vs 47-56%
   same-calendar). Surviving firm-specific finding.
5. **GE/IB strategic-style heterogeneity**: GE cap pricing, IB
   gradient pricing. Cross-firm typology.
6. **NEW: post-MTU15-DA 15-min granularity adoption** is heterogeneous
   across Big-4: IB/HC differentiate 80-92% of 4Q cells, GE/GN only
   ~50%.

Reproducing: `scripts/analysis/dispersion_15min_check.py`.
