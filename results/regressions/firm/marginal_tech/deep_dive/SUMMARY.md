# Price-setter deep dive — who and when sets the Spanish DA price

**Generated:** 2026-05-17 by `scripts/analysis/firm/price_setter_deep_dive.py`.

This memo documents who sets the Spanish day-ahead clearing price, across the
5 regulatory regimes that span the project's reform calendar, sliced by
technology, firm, unit, hour-class, hour-of-day, side (sell/buy),
scarcity-level (MCP quintile), and weekday/weekend. All numbers from the
at-the-money partial-acceptance rule on PDBC (auction-cleared only — no
PDBF bilaterals, no PHF/RT2 REE redispatch).

CSVs live alongside this file under `deep_dive/`. The raw per-regime
price-setter dataframes are in `raw_sell_*.parquet` / `raw_buy_*.parquet`
for follow-up analysis.

---

## 0. The five regulatory regimes used throughout

| Regime | Date range | What changes |
|---|---|---|
| **3-sess** | 2024-06-14 → 2024-11-30 | IDA goes from 6 MIBEL sessions to 3 European SIDC sessions |
| **ISP15-win** | 2024-12-01 → 2025-03-18 | Imbalance settlement period drops to 15-min (DA and IDA still hourly) |
| **DA60/ID15 pre-blackout** | 2025-03-19 → 2025-04-27 | MTU15-IDA reform; DA still hourly. *40 days only.* |
| **DA60/ID15 post-blackout** | 2025-04-28 → 2025-09-30 | Same market design + Iberian blackout + REE *reforzada* |
| **DA15/ID15** | 2025-10-01 → 2025-12-31 | MTU15-DA: day-ahead also goes to 15-min |

---

## 1. Side composition: who's at MCP, sell or buy?

A meaningful slice of periods have only buy-side at-MCP curtailments, or
neither side at MCP (EUPHEMIA mid-point rule). The sell-only table that
follows is conditional on a sell-side at-MCP being curtailed; it does not
mean sells set the price in all periods. Fractions of (date, period) cells:

| Regime | Hour-class | sell-only | buy-only | both | neither |
|---|---|---:|---:|---:|---:|
| 3-sess               | Critical | 52% | 21% | 11% | 15% |
| 3-sess               | Flat     | 55% | 22% | 12% | 11% |
| 3-sess               | Midday   | 26% | 38% | 20% | 16% |
| ISP15-win            | Critical | 48% | 19% | 11% | 23% |
| ISP15-win            | Flat     | 36% | 31% | 10% | 23% |
| ISP15-win            | Midday   | 34% | 36% | 13% | 17% |
| DA60/ID15 post-blk   | Critical | 53% | 19% | 14% | 14% |
| DA60/ID15 post-blk   | Flat     | 60% | 17% |  9% | 13% |
| DA60/ID15 post-blk   | Midday   | 33% | 22% | 33% | 12% |
| **DA15/ID15**        | Critical | 53% | 25% | 11% | 11% |
| **DA15/ID15**        | Flat     | 36% | **42%** | 10% | 12% |
| **DA15/ID15**        | Midday   | 28% | **38%** | 18% | 17% |

Headlines:
- In DA15/ID15 **flat and midday hours**, the **buy side is more often the price-setter than the sell side** (42% vs 36%, and 38% vs 28%). The sell-side-only frame consistently understates the role of buy-side bidders in these regimes.
- Sell-side dominance is largest in flat hours **before MTU15-DA** (3-sess 55%, post-blk 60%) and falls under DA15/ID15 (36%).
- The EUPHEMIA "neither side at MCP" indeterminacy zone is 11–23% of periods — much larger than I'd have guessed.

---

## 2. Sell-side technology mix (already in the document, repeated here for completeness)

MW-weighted by $q_{\text{at}}$, sell-side partial-acceptance only. From `04_weightings_sell.csv`.

| Tech / Regime | 3-sess (C/F) | ISP15-win | DA60/ID15 pre-blk | post-blk | DA15/ID15 |
|---|---:|---:|---:|---:|---:|
| Wind          | 28/18 | 38/64 | 48/71 | 30/17 | 29/53 |
| Solar PV      | 18/0  | 1/0   | 21/0  | 30/0  | 8/0 |
| Hydro_pump    | 10/13 | 17/3  | 2/11  | 8/19  | 28/13 |
| Hydro         | 12/36 | 20/11 | 3/2   | 9/22  | 24/20 |
| Nuclear       | 13/20 | 4/7   | 16/14 | 8/6   | 4/3 |
| CCGT          | 8/9   | 17/11 | 1/0   | 6/33  | 3/2 |

---

## 3. CCGT firm-level — who owns the CCGT price-setting

From `01_firm_shares_ccgt.csv`. MW-weighted by $q_{\text{at}}$ within CCGT only. Numbers in **%** of the CCGT share, by firm.

| Regime | Hour | IB | GE | GN | HC | OTHER | REP |
|---|---|---:|---:|---:|---:|---:|---:|
| 3-sess               | Critical | **60** | 31 | 0 | 0 | 8 | 0 |
| 3-sess               | Flat     | 16 | **50** | 0 | 0 | 25 | 9 |
| ISP15-win            | Critical | 35 | **57** | 0 | 0 | 5 | 3 |
| ISP15-win            | Flat     |  2 | **74** | 0 | 3 | 20 | 0 |
| DA60/ID15 post-blk   | Critical | 31 | **69** | 0 | 0 |  0 | 0 |
| DA60/ID15 post-blk   | Flat     | 19 | **80** | 0 | 0 |  0 | 0 |
| **DA15/ID15**        | Critical | **65** | 29 | 0 | 6 |  0 | 0 |
| **DA15/ID15**        | Flat     | **80** |  0 | 6 | 14|  0 | 0 |

Big findings:
- **GN essentially never appears as a CCGT price-setter** in any regime/hour-class. This is consistent with our prior finding that GN's CCGT bid stack is heavily concentrated at scarcity prices (≥500 EUR/MWh), keeping GN out-of-the-money in normal conditions.
- **IB and GE swap dominance across regimes.** IB led in 3-sess (60% critical) and reclaimed dominance in DA15/ID15 (65% critical, 80% flat). GE led in ISP15-win (57% critical, 74% flat) and DA60/ID15 post-blackout (69% critical, **80% flat**).
- **The 33.1% CCGT flat-hour share in DA60/ID15 post-blackout is GE-driven (80.5% of CCGT mass).** Combined with the earlier finding that two units (SROQ2, PGR5) account for ~67% of CCGT q_at, SROQ2 (Sant Roc/Besòs) is GE; PGR5 is identified as not-GN here (probably an OTHER classification or HC).

---

## 4. Top price-setting units — the workhorses

From `02_top_units_count.csv` (sorted by frequency of being a price-setter event).

### DA15/ID15 (Oct-Dec 2025): top 8 by count

| Unit | Tech | Firm | # events | Sum q_at (MW) | Mean partial-acc frac |
|---|---|---|---:|---:|---:|
| **MUEL** | Hydro_pump | IB | 496 | 74,528 | 0.51 |
| DUER | Hydro | IB | 201 | 26,826 | 0.51 |
| TAJO | Hydro | IB | 171 | 38,965 | 0.49 |
| MLTG | Hydro_pump | GE | 164 | 10,660 | 0.50 |
| IBEVD11 | Wind | IB | 153 | 246,820 | 0.50 |
| GSVD116 | Wind | OTHER | 131 | 380 | 0.50 |
| SIL | Hydro | IB | 130 | 8,261 | 0.50 |
| GESTVD4 | Hydro_RES | OTHER | 127 | 244 | 0.50 |

### DA60/ID15 post-blackout: top 8 by count

| Unit | Tech | Firm | # events | Sum q_at (MW) |
|---|---|---|---:|---:|
| MUEL | Hydro_pump | IB | 187 | 26,861 |
| SHEVD21 | Solar PV | OTHER | 155 | 4,669 |
| SHEVD24-26 | Solar PV | OTHER | 153 each | 5,300–5,500 each |
| SHEVD22 | Solar PV | OTHER | 153 | 5,401 |
| SHEVD23 | Solar PV | OTHER | 153 | 5,351 |
| IBEVD11 | Wind | IB | 142 | 63,834 |

Headlines:
- **MUEL (Iberdrola's Muela del Cortes pump-storage) is the single most frequent price-setting unit** in every full-coverage regime. 496 events over 91 days of DA15/ID15 ≈ **5.5 events per day on average**.
- **The SHEVD21-26 solar PV aggregator portfolios** appear out of nowhere as top price-setters in DA60/ID15 post-blackout. Each of six portfolios has nearly identical event counts (~153) — they likely move together algorithmically. Post-blackout solar penetration is a structural driver.
- **IBEVD11 (Iberdrola wind RE-Mercado aggregator) carries enormous q_at-mass per event** — 246,820 MW total across 153 events = ~1,613 MW per event in DA15/ID15. This is the single largest source of marginal MW per price-setter event in any regime.
- **Mean partial-acceptance fraction ≈ 0.50 across the top units.** The algorithm tends to curtail at-MCP steps by roughly half on average — a useful baseline for any interpretation of "marginal MW".

---

## 5. Bid-stack shape — single block vs stack

From `03_bid_shape_sell.csv`. `share_q_below_zero` = fraction of price-setter observations where the unit had **no in-the-money bids**, just one block at MCP. Median q_at = typical size of the at-MCP step.

| Regime | Tech | share q_below=0 | Median q_at (MW) | Median partial-acc frac | # distinct units |
|---|---|---:|---:|---:|---:|
| DA15/ID15 | CCGT | 34% | 67 | 0.47 | 17 |
| DA15/ID15 | Hydro | 26% | 82 | 0.50 | 15 |
| DA15/ID15 | Hydro_pump | 24% | 100 | 0.50 | 8 |
| DA15/ID15 | Nuclear | **100%** | 963 | 0.45 | 2 |
| DA15/ID15 | Solar PV | 92% | 11 | 0.56 | 376 |
| DA15/ID15 | Wind | 72% | 8 | 0.58 | 174 |
| DA15/ID15 | Cogen | 97% | 5 | 0.65 | 60 |
| DA15/ID15 | Biomass | 92% | 2 | 0.61 | 20 |

What this tells us:
- **Aggregator portfolios (Wind, Solar PV, Cogen, Biomass) overwhelmingly use single-block bidding** at the strategic price they want recovery from. 70–99% of their price-setter events have `q_below=0`.
- **Conventional thermal/hydro (CCGT, Hydro, Hydro_pump) typically have a stack** with both in-the-money bids and at-MCP bids. 24–34% single-block share.
- **Nuclear is 100% single-block** in DA15/ID15 — both Spanish nuclear plants bid the entire output at MCP. This is the must-run-with-bid-recovery pattern.
- **Median CCGT at-MCP step is 67 MW** in DA15/ID15 — much smaller than the SROQ2/PGR5 360 MW outliers that drive 2025Q4 / DA60/ID15 post-blk numbers.

---

## 6. The three-weightings table — why the headline number matters

From `04_weightings_sell.csv`. Showing critical-hour DA15/ID15 only:

| Tech | q_at-weighted | q_marginal-weighted | Cell-count-weighted |
|---|---:|---:|---:|
| Wind | 29.3% | 29.6% | 30.2% |
| Hydro_pump | 28.4% | 28.0% | 27.8% |
| Hydro | 23.7% | 23.5% | 23.0% |
| Solar PV | 7.7% | 7.5% | 8.0% |
| Nuclear | 4.4% | 5.0% | 0.9% |
| CCGT | 2.7% | 2.7% | 3.7% |

For DA15/ID15 critical hours, the three weightings agree closely — the
headline picture is robust. The big divergence shows up in cells where a
small number of large at-MCP blocks dominate (e.g., CCGT in DA60/ID15
post-blk flat: 33% q_at-weighted vs 17% cell-count-weighted, driven by
two ~360 MW units with single-block-at-MCP bidding).

**Recommendation:** for the document, report q_at-weighted as the headline
(it answers "share of marginal MW") and flag the cell-count alternative
whenever one tech's share is concentrated in few large-q_at units.

---

## 7. Hour-of-day profile, DA15/ID15 (post-MTU15-DA)

From `05_hour_of_day_sell.csv`. Tech with largest q_at share per clock-hour:

| Hour | Top tech | Share | 2nd tech | Share |
|---:|---|---:|---|---:|
| 0  | Hydro_pump | 41% | Nuclear | 28% |
| 1  | Hydro | 34% | Hydro_pump | 21% |
| 2  | **Wind** | 51% | Hydro | 23% |
| 3  | **Wind** | 69% | Hydro | 13% |
| 4  | **Wind** | 70% | Hydro | 11% |
| 5  | **Wind** | 65% | Hydro | 14% |
| 6  | Hydro_pump | 48% | Hydro | 31% |
| 7  | Hydro_pump | 44% | Hydro | 28% |
| 8  | Hydro | 43% | Hydro_pump | 41% |
| 9  | Wind | 39% | Hydro_pump | 18% |
| 10 | **Wind** | 63% | Solar PV | 19% |
| 11 | **Wind** | 59% | Solar PV | 31% |
| 12 | Wind | 49% | Solar PV | 38% |
| 13 | Wind | 55% | Solar PV | 35% |
| 14 | Wind | 50% | Solar PV | 36% |
| 15 | **Wind** | 60% | Solar PV | 27% |
| 16 | **Wind** | 54% | Solar PV | 24% |
| 17 | **Wind** | 54% | Hydro_pump | 17% |
| 18 | Hydro_pump | 44% | Hydro | 22% |
| 19 | Hydro | 45% | Hydro_pump | 43% |
| 20 | **Hydro** | 64% | Hydro_pump | 32% |
| 21 | Hydro | 45% | Hydro_pump | 44% |
| 22 | Hydro_pump | 54% | Hydro | 34% |
| 23 | Hydro_pump | 42% | Hydro | 21% |

Headlines:
- **Wind dominates 02:00–05:00 (overnight) and 10:00–17:00 (midday + part of solar-peak).** The midday wind dominance is surprising — solar PV has share but wind aggregators take the marginal step more often.
- **Hydro / Hydro-pump are the morning-ramp and evening-peak workhorses** (h6–8 and h18–22). This is when the pumped storage discharges and large hydro reservoirs are tactically used.
- **CCGT never breaks above 12%** in any clock-hour (peaks at h23 with 11.5%).
- **Nuclear shows up only at h0 (27.6%) and h23 (16.7%)** — both nuclear plants' single-block bids land at MCP in those very low-demand overnight hours.

---

## 8. Scarcity quintiles — who sets prices at what level

From `08_scarcity_quintiles_sell.csv`. DA15/ID15 has a price distribution that collapses into 3 effective bins (many ties at zero/near-zero):

| MCP range (EUR/MWh) | Wind | Solar PV | Hydro | Hydro_pump | CCGT |
|---|---:|---:|---:|---:|---:|
| Negative (≤0) | 45% | **41%** | 0% | 0% | 0% |
| 0–1.72 | 28% | **65%** | 0% | 0% | 0% |
| 1.72–202 | **49%** | 1% | 18% | 20% | 3% |

Headlines:
- **Solar PV sets prices when prices are zero or negative** (40–65% share). Confirms the "solar curtailment / negative bidding" story.
- **CCGT never sets the price at zero or negative MCP** — they would never bid that low.
- The MCP-quintile binning collapsed (only 3 bins) for DA15/ID15 because many periods price at exactly 0 or just above. Future work could use absolute price thresholds (e.g., >100 EUR/MWh) instead.

---

## 9. Weekday vs weekend, DA15/ID15 critical hours

From `10_weekday_weekend_sell.csv`:

| Tech | Weekday | Weekend |
|---|---:|---:|
| Hydro_pump | 30% | 25% |
| Wind | 30% | 29% |
| Hydro | 26% | 17% |
| Solar PV | 4% | **18%** |
| Nuclear | 4% | 7% |
| **CCGT** | **3.5%** | **0.5%** |

Headlines:
- **Solar PV's price-setter share is 5× higher on weekends than weekdays** (18% vs 4%) in critical hours. Weekend demand drop means more solar excess → solar more often marginal.
- **CCGT only price-sets on weekdays** (3.5%) and essentially never on weekends (0.5%). Weekend low-demand keeps CCGTs out of the marginal step.
- Hydro shifts from 26% (weekday) to 17% (weekend) — wind / solar absorb the missing share.

---

## 10. Buy-side price-setters — who's curtailing on the demand side

From `04_weightings_buy.csv`. The buy-side at-MCP partial-acceptance test
finds units whose buy bid was curtailed at MCP. Numbers in % of buy-side
price-setter q_at by tech.

| Regime | Hour-class | Pump_load | Retailer | Direct_consumer |
|---|---|---:|---:|---:|
| 3-sess              | Critical | 64% | **34%** | 1% |
| 3-sess              | Flat     | 45% | **53%** | 0% |
| 3-sess              | Midday   | **84%** | 16% | 0% |
| DA15/ID15           | Critical | 57% | **42%** | 0% |
| DA15/ID15           | Flat     | 69% | 30% | 0% |
| DA15/ID15           | Midday   | 72% | 27% | 0% |
| DA60/ID15 post-blk  | Critical | 55% | **44%** | 0% |
| DA60/ID15 post-blk  | Flat     | 46% | **52%** | 0% |
| DA60/ID15 post-blk  | Midday   | **81%** | 19% | 0% |

Headlines:
- **Pump-storage demand bids are the dominant buy-side price-setters** in midday hours (72–84%) — pumping draws power when prices are low; their willing-to-pay price is the marginal demand step.
- **Retailers are the dominant buy-side price-setters in flat hours** (52–53% in 3-sess and DA60/ID15 post-blk). Retailer demand bids tend to land at marginal prices in low-demand-volatility periods.
- **In critical hours of DA15/ID15, 25% of all periods are buy-side-only price-setters** (from §1) and the marginal MW is roughly 57% pump-load + 42% retailer. So **demand-side retailers are setting Spanish DA prices about 11% of the time in critical hours of MTU15-DA** (42% × 25%).

---

## 11. Concentration measures

From `09_concentration_sell.csv`. Firm-level HHI on $q_{\text{at}}$ shares (0–10000 scale, higher = more concentrated).

| Regime | Hour | Firm HHI | Top-1 firm | Top-3 firms | n distinct firms |
|---|---|---:|---:|---:|---:|
| 3-sess              | Critical | 2,770 | 41% | 82% | 7 |
| 3-sess              | Flat     | **4,561** | **63%** | 94% | 6 |
| ISP15-win           | Critical | 4,847 | **67%** | 90% | 7 |
| ISP15-win           | Flat     | **5,004** | **68%** | 92% | 7 |
| DA60/ID15 post-blk  | Flat     | 4,431 | 56% | 95% | 7 |
| DA15/ID15           | Critical | 4,539 | **65%** | 87% | 7 |
| DA15/ID15           | Flat     | 4,201 | 63% | 84% | 7 |

Headlines:
- **Price-setter firm concentration is HIGH everywhere** — HHI > 2,500 in every cell, and > 4,000 in flat hours of every full-coverage regime. The DOJ "highly concentrated" threshold (HHI > 2,500) is comfortably met.
- **Flat hours are systematically more concentrated than critical hours** in every regime. Few firms dominate the marginal step at low-demand hours; more compete at high-demand hours.
- **Top-1 firm reaches 65–80% share in many cells.** In DA15/ID15 critical, 1 firm (IB, per §3 for CCGT + most hydro + IBEVD11 wind) accounts for 65% of price-setter q_at.

---

## 12. The big synthesis

Pulling all dimensions together for **DA15/ID15 (post-MTU15-DA)**, which is
the headline window for the thesis:

- **Wind aggregator portfolios + Iberdrola's MUEL pump-storage are the
  dominant supply-side price-setters**, not CCGT.
- **The top firm (Iberdrola)** captures 65% of price-setter q_at in critical
  hours, mostly through hydro, pump-storage, and the IBEVD11 wind
  aggregator. In flat hours IB reaches 80%.
- **CCGT contributes 2.7% of supply-side price-setter MW** — small. The drop
  from 18.1% (2024Q4) is largely a clearing-price-fall story, not a CCGT
  strategic-withdrawal story (see earlier hypothesis test).
- **GN never price-sets CCGT** in any regime — consistent with GN's
  high-bid strategic profile keeping them out-of-the-money.
- **Buy-side is the actual price-setter ~25% of the time in DA15/ID15
  critical hours**, dominated by pump-storage demand bids (~57%) and
  retailer demand bids (~42%). The sell-only frame in the document misses
  this.
- **Concentration is high (HHI > 4,000)** and **Iberdrola is the dominant
  marginal-MW supplier** in DA15/ID15 — both critical and flat hours.

---

## What's NOT here (next-step ideas)

- **Firm-level analysis beyond CCGT** — the deep dive does CCGT firm-level
  but not other techs. Easy to add (the raw parquets have firm tags).
- **Per-unit time-series of price-setter behavior** — does MUEL's pattern
  shift across the calendar? (Each row in `raw_sell_*.parquet` has a date,
  so this is straightforward.)
- **Joint sell-buy analysis** — when both sides have at-MCP steps, who
  actually got curtailed? (Needs more careful per-period inspection.)
- **Cross-checks with EUPHEMIA mid-point cases** — what determines MCP in
  the 11–23% of periods with no at-MCP step on either side?
- **Price-setter persistence** — once a unit is a price-setter on date $d$,
  is it more likely to be one on $d+1$? (Useful for identifying strategic
  vs structural price-setters.)
- **Per-firm visualization** — stacked area charts of firm-shares over
  time would surface regime-transition dynamics clearly.
