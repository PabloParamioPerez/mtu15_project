# CNMC sanctioning resolutions — economic methodology extracted

This folder holds the full PDFs of the six thesis-relevant CNMC sanction
resolutions. The summary below pulls the **economic methodology** from
the most modern of them (Sabón 3, 2023) and notes how it relates to
methodology used in our own analyses (F1/F2/F7/F13).

## Files

| File | Case | Period | Fine + indemnif. | Pages |
|---|---|---|---|---|
| `cnmc_iberdrola_hidro_2015_SNC-DE-046-14_resolucion.pdf` | IB hydro Duero/Sil/Tajo | 30 Nov–23 Dec 2013 | €25M | 81 |
| `cnmc_naturgy_2019_SNC-DE-175-17_resolucion.pdf` | Naturgy CCGT | Oct 2016–Jan 2017 | €19.5M (~€13M benefit) | ~ |
| `cnmc_endesa_BES_2019_SNC-DE-174-17_resolucion.pdf` | Endesa Besós 3+5 | Oct 2016–Jan 2017 | €5.8M (~€4M benefit) | ~ |
| `cnmc_naturgy_sbo3_2023_resolucion.pdf` | Naturgy Sabón 3 (Galicia) | 23 Mar 2019–31 Dec 2020 | €6M fine + €35.5M indemnif. | 51 |
| `cnmc_engie_castelnou_2023_SNC-DE-152-22_resolucion.pdf` | Engie CTNU availability | — | Art 65.27 fine | ~ |
| `cnmc_ignis_ECT2_2023_SNC-DE-151-22_resolucion.pdf` | Ignis ECT2 availability | — | Art 65.27 fine | ~ |

The post-2026-04 batch of expedientes (~50) opened in the post-blackout
investigation does NOT yet have published resolutions — only the
incoación press release (`cnmc_NP_incoaciones_28A_20260417.pdf` in
the parent `regulation/` folder) and the CSV expediente list
(`cnmc_blackout_expedientes_2026.{md,csv}`). Resolutions are expected
9–18 months after incoación.

---

## The CNMC's economic methodology — extracted from SBO3 2023

The Sabón 3 resolution (51 pages) lays out a complete framework for
proving Article 65.33 LSE 24/13 — *manipulación del precio de los
servicios de ajuste mediante la realización de ofertas a precios
excesivos*. The same framework is reused in more abbreviated form in
the 2019 cases. Five components:

### 1. Two empirical observations that establish the case

The CNMC opens with a striking pair of facts about Sabón 3 over
2019-03-23 → 2020-12-31:

- **Inverted income ratio**: Sabón 3 earned **92.6% of its income in
  restrictions and 7.4% in DA** — vs the rest of Spanish CCGTs at
  **15.8% / 84.2%** (other CCGTs are mainly DA plants; Sabón 3 became
  essentially a "restrictions-market specialist").
- **Bid disparity**: Sabón 3's restrictions-market bids ran €54-122/MWh
  (around €100 sustained 2019, around €85 sustained 2020), while DA bids
  ran €40-60/MWh. **Daily DA-vs-restrictions wedge ranged from €4 to €70.**

(Resolution pages 1-3, with Gráfico 1 showing the time series and
Gráficos 2/3 showing the income decomposition.)

### 2. Cost-decomposition discovery procedure (information request)

The CNMC opened a parallel supervisory expediente (IS/DE/030/21) and
required Naturgy to provide, **with daily breakdown for 2019-2020**:

- Reflected gas-natural price (with justification of the gas-price reference)
- CO2 emission-rights price
- Variable O&M costs
- Performance ratio (yield) used in the offer construction
- "Otros" — economic decomposition of any other components

Plus: detailed justification for any variation in restrictions-market
offer prices that does NOT correspond to variations in those components.

(Resolution pages 4-5, "Requerimiento de información".)

The firm responded but **failed to provide a detailed breakdown of the
"Otros" component** — which the CNMC then identified as the one carrying
most of the variance in the offer (Gráfico 6, redacted).

### 3. The three-situation pivotality classification (the methodological core)

The Galicia electrical zone has **only two CCGTs that can resolve local
restrictions**: Sabón 3 (Naturgy, 391 MW) and Puentes-García-Rodríguez 5
(PGR5, Endesa, 856 MW). Plus four coal plants (PGR1-4, 350 MW each, all
Endesa) and Meirama-1 (Naturgy, 542 MW coal). With CO2 making coal
expensive and PGR5 having 48% unavailability over the period, Sabón 3
became frequently pivotal.

The CNMC partitions every hour into three situations:

- **Situation 1** — both CCGTs available, only one programmed (1,377 hours, 8.8%)
  → competitive environment; the bid-pair Sabón 3 vs PGR5 disciplines prices
- **Situation 2** — PGR5 unavailable, Sabón 3 only CCGT (7,481 hours, **48%**)
  → Sabón 3 is the **only** CCGT capable of resolving restrictions; pure
  pivotality
- **Situation 3** — both Sabón 3 and PGR5 needed simultaneously (3,080 hours, 19.7%)
  → still pivotal in the sense that Sabón 3's withdrawal would force more
  expensive solutions

Plus 23.5% of hours where no CCGT was needed at all.

### 4. The pivotality-conditional bid-disparity test

For each situation, the CNMC measures **average DA-vs-restrictions bid
disparity** for Sabón 3:

- Situation 1 (competitive): **+34%** on average
- Situation 2 (sole CCGT): **+103%**
- Situation 3 (both needed): **+78%**
- Aggregate Sit 2+3 (reduced competition): **+95%**

The 95% vs 34% gap is the empirical signature of market-power
exploitation. The conduct is "increment of bid by 61pp specifically when
PGR5 is out of service" — the firm sets bids "not in function of its
own production cost but of the cost of its potential competitors"
(page 20).

### 5. The benefit calculation — €43.2M (the indemnification base)

The resolution at page 21 quantifies the benefit at **€43.2 million**
(the press release's €35.5M was an earlier figure; the final
indemnification was set at €35.5M with the difference reflecting
defensible portion of the high bids). The calculation:

> *Dicho beneficio se ha calculado como la diferencia entre los ingresos
> obtenidos por la central en aquellas horas en que el entorno fue menos
> competitivo (situación 2 y 3 del Hecho Probado Tercero) y los que
> habría obtenido de aplicar un diferencial sobre el mercado diario para
> todo el periodo acorde al obtenido en el entorno más competitivo
> (situación 1).*

In plain English:

```
benefit = sum_{h in Situation 2 ∪ 3} [
  q_h * (p_actual_RTT_h - p_DA_h * 1.34)
]
```

Where:
- `q_h` = MWh dispatched in restrictions at hour h
- `p_actual_RTT_h` = the bid price actually realised (pay-as-bid)
- `p_DA_h` = Sabón 3's own DA bid at hour h
- `1.34` = the average disparity revealed by the firm itself in
  Situation 1 (the competitive benchmark)

**The counterfactual is the firm's OWN bidding in competitive periods.**
The CNMC does NOT try to identify a "true marginal cost"; it uses the
firm's revealed-preference behaviour when it had to compete.

### 6. The legal standard — Article 65.33 LSE

> *La manipulación del precio de los servicios de ajuste por parte de un
> agente del mercado mediante la realización de ofertas a precios
> excesivos, que resulten dispares de forma no justificada de los
> precios ofertados por el mismo en otros segmentos del mercado de
> producción.*

Two elements (page 24):

1. **Manipulación**: presenting excessive-priced bids in the adjustment-
   services market segment (restricciones técnicas is part of "servicios
   de ajuste").
2. **Disparity**: the bids must be "disparate in a non-justified way"
   relative to bids by the **same agent** in other segments. The
   benchmark is internal to the firm — own DA bids — NOT a peer-firm
   benchmark or a marginal-cost computation.

The "non-justified" element is the burden the firm fails: if the firm
cannot show that the wedge tracks its actual cost components (gas, CO2,
yield, variable O&M), the disparity is presumed manipulative.

---

## How this maps to our own analyses

| CNMC concept | Our analogue |
|---|---|
| Three-situation pivotality classification | **F13** (IB price-setting power varies with competitive thinness at the margin) — same idea applied at the DA-clearing margin instead of the restrictions market |
| Counterfactual = own bidding in competitive periods | Conceptually parallel to **F7** Ciarreta-Espinosa synthetic-firm method (use Fringe-firm bids as counterfactual), but the CNMC uses **within-firm same-plant** counterfactual; F7 uses **across-firm same-tech** |
| Bid-disparity within-firm DA vs restrictions | We don't have a directly comparable analysis. **OPEN ANALYSIS**: with REE's restricciones-técnicas bid data (ESIOS `totalrp48preccierre` already in our `data/processed/esios/restricciones/`), we could replicate the CNMC's three-situation test for ALL Big-4 CCGTs in 2024-2025 (post-MTU15 era) and test whether the conduct persists or has been corrected after the 2023 sanctions |
| Pivotality factors known ex-ante (CO2, PGR5 unavailability) | Maps to our **F14/F18 availability sweep** — we already documented which CCGT plants face systematically higher unavailability in 2024-25; the CNMC framework asks whether the SURVIVING plants are pivotal in their zones |
| Bid construction "based on competitor cost, not own cost" | This is exactly the **B5/B6 forecast-error → imbalance pass-through** mechanism we already document at the system level; the CNMC version is plant-level |
| €43.2M benefit (Sabón 3 alone) over 21 months | For comparison: **F7** estimates €820M IB DA-clearing transfer over 14 months (post-MTU15-IDA window); SBO3 was a single plant in a single zone, the F7 number is system-wide |

## Suggested next analyses inspired by the methodology

1. **Replicate the three-situation framework for our F15/F17/F18 plants**:
   for each Big-4 CCGT in 2024-2025, classify hours by (zone-pivotal,
   non-pivotal) using A73 per-unit dispatch + A80 outages + zonal supply
   info. Then compute within-firm DA-vs-restrictions wedge in each class.
   This is THE direct translation of the CNMC test to the post-blackout
   period.

2. **Use ESIOS `totalrp48preccierre` data already on disk**: we have 11+
   years of monthly restrictions-zone closure prices. Build the firm-
   level RZ revenue panel and compute share by firm. This complements
   F19 (aFRR per-firm revenue) at the restrictions layer.

3. **Cross-reference our F14/F17 availability sweep with the CNMC's
   framework**: the CCGT plants we flagged with >12pp CF drop 2024-25
   (SROQ1, ARCOS3, CTN4, BES4, TAPOWER, ALG3, MALA1, CTGN2, PVENT2, SBO3)
   may be unavailability-induced pivotality engineering — i.e. firms
   strategically idling some plants knowing it raises pivotality
   premium for surviving plants. The CNMC's "Sabón 3 set bids based on
   knowing PGR5 was out 48% of the time" reading suggests that
   intra-firm fleet management can BE the manipulation.

The 2019 Naturgy and Endesa cases (€19.5M and €5.8M) and the 2015 IB
hydro €25M case are likely earlier-vintage versions of the same
framework. Worth reading for: (a) the pre-restrictions-market era of
the IB hydro case (which uses DA marginal-clearing analysis instead),
and (b) any methodological refinements between 2019 and 2023.
