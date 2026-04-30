# Spanish electricity market — full sequential structure

Reference for the project. Covers the wholesale-market sequence (DA → IDA auctions → continuous intraday → balancing) plus REE technical-restriction processes and balancing services. Compiled from:

- REE _Ser proveedor de servicios de ajuste_, v5.0, December 2024 (`docs/regulation/spain/ree_guia_proveedor_ajuste.pdf`)
- OMIE _Detalle del funcionamiento del mercado intradiario_ (`docs/omie/mercados_intradiario_y_continuo.pdf`)
- OMIE _Modelo de Ficheros para la distribución pública de Información del mercado de electricidad_ v1.37, September 2025 (`docs/omie/ficherosomie137.pdf`)
- CNMC Circular 3/2019 (`docs/regulation/spain/20191120_cnmc_circular_3-2019.pdf`)
- CNMC Resolución 23-May-2024 SIDC IDAs (`docs/regulation/spain/20240523_cnmc_sidc_idas.pdf`)
- CNMC Resolución MTU15 28-Feb-2025 (`docs/regulation/spain/20250228_cnmc_mtu15.pdf`)
- EU Regulations: 2017/1485 (SO), 2017/2195 (EB), 2015/1222 (CACM)

---

## 1. Big picture — the sequential markets

The Spanish wholesale electricity market for any given delivery hour has multiple sequential clearings spanning **both D-1 (the day before delivery) and D (the delivery day itself)**. Each clearing produces a *firm program* that can be adjusted by the next. The IDA sessions and the continuous market both straddle the D-1 / D boundary; only the DA market and the pre-IDA REE restriction process are entirely D-1.

```
══════════════════════════════════════════════════════════════════════════════
DAY  D-1  (the day before delivery)
══════════════════════════════════════════════════════════════════════════════

  [OMIE]  Day-ahead auction                                ──►  PDBC
                                                                  │
          + bilateral contracts                            ──►  PDBF
                                                                  │
  [REE]   Pre-IDA technical-restriction process                   │
          (Fase 1 security  +  Fase 2 rebalance)           ──►  PDVD  (firm)
                                                                  │
  [OMIE]  Intraday auctions  IDA-1 ,  IDA-2                       │
                            ──►  PIBCA  ─[REE post-IDA RT]──►  PHF
                                                                  │
  [OMIE]  Continuous market (XBID/SIDC) opens ════════════════════╪══════►
                                                                  │
══════════════════════════════════════════════════════════════════╪══════════
DAY  D  (delivery day)                                            │
══════════════════════════════════════════════════════════════════╪══════════
                                                                  │
  [OMIE]  Intraday auction  IDA-3                                 │
                            ──►  PIBCA  ─[REE post-IDA RT]──►  PHF
                                                                  │
  [OMIE]  Continuous market keeps trading  ════════►  closes ~1h before
                                                       each delivery period
                                                                  │
  [REE]   Real-time technical-restriction process            ──►  P48  (live)
                                                                  │
  [REE]   Real-time balancing services                            │
          (FCR / aFRR / mFRR / RR / SRAD)                         │
                                                                  │
  [post]  Per-ISP imbalance settlement                            ▼
══════════════════════════════════════════════════════════════════════════════
```

The arrows trace **firm programs**, the checkpoints that each market produces and the next market starts from: PDBC → PDBF → PDVD → PHF (per IDA session) → P48. Times and exact session schedules are deliberately omitted here; see §4 for IDA timing and §5 for SIDC opening rules.

**Two important distinctions:**

1. **OMIE vs REE.** OMIE (market operator) runs the market clearings (DA, IDA, continuous). REE (system operator) runs the technical-restriction processes and the balancing services. The PDBC → PDBF → PDVD chain is the handoff: OMIE delivers the DA result + bilaterals to REE, REE returns the firm pre-IDA program. Subsequent IDA / continuous outputs feed into REE's PHF / PHFC / P48.

   **Terminology bridge.** OMIE's public materials (and the OMIE-published `pdvd_*.v` file, codebook §5.4) call the firm pre-IDA program **PDVD** — *Programa Diario Viable Definitivo*. REE's *Guía del Proveedor de Servicios de Ajuste* (p.236 verbatim) calls the same operational stage **PDVP** — *Programa Diario Viable Provisional*. We use **PDVD throughout** because that is the OMIE file we would ingest if we ever download it. PDVP appears below only when quoting REE.

2. **D-1 vs D boundary is fuzzy in the middle.** The DA market and the pre-IDA RT process are firmly D-1. The first IDA session(s) and PHF publication(s) are D-1. But IDA sessions can clear on D (sessions 4–6 pre-SIDC; IDA-3 post-SIDC), and the continuous market straddles both days. Real-time balancing and per-ISP imbalance settlement are firmly D (delivery day) and post-delivery.

---

## 2. Day-ahead market (Mercado Diario, MD)

**Clearing:** D-1 at 12:00. Single uniform-price auction managed by OMIE under the EUPHEMIA algorithm (SDAC — Single Day-Ahead Coupling).

**Bid types (Spec §5.1.4):**
- Simple: 1–25 price-quantity pairs per unit per period.
- Block: minimum-acceptance, multi-period, fill-or-kill.
- Complex: load-gradient + minimum-income conditions.

**Time grid:** 60-min until **2025-09-30**, 15-min from **2025-10-01** onward.

**Price limits (MIBEL zone, per OMIE):** max **+4,000 EUR/MWh**, min **−500 EUR/MWh**. Notification thresholds at +200 / −20 EUR/MWh.

**Output file (OMIE):**
- `pdbc` — per-unit cleared volumes (Spec §5.1.2.1)
- `pdbce` — same, with `grupo_empresarial` (Spec §5.1.2.2)
- `marginalpdbc` — clearing prices (Spec §5.1.1.1)
- `cab`, `det` — bid headers + details (Spec §5.1.4)
- `curva_pbc` — aggregate supply/demand curve (Spec §5.1.3.1)

**Then:** OMIE adds bilateral-contract executions communicated by BRPs to produce **PDBF** (Programa Diario Base de Funcionamiento) — the *base operational program*. PDBF includes everything from the market plus bilaterals. **No technical restrictions applied yet.**

PDBF file: `pdbf` (Spec §5.1.2.3). Quarter-hourly resolution since MTU15-DA.

---

## 3. Pre-IDA technical-restriction process (REE)

REE runs this immediately after PDBF publication, on day D-1, applied to the entire programming horizon of day D. **Two internal phases, both pre-IDA** (per REE guide §4.1):

### Fase 1 — security-criteria modifications

REE imposes "limitaciones por seguridad" and program modifications to satisfy security constraints:

- Continuity-of-operation conditions in steady state and post-contingency (per Operating Procedure 1.1 / SO Regulation EU 2017/1485)
- Reserve sufficiency for regulation and balance
- Reactive reserve for transmission-grid voltage control
- Reserve sufficiency for service restoration
- Distribution-grid security conditions (communicated by DSOs)

**Settlement:** Increases of energy are valued via the technical-restriction offers presented by service providers (least-cost solution from valid technical alternatives). Reductions are valued at the DA marginal price (zero extra cost — equivalent to canceling the DA assignment).

### Fase 2 — generation-demand rebalance

After Phase 1, REE adjusts to maintain the generation-demand balance, **respecting the Phase-1 security limits**. Increments and reductions are settled via the same technical-restriction offer pool.

Phase-2 increments and reductions carry no security-derived limits, so they CAN be modified in subsequent markets (intraday auctions or balancing services).

**Output:** **PDVD** (Programa Diario Viable Definitivo, OMIE convention; called *PDVP — Provisional* in REE's *Guía del Proveedor de Servicios de Ajuste*, p.236). **Firm.**

**Data file (ESIOS):** `totalrp48preccierre` — Phase-1 + Phase-2 redispatch quantities by `tipo_redespacho` code, `qty_up_mwh` / `qty_down_mwh` / `price_up_eur` / `price_down_eur`. Indexed by `period_start_utc`. Aggregate (not per-unit at this layer, though per-unit detail is in subscription-only ESIOS files).

---

## 4. Intraday auctions (IDA)

OMIE-operated, uniform-price-clearing implicit-allocation auctions. Coupled at European level (SIDC — Single Intraday Coupling) since **2024-06-14**.

**Price limits (per OMIE):** max **+9,999 EUR/MWh**, min **−9,999 EUR/MWh**. Notification thresholds at +200 / −20 EUR/MWh. Wider than DA (±9,999 vs +4,000/−500) because IDA is closer to delivery and used to absorb tighter, larger-magnitude residual imbalances.

### Pre-2024-06-14: 6 MIBEL sessions

Six regional Iberian sessions; managed Spain-Portugal interconnection plus Spain-Morocco and Spain-Andorra capacities. Schedule (per OMIE doc):

| Session | Open  | Close | Cleared | PIBCA pub | PHF pub | Horizon |
|---------|-------|-------|---------|-----------|---------|---------|
| 1ª      | 14:00 | 15:00 | 15:00   | 15:07     | 16:20   | 24h (1–24 D+1) |
| 2ª      | 17:00 | 17:50 | 17:50   | 17:57     | 18:20   | 28h (21–24 D, 1–24 D+1) |
| 3ª      | 21:00 | 21:50 | 21:50   | 21:57     | 22:20   | 24h (1–24 D+1) |
| 4ª      | 1:00  | 1:50  | 1:50    | 1:57      | 2:20    | 20h (5–24) |
| 5ª      | 4:00  | 4:50  | 4:50    | 4:57      | 5:20    | 17h (8–24) |
| 6ª      | 9:00  | 9:50  | 9:50    | 9:57      | 10:20   | 12h (13–24) |

### 2024-06-14 onward: 3 SIDC sessions

Three European-coupled sessions. Schedule per CNMC Resolution 23-May-2024:
- IDA-1, IDA-2, IDA-3 — exact times depend on SDAC/SIDC market calendar
- Last clearing closes ~ 1 hour before delivery hour begins

### MTU change: 2025-03-19

IDA market clock switched from **MTU60 → MTU15**. Bids and clearing now at 15-min resolution.

**Bid types (Spec §5.2.4 + OMIE intraday operations doc §2.1):** simple (1–5 tranches per period per unit, OMIE intraday doc §2.1 verbatim) + complex conditions (load gradient, minimum income / maximum payments, full first-tranche acceptance per period or per hour, minimum consecutive hours, maximum energy, block bids).
The DA simple-bid count is wider than IDA (DA bids may have more tranches per period — verify against the OMIE files spec §5.1.4 before citing the exact maximum). The asymmetry — IDA is bid-fragmentation-restricted relative to DA — is one mechanism behind why strategic spot conduct can be costly under our Block 3 friction interpretation.

**Bid presentation:** ofertas can be updated continuously after PDBF publication and during the aFRR-reserve assignment process; default offers apply if none submitted.

**Output files (OMIE):**
- `pibca` — accumulated post-IDA program per unit per session (LEVEL, signed; flag_redespacho ≡ 0 → no RT) (Spec §5.2.2.1)
- `pibci` — incremental cleared volumes per unit per session (signed CHANGE) (Spec §5.2.2.2)
- `pibcie` — same as pibci with `grupo_empresarial` (Spec §5.2.2.3)
- `phf` — final hourly program post-IDA + RT2 + rebalance per unit per session (Spec §5.2.2.4)
- `marginalpibc` — clearing prices by session (Spec §5.2.1.1)
- `icab`, `idet` — bid headers + details by session (Spec §5.2.4)
- `curva_pibc` — aggregate supply/demand curves (Spec §5.2.3.1)

**Important:** PIBCA `assigned_power_mw` is signed natively (range −99,999.9 to +99,999.9) per Spec §5.2.2.1 — no offer_type-based sign-flipping needed. Simple SUM gives net IDA position change. (See `notebooks/memos/_modelling_track.md` for why this matters in our q₂ definition.)

### Post-IDA REE intervention

After each IDA session, REE applies further security/rebalance modifications and integrates everything into **PHF** (Programa Horario Final). This is what we informally call "RT2" in the project — it's the post-IDA leg of REE's continuous restriction-resolution. PHF can therefore differ from PIBCA at unit-period level; the difference (PHF − PIBCA) is the post-IDA REE intervention measure.

**Project operational measure.** In our scripts, **post-IDA REE intervention magnitude per unit-period = PHF.assigned_power_mw − PIBCA.assigned_power_mw** at the last-session level (both files indexed by `session_number`; we take the maximum-session row per unit-period). Since PIBCA is RT-free by spec and PHF includes RT2 + rebalance, this difference is the cleanest available proxy for REE's post-IDA intervention without needing per-unit ESIOS subscription data. See `scripts/analysis/regulatory/rt2_post_blackout_channel.py` and the verification result in `results/summaries/HEAVY_RUN_SUMMARY.md` showing this measure has a publishing-convention discontinuity at MTU15-DA that ESIOS aggregate data does not show.

---

## 5. Continuous intraday market (Mercado Intradiario Continuo, MIC / XBID)

**Operator:** XBID — pan-European continuous-trading platform under SIDC (CACM Regulation 2015/1222). NEMOs operating: OMIE, EPEXSPOT, EMCO. Spain participates via OMIE's local trading solution (LTS).

**Mechanism:** Pay-as-bid order book. Each order has:
- contract reference (a specific MTU, e.g. "2026-04-30, 17:30 ESP")
- quantity, price, side (buy/sell)
- execution conditions: NON, IOC (immediate-or-cancel), FOK (fill-or-kill), ICEBERG, ICEBERG with price-increment
- validity conditions: GFS (good-for-session, default), GTD (good-till-date)
- Basket Orders allowed

**Time grid:**
- 60-min until 2025-03-18
- **15-min from 2025-03-19** (MTU15-IDA reform)

**Opening trigger (per OMIE):** Trading on D+1 contracts opens once **two conditions are met jointly** — (i) the first IDA of the current day D has cleared, and (ii) REE has published the **PDVD for D+1**. Verbatim: *"La apertura de la negociación de todos los contratos del mercado intradiario continuo para el día siguiente (D+1)... se hará a partir de la finalización de la primera subasta del día en curso (D), siempre que el operador del sistema haya publicado el Programa Diario Viable Definitivo para el día siguiente (D+1)."*

**Open period:** Trading per delivery period closes ~1h before delivery hour begins.

**Auction-handover rule (per OMIE).** Twenty minutes before each IDA closure, cross-border continuous trading **halts** for contracts entering that auction's horizon, then resumes locally (Iberian zone only) until auction bids close. This prevents round-tripping liquidity between the order book and the auction during the lead-in.

**Price limits (per OMIE):** max **+1,500 EUR/MWh**, min **−150 EUR/MWh**. Tightest of the three markets — reflects continuous-trading volatility containment and the fact that SIDC sits within the European order-book caps.

**Negotiation rounds:** OMIE structures negotiation by "rondas" (rounds) corresponding to specific time-blocks of the delivery day; cleared incremental and accumulated results are published per round.

**Output files (OMIE, Spec §5.3):**
- `pibcic` — incremental cleared volumes per unit per round (Spec §5.3.2.2)
- `pibcice` — same with firm column (`grupo_short` for short codes; `grupo_empresarial` for long codes ENDEG/IBGEG/GNCOG/HCANG) (Spec §5.3.2.3)
- `pibcac` — accumulated program per unit per round (Spec §5.3.2.1)
- `phfc` — final hourly program post-continuous + RT + rebalance per round (Spec §5.3.2.4)
- `precios_pibcic` — aggregate prices (Spec §5.3.1.1)
- `precios_pibcic_ronda` — mean price by round and period (Spec §5.3.1.2)
- `orders` — XBID limit orders (Spec §5.3.3.1)
- `trades` — XBID matched transactions (Spec §5.3.2.7)

**Project note (PIBCICE codes):** PIBCICE uses `grupo_short` for the Big-4 short codes ("GE", "IB", "GN", "HC"), while PIBCIE uses `grupo_empresarial`. This was discovered the hard way during the heavy run of 2026-04-29 — see `notebooks/memos/RESEARCH_DIARY.md`.

---

## 6. Real-time technical restrictions (REE)

After PDVD publication and continuously through real-time operation, REE analyzes system security state and detects technical restrictions that may emerge from contingencies, forecast updates, or generation/demand changes.

For periods that can still be addressed via the intraday market, modifications are made through IDA participation. For periods past the intraday close, REE applies real-time redispatches directly.

**Output:** **P48** (Programa Operativo) — the live operational program updated continuously during day D.

**Data files (ESIOS):**
- `totalrp48preccierre` (general restriction quantities, all phases; aggregate)
- per-unit P48 detail is in subscription-only ESIOS files we do not have

---

## 7. Balancing services (REE, real-time)

The "balance del sistema" per EU Regulation 2017/2195 (Electricity Balance Guideline). Spanish-peninsular system has the four standard European products plus one local product (SRAD).

### Hierarchy

```
Balance services
├── Automatic activation
│   ├── FCR (Frequency Containment Reserve / Regulación Primaria)
│   └── aFRR (automatic Frequency Restoration Reserve / Regulación Secundaria)
│       + Imbalance Netting (IGCC platform)
└── Manual activation
    ├── mFRR (manual Frequency Restoration Reserve / Regulación Terciaria)
    ├── RR (Replacement Reserves)
    └── SRAD (Servicio de Respuesta Activa de la Demanda) — Spain-specific
```

### 7.1 FCR — Regulación Primaria (REE guide §6.1)

- **Type:** automatic, mandatory, **unpaid** complementary service
- **Provided by:** all coupled generators, automatically via turbine governors responding to frequency deviations
- **Spec:** governor must support a *droop* (estatismo) such that unit output can vary by ±1.5% of nominal power; full power response within 15 s for frequency deviations <100 mHz, linear ramp between 15 and 30 s for deviations of 100–200 mHz (per Operating Procedure 7.1)
- **No European exchange platform** (EB Regulation does not require one for FCR)
- **Data:** not directly published per provider (mandatory unpaid service)

### 7.2 aFRR — Regulación Secundaria (REE guide §6.2)

- **Type:** automatic, optional (BSP must be habilitated), 5-min European standard activation product
- **Time horizon of action:** 20 s to 15 min
- **European platforms (two distinct):**
  - **IGCC** — *imbalance netting*. Spain connected **October 2020**. Compensates aFRR-energy needs of opposite-signed control blocks before any actual aFRR activation, reducing total activation needs across the European interconnected system.
  - **PICASSO** — *aFRR-energy activation*. First-country go-live June 2022; Spain connected **later** (date not specified in the REE 2024-12 guide; verify against current PO 7.2 if needed). Handles cross-border anonymised optimization of aFRR offers.
- **Local market structure:** TWO-stage market:
  - **Capacity (reservation) market:** Each day, REE communicates aFRR up/down reserve needs per quarter-hour. Providers submit offers before **16:00 D-1** (per REE §6.2; **footnote: in any case up to 75 min after PDVD publication** — the binding cutoff depends on PDVD timing; REE's guide uses the synonym PDVP here). Allocation independently for up and down per quarter-hour to minimize total system cost subject to PDBF security limits. Marginal-price clearing.
  - **Energy market:** Allocated providers must submit valid energy offers for activation in their assigned quarter-hours; voluntary offers exceeding allocated reserve also accepted. Offers updateable up to 25 min before delivery period start.

  **Caveat for thesis-grade citation:** the 16:00 D-1 capacity-offer cutoff was set by PO 7.2 prior to the SIDC IDA reorganisation of June 2024; verify against current PO 7.2 before citing the exact time.
- **AGC (Automatic Generation Control):** local master regulator continuously incorporates correction signals from IGCC (imbalance netting) and PICASSO (energy activation), then assigns offer blocks from cheapest to most-expensive until aFRR-energy need is met
- **Settlement:** marginal price computed in each control cycle
- **BSP requirements:** ≥ 100 MW habilitated for aFRR (combined up + down)
- **Data files (ESIOS):**
  - `liquicierre` — per-BSP closing settlement (since 2015)
  - `liquicierresrs` — per-BSP secondary-reserve closing settlement (since 2024-11)
  - `curvas_ofertas_afrr` — aFRR bid curves (since 2024-11)
  - `balancing_bids` — generic balancing bids

### 7.3 mFRR — Regulación Terciaria (REE guide §6.3)

- **Type:** manual, max activation time **12.5 min**, European standard product for frequency restoration after aFRR depletion
- **European platform:** **MARI** — Spain connected since **December 2024**
- **Quarter-hourly resolution** with delivery period 5–30 min
- **Offer types:**
  - Direct or programmed activation
  - Divisibility: completely divisible / divisible / indivisible
  - Complex characteristics: exclusivity, multipart
  - Conditions: technical link (no two consecutive activations), conditional link (depends on prior periods)
- **Settlement:** marginal price per quarter-hour and direction (up/down)

### 7.4 RR — Replacement Reserves (REE guide §6.4)

- **Type:** manual, max activation time **30 min**, restores aFRR + mFRR reserve levels post-IDA close
- **European platform:** **TERRE** — Spain connected since **March 2020**
- **Bid horizon:** offers up to 60 min before delivery period
- **Offer types:** simple (divisible / indivisible) or complex (exclusivity, multipart, time-linked)
- **Settlement:** marginal price set on TERRE platform

### 7.5 SRAD — Servicio de Respuesta Activa de la Demanda (REE guide §6.5)

- **Type:** Spain-specific local balancing product, 15-min FAT (similar to mFRR but longer minimum delivery)
- **Resource:** demand-side units only. Each habilitating physical demand unit (uniquely identified by CUPS, integrated into a programming unit) must individually accredit ≥1 MW offer capacity in the service-delivery periods (REE §6.5)
- **Procurement:** **annual auction** with rotating-shift scheduled activation; max one activation per day
- **Settlement:**
  - capacity (MW): valued at auction's resulting marginal price
  - energy: valued at the maximum mFRR up marginal price for the corresponding quarter-hour

### 7.6 IGCC — Imbalance Netting

European compensation platform. Aggregates aFRR-energy needs of all European control blocks and nets opposite-signed imbalances before activating actual aFRR. Reduces total aFRR-activation needs and improves reserve availability.

---

## 8. Settlement

### Imbalance settlement

BRPs (Balance Responsible Parties) are settled on residual imbalance = actual delivery − final scheduled program. Spain uses **dual-pricing**: BRPs whose imbalance has the same sign as system imbalance pay a penalty rate; BRPs with opposite sign are settled at the DA price.

### ISP — Imbalance Settlement Period

- **Until 2024-11-30:** 60 min
- **From 2024-12-01:** 15 min (Reform: ISP15 / "imbalance settlement period 15-min")

The **asymmetric-granularity window** is the period where ISP = 15 min but at least one market clears at 60 min. In our project this spans:
- ISP15-win (Dec 2024 – Mar 2025): DA60 + ID60 + ISP15
- DA60/ID15 (Mar – Sep 2025): DA60 + ID15 + ISP15
- DA15/ID15 (Oct 2025 onward): DA15 + ID15 + ISP15 — symmetric

---

## 9. Reform timeline (2024-2026)

| Date | Reform | What changed | CNMC reference |
|---|---|---|---|
| **2024-06-14** | IDA → SIDC | 6 MIBEL sessions → 3 European-coupled sessions; introduces block_order fields | `20240523_cnmc_sidc_idas.pdf` |
| **2024-12-01** | ISP15 | Imbalance settlement period 60 → 15 min | `20241003_cnmc_isp15.pdf` |
| **2024-12** | mFRR via MARI | mFRR connected to European MARI platform | (REE guide §6.3) |
| **2025-03-19** | MTU15-IDA | IDA auctions + continuous market clock 60 → 15 min | `20250228_cnmc_mtu15.pdf` |
| **2025-04-28** | Iberian blackout | System-wide blackout; REE adopts "operación reforzada" | `20260319_cnmc_informe_apagon.pdf` |
| **2025-10-01** | MTU15-DA | Day-ahead market clock 60 → 15 min | `20250228_cnmc_mtu15.pdf` |

These four reform dates appear as constants in `src/mtu/notebook_utils.py` and define the five regimes used throughout the project.

---

## 10. Project regime nomenclature

| Regime | Window | DA clock | IDA clock | Settlement clock | Sessions | Empirical objects available |
|---|---|---|---|---|---|---|
| **pre-IDA** | before 2024-06-14 | MTU60 | MTU60 | MTU60 | 6 | DA/IDA at MTU60; settlement at MTU60. Symmetric clocks → no Φ-driven friction object available. Long sample (~6 yrs); seasonality + capacity-growth controls essential for any pre-vs-post comparison. |
| **3-sess** | 2024-06-14 → 2024-11-30 | MTU60 | MTU60 | MTU60 | 3 | First post-SIDC window. Identifies the session-architecture wedge ℓ_r in the model (ΔΦ = 0 between pre-IDA and 3-sess; any change in q₂ here is attributable to ℓ, not Φ). 6 months. |
| **ISP15-win** | 2024-12-01 → 2025-03-18 | MTU60 | MTU60 | **MTU15** | 3 | First *asymmetric-clock* regime: settlement clock finer than market clocks. Φ object first activated. ~3.5 months — short sample, careful pooling. |
| **DA60/ID15** | 2025-03-19 → 2025-09-30 | MTU60 | **MTU15** | **MTU15** | 3 | Mixed-asymmetric regime: IDA technically spans settlement clock but DA does not, so Φ is intermediate between ISP15-win and DA15/ID15. Contains the April-2025 blackout — split into PRE/POST for blackout-confound robustness. ~6 months. |
| **DA15/ID15** | 2025-10-01 → onward | **MTU15** | **MTU15** | **MTU15** | 3 | Second symmetric-clock regime, post-reform. Φ collapses back to zero. The clean "recovery" boundary — comparison with pre-IDA disciplines the model's boundary-symmetry prediction. Contains post-blackout *operación reforzada*. ~4+ months as of 2026-04. |

**Symmetric clocks:** pre-IDA, 3-sess, DA15/ID15. **Asymmetric clocks (the friction window):** ISP15-win, DA60/ID15.

**Identification design notes (caveats inline; not a closed claim of which test "needs" which window).**

- Tests that *identify* the friction parameter Φ require contrast across asymmetric vs symmetric regimes. Both layers A (system transfer) and B (pass-through) draw their main signal from the asymmetric regimes ISP15-win and DA60/ID15.
- Tests that *identify* the post-SIDC session-architecture effect ℓ_r require the pre-IDA → 3-sess transition specifically (ΔΦ = 0, only ℓ moves). The 3-sess regime's short window is a power constraint here.
- The boundary-symmetry comparison (Layer A and B should match between pre-IDA and DA15/ID15; Layer C should match between 3-sess and DA15/ID15) requires both symmetric regimes to have enough power individually. Pre-IDA is long; DA15/ID15 is short.
- Bid-revision and bid-shape tests can be run within any regime (intra-regime) or across regimes (inter-regime). The economically interesting hypothesis is whether bid behaviour responds to the *clock structure*, which is an inter-regime test; intra-regime descriptive tests baseline that.
- Ito-Reguant-style strategic-conduct tests (Big-4 q₂ on regime × Big-4 interaction) need cross-regime variation; they are **not** regime-agnostic — they rely on regime contrast as the source of identifying variation.

---

## 11. Glossary

### Programs (sequential outputs)

| Code | Name | Stage | RT applied? |
|---|---|---|---|
| PDBC | Programa Diario Base de Casación | DA cleared | No |
| PDBCE | …por Empresa | DA cleared per firm | No |
| PDBF | Programa Diario Base de Funcionamiento | DA cleared + bilaterals | No |
| PDVD | Programa Diario Viable Definitivo (OMIE term; REE *Guía* calls the same stage PDVP — Provisional) | After Phase 1 + Phase 2 RT | Yes (pre-IDA RT) |
| PIBCA | Programa Intradiario Base Acumulativo | After IDA-k clearing | No (RT-free per spec) |
| PIBCI | Programa Intradiario Base de Casación Incremental | IDA-k incremental | No |
| PIBCIE | …por Empresa | per firm | No |
| PHF | Programa Horario Final (per IDA session) | After IDA + post-IDA RT + rebalance | Yes (post-IDA RT) |
| PIBCAC | Programa Intradiario Base Acumulativo Continuo | After continuous round | No |
| PHFC | Programa Horario Final Continuo (per round) | After continuous + RT + rebalance | Yes |
| P48 | Programa Operativo | Live, real-time updated | Yes (real-time RT) |

### Markets

| Acronym | Spanish | English |
|---|---|---|
| MD | Mercado Diario | Day-Ahead Market |
| MIBEL | Mercado Ibérico | Iberian Electricity Market (Spain + Portugal) |
| MIC | Mercado Intradiario Continuo | Continuous Intraday Market |
| IDA | Subasta Intradiaria | Intraday Auction |
| SIDC | — | Single Intraday Coupling (EU) |
| SDAC | — | Single Day-Ahead Coupling (EU) |
| XBID | — | Cross-Border Intraday (the SIDC continuous platform) |

### Operators

| Acronym | Full | Role |
|---|---|---|
| OMIE | Operador del Mercado Ibérico de Energía – Polo Español | Market operator (clears DA, IDA, hosts continuous) |
| OMIP | Operador del Mercado Ibérico de Energía – Polo Portugués | Portuguese market operator (forward markets) |
| REE | Red Eléctrica de España | System operator (TSO); runs RT and balancing |
| CNMC | Comisión Nacional de los Mercados y la Competencia | Regulator |
| ACER | — | EU agency for energy regulators |
| ENTSO-E | — | European Network of TSOs for Electricity |

### Balancing services + platforms

| Service | Spanish | Activation | Time | Platform |
|---|---|---|---|---|
| FCR | Regulación Primaria | Auto | 15–30 s | (none — local) |
| aFRR | Regulación Secundaria | Auto | 5 min | **PICASSO** + IGCC (netting) |
| mFRR | Regulación Terciaria | Manual | 12.5 min | **MARI** (since 2024-12) |
| RR | Reservas de Sustitución | Manual | 30 min | **TERRE** (since 2020-03) |
| SRAD | Respuesta Activa de la Demanda | Manual | 15 min FAT | (Spain-only annual auction) |

### Other terms

| Term | Meaning |
|---|---|
| BRP | Balance Responsible Party — entity responsible for matching its scheduled vs actual delivery |
| BSP | Balancing Service Provider — habilitated to provide aFRR / mFRR / RR / SRAD |
| ISP | Imbalance Settlement Period (15 min since 2024-12-01) |
| MTU | Market Time Unit — clock granularity of a market clearing |
| AGC | Automatic Generation Control — REE's local regulator for aFRR |
| EUPHEMIA | Algorithm used by SDAC to clear DA across coupled European markets |
| GRT / TSO | Gestor de la Red de Transporte / Transmission System Operator |
| GRD / DSO | Gestor de la Red de Distribución / Distribution System Operator |
| UF | Unidad Física (physical unit, e.g. one CCGT plant) |
| UP | Unidad de Programación (programming unit; can aggregate multiple UFs) |
| CCGD | Centro de Control de Generación y Demanda |
| OS | Operador del Sistema = REE |
| OM | Operador del Mercado = OMIE |
| PO | Procedimiento de Operación (REE operating procedure) |

### Restriction-resolution phases (REE)

| Stage | When | Output |
|---|---|---|
| Pre-IDA RT process | After PDBF, two phases (Fase 1 security + Fase 2 rebalance) | PDVD |
| Post-IDA RT process | After each IDA session and after continuous rounds | PHF, PHFC |
| Real-time RT process | Continuous during day D, post-PDVD | P48 |

---

## 12. References (full document set in repo)

For the formal regulatory texts:
- `docs/regulation/spain/ree_guia_proveedor_ajuste.pdf` (REE, 2024-12)
- `docs/regulation/spain/20191120_cnmc_circular_3-2019.pdf` (Circular 3/2019)
- `docs/regulation/spain/20240523_cnmc_sidc_idas.pdf` (SIDC IDAs reform)
- `docs/regulation/spain/20250228_cnmc_mtu15.pdf` (MTU15 reform)
- `docs/regulation/spain/20241003_cnmc_isp15.pdf` (ISP15 reform)
- `docs/regulation/spain/20240425_cnmc_mari_picasso.pdf` (MARI / PICASSO)
- `docs/regulation/eu/20171123_eu_ebgl_2017-2195.pdf` (EU Electricity Balance GL)
- `docs/regulation/eu/20150724_eu_cacm_2015-1222.pdf` (EU CACM Regulation)
- `docs/regulation/eu/20190124_acer_dec_01-2019_id_pricing.pdf` (ACER intraday pricing)

For the OMIE file specifications:
- `docs/omie/ficherosomie137.pdf` (OMIE files spec v1.37, 2025-09-30)
- `docs/omie/mercados_intradiario_y_continuo.pdf` (OMIE intraday operations)
- `docs/omie/euphemia-public-description.pdf` (DA clearing algorithm)
- `docs/omie/public-description-continuous-trading-matching-algorithm-updated.pdf` (XBID matching)

For ENTSO-E codes:
- `docs/entsoe/entsoe-codelist-v94.pdf` (codelist)
- `docs/entsoe/EDI_Best_Practices_v2.2.pdf` (data interchange best practices)
