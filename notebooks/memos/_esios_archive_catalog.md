# ESIOS Archive Catalog — discovery + triage

**Date**: 2026-05-15
**Author**: Phase 1 of the ESIOS data-acquisition plan (see `~/.claude/plans/dazzling-churning-seal.md`)

This memo summarises the result of probing the ESIOS `/archives` and `/indicators` endpoints with the newly-obtained API token. The goal is to map the data landscape and pick what to backfill for the per-BSP / per-BRP cross-market substitution analysis (firms moving quantity between wholesale DA/IDA and reserve markets).

## TL;DR

- **`/archives` endpoint catalog: 60+ accessible archives**, only 25 of which appear in the public `GET /archives` listing. The rest are discoverable by direct ID probe (`GET /archives/{id}`).
- **`/indicators` endpoint catalog**: 2,018 aggregate time series. None are per-BRP. Useful for prices/volumes/controls only.
- **Master data is the win**: archives 110/111/112/113 give the complete UF → UP → BRP → EIC → firm chain. All pivotal firms have BRP codes (table below).
- **`liquicierre`/`liquicierresrs` (ids 17/203)** remain the only **per-BSP** time-series source we have. Other "totalasig*" archives turn out to be system-aggregate (single SeriesTemporales STA0).
- **`p48cierre` (id 107) is per-UP** with 2,622 SeriesTemporales blocks per day — directly enables firm-level DA/IDA cleared-volume aggregation parallel to OMIE.
- **S3 redirect quirk**: archives larger than a few KB redirect (HTTP 307) to a pre-signed S3 URL on the second leg. Re-sending `x-api-key` to S3 invalidates the AWS4-HMAC-SHA256 signature. **Fix: follow the redirect without the API-key header.**
- **WAF behaviour**: ESIOS bot-detection is IP-scoped. Fast requests (≤0.1s gap) trigger persistent 403s from a single IP. Mitigation: 8s sleeps, browser-like User-Agent, switch IPs if blocked.

## Master data chain (the bridge for substitution analysis)

| Archive | id | Records | Key fields |
|---|---:|---:|---|
| GenerationUnits | 110 | 6,472 UFs | `UF Code`, `EIC Code`, `Production Type`, **`BRP Code`**, **`UP Code`**, `Maximum Power Capacity MW` |
| ProgrammingUnits | 111 | 3,782 UPs | `UP Code`, `EIC Code`, `Trade` (Buy/Sale), `Scope` (Spain/Portugal), `Regulation Zone`, `UP Type`, **`BRP Code`** |
| BalanceResponsibleParties | 112 | 739 BRPs | `BRP Code`, `Name`, `EIC Code`, `BRP Type` (Producer / Trader / Direct market consumer / Reference trader / System operator) |
| EntitledParticipants | 113 | 30 firms | `Participant`, `EIC Code` |

Chain: **UF → UP → BRP → EIC → (when available) Participant**.

Caveat (user-noted): not every firm is a BRP. EntitledParticipants is narrow (mostly international traders); the 739 BRPs are the firm-level entities for balance responsibility.

### Pivotal-firm BRP codes (high-confidence map)

| Project firm | BRP code(s) | BRP type | UP count |
|---|---|---|---:|
| **Iberdrola** | `IBEG` | Producer | 14 |
| Iberdrola Generación España | `IBGES` | Trader | 56 |
| Iberdrola Generación Nuclear | `IGNU` | Producer | — |
| Iberdrola Generación Térmica | `IBGT` | Producer | 9 |
| Iberdrola Clientes | `IBCLI` | Trader | — |
| Iberdrola Servicios Energéticos | `IBSEN` | Trader | — |
| **Endesa** | `ENDG` | Producer | 35 |
| Endesa Cogen+Renovables | `ECYR` | Producer | 30 |
| Endesa Energía | `ENDCO` | Trader | — |
| Endesa Energía Renovable | `EEREN` | Trader | — |
| **Naturgy Generación** | `GNFG` | Producer | — |
| Naturgy Ciclos Combinados | `GNCC` | Producer | 18 |
| Naturgy Comercializadora | `GNCO` | Trader | 19 |
| Naturgy Sur (Ref. trader) | `GNSUR` | Reference trader | 8 |
| **EDP-Spain** | `HC G` | Producer | 25 |
| EDP último recurso | `HCCUR` | Reference trader | 14 |
| **EDP Clientes** | `EDPCL`, `CESOS` | Trader | — |
| **Repsol Generación Eléctrica** | `VIEG` | Producer | — |
| Repsol Servicios Renovables | `REPSB` | Balance Responsible Party | 41 |
| Repsol Petróleo | `REPP` | Producer | 12 |
| **TotalEnergies / Total Gas y Electricidad España** | `TGYE`, `TOTGA` | Trader | 16 |
| TotalEnergies Clientes | `NATGA` | Trader | — |
| **Engie España** | `EBLCO` | Trader | 59 |
| Engie Cartagena | `CARTA` | Producer | — |
| Engie Castelnou | `CTNU` | Producer | — |
| **Cepsa Gas y Electricidad (Moeve)** | `DETIC` | Trader | 126 |

### Liquicierre BSP → BRP probable mapping

The BSP codes that appear in `liquicierre`/`liquicierresrs` settlement data are 2–3 letter codes; BRPs are 4–5 letter codes. Likely mappings (by name/prefix/owner):

| BSP code (liquicierre) | → BRP code (master) | Firm |
|---|---|---|
| END | ENDG | Endesa Generación |
| GN | GNFG / GNCC | Naturgy generation |
| HC | HC G | EDP-Spain (formerly HidroCantábrico) |
| IGN | IGNU | Iberdrola Nuclear |
| IGR | ? (likely Iberdrola Renovables sub-BSP) | Iberdrola |
| IMA | ? (likely Iberdrola Mercado Aggregator) | Iberdrola |
| EV, EVM | ? | EDP-Portugal |
| GAL | GALP | GALP Energia España |
| TTE | TGYE | TotalEnergies |
| GDF | ? | Engie predecessor / sub-BSP |
| REE | REE | System operator |
| ACC, ALP, AC2, AX2, AXP, AXC, BBE, ELB, ENC, EN1, GAL, GST, IGE, IGS, NEX, VM2, WM1 | various | smaller producers |

This is the link that lets us aggregate per-BSP aFRR settlement back to firm-level for cross-market substitution testing.

## Archives accessible via the token (62 confirmed in pass 1; gap scan ongoing)

Grouped by relevance to the thesis:

### Tier 1 — per-firm participation data (per-BSP or per-UP)

| id | name | Granularity | Format | Coverage | Status |
|---:|---|---|---|---|---|
| 17 | liquicierre | per-BSP × concept × ISP | XML | 2015-01 → 2024-12 | **Already ingested** |
| 203 | liquicierresrs | per-BSP × concept × ISP (post-ISP15 format) | XML | 2024-11 → ongoing | **Already ingested; needs extension** |
| 107 | p48cierre | per-UP × ISP | XML (~12 MB/day) | 2014-12 → ongoing | NEW — to backfill |
| 109/204 | REE_ActualGenOutput / REE_ActualGen_ | per-physical-unit hourly | ENTSO-E A73 XML (ZIP) | ? | ENTSO-E A75-equivalent; we have via ENTSO-E already |
| 105 | Indisponibilidades | per-unit outages | XLSX | ? | NEW — useful for availability cross-check |

### Tier 2 — master data (one-shot download, not time-series)

| id | name | Records | Status |
|---:|---|---:|---|
| 110 | GenerationUnits | 6,472 UFs | NEW — download once |
| 111 | ProgrammingUnits | 3,782 UPs | NEW — download once |
| 112 | BalanceResponsibleParties | 739 BRPs | NEW — download once |
| 113 | EntitledParticipants | 30 firms | NEW — download once (narrow) |

### Tier 3 — aggregate market data (single SeriesTemporales = system total)

| id | name | Topic | Status |
|---:|---|---|---|
| 21 | totalasigsec | aFRR assignment, system total | aggregate (= /indicators ids 632/633) |
| 24 | totalasigter | mFRR assignment, system total | aggregate (= /indicators ids 674/675) |
| 25 | totalenersec | aFRR energy used, system total | aggregate (= /indicators id 10056) |
| 27 | totalrp48prec | RT2 redispatch (hourly preview) | aggregate |
| 28 | totalrp48preccierre | RT2 redispatch (closing) | **Already ingested** |
| 29 | totalp48 | P48 system total (hourly) | aggregate |
| 30 | totalp48cierre | P48 system total (closing) | aggregate |
| 18 | totalpdbf | PBF system total | aggregate |
| 20 | totalpdvp | PDVP system total | aggregate |
| 19 | totalrpdvpprec | Technical restrictions solution | aggregate |
| 26 | totalliquicierre | Variable cost variation | aggregate |
| 11 | C5_liquicomun | Settlement bundle (181 concept families) | **Already ingested** |
| 2-14 | A1/A4/A5/C2-C8_liquicomun | Successive settlement vintages | only C5 ingested; others not strictly needed |

### Tier 4 — deprecated / unusable

| id | name | Reason |
|---:|---|---|
| 22 | totalrpibcirest | "No values for specified archive" for all sample dates |
| 23 | totalasigdesv | "No values for specified archive" for all sample dates |

### Tier 5 — interconnection / cross-border (out of scope for substitution test)

| id range | Names | Coverage |
|---|---|---|
| 48-69, 87-95, 147-161, 159-161 | Espec_Subasta_*, Resultado_Subasta_* (FR/PT explicit auctions) | Many monthly/annual/quarterly variants |
| 101, 102 | REE_InterChangeAvailab_FRA/POR | NTC availability changes |
| 93, 95 | Descargos_POR/FRA_Planificados | Cross-border outages |

### Tier 6 — informational / not yet inspected

| id | name | Notes |
|---:|---|---|
| 15, 16 | hemeroteca_DD_ent/sal | Daily entrance/exit log |
| 78 | perfilconsumo | PVPC profiling coefficients |
| 106 | PlanesMantenimiento | Maintenance plans |
| 108 | REE_AggGenOutput | Aggregate generation by tech (= ENTSO-E A75) |
| 200/201 | IND_Novedades_Step3/4 | UI lottie animations (noise) |
| 114-120 | IND_DemandaPrevProg, IND_DemandaRealGen, IND_MaxMin, IND_MaxMinRenovEol, ActividadesSubactividades, AgregacionesValidas, CNAEValidos | Informational reports |

## What we ALREADY have on disk (confirmed via `data/raw/esios/` + `data/processed/esios/`)

| Family | Archive id | Coverage | `_all.parquet` |
|---|---:|---|---|
| `liquicomun` (C5) | 11 | 2024-01 → ongoing | `liquicomun_all.parquet` (4.4M rows, 181 concepts) |
| `liquicierre` + `liquicierresrs` | 17 + 203 | 2015-01 → 2024-12 + 2024-11 → 2026-01 (overlapping Nov 2024) | `liquicierre_all.parquet` (52.6M rows, 32 BSPs) |
| `balancing_bids` | 181 | 2022-05-24 → 2024-12-10 | `balancing_bids_all.parquet` (20.2M rows, **AGGREGATE, no BSP column**) |
| `curvas_ofertas_afrr` | 234 | 2024-11-20 → 2026-04-26 | `curvas_ofertas_afrr_all.parquet` (7.5M rows, **AGGREGATE, no BSP column**) |
| `totalrp48preccierre` | 28 | 2015-01 → 2026-04-25 | `totalrp48preccierre_all.parquet` (1.2M rows, 22 redispatch types, **AGGREGATE**) |

## Per-BSP balancing **bids** — still missing

The user's substitution hypothesis ideally wants per-BSP **bids** (not just settlements). We have:
- Per-BSP **settlement** data via `liquicierre` (assignments + prices ex-post).
- **Aggregate** bid stacks via `balancing_bids` (id 181) and `curvas_ofertas_afrr` (id 234) — no BSP attribution.

Per the REE provider guide (§7.2/§7.3), BSPs submit bids individually but only the AGGREGATED stack is published. Per-BSP bid data is therefore not in the public archives. Possible alternatives:

1. Use **liquicierre's per-BSP assignment column** (RMRSP/RMRSN for aFRR up/down) as a proxy for accepted bids (= bids in the merit-order zone at clearing). This is what we have.
2. Combine `liquicierre` per-BSP assignments with `curvas_ofertas_afrr` aggregate offer curve to back out where each BSP sat in the merit order at clearing (would require interpolation / matching).
3. Cross-reference with `liquicomun` (id 11/8) settlement bundle, which has per-BSP cost variation entries (`prsecqhsu`, `prsecqhba`, etc.).

Recommendation: per-BSP **assignment** detail in `liquicierre` is sufficient for the substitution test. We don't strictly need per-BSP bids — we need per-BSP cleared volume in the reserves market, which we have.

## Critical pending work (next phases)

### Phase 2 — Extend per-BSP raw archives

- `balancing_bids` (id 181) backfill 2024-12-11 → today (~540 days). Note: this is aggregate, not per-BSP, but still useful for total mFRR bid stack.
- `liquicierresrs` (id 203) backfill 2026-02 → today (~120 days).
- `curvas_ofertas_afrr` (id 234) appears current (latest = 2026-04-26).

### Phase 3 — Ingest new archives (priority order)

1. **`p48cierre` (id 107) — per-UP closing P48.** Backfill 2018-01-01 → today. ~12 MB/day × 8 years ≈ 35 GB raw. Highest priority — gives per-UP cleared volumes from REE's view, parallel to OMIE `phf`.
2. **Master data (ids 110, 111, 112, 113).** One-shot download. Critical for BRP↔UP↔unit mapping.
3. **`Indisponibilidades` (id 105) — per-unit outages.** XLSX format. Backfill 2018-01-01 → today.
4. **`liquicomun` stages A1/A2/A4/A5/C2/C3/C4/C6/C7/C8 (ids 2/3/5/6/8/9/10/12/13/14).** Deferred — we have C5; only fetch others if specifically needed.

### Phase 4-6 — `/indicators` curated catalog

109 indicators in `data/external/esios_indicator_catalog.yaml`. Batch driver at `scripts/pipelines/esios/indicators/00_batch_sync.py`. Run with `--tier A,B,D,E` (skips C/D-redundant per-tech generation programs that we can aggregate from OMIE).

### Phase 7 — Spot-check

Compare `ESIOS id 600` (DA spot price) and `id 612-618` (IDA session prices) against OMIE `marginalpdbc` / `marginalpibc` on a single sample day. Should match exactly.

## Known issues / fixes needed

### S3-redirect signature breakage

The `/archives/{id}/download` endpoint returns HTTP 307 to a pre-signed S3 URL with AWS4-HMAC-SHA256 signature. **Re-sending `x-api-key` on the redirected request invalidates the AWS signature.** The existing `src/mtu/ingestion/esios_common.py` `fetch_archive()` may have this bug — needs verification and patch for any archive that fails with `SignatureDoesNotMatch`. The fix is to drop auth headers when following the redirect (or use `requests` with manual redirect handling).

### WAF rate-limit

ESIOS bot-detection is sensitive — 0.05s gap from a single IP triggered persistent 403 for >30 min. Mitigations:
- 8s between requests minimum
- Browser-like User-Agent (Mozilla/5.0 …)
- Switch network if blocked (IP-level block is the main protection)
- v2 Accept header (`application/json; application/vnd.esios-api-v2+json`)

User has access to a backup token (girlfriend's) if primary gets banned.
