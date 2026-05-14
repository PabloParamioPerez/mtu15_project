# MTU15 Project — Spanish electricity market: granularity reforms 2024–2025

Master's thesis (CEMFI, 2026) on the Spanish wholesale electricity market reforms that progressively reduced the **Market Time Unit** (MTU) from 60 to 15 minutes across three sequential stages, and the **Imbalance Settlement Period** (ISP) on the same day. Builds a fully reproducible data pipeline for OMIE (market operator), ESIOS / REE (system operator), and ENTSO-E (pan-European TSO data) to study how dominant firms exploit within-hour granularity to extract rents.

The empirical strategy is a **within-day difference-in-differences** comparing critical hours (05:00–09:00, 16:00–23:00 — high systematic within-hour demand variation) against flat hours (01:00–04:00 — low variation), across firms partitioned by CNMC's dominant-operator classification.

---

## Quick orientation

| Where to look | What you'll find |
|---|---|
| [`thesis/paper/paper.tex`](thesis/paper/paper.tex) → [`paper.pdf`](thesis/paper/paper.pdf) | Single-file thesis paper (the deliverable). Sections, not chapters. |
| [`CLAUDE.md`](CLAUDE.md) | Canonical project rules: data layers, claim-status discipline, OVB protocol, file conventions. |
| [`CLAIMS_LEDGER.md`](CLAIMS_LEDGER.md) | Single source of truth for empirical claims (alive / wounded / dead). One row per claim. |
| [`notebooks/memos/`](notebooks/memos/) | Research diary, modelling-track memos, audit trail, identification-history appendix. |
| [`notebooks/memos/RESEARCH_DIARY.md`](notebooks/memos/RESEARCH_DIARY.md) | Append-only chronological log of decisions and claim changes. |
| [`notebooks/memos/_esios_archive_catalog.md`](notebooks/memos/_esios_archive_catalog.md) | ESIOS API archive triage memo (which archives we ingest and why). |
| [`data/external/esios_indicator_catalog.yaml`](data/external/esios_indicator_catalog.yaml) | 139 curated ESIOS indicators with per-indicator native granularity. |

Run `uv run pytest` for the test suite; `uv run python scripts/pipelines/.../00_*.py` for any pipeline step. All pipeline scripts are idempotent.

---

## Reform timeline

| Reform | Date | What changed |
|---|---|---|
| **ISP15** | 2024-12-01 (effective 2024-12-09) | Imbalance settlement period changed from 60 min to 15 min |
| **MTU15-IDA** | 2025-03-19 | Intraday markets (3 auctions + continuous XBID) switched from MTU60 to MTU15 |
| **MTU15-DA** | 2025-10-01 | Day-ahead market switched from MTU60 to MTU15 |

These three dates appear as constants `ISP15_REFORM`, `INTRADAY_REFORM`, `DAY_AHEAD_REFORM` throughout the codebase.

---

## Data sources

Three sources, kept strictly separate at the data layer (see `CLAUDE.md` § Source separation rule).

### OMIE — Iberian market operator

#### Day-ahead market

| Family | Description | Coverage | Spec |
|---|---|---|---|
| `marginalpdbc` | Clearing prices | 2018-01-01 – present | §5.1.1.1 |
| `pdbc` | Final programs by unit (auction-cleared only) | 2018 – present | §5.1.2.1 |
| `pdbce` | Final programs by firm | 2018 – present | §5.1.2.2 |
| `pdbf` | PDBC + bilateral-contract executions (`offer_type=4`) | 2018 – present | §5.1.2.3 |
| `curva_pbc` | Aggregate supply / demand curves | 2018 – present | §5.1.3.1 |
| `cab` | Offer headers | 2017 – present | §5.1.4.1 |
| `det` | Offer details (price / quantity tranches) | 2018 – present | §5.1.4.2 |
| `capacidad_inter_pbc` | Interconnection capacity post-PBC | 2018 – present | §5.1.6.1 |
| `capacidad_inter_pvp` | Interconnection capacity post-restrictions | 2018 – present | §5.1.6.2 |

#### Intraday auctions

| Family | Description | Coverage | Spec |
|---|---|---|---|
| `marginalpibc` | Clearing prices by session | 2017 – present | §5.2.1.1 |
| `pibca` | Accumulated programs by unit | 2019 – present | §5.2.2.1 |
| `pibci` | Programs by unit and session | 2019 – present | §5.2.2.2 |
| `pibcie` | Programs by firm and session | 2019 – present | §5.2.2.3 |
| `phf` | Final hourly program by unit and session (OS-established) | 2019 – present | §5.2.2.4 |
| `curva_pibc` | Aggregate supply / demand curves | 2018 – present | §5.2.3.1 |
| `icab` / `idet` | Offer headers / details | 2018 – present | §5.2.4.1–2 |
| `osanulaintra` / `omanulaintra` | Cancelled offers (SO / MO) | sparse | §5.2.6 |

#### Intraday continuous (XBID)

| Family | Description | Coverage | Spec |
|---|---|---|---|
| `precios_pibcic` / `precios_pibcic_ronda` | Continuous-market prices | 2018-06 – present | §5.3.1 |
| `pibcac` | Accumulated programs by unit | 2018-06 – present | §5.3.2.1 |
| `pibcic` / `pibcice` | Programs by unit / firm and round | 2018-06 – present | §5.3.2.2–3 |
| `phfc` | Final hourly program by unit and round (OS-established) | 2018-06 – present | §5.3.2.4 |
| `trades` | XBID matched transactions | 2018-06 – present | §5.3.2.7 |
| `orders` | XBID limit orders | 2018-06 – present (90-day delay) | §5.3.3.1 |

OMIE file specification: `docs/omie/ficherosomie137.pdf` (v1.37, 2025-09-30).

### ESIOS — REE (Spanish TSO)

Two endpoint families on the ESIOS API (`https://api.esios.ree.es`), with `ESIOS_TOKEN` from `.env`:

**Indicators** (`/indicators/{id}`) — 2,018 system-wide time series. We curate **139 indicators** in [`data/external/esios_indicator_catalog.yaml`](data/external/esios_indicator_catalog.yaml), tagged with their native granularity (15-min / hour / day / month) and grouped into priority tiers:

- **Tier A** — fuel / CO₂ prices, renewable forecasts, demand actuals / forecasts (controls for parallel-trends regressions)
- **Tier B** — DA + IDA clearing prices (sanity-check against OMIE), 13 PVPC quarter-hour components, demand program stages
- **Tier D** — reserve market prices: aFRR / mFRR / RR, RPA, Gestión de Desvíos, technical-restrictions Fase I & II
- **Tier E** — reserve market volumes, cross-border balancing flows, SRAD demand response

**Archives** (`/archives/{id}`) — daily/monthly file dumps per archive ID. Currently ingested:

| Family | Archive ID | Granularity | Coverage | Format |
|---|---:|---|---|---|
| `liquicierre` | 17 | per-BSP × concept × ISP | 2015-01 → 2024-12 | XML |
| `liquicierresrs` | 203 | per-BSP × concept × ISP (post-ISP15 format) | 2024-11 → present | XML |
| `liquicomun_c5` | 11 | settlement bundle (181 concept families) | 2015 → present | XML |
| `balancing_bids` | 181 | aggregate mFRR bid stack | 2022-05 → 2024-12 (archive retired post-ISP15) | XML |
| `curvas_ofertas_afrr` | 234 | aggregate aFRR offer curves | 2024-11 → present | XLS |
| `totalrp48preccierre` | 28 | RT2 (reforzada) redispatch closing | 2015 → present | XML |
| `indisponibilidades` | 105 | per-unit outage snapshots (UF and UP) | 2018 – present (monthly snapshots) | XLS |
| `GenerationUnits`, `ProgrammingUnits`, `BalanceResponsibleParties`, `EntitledParticipants` | 110, 111, 112, 113 | master-data references | one-shot dump | JSON |

Archive triage memo: [`notebooks/memos/_esios_archive_catalog.md`](notebooks/memos/_esios_archive_catalog.md).

### ENTSO-E — pan-European TSO data

| Code | Description | Used for |
|---|---|---|
| A65 | System total load | Demand-class controls |
| A72 | Reservoir filling | Hydro water-value covariate |
| A73 | Actual generation per type | Renewable-share computation |
| A75 | Actual generation per unit | Per-unit tech composition (post-MTU15-DA) |

Spain control-area EIC: `10YES-REE------0`. Token in `.env` as `ENTSOE_TOKEN`.

---

## Pipeline structure

All pipelines follow the same three-stage convention:

```
scripts/pipelines/{omie,esios,entsoe}/{family}/
  00_sync_<family>.py        # download raw files (idempotent)
  10_parse_<family>.py       # raw → per-file parquet
  20_build_<family>_all.py   # consolidate into <family>_all.parquet
```

### OMIE pipelines

Many OMIE families publish both as **daily files** (recent) and **monthly ZIPs** (historical), reflected in the script naming:

```bash
# Historical bulk sync (monthly ZIPs)
uv run scripts/pipelines/omie/{family}/00_sync_{family}_zips.py --start-month 2019-01 --end-month 2025-12

# Recent daily files (90-day window or fewer; trades has a 90-day confidentiality lag)
uv run scripts/pipelines/omie/{family}/00_download_{family}.py --recent-days 7

# Parse and build
uv run scripts/pipelines/omie/{family}/10_parse_{family}.py
uv run scripts/pipelines/omie/{family}/20_build_{family}_all.py
```

`det` / `icab` / `idet` parsers automatically detect pre- vs. post-MTU15-IDA file layouts (different fixed-width column widths). See `src/mtu/parsing/` for parser source.

### ESIOS pipelines

```
scripts/pipelines/esios/
├── 00_download_indicator.py              # token-aware /indicators workhorse
├── indicators/
│   ├── 00_batch_sync.py                  # YAML-driven batch driver over the 139-indicator catalog
│   ├── 10_parse_indicators.py            # JSON → per-indicator parquet
│   └── 20_build_indicators_all.py        # consolidate to indicators_all.parquet
├── liquidaciones/                        # liquicierre + liquicierresrs + liquicomun_c5
├── reservas/                             # curvas_ofertas_afrr + balancing_bids
├── restricciones/                        # totalrp48preccierre
└── indisponibilidades/                   # archive id=105 (outage snapshots)
```

Each sub-family has its own `00_sync_*.py`, `10_parse_*.py`, `20_build_*all.py`. The batch driver `indicators/00_batch_sync.py` reads `data/external/esios_indicator_catalog.yaml` and requests each indicator at its **native** granularity (15-min for `Quince minutos`, hourly for `Hora`, etc.) — 88 of 139 indicators are natively 15-min.

S3-redirect quirk: ESIOS issues HTTP 307 redirects to pre-signed S3 URLs for large payloads; re-sending the `x-api-key` on the S3 leg invalidates the AWS signature. The shared helper `src/mtu/ingestion/esios_common.py` follows the redirect without auth headers.

WAF behaviour: bot-detection is IP-scoped. Fast requests (< 0.1 s gap) trigger persistent 403 from a single IP. Use `--sleep 0.5` (or higher); switch IP if blocked.

### ENTSO-E pipelines

```bash
# Bulk pull for a calendar year
ENTSOE_TOKEN=<token> uv run scripts/pipelines/entsoe/{family}/00_sync_{family}.py --year 2024
```

---

## Repository layout

The project is organised by **purpose**. Each top-level directory has a single responsibility.

```
mtu15_project/
├── data/                                  # DATA ONLY — never analytical outputs
│   ├── raw/{omie,esios,entsoe}/           # verbatim source files (symlinks to external SSD)
│   ├── processed/{omie,esios,entsoe}/     # canonical parquet, one per family (symlinks)
│   ├── derived/
│   │   ├── panels/                        # analysis-ready panels (multi-source merges)
│   │   └── attic/                         # retired derived datasets
│   ├── interim/                           # parsing intermediates
│   ├── metadata/                          # download manifests, ingestion logs
│   └── external/                          # reference tables (unit codes, BSP lists, esios_indicator_catalog.yaml)
│
├── results/                               # ANALYTICAL OUTPUTS (code-dependent products of analysis scripts)
│   ├── regressions/{system,firm,bid,balancing,regulatory,descriptive}/
│   └── attic/                             # retired analytical outputs from pre-pivot framings
│
├── figures/                               # ALL FIGURES (canonical location)
│   ├── thesis/                            # figures referenced by paper.tex (via \graphicspath)
│   ├── presentation/                      # workshop / defense-only figures
│   ├── working/                           # WIP figures during analysis
│   └── attic/                             # retired figures
│
├── scripts/
│   ├── pipelines/{omie,esios,entsoe}/     # numbered: 00_sync → 10_parse → 20_build
│   ├── analysis/{system,firm,bid,regulatory,balancing,modelling,panels,attic}/
│   ├── admin/                             # one-off audits, inspects, forensic scripts
│   └── stata/                             # Stata .do files
│
├── src/mtu/
│   ├── parsing/                           # one module per data family (or per market for OMIE)
│   ├── transform/                         # period normalisation, shared transforms
│   ├── validation/                        # post-parse checks
│   ├── ingestion/                         # shared HTTP / auth / retry helpers (entsoe_common, esios_common)
│   └── classification/                    # firm-unit-tech panel builders
│
├── notebooks/
│   ├── eda/                               # numbered EDA notebooks
│   ├── memos/                             # research diary, modelling track, audit trail (markdown only)
│   └── attic/                             # superseded exploratory work
│
├── thesis/
│   ├── paper/                             # paper.tex + tables/, references.bib, paper.pdf
│   ├── model/                             # reserved for new structural model (not yet written)
│   ├── narratives/                        # presentation narratives, planning docs
│   └── presentations/
│       ├── workshop_february_2026/
│       └── workshop_may_2026/
│
├── docs/                                  # external reference material (gitignored)
│   ├── omie/                              # OMIE platform specs + press releases
│   ├── esios/                             # ESIOS docs
│   ├── entsoe/                            # ENTSO-E standards
│   ├── regulation/{eu,spain}/             # CNMC resolutions, REE operational guides, EU regulations
│   ├── references/                        # academic papers
│   └── notes/                             # research notes (includes SPANISH_MARKET_STRUCTURE.md)
│
├── tests/                                 # pytest suite
├── logs/                                  # runtime logs
├── attic/                                 # project-level retired material
├── CLAUDE.md                              # canonical project rules
├── CLAIMS_LEDGER.md                       # claim status registry
└── README.md                              # this file
```

---

## Data layers

| Layer | Purpose | Modify? |
|---|---|---|
| **raw** | Verbatim source files | Never |
| **processed** | Canonical parquet, one per family, all raw rows preserved with `source_file` snapshot identity | Only via 10_parse / 20_build |
| **derived** | Analysis-ready panels and cross-source merges | Only via `scripts/analysis/` |
| **results** | Regression CSVs, summary tables, run reports | Generated by `scripts/analysis/`; do not edit by hand |

**Source separation rule**: never mix sources in a single processed parquet without an explicit `source` column. If ESIOS content lives under `entsoe/` (or vice versa) it is a bug.

**Source-overlap policy**: OMIE programmes (`pdbc`, `pdbce`, `pibci`, `pibcic`, `phf`, `phfc`) cover the same conceptual ground as ESIOS `p48cierre` / `totalp48*` / `totalpdbf`. We use the OMIE versions (finer granularity, longer history) and skip the ESIOS duplicates. ENTSO-E A75 covers ESIOS `REE_ActualGen_*`; we use ENTSO-E.

---

## Python / uv workflow

- All scripts run with `uv run`; never `pip` or raw `python`
- Dependencies declared in `pyproject.toml` / `uv.lock`
- Lint: `uv run ruff check .`
- Type-check: `uv run mypy src/`
- Test: `uv run pytest`

`.env` (gitignored) holds `ESIOS_TOKEN`, `ENTSOE_TOKEN`. Never commit credentials.

---

## Reading the empirical work

Each script and active notebook carries a 4-line STATUS block:

```python
# STATUS: ALIVE | WOUNDED | DEAD-KEPT-AS-RECORD
# LAST-AUDIT: YYYY-MM-DD
# FEEDS: <claim-IDs from CLAIMS_LEDGER, comma-separated>
# CLAIM: <one-line summary>
```

Discipline cycle when a claim's status changes (see `CLAUDE.md` § Claim-status discipline):

1. Update the row in `CLAIMS_LEDGER.md` (status, `Date_changed`, reason — never delete)
2. Update the producing script's STATUS header
3. Update the consuming notebook's synthesis cell (strikethrough dead claims, do not delete)
4. Append a dated line to `notebooks/memos/RESEARCH_DIARY.md`

Claim-status semantics:

| Status | Meaning |
|---|---|
| Alive | Passed all documented robustness checks; safe to cite |
| Wounded | Survives in narrowed form; cite with caveat |
| Dead | Retracted or contradicted; do not cite as positive result |

Methodological protocols for regression-based claims (OVB-robustness, good vs bad controls, seasonality + weather controls for cross-regime claims) live in [`CLAUDE.md`](CLAUDE.md) under the respective sections.

---

## Contact

Pablo Paramio — `mochilarojaverde@gmail.com`. Master's thesis advisor: Pedro Mira (CEMFI). Deadline: mid-June 2026.
