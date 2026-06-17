# MTU15 Project — Spanish electricity market: granularity reforms 2024–2025

Master's thesis (CEMFI, 2026) by **Pablo Paramio Pérez** (advisor: Pedro Mira) on the Spanish wholesale electricity market reforms that reduced the **Market Time Unit** (MTU) from 60 to 15 minutes across two sequential auction reforms — first the intraday market (**ID15**, 2025-03-19), then the day-ahead market (**DA15**, 2025-10-01) — with the **Imbalance Settlement Period** tightened in parallel (**ISP15**, 2024-12-11). The project builds a fully reproducible data pipeline for OMIE (market operator), ESIOS / REE (system operator), and ENTSO-E (pan-European TSO data) to ask **how finer time granularity changes the way firms bid and the market power they hold**: (i) it reshapes strategic bidding — price-setters spread their offers into more, finer steps in the volatile ramp hours, while price-taking renewables only adjust how much they sell; (ii) the effect is **asymmetric across the two reforms** — new within-hour contractibility, and the market re-clearing that drops the clearing price (about a fifth) and the imbalance cost (about forty per cent), arrive only at the intraday (spot) reform, while the day-ahead reform merely relocates bid shape between markets; (iii) underneath both, the residual demand each firm faces grows more price-elastic and markups narrow. The post-2025-04-28 *operación reforzada* regime is held separable from the granularity effects (the 40-day window 2025-03-19 → 2025-04-27 is the only post-ID15, pre-blackout sample) so the two are never confounded.

The theoretical and empirical work is consolidated into a single deliverable:

- [`thesis/paper/thesis.tex`](thesis/paper/thesis.tex) → [`thesis.pdf`](thesis/paper/thesis.pdf) — the thesis paper. Theory (a sequential forward–spot Cournot game in which a residual-demand monopolist bids step offers and granularity widens the within-hour state each product screens); data and identification (a bid-level difference-in-differences on bid shape with a critical/flat hour partition, plus BSTS counterfactuals on prices, cleared MW, and the within-hour price gap); results (bid-shape widening in volatile hours, offered-energy migration into the finer market, the ID15 cross-market price drop, the imbalance-cost fall, the asymmetric null at DA15, and the markup-compression channel), with a granularity-vs-*reforzada* system-cost decomposition; conclusion; and appendices (robustness, continuous-market response, market-structure context, theoretical derivations).
- **Submission deliverables:** [`Paramio_Pablo_Thesis.pdf`](Paramio_Pablo_Thesis.pdf) (the compiled paper) and `Paramio_Pablo_Replication.zip` (the packaged replication snapshot — built from this repo, see [Replication](#replication) below).

### Replication

`replication/run_replication.py` reproduces every reported number and figure end-to-end (data download → parse/build → derived panels → analyses), organised **section by section** (Q1 prices, the margin channel, the within-hour wedge, bid-shape DiD, imbalance, migration, robustness). The headline values are then typeset into `thesis.tex` by hand (the paper has no auto-generated tables). The full exhibit→script map is in [`REPLICATION.md`](REPLICATION.md); the replicator's guide (requirements, credentials, staging) is in [`replication/README.md`](replication/README.md).

```bash
uv sync                                              # build the Python env from the lockfile
uv run python replication/run_replication.py --list  # print the section-by-section plan
uv run python replication/run_replication.py --from 2 # panels + analyses (skip data download)
```

Superseded exploratory analyses (≈280 scripts) live under `scripts/analysis/attic/`; every script that produces a paper exhibit is referenced in `REPLICATION.md` and run by the driver. Pre-pivot write-ups are archived under [`attic/`](attic/).

---

## Quick orientation

| Where to look | What you'll find |
|---|---|
| [`thesis/paper/thesis.tex`](thesis/paper/thesis.tex) → [`thesis.pdf`](thesis/paper/thesis.pdf) | The thesis paper itself. Single-file LaTeX (sections, not chapters). |
| [`thesis/provisional/additional_results.tex`](thesis/provisional/additional_results.tex) | Working scratch pad of supplementary findings outside the main paper. |
| [`docs/notes/SPANISH_MARKET_STRUCTURE.md`](docs/notes/SPANISH_MARKET_STRUCTURE.md) | Project reference for the sequential market structure (DA $\to$ IDA $\to$ continuous $\to$ balancing $\to$ P48), `tipo_redespacho` codes, TR cause-code dictionary, ESIOS indicator inventory. |
| [`notebooks/memos/_esios_archive_catalog.md`](notebooks/memos/_esios_archive_catalog.md) | ESIOS API archive triage memo (which archives we ingest and why) — the only memo we actively maintain. |
| [`data/external/esios_indicator_catalog.yaml`](data/external/esios_indicator_catalog.yaml) | 279 curated ESIOS indicators across 32 families, tagged with their native granularity. |

Run `uv run pytest` for the test suite; `uv run python scripts/pipelines/.../00_*.py` for any pipeline step. All pipeline scripts are idempotent.

---

## Reform timeline

| Event | Date | What changed |
|---|---|---|
| **ISP15** | 2024-12-11 | Imbalance settlement period changed from 60 min to 15 min |
| **MTU15-IDA** | 2025-03-19 | Intraday markets (3 European auctions + continuous XBID) switched from MTU60 to MTU15 |
| **Iberian blackout** | 2025-04-28 | Triggered REE's *operación reforzada* (continuous post-blackout reinforced-operation stance: larger pre-IDA and post-IDA RT, primarily Fase~I-up, to keep zonal voltage and inertia margins) |
| **MTU15-DA** | 2025-10-01 | Day-ahead market switched from MTU60 to MTU15 (overlaps with continuing *operación reforzada*) |

These dates appear as constants `ISP15_REFORM`, `INTRADAY_REFORM`, `BLACKOUT`, `DAY_AHEAD_REFORM` throughout the codebase. The MTU15-DA reform is structurally confounded with *operación reforzada* (both active continuously from October 2025 onward); the 40-day window 2025-03-19 → 2025-04-27 is the only sample period that is post-MTU15-IDA *and* pre-blackout, and therefore the only one where the MTU15-IDA effect can be read free of reforzada contamination.

---

## Data sources

Three sources, kept strictly separate at the data layer (the source-separation rule below).

### OMIE — Iberian market operator

#### Day-ahead market

| Family | Description | Coverage | Spec |
|---|---|---|---|
| `marginalpdbc` | Clearing prices (`ES` and `PT` columns; full-coverage source for DA spot) | 2018-01-01 – present | §5.1.1.1 |
| `pdbc` | Final programs by unit (auction-cleared only) | 2018 – present | §5.1.2.1 |
| `pdbce` | Final programs by firm | 2018 – present | §5.1.2.2 |
| `pdbf` | PDBC + bilateral-contract executions (`offer_type=4`) | 2018 – present | §5.1.2.3 |
| `curva_pbc` | Aggregate supply / demand curves | 2018 – present | §5.1.3.1 |
| `cab` | Offer headers ("ofertas que entran en casación") | 2017 – present | §5.1.4.1 |
| `det` | Offer details (price / quantity tranches) | 2018 – present | §5.1.4.2 |
| `capacidad_inter_pbc` | Interconnection capacity post-PBC | 2018 – present | §5.1.6.1 |
| `capacidad_inter_pvp` | Interconnection capacity post-restrictions | 2018 – present | §5.1.6.2 |

#### Intraday auctions

| Family | Description | Coverage | Spec |
|---|---|---|---|
| `marginalpibc` | Clearing prices by session | 2017 – present | §5.2.1.1 |
| `pibca` | Accumulated programs by unit (RT-free; level-after-IDA-clear) | 2019 – present | §5.2.2.1 |
| `pibci` | Programs by unit and session (incremental MW per IDA round) | 2019 – present | §5.2.2.2 |
| `pibcie` | Programs by firm and session | 2019 – present | §5.2.2.3 |
| `phf` | Final hourly program by unit and session (REE-established, includes RT) | 2019 – present | §5.2.2.4 |
| `curva_pibc` | Aggregate supply / demand curves | 2018 – present | §5.2.3.1 |
| `icab` / `idet` | Offer headers / details (bid stack) | 2018 – present | §5.2.4.1–2 |
| `osanulaintra` / `omanulaintra` | Cancelled offers (SO / MO) | sparse | §5.2.6 |

#### Intraday continuous (XBID)

| Family | Description | Coverage | Spec |
|---|---|---|---|
| `precios_pibcic` / `precios_pibcic_ronda` | Continuous-market prices | 2018-06 – present | §5.3.1 |
| `pibcac` | Accumulated programs by unit | 2018-06 – present | §5.3.2.1 |
| `pibcic` / `pibcice` | Programs by unit / firm and round | 2018-06 – present | §5.3.2.2–3 |
| `phfc` | Final hourly program by unit and round (REE-established) | 2018-06 – present | §5.3.2.4 |
| `trades` | XBID matched transactions | 2018-06 – present (90-day delay) | §5.3.2.7 |
| `orders` | XBID limit orders | 2018-06 – present (90-day delay) | §5.3.3.1 |

OMIE file specification: [`docs/omie/ficherosomie137.pdf`](docs/omie/ficherosomie137.pdf) (v1.37, 2025-09-30).

### ESIOS — REE (Spanish TSO)

Two endpoint families on the ESIOS API (`https://api.esios.ree.es`), with `ESIOS_TOKEN` from `.env`:

**Indicators** (`/indicators/{id}`) — 2,018 system-wide time series. We curate **279 indicators across 32 families** in [`data/external/esios_indicator_catalog.yaml`](data/external/esios_indicator_catalog.yaml), tagged with their native granularity (15-min / hour / day / month). The catalog groups indicators by topic, including:

- Fuel / CO₂ prices, renewable forecasts, demand actuals / forecasts (controls for parallel-trends regressions; OVB defense).
- DA + IDA clearing prices (sanity-check against OMIE), 13 PVPC quarter-hour components, demand-program stages.
- Reserve-market prices: aFRR (capacity + energy), mFRR (programada + directa), RR, RPA, Gestión de Desvíos, technical-restrictions Fase~I and Fase~II.
- Reserve-market volumes, cross-border balancing flows, SRAD demand response.
- Per-cause-code TR volumes (SCB / SCA / CT / RTD / ASE), daily Fase~I volumes per zone, *operación reforzada* operational signals (id 1880/1881), voltage-control prices, indisponibilidades aggregates.

**Archives** (`/archives/{id}`) — daily/monthly file dumps per archive ID:

| Family | Archive ID | Granularity | Coverage | Format |
|---|---:|---|---|---|
| `liquicierre` | 17 | per-BSP × concept × ISP (legacy XML schema) | 2015-01 → 2024-12 | XML |
| `liquicierresrs` | 203 | per-BSP × concept × ISP (post-ISP15 format) | 2024-11 → present | XML |
| `liquicomun_c5` | 11 | settlement bundle (181 concept families) | 2015 → present | XML |
| `balancing_bids` | 181 | aggregate mFRR bid stack — **retired post-ISP15** (use `curvas_ofertas_afrr` for the active series) | 2022-05 → 2024-12 | XML |
| `curvas_ofertas_afrr` | 234 | aggregate aFRR offer curves | 2024-11 → present | XLS |
| `totalrp48preccierre` | 28 | per-unit RT2 redispatch (closing) by `tipo_redespacho` code | 2015 → present | XML |
| `indisponibilidades` | 105 | per-unit outage snapshots (UF and UP) | 2018 → present (monthly snapshots) | XLS |
| `GenerationUnits`, `ProgrammingUnits`, `BalanceResponsibleParties`, `EntitledParticipants` | 110, 111, 112, 113 | master-data references | one-shot dumps | JSON |

Archive triage memo: [`notebooks/memos/_esios_archive_catalog.md`](notebooks/memos/_esios_archive_catalog.md).

### ENTSO-E — pan-European TSO data

| Code | Description | Used for |
|---|---|---|
| A65 | System total load | Demand-class controls; hour-class taxonomy |
| A72 | Reservoir filling | Hydro water-value covariate |
| A73 | Actual generation per type | Renewable-share computation |
| A75 | Actual generation per type / per unit | RES capture rate (apuntamiento) and per-unit tech composition |
| A82 / A83 | Balancing energy | Reserved for balancing-market validation |
| A86 / A87 | Imbalance prices / volumes | Reserved for imbalance settlement cross-checks |

Spain control-area EIC: `10YES-REE------0`. Token in `.env` as `ENTSOE_TOKEN`. See [`src/mtu/ingestion/entsoe_common.py`](src/mtu/ingestion/entsoe_common.py).

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

Many OMIE families publish both as **daily files** (recent) and **monthly ZIPs** (historical):

```bash
# Historical bulk sync (monthly ZIPs)
uv run scripts/pipelines/omie/{family}/00_sync_{family}_zips.py --start-month 2019-01 --end-month 2025-12

# Recent daily files (90-day window; trades has a 90-day confidentiality lag)
uv run scripts/pipelines/omie/{family}/00_download_{family}.py --recent-days 7

# Parse and build
uv run scripts/pipelines/omie/{family}/10_parse_{family}.py
uv run scripts/pipelines/omie/{family}/20_build_{family}_all.py
```

`det` / `icab` / `idet` parsers automatically detect pre- vs. post-MTU15-IDA file layouts (different fixed-width column widths). See [`src/mtu/parsing/`](src/mtu/parsing/) for parser source.

### ESIOS pipelines

```
scripts/pipelines/esios/
├── 00_download_indicator.py              # token-aware /indicators workhorse
├── indicators/
│   ├── 00_batch_sync.py                  # YAML-driven batch driver over the 279-indicator catalog
│   ├── 10_parse_indicators.py            # JSON → per-indicator parquet
│   └── 20_build_indicators_all.py        # consolidate to indicators_all.parquet
├── liquidaciones/                        # liquicierre + liquicierresrs + liquicomun_c5
├── reservas/                             # curvas_ofertas_afrr + balancing_bids
├── restricciones/                        # totalrp48preccierre
└── indisponibilidades/                   # archive id=105 (outage snapshots)
```

Each sub-family has its own `00_sync_*.py`, `10_parse_*.py`, `20_build_*all.py`. The batch driver `indicators/00_batch_sync.py` reads `data/external/esios_indicator_catalog.yaml` and requests each indicator at its **native** granularity (15-min for `Quince minutos`, hourly for `Hora`, etc.) — most reserve-market and TR indicators are natively 15-min.

S3-redirect quirk: ESIOS issues HTTP 307 redirects to pre-signed S3 URLs for large payloads; re-sending the `x-api-key` on the S3 leg invalidates the AWS signature. The shared helper [`src/mtu/ingestion/esios_common.py`](src/mtu/ingestion/esios_common.py) follows the redirect without auth headers.

WAF behaviour: bot-detection is IP-scoped. Fast requests (under 0.1 s gap) trigger persistent 403 from a single IP. Use `--sleep 0.5` (or higher); switch IP if blocked.

### ENTSO-E pipelines

```bash
ENTSOE_TOKEN=<token> uv run scripts/pipelines/entsoe/{family}/00_sync_{family}.py --year 2024
```

---

## Repository layout

The project is organised by **purpose**. Each top-level directory has a single responsibility.

```
mtu15_project/
├── data/                                  # DATA ONLY — never analytical outputs
│   ├── raw/{omie,esios,entsoe}/           # verbatim source files (symlinks to external SSD)
│   │                                      # ENTSO-E raw + processed both share 6 topical
│   │                                      # buckets: balancing/ generation/ load/ outages/
│   │                                      # prices/ transmission/. ESIOS reservas/ uses
│   │                                      # per-family subfolders (balancing_bids/,
│   │                                      # curvas_ofertas_afrr/, liquicierre/,
│   │                                      # liquicierresrs/) consistently in raw + processed.
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
│   ├── thesis/                            # figures referenced by thesis.tex (via \graphicspath)
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
│   ├── memos/                             # one memo only: _esios_archive_catalog.md
│   └── attic/                             # superseded exploratory work
│
├── thesis/
│   ├── paper/                             # thesis.tex + tables/, references.bib, thesis.pdf — the deliverable
│   ├── provisional/                       # additional_results.tex — supplementary findings scratch pad
│   ├── presentations/                     # workshop and defence decks
│   └── research_workshop/                 # advisor outline (Outline_Paramio.tex) + spring 2026 materials
│       ├── workshop_february_2026/
│       └── workshop_may_2026/
│
├── docs/                                  # external reference material (gitignored except SPANISH_MARKET_STRUCTURE.md
│                                          # and the regulation/{eu,spain,cnmc_resolutions} PDFs that anchor the doc)
│   ├── omie/                              # OMIE platform specs + press releases
│   ├── entsoe/                            # ENTSO-E standards
│   ├── notes/                             # research notes — SPANISH_MARKET_STRUCTURE.md is the authoritative
│   │                                      # project reference (market structure, tipo_redespacho codes, cause codes)
│   ├── regulation/
│   │   ├── eu/                            # EU regulations + ACER decisions
│   │   ├── spain/                         # Spanish law, REE operational guides (incl. ree_guia_proveedor_ajuste.pdf),
│   │   │                                  # blackout-cohort expedientes CSV
│   │   ├── cnmc_resolutions/              # CNMC sanction resolutions (per-firm)
│   │   └── cnmc_clearing/                 # CNMC supervision report + extracted findings
│   ├── references/                        # academic papers grouped by topic
│   │   ├── auction_methods/
│   │   ├── balancing_forward/
│   │   ├── market_power_methods/
│   │   ├── spanish_market/
│   │   └── strategic_bidding_design/
│   └── general_references/                # student's own research material
│       ├── proposal/                      # research proposal + advisor briefings
│       └── methodology/                   # econometrics textbooks / methods papers
│
├── tests/                                 # pytest suite
├── logs/                                  # runtime logs
├── attic/                                 # project-level retired material
└── README.md                              # this file
```

---

## Data layers

| Layer | Purpose | Modify? |
|---|---|---|
| **raw** | Verbatim source files | Never |
| **processed** | Canonical parquet, one per family, all raw rows preserved with `source_file` snapshot identity | Only via `10_parse` / `20_build` |
| **derived** | Analysis-ready panels and cross-source merges | Only via `scripts/analysis/` |
| **results** | Regression CSVs, summary tables, run reports | Generated by `scripts/analysis/`; do not edit by hand |

**Source-separation rule**: never mix sources in a single processed parquet without an explicit `source` column. If ESIOS content lives under `entsoe/` (or vice versa) it is a bug.

**Source-overlap policy**: OMIE programmes (`pdbc`, `pdbce`, `pibci`, `pibcic`, `phf`, `phfc`) cover the same conceptual ground as ESIOS `p48cierre` / `totalp48*` / `totalpdbf`. We use the OMIE versions (finer granularity, longer history) and skip the ESIOS duplicates. ENTSO-E A75 covers ESIOS `REE_ActualGen_*`; we use ENTSO-E. The DA spot price has two sources — OMIE `marginalpdbc` (used as the canonical full-coverage series) and ESIOS indicator 600 (used for sanity cross-check; has month gaps in 2025–2026).

---

## Python / uv workflow

- All scripts run with `uv run`; never `pip` or raw `python`
- Dependencies declared in `pyproject.toml` / `uv.lock`
- Lint: `uv run ruff check .`
- Type-check: `uv run mypy src/`
- Test: `uv run pytest`

`.env` (gitignored) holds `ESIOS_TOKEN`, `ENTSOE_TOKEN`. Never commit credentials.

---

## Methodological protocols

The identification and robustness methodology — OVB-robustness and good-vs-bad controls (the simultaneity / mediator-bias rules), seasonality and weather controls for cross-regime claims, and the power-vs-energy discipline (MW vs MWh under mixed-granularity samples) — is documented in the thesis (§5–§6 and the appendices) and applied throughout the analysis code.

The thesis paper itself (`thesis/paper/thesis.tex`) presents the integrated theory, identification and empirical evidence; the identification strategy (per-curve DiD on bid shape with a critical/flat hour partition, and BSTS counterfactuals with same-calendar placebos for prices and cleared MW) is documented in §5 and §6 of that document, with the pre-window choice and the solar-coefficient symmetry diagnostic in Appendix A.6.

---

## Contact

Pablo Paramio — `mochilarojaverde@gmail.com`. Master's thesis advisor: Pedro Mira (CEMFI). Deadline: mid-June 2026.
