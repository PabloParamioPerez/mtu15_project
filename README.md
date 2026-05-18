# MTU15 Project — Spanish electricity market: granularity reforms 2024–2025

Master's thesis (CEMFI, 2026) on the Spanish wholesale electricity market reforms that progressively reduced the **Market Time Unit** (MTU) from 60 to 15 minutes across three sequential stages, with the **Imbalance Settlement Period** (ISP) tightened in parallel. The project builds a fully reproducible data pipeline for OMIE (market operator), ESIOS / REE (system operator), and ENTSO-E (pan-European TSO data) to study (i) how dominant firms organise their bid stack within the hour, (ii) how the post-DA cascade of REE redispatch absorbs and re-monetises the bid-stack reallocation, and (iii) how the post-2025-04-28 *operación reforzada* regime interacts with the MTU15 reforms in the same calendar window.

The empirical work is currently organised in two layers:

- **Descriptive evidence** (the primary deliverable at this stage) — bid-shape, price-setter, $D_w$, functional-PCA, and pairwise reform-window descriptive readings, plus the post-clearing cascade and zonal-concentration evidence. Lives in [`thesis/provisional/bidding_internal.tex`](thesis/provisional/bidding_internal.tex) → [`bidding_internal.pdf`](thesis/provisional/bidding_internal.pdf).
- **Identification design** (deferred until the descriptive foundation is stable) — pairwise reform-window comparisons with calendar-shifted placebo years (DA-side), functional PCA score regressions with parametric Fourier(doy) seasonal control, and the regression-based reform-window evidence in [`thesis/provisional/regression_results.tex`](thesis/provisional/regression_results.tex).

The thesis paper itself, [`thesis/paper/paper.tex`](thesis/paper/paper.tex) → [`paper.pdf`](thesis/paper/paper.pdf), is the public-facing single-file deliverable; the provisional directory is a working set where the design is iterated before being folded into the paper.

---

## Quick orientation

| Where to look | What you'll find |
|---|---|
| [`thesis/paper/paper.tex`](thesis/paper/paper.tex) → [`paper.pdf`](thesis/paper/paper.pdf) | The thesis paper itself. Single-file LaTeX (sections, not chapters). |
| [`thesis/provisional/bidding_internal.tex`](thesis/provisional/bidding_internal.tex) → [`bidding_internal.pdf`](thesis/provisional/bidding_internal.pdf) | The main descriptive-evidence document. Part A (bid-shape, price-setter, $D_w$, fPCA + parallel-trends placebos); Part B (REE post-clearing cascade, geographic concentration, system-cost view, apuntamiento, CNMC enforcement). |
| [`thesis/provisional/regression_results.tex`](thesis/provisional/regression_results.tex) | Working regression results — reform-window pairwise designs and BA-vs-DDD comparisons. |
| [`CLAUDE.md`](CLAUDE.md) | Canonical project rules — data layers, OVB / seasonality protocols, file conventions, source-separation rule. |
| [`docs/notes/SPANISH_MARKET_STRUCTURE.md`](docs/notes/SPANISH_MARKET_STRUCTURE.md) | Project reference for the sequential market structure (DA $\to$ IDA $\to$ continuous $\to$ balancing $\to$ P48), `tipo_redespacho` codes, TR cause-code dictionary, ESIOS indicator inventory. |
| [`notebooks/memos/_esios_archive_catalog.md`](notebooks/memos/_esios_archive_catalog.md) | ESIOS API archive triage memo (which archives we ingest and why) — the only memo we actively maintain. |
| [`data/external/esios_indicator_catalog.yaml`](data/external/esios_indicator_catalog.yaml) | 279 curated ESIOS indicators across 32 families, tagged with their native granularity and priority tier. |

Run `uv run pytest` for the test suite; `uv run python scripts/pipelines/.../00_*.py` for any pipeline step. All pipeline scripts are idempotent.

---

## Reform timeline

| Event | Date | What changed |
|---|---|---|
| **ISP15** | 2024-12-01 (effective 2024-12-09) | Imbalance settlement period changed from 60 min to 15 min |
| **MTU15-IDA** | 2025-03-19 | Intraday markets (3 European auctions + continuous XBID) switched from MTU60 to MTU15 |
| **Iberian blackout** | 2025-04-28 | Triggered REE's *operación reforzada* (continuous post-blackout reinforced-operation stance: larger pre-IDA and post-IDA RT, primarily Fase~I-up, to keep zonal voltage and inertia margins) |
| **MTU15-DA** | 2025-10-01 | Day-ahead market switched from MTU60 to MTU15 (overlaps with continuing *operación reforzada*) |

These dates appear as constants `ISP15_REFORM`, `INTRADAY_REFORM`, `BLACKOUT`, `DAY_AHEAD_REFORM` throughout the codebase. The MTU15-DA reform is structurally confounded with *operación reforzada* (both active continuously from October 2025 onward); the 40-day window 2025-03-19 → 2025-04-27 is the only sample period that is post-MTU15-IDA *and* pre-blackout, and therefore the only one where the MTU15-IDA effect can be read free of reforzada contamination.

---

## Data sources

Three sources, kept strictly separate at the data layer (see `CLAUDE.md` §Source separation rule).

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

**Indicators** (`/indicators/{id}`) — 2,018 system-wide time series. We curate **279 indicators across 32 families** in [`data/external/esios_indicator_catalog.yaml`](data/external/esios_indicator_catalog.yaml), tagged with their native granularity (15-min / hour / day / month) and grouped into priority tiers:

- **Tier A** — fuel / CO₂ prices, renewable forecasts, demand actuals / forecasts (controls for parallel-trends regressions; OVB defense).
- **Tier B** — DA + IDA clearing prices (sanity-check against OMIE), 13 PVPC quarter-hour components, demand-program stages.
- **Tier D** — reserve-market prices: aFRR (capacity + energy), mFRR (programada + directa), RR, RPA, Gestión de Desvíos, technical-restrictions Fase I and Fase II.
- **Tier E** — reserve-market volumes, cross-border balancing flows, SRAD demand response.
- **Tier F** — recent additions: per-cause-code TR volumes (SCB / SCA / CT / RTD / ASE), daily Fase~I volumes per zone, reforzada operational signals (id 1880/1881), voltage-control prices, indisponibilidades aggregates.

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

Each sub-family has its own `00_sync_*.py`, `10_parse_*.py`, `20_build_*all.py`. The batch driver `indicators/00_batch_sync.py` reads `data/external/esios_indicator_catalog.yaml` and requests each indicator at its **native** granularity (15-min for `Quince minutos`, hourly for `Hora`, etc.) — most Tier-D and Tier-F indicators are natively 15-min.

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
│   ├── memos/                             # one memo only: _esios_archive_catalog.md
│   └── attic/                             # superseded exploratory work
│
├── thesis/
│   ├── paper/                             # paper.tex + tables/, references.bib, paper.pdf — the deliverable
│   ├── provisional/                       # bidding_internal.tex + regression_results.tex + advisor_memo.tex —
│   │                                      # the working descriptive-evidence + identification-design layer
│   └── presentations/                     # workshop decks
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
├── CLAUDE.md                              # canonical project rules
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

OVB-robustness, good-vs-bad controls (the simultaneity / mediator-bias rules), seasonality + weather controls for cross-regime claims, and the power-vs-energy discipline (MW vs MWh under mixed-granularity samples) all live in [`CLAUDE.md`](CLAUDE.md) under the respective sections.

The descriptive-evidence layer (`thesis/provisional/bidding_internal.tex`) explicitly flags identification problems by outcome variable but **does not estimate** ATT / ATE / treatment effects — regressions there are instrumental tools for extracting descriptive patterns, not for causal estimation. See `thesis/provisional/regression_results.tex` for the regression-based identification work, which is kept in a separate document precisely so that the descriptive foundation is read independently of the identification design.

---

## Contact

Pablo Paramio — `mochilarojaverde@gmail.com`. Master's thesis advisor: Pedro Mira (CEMFI). Deadline: mid-June 2026.
