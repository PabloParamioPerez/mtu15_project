# MTU15 Project: Spanish Electricity Market Data Pipeline

Master thesis project on the Spanish electricity market reform that changed the Market Time Unit (MTU) from hourly to 15-minute trading. Builds a reproducible data pipeline for OMIE (market operator) and ESIOS/REE (system operator) data to study the effect of the reform on price formation, quantities, and balancing.

---

## Reform timeline

| Event | Date |
|---|---|
| ISP / imbalance settlement | December 2024 |
| Intraday markets (auctions + continuous) | 19 March 2025 |
| Day-ahead market | 1 October 2025 |

---

## Data sources

### OMIE (market operator)

#### Day-ahead market

| Family | Type | Coverage |
|---|---|---|
| `marginalpdbc` | Prices | 2018-01-01 – 2026-04-11 |
| `pdbc` | Programs | 2018-02, 2023-12 – 2026-01-08 |
| `curva_pbc` | Aggregate curves | 2018-01-01 – 2026-04-08 |
| `cab` | Matched offer headers | 2017-01-01 – 2026-01-09 |
| `det` | Matched offer detail (price/qty per period) | 2018-01-01 – 2026-01-09 |

#### Intraday auctions

| Family | Type | Coverage |
|---|---|---|
| `marginalpibc` | Prices | 2017-12-31 – 2026-04-09 |
| `icab` | Matched offer headers | 2018-01-01 – 2026-01-10 |
| `idet` | Matched offer detail (price/qty per period) | 2018-01-01 – 2026-01-10 |
| `pibca` | Programs (accumulated) | 2019-02-28 – 2025-01-14 |
| `pibci` | Programs (incremental) | 2019-02-28 – 2026-01-02 |
| `curva_pibc` | Aggregate curves | 2018-01-01 – 2026-04-08 |
| `omanulaintra` | Cancelled hours (MO) | sparse (event-driven) |
| `osanulaintra` | Cancelled hours (SO) | sparse (event-driven) |

#### Intraday continuous

| Family | Type | Coverage |
|---|---|---|
| `pibcac` | Programs (accumulated) | 2018-06-13 – 2025-03-31 |
| `pibcic` | Programs (incremental) | 2018-06-13 – 2026-01-10 |
| `precios_pibcic` | Prices | incomplete |
| `precios_pibcic_ronda` | Round prices | 2018-06-13 – 2026-04-09 |

### ESIOS / REE (system operator)

| Family | Type | Coverage |
|---|---|---|
| `restricciones` | Technical constraints | — |
| `rampas` | Ramp limitations | — |
| `desvios` | Imbalance settlement | — |

---

## Pipeline structure

### OMIE

```
scripts/pipelines/omie/
  00_download_{family}.py    # Download raw daily files from OMIE
  00_sync_{family}_zips.py   # Download and extract monthly ZIP archives
  10_parse_{family}.py       # Parse raw files → per-file parquet
  20_build_{family}_all.py   # Build consolidated parquet
```

Families published only as monthly ZIPs (`cab`, `det`, `icab`, `idet`, `pibca`, `pibci`, `pibcac`, `pibcic`, `pdbc`) use `00_sync_*_zips.py` only. Families with both a daily endpoint and historical ZIPs (`curva_pbc`, `curva_pibc`, `precios_pibcic`, `precios_pibcic_ronda`, `marginalpdbc`, `marginalpibc`, `omanulaintra`, `osanulaintra`) have both scripts.

`det` files have two fixed-width layouts: pre-reform (57-char lines, before 2025-03-19) and post-reform (60-char lines); the parser detects the format automatically. `icab` files have two layouts: pre-reform (195-char) and post-reform (94-char). `idet` files have two layouts: pre-reform (76-char) and post-reform (60-char). All parsers detect the format automatically from the first line length. Filename suffix encodes the session number (1–6 pre-2024-06-14, 1–3 after).

### ESIOS

```
scripts/pipelines/esios/
  00_download_indicator.py   # Download indicator time series (one JSON per day)
```

---

## Running the pipeline

### OMIE

```bash
# Historical bulk sync (monthly ZIPs)
uv run scripts/pipelines/omie/00_sync_{family}_zips.py --start-month 2019-01 --end-month 2025-03

# Recent daily files
uv run scripts/pipelines/omie/00_download_{family}.py --recent-days 7

# Parse
uv run scripts/pipelines/omie/10_parse_{family}.py

# Build consolidated parquet
uv run scripts/pipelines/omie/20_build_{family}_all.py
```

### ESIOS

```bash
# Requires ESIOS_TOKEN env var (request from consultasios@ree.es)
ESIOS_TOKEN=<token> uv run scripts/pipelines/esios/00_download_indicator.py \
  --indicator-id <id> --start-date 2024-01-01 --end-date 2025-03-31
```

All stages are idempotent — safe to rerun.

---

## Repository layout

```
src/mtu/parsing/          # One parser module per family
src/mtu/transform/        # Period normalisation and shared transforms
src/mtu/validation/       # Post-parse checks
scripts/pipelines/omie/   # Numbered OMIE pipeline steps
scripts/pipelines/esios/  # ESIOS pipeline steps
scripts/admin/            # Audit and inspection scripts
data/raw/                 # Verbatim source files (symlink to external SSD)
data/processed/           # Canonical parquet outputs (symlink to external SSD)
data/metadata/            # Download manifests and ingestion logs
```

---

## Data design principles

- **Raw layer**: verbatim source files, never modified
- **Processed layer**: canonical parquet, one per family, preserving all raw rows and snapshot identity (`source_file`)
- **Derived layer**: reconciliation and collapsed views, clearly marked, never substitutes for canonical tables
