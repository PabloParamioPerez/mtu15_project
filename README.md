# MTU15 Project: OMIE Electricity Market Data Pipeline

Master thesis project on the Spanish electricity market reform that changed the Market Time Unit (MTU) from hourly trading to 15-minute trading.

**Status date:** 30 March 2026

## Project goal

The project builds a reproducible data pipeline for OMIE market data to study the effect of the MTU15 reform on electricity price formation in Spain.

The current focus is on collecting, parsing, validating, and consolidating OMIE price data in a way that is robust, rerunnable, and suitable for later empirical analysis.

## Reform timeline

Main dates relevant for this project:

- **Intraday markets (auctions + continuous):** 19 March 2025
- **Day-ahead market:** 1 October 2025
- **ISP / imbalance settlement:** December 2024

## What is currently implemented

As of 30 March 2026, the repository has working pipelines for:

### 1. Day-ahead prices (`MARGINALPDBC`)
Implemented pipeline stages:

- download raw OMIE day-ahead marginal price files
- parse raw files into per-file parquet outputs
- build a consolidated `marginalpdbc_all.parquet`

Current status:

- working
- manually checked on real dates and periods
- rerun-safe
- downloader skips already downloaded raw files
- parser skips files whose output parquet already exists
- combined builder reruns deterministically and keeps the latest version when multiple OMIE file versions exist

### 2. Intraday auction prices (`MARGINALPIBC`)
Implemented pipeline stages:

- download raw OMIE intraday auction marginal price files
- parse raw files into per-file parquet outputs
- build a consolidated `marginalpibc_all.parquet`

Current status:

- working
- manually checked on real quarter-hour prices
- supports historical multi-date / midnight-spanning session files
- blank historical price fields are handled as `NaN`
- downloader skips already downloaded raw files
- parser skips files whose output parquet already exists
- combined builder reruns deterministically and keeps the latest version when multiple OMIE file versions exist

## Current data coverage

### Day-ahead prices (`MARGINALPDBC`)
Current combined output:

- file: `data/processed/omie/mercado_diario/precios/marginalpdbc_all.parquet`
- date range: **2018-01-01 to 2026-03-13**

The builder correctly distinguishes:

- **MTU60** days before day-ahead reform
- **MTU15** days after day-ahead reform
- DST-compatible row counts

### Intraday auction prices (`MARGINALPIBC`)
Current combined output:

- file: `data/processed/omie/mercado_intradiario_subastas/precios/marginalpibc_all.parquet`
- date range: **2017-12-31 to 2026-02-24**

Current validated split:

- **pre-reform:** MTU60 until **2025-03-18**
- **post-reform:** MTU15 from **2025-03-19**

The post-reform data currently reflects the expected quarter-hour structure for intraday auction sessions.

## Manual checks already performed

The following checks have already been done successfully:

- direct manual comparison of day-ahead and intraday prices for specific dates and quarter-hours
- confirmation that post-reform quarter-hour indexing is correct
- rerun/idempotency checks for:
  - day-ahead downloader
  - intraday auction downloader
  - day-ahead parser
  - intraday auction parser
  - day-ahead combined builder
  - intraday auction combined builder

Operational conclusion:

- **download stage:** idempotent by skipping existing raw files
- **parse stage:** idempotent by skipping existing parquet outputs
- **build stage:** rerun-safe and deterministic

## Repository structure

### Core package
- `src/mtu/`

### Parsing logic
- `src/mtu/parsing/marginalpdbc.py`
- `src/mtu/parsing/marginalpibc.py`
- `src/mtu/parsing/omie_common.py`

### Pipeline scripts
- `scripts/pipelines/omie/00_download_marginalpdbc.py`
- `scripts/pipelines/omie/00_download_marginalpibc.py`
- `scripts/pipelines/omie/10_parse_marginalpdbc.py`
- `scripts/pipelines/omie/10_parse_marginalpibc.py`
- `scripts/pipelines/omie/20_build_marginalpdbc_all.py`
- `scripts/pipelines/omie/20_build_marginalpibc_all.py`
- `scripts/pipelines/omie/90_sync_marginalpdbc.sh`

### Admin / support scripts
- `scripts/admin/backfill_download_manifest_marginalpdbc.py`

### Data folders
- `data/raw/`
- `data/processed/`
- `data/interim/`
- `data/metadata/`

## Data conventions currently handled

The current pipelines handle the following OMIE conventions:

- semicolon-separated text files
- European decimal format
- hourly period structure before reform
- quarter-hour period structure after reform
- DST-compatible counts
- historical file versioning
- midnight-spanning historical intraday auction sessions

## What is not yet treated as finished

The following parts should still be considered under development or not yet part of the stable core pipeline:

- validation layer in `src/mtu/validation/checks.py`
- configuration layer in `src/mtu/config.py`
- extra tests and sample fixtures not yet consolidated into the main workflow
- auxiliary ZIP sync scripts for older archive-based years
- broader thesis datasets beyond the currently stable price pipelines

## Immediate next priorities

The current stable base is the day-ahead and intraday auction price layer.

Natural next steps for the project are:

1. extend the pipeline to the next necessary market layer
2. add the quantity / volume information required for substantive analysis
3. integrate validation and tests cleanly
4. keep the repository structure conservative and avoid unnecessary refactoring

## Reproducibility note

The repo is being organized so that raw downloads, per-file parsed outputs, and consolidated outputs can be regenerated safely.

Generated metadata files such as ingestion logs and download manifests are not intended to be tracked permanently in Git.

## Current Git status of the project

As of this checkpoint, the repository already has clean commits for:

- pipeline reorganization and metadata tracking cleanup
- project-root fixes after script relocation
- package / CLI entry point setup

The day-ahead and intraday auction price pipelines should be treated as the current stable working base of the project.
