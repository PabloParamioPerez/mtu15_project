# MTU15 Project: OMIE Electricity Market Data Pipeline

Master thesis project on the Spanish electricity market reform that changed the Market Time Unit (MTU) from hourly to 15-minute trading. Builds a reproducible data pipeline for OMIE market data to study the effect of the reform on price formation and quantities.

**Status date:** 10 April 2026

---

## Reform timeline

| Event | Date |
|---|---|
| ISP / imbalance settlement | December 2024 |
| Intraday markets (auctions + continuous) | 19 March 2025 |
| Day-ahead market | 1 October 2025 |

---

## Data families

| Family | Market | Type | Status |
|---|---|---|---|
| `marginalpdbc` | Day-ahead | Prices | Stable |
| `marginalpibc` | Intraday auctions | Prices | Stable |
| `pdbc` | Day-ahead | Programs | Stable |
| `pibca` | Intraday auctions | Programs (accumulated) | Stable |
| `pibci` | Intraday auctions | Programs (incremental) | Stable |
| `pibcac` | Continuous | Programs (accumulated) | Stable |
| `pibcic` | Continuous | Programs (incremental) | Stable |
| `precios_pibcic` | Continuous | Prices | Stable |
| `precios_pibcic_ronda` | Continuous | Round prices | Stable |
| `curva_pbc` | Day-ahead | Aggregate curves | Stable |
| `curva_pibc` | Intraday auctions | Aggregate curves | Stable |
| `omanulaintra` | Intraday auctions | Cancelled hours (MO) | Stable |
| `osanulaintra` | Intraday auctions | Cancelled hours (SO) | Stable |

---

## Pipeline structure

Each family follows the same three-stage pipeline:

```
scripts/pipelines/omie/
  00_download_{family}.py   # Download raw files from OMIE
  10_parse_{family}.py      # Parse raw files → per-file parquet
  20_build_{family}_all.py  # Build consolidated parquet
```

Historical ZIP archives (where available) are handled by separate sync scripts:

```
scripts/
  sync_{family}_zips.py
```

---

## Running the pipeline

```bash
# Download
uv run scripts/pipelines/omie/00_download_{family}.py --recent-days 7

# Parse
uv run scripts/pipelines/omie/10_parse_{family}.py

# Build
uv run scripts/pipelines/omie/20_build_{family}_all.py
```

All stages are idempotent — safe to rerun.

---

## Repository layout

```
src/mtu/parsing/       # One parser module per family
src/mtu/transform/     # Period normalisation and shared transforms
src/mtu/validation/    # Post-parse checks
scripts/pipelines/omie/  # Numbered pipeline steps
scripts/admin/         # Audit and inspection scripts
data/raw/              # Verbatim OMIE files (symlink to external SSD)
data/processed/        # Canonical parquet outputs (symlink to external SSD)
data/metadata/         # Download manifests and ingestion logs
```

---

## Data design principles

- **Raw layer**: verbatim OMIE files, never modified
- **Processed layer**: canonical parquet, one per family, preserving all raw rows and snapshot identity (`source_file`)
- **Derived layer**: reconciliation and collapsed views, clearly marked, never substitutes for canonical tables
