# `data/derived/` — derived panels and result tables

Files here are **gitignored, regenerable** outputs from `scripts/analysis/`. Organised into four subfolders by purpose; each entry maps to its producer script and the `CLAIMS_LEDGER.md` rows it feeds.

To rebuild any panel: `uv run python scripts/analysis/<producer>.py`.

## Layout

```
data/derived/
├── panels/         long-lived source-of-truth panels (consumed by many downstream analyses)
├── robustness/     overnight Lerner robustness sweep outputs (bootstrap / placebo / sensitivity)
├── results/        per-claim CSV outputs from individual Phase 2 analyses
├── attic/          orphans / superseded files
└── README.md       this file
```

## `panels/` — source-of-truth panels (13 files)

| File | Producer | Feeds |
|---|---|---|
| `firm_lerner_hourly.parquet` | `build_firm_lerner_panel.py` | F1, F2, F3, F4, W1 |
| `supply_slope_hourly.parquet` | `build_supply_slope_panel.py` | F1 (input to firm Lerner) |
| `firm_ccgt_lerner_panel.parquet` | `within_tech_lerner.py` | F1, W1, W2 |
| `firm_revenue_panel.parquet` | `build_firm_bid_revenue.py` | B2, B5 |
| `firm_ida_bid_sell_panel.parquet` | (older nb13 build) | nb13 P2 |
| `xbid_liquidity_hourly.parquet` | `build_xbid_liquidity.py` | B3, B4 |
| `passthrough_panel.parquet` | `passthrough_forecast_imbalance.py` | B6 |
| `welfare_proxy_panel.parquet` | `welfare_proxy.py` | (modelling-track input) |
| `bid_function_shape_panel.parquet` | `bid_function_shape.py` | B2 |
| `anticipation_test_panel.parquet` | `anticipation_test.py` | (diagnostic for B1, F1) |
| `allaz_vila_panel.parquet` | `allaz_vila_commitment_test.py` | F5 |
| `reform_panel.parquet` | (legacy build, pre-restructure) | smoke-test fixture only |
| `reform_panel.dta` | Stata twin of `reform_panel.parquet` | `scripts/stata/smoke_test.do` |

## `robustness/` — overnight Lerner sweep (5 files)

Produced by `overnight_robustness.py`. Consumed historically by `_robustness_summary.md` (now in `explore/archive/`); evidence pointers live in `CLAIMS_LEDGER.md`.

| File | Description |
|---|---|
| `bootstrap_lerner.parquet` | 500 bootstrap-resample medians per (firm, regime) |
| `slope_sensitivity_lerner.parquet` | Lerner recomputed at ±€5/€10/€15/€25 finite-difference slope windows |
| `placebo_lerner.parquet`, `placebo_lerner_summary.parquet` | 200 fake-reform-date placebo runs |
| `hour_of_day_lerner.parquet` | GE Lerner by (regime × hour-of-day) |

## `results/` — per-claim Phase 2 result tables (11 files)

Small CSVs, one per analysis. Read with `pandas.read_csv()`.

| File | Producer | Feeds |
|---|---|---|
| `cournot_tercile_results.csv` | `cournot_slope_tercile.py` | F6 (Cournot tercile sort) |
| `cournot_loglog_results.csv` | `cournot_slope_tercile.py` | F6 (tautology check) |
| `allaz_vila_results.csv` | `allaz_vila_commitment_test.py` | F5 |
| `allaz_vila_peak_offpeak.csv` | `allaz_vila_portfolio_split.py` | F5 |
| `allaz_vila_price_quartile.csv` | `allaz_vila_portfolio_split.py` | F5 |
| `pigouvian_clean_results.csv` | `pigouvian_clean_regression.py` | S7 |
| `ccgt_extensive_margin_exit.csv` | `ccgt_extensive_margin_exit.py` | D4; contradicts old W3 wound rationale |
| `ccgt_within_unit_tranche_count.csv` | `ccgt_within_unit_tranche_count.py` | X14 (W3 retraction); B8 |
| `ccgt_tranche_count_monthly.csv` | `ccgt_within_unit_tranche_count.py` | B8 (monthly per-unit panel) |
| `bid_complexity_monthly.csv` | `bid_complexity_panel.py` | B8 (firm-aggregate) |
| `bid_complexity_unit_level.csv` | `bid_complexity_unit_level.py` | B8 (within-unit; canonical) |

## `attic/` — orphans / superseded (1 file)

| File | Reason |
|---|---|
| `bid_panel.parquet` | Predecessor of `firm_revenue_panel.parquet`. No live consumer found via grep on 2026-04-26. |

## Conventions

- Python writes via `pandas.to_parquet()` or `duckdb`.
- R reads via `arrow::read_parquet()`.
- R writes back as `<name>_r_<suffix>.parquet`.
- Don't encode metadata in the filename alone — keep it in the producer script's docstring and the `CLAIMS_LEDGER.md` row.
- Everything here is gitignored. The ledger row is the persistent record of what a derived file means.
