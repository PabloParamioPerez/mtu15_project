# `data/derived/` — analysis-ready panels

Multi-source merges and reconciliation tables built from canonical `data/processed/` files. Treated as **derived** (regenerable from raw + scripts), never a substitute for `data/processed/` tables.

Each panel maps to its producer script and the `CLAIMS_LEDGER.md` rows it feeds. To rebuild a panel: `uv run python scripts/analysis/<topic>/<producer>.py`.

## Layout

```
data/derived/
├── panels/         analysis-ready panels (current; consumed by live analyses)
├── attic/          retired panels (pre-pivot Lerner work + superseded predecessors)
└── README.md       this file
```

## `panels/` — current analysis-ready panels

| File | Producer (under `scripts/analysis/`) | Feeds |
|---|---|---|
| `B1_critical_hours_thesis.parquet` | `firm/build_B1_panel.py` (or equivalent) | B1 within-day DiD on $q_2$ — main results table |
| `supply_slope_hourly.parquet` | `firm/build_supply_slope_panel.py` | inputs to F-series firm regressions |
| `firm_revenue_panel.parquet` | `bid/build_firm_bid_revenue.py` | B2, B5 |
| `firm_ida_bid_sell_panel.parquet` | nb13 / bid IDA panel builder | nb13 P2 |
| `bid_function_shape_panel.parquet` | `bid/bid_function_shape.py` | B2 |
| `xbid_liquidity_hourly.parquet` | `balancing/build_xbid_liquidity.py` | B3, B4 |
| `passthrough_panel.parquet` | `balancing/passthrough_forecast_imbalance.py` | B6 |
| `welfare_proxy_panel.parquet` | `modelling/welfare_proxy.py` | modelling-track input |
| `anticipation_test_panel.parquet` | `firm/anticipation_test.py` | diagnostic for B1, F1 |
| `entsoe_system_panel.parquet` | `panels/build_entsoe_system_panel.py` | system-side VRE + load controls (B16/B18/B19) |
| `synthetic_plant_inventory.parquet` | `panels/build_synthetic_plant_inventory.py` | tech-stratified controls |
| `synthetic_plant_match.parquet` | `panels/build_synthetic_plant_match.py` | tech-stratified match for synthetic panels |
| `reform_panel.parquet` | (legacy build, pre-restructure) | smoke-test fixture only |
| `reform_panel.dta` | Stata twin of `reform_panel.parquet` | `scripts/stata/smoke_test.do` |
| `bid_shape_critical_flat/` | `bid/build_bid_shape_critical_flat.py` (sharded outputs) | bid-shape DiD diagnostics |

## `attic/` — retired panels

Pre-pivot Lerner work (May 2026 pivot to within-day DiD) plus orphan predecessors. Kept for transparency; **do not consume**.

| File | Reason retired |
|---|---|
| `firm_lerner_hourly.parquet` | Lerner-margin framing superseded by within-day DiD |
| `firm_ccgt_lerner_panel.parquet` | Same |
| `allaz_vila_panel.parquet` | F5 demoted post-pivot |
| `bootstrap_lerner.parquet` | Overnight robustness for retired Lerner claims |
| `placebo_lerner.parquet`, `placebo_lerner_summary.parquet` | 200 fake-reform-date placebo runs (Lerner framing) |
| `slope_sensitivity_lerner.parquet` | Lerner recomputed at ±5/10/15/25 €/MWh finite-difference windows |
| `hour_of_day_lerner.parquet` | GE Lerner by (regime × hour-of-day) |
| `bid_panel.parquet` | Predecessor of `firm_revenue_panel.parquet`; no live consumer |

## Conventions

- Python writes via `pandas.to_parquet()` or `duckdb`
- R reads via `arrow::read_parquet()`; R writes back as `<name>_r_<suffix>.parquet`
- Don't encode metadata in the filename alone — keep it in the producer's docstring and the `CLAIMS_LEDGER.md` row
- Files are gitignored. The ledger row is the persistent record of what a derived file means.
- A retired panel **stays in attic forever**; never deletes, never re-promotes without an explicit `RESEARCH_DIARY.md` entry.
