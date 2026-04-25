# `scripts/analysis/` — multi-language analysis scripts

Scratch / standalone analysis scripts that sit outside the OMIE and ENTSO-E
pipelines. This directory hosts the cross-language work that the Python
exploratory notebooks call out to: most of it is **R**, with a **Stata**
placeholder for if/when Stata is needed.

## Toolchain

| Language | Runtime | Packages | Entry point |
|---|---|---|---|
| Python | `uv`, `pyproject.toml`, `.venv/` | `pyproject.toml`/`uv.lock` | `uv run <script>.py` |
| R | `rig` (R 4.5.3), `renv`, `renv.lock` | `renv.lock` | `Rscript <script>.R` |
| Stata | (not installed — placeholder) | — | `stata -b do <script>.do` |

### R

R is managed with [`rig`](https://github.com/r-lib/rig) (R Installation
Manager). Current default is R 4.5.3 (release). Project-level dependencies
are pinned via [`renv`](https://rstudio.github.io/renv/), analogous to
`uv` for Python. `renv` is activated automatically when R is launched
inside the project directory via the project's `.Rprofile`.

To reproduce the R environment on another machine:

```bash
# 1. Install rig (the R version manager) and R 4.5 release
brew install --cask rig
rig install release
rig default release

# 2. From the project root, restore the renv lockfile
Rscript -e 'renv::restore()'
```

User-global R packages (installed outside renv, used by VS Code):

- `languageserver` — LSP backend for the VS Code R extension.
- `httpgd` — in-editor plot device.
- `pak` — fast parallel package installer (used by rig and renv).
- `renv` — the project-level dependency manager itself.

### Python ↔ R interop

No shared runtime. The two ecosystems communicate via **Parquet files** in
`data/derived/`. Conventions:

- **Python writes** a derived panel to `data/derived/<name>.parquet` via
  `pandas.to_parquet()` or `duckdb`.
- **R reads** via `arrow::read_parquet("data/derived/<name>.parquet")`.
- **R writes** outputs back to `data/derived/<name>_r_<suffix>.parquet`
  via `arrow::write_parquet()`.
- **Python reads** R outputs via `pandas.read_parquet()`.

Everything in `data/derived/` is gitignored (large, regenerable). Persist
metadata about what the file contains in the calling script or notebook,
not in the filename alone.

### Stata ↔ Python/R interop (placeholder)

Stata natively reads/writes `.dta`. To cross languages:

- Python can read `.dta` via `pandas.read_stata()` or `pyreadstat`.
- R can read `.dta` via `haven::read_dta()`.
- Parquet → `.dta` conversion: write a small `scripts/analysis/parquet_to_dta.R`
  (or `.py`) that reads parquet and emits `.dta`. No Stata script is
  committed yet.

### Editor setup (VS Code)

See `.vscode/settings.json` for the active configuration. Recommended
extensions are in `.vscode/extensions.json` and VS Code will prompt to
install them on first open.

Key extensions:

- **R** (REditorSupport.r) — language support + session watcher + plot viewer.
- **Quarto** (quarto.quarto) — if using Quarto documents.
- **Stata Enhanced** (kylebarron.stata-enhanced) — `.do` syntax highlighting.
- **Python** + **Pylance** + **Jupyter** + **Ruff** — already in use.

The VS Code R extension uses `radian` as the REPL (installed via
`uv tool install radian`). `httpgd` is configured as the default graphics
device so plots render in a built-in VS Code webview rather than a native
quartz window.

## Layout

```
scripts/analysis/
├── README.md
├── smoke_test.R          (R + renv + arrow toolchain test, runs from project root)
├── panels/               (5 files: long-lived panel + price-series builders)
├── lerner/               (6 files: structural-firm Lerner regressions; F1–F6, W1)
├── bid/                  (9 files: bid-level / participation / behavioural; B1–B8, D1–D4)
├── modelling/            (14 files: Allaz–Vila §2, Pigouvian §3, asymmetric-granularity §4, system-level cross-validation)
├── synthetic/            (3 files: Ciarreta–Espinosa synthetic-firm pipeline; F7)
└── attic/                (1 file: superseded scripts)
```

Mirrors the `data/derived/` subfolder convention. Run all scripts from the project root: `uv run python scripts/analysis/<sub>/<script>.py`.

## Scripts in this directory

Every `.py` and `.R` file has a 4-line STATUS header naming the `CLAIMS_LEDGER.md` rows it feeds. The catalog below groups them by subfolder. Open any script's header to confirm its current status (alive / wounded / dead-kept-as-record).

### `panels/` — long-lived panel + price-series builders

| Script | Output | Feeds |
|---|---|---|
| `build_firm_lerner_panel.py` | `firm_lerner_hourly.parquet` | F1, F2, F3, F4, W1 |
| `build_supply_slope_panel.py` | `supply_slope_hourly.parquet` | input to firm Lerner |
| `build_firm_bid_revenue.py` | `firm_revenue_panel.parquet` (+ bid panel) | B2, B5 |
| `build_xbid_liquidity.py` | `xbid_liquidity_hourly.parquet` | B3, B4 |
| `sync_parse_a44_fr.py` | `data/processed/entsoe/prices/fr_da_all.parquet` | B7 |

### `lerner/` — structural-firm market-power (F1–F6, W1)

| Script | Purpose | Feeds |
|---|---|---|
| `seasonal_correction_lerner.py` | Spec 3 matched-price contrasts (canonical) | F1, F2, F3, F4 |
| `within_tech_lerner.py` | Tech-decomposition (CCGT-only vs full) | F1, W1, W2 |
| `cournot_slope_tercile.py` | F6 Cournot tercile + log-log structural test | F1, F2, F6 |
| `overnight_robustness.py` | Bootstrap, slope sensitivity, placebo dates, hour-of-day | F1, F2, F3 |
| `check_blackout_seasonal_confound.py` | Pre-blackout same-calendar test | F1 |
| `firm_collapse_timing.py` | Why IB holds reservation pricing longer than peers | F4 |

### `bid/` — bid-level / behavioural / participation (B1–B8, D1–D4, plus dispersion checks)

| Script | Purpose | Feeds |
|---|---|---|
| `bid_function_shape.py` | Bid-function moments (p25–p95, reservation share) | B2 |
| `hhi_withholding_bidshading.py` | HHI panel + withholding ratio + bid-shading regression | B1; X12 (dead) |
| `skepticism_check_patterns.py` | Same-calendar test for forecast pass-through R² and reservation share | B2, B6 |
| `buy_side_skeptical.py` | Buy-side reservation bidding with same-calendar skepticism | (descriptive) |
| `ccgt_extensive_margin_exit.py` | CCGT exit at MTU15-IDA (per-unit) | D4; contradicts old W3 wound rationale |
| `ccgt_within_unit_tranche_count.py` | Within-unit tranche count for named complex-bidders | X14 (W3 retraction); B8 |
| `bid_complexity_panel.py` | Firm-aggregate tranches-per-period | B8 |
| `bid_complexity_unit_level.py` | Within-unit tranches-per-period (canonical for B8) | B8 |
| `dispersion_15min_check.py` | Within-month price dispersion + post-MTU15-DA bid replication rate | D1, D2 |

### `modelling/` — Allaz–Vila, Pigouvian, asymmetric-granularity, system-level

#### Allaz–Vila / commitment

| Script | Purpose | Feeds |
|---|---|---|
| `allaz_vila_commitment_test.py` | F5 baseline test (firm × regime) | F5 |
| `allaz_vila_portfolio_split.py` | F5 refinement: peak/off-peak + price-quartile partitions | F5 |
| `da_ida_wedge_structure.py` | DA–IDA wedge time-series (mean, variance, autocorrelation) by regime | modelling-track §2 input |

#### Pigouvian imbalance settlement

| Script | Purpose | Feeds |
|---|---|---|
| `pigouvian_clean_regression.py` | Per-segment marginal imbalance cost with month + hour FE (canonical) | S7 |

#### Welfare / asymmetric-granularity (system layer)

| Script | Purpose | Feeds |
|---|---|---|
| `asymmetric_granularity_welfare.py` | A87 NET fiscal balance (A02 − A01) cumulative excess | S6 |
| `welfare_proxy.py` | Producer-surplus proxy from high-price IDA bid revenue | modelling-track §4 input |
| `theta_calibration.py` | Calibrate regime-dependent settlement-risk θ | modelling-track §1, §2 input |
| `a87_reserve_decomposition.py` | A87 net income decomposition into impdsvqh + reserve cost | S1; X13 (dead) |
| `esios_a87_cross.py` | Cross-check ESIOS impdsvqh vs ENTSO-E A87 | S1 |
| `forecast_bias_curtailment.py` | Forecast-bias direction and curtailment timing | B6 |
| `passthrough_forecast_imbalance.py` | Forecast-error → imbalance pass-through R² by regime | B6 |
| `anticipation_test.py` | Tests if 3-sess reservation pricing jumps at ISP15 announcement | (diagnostic) |

#### Cross-validation / placebos / robustness

| Script | Purpose | Feeds |
|---|---|---|
| `france_da_placebo.py` | Cross-country placebo: regime contrasts on French DA prices | B7, D1 |
| `survivor_skeptic.py` | Stress-test "remaining robust findings" for pre-2024 secular trends | S1, S5, B3 |

(Note: `dispersion_15min_check.py` is in `bid/`, not here, since its core measure is bid-replication rate — D1 and D2 alive claims.)

### `synthetic/` — Ciarreta–Espinosa synthetic-firm pipeline (F7)

| Script | Purpose | Feeds |
|---|---|---|
| `synthetic_firm_matching.py` | Plant-pair matching: Big-4 plant L → same-tech same-capacity Fringe plant S | F7 |
| `synthetic_firm_clearing.py` | Per-ISP synthetic supply (substitute matched offers, scale by K_L/K_S) + auction re-clearing | F7 |
| `synthetic_firm_aggregate.py` | Per-regime market-power index + welfare aggregation | F7 |

### R / future

| Script | Status |
|---|---|
| `smoke_test.R` | Verifies R + renv + arrow stack. `Rscript smoke_test.R`. |
| (Future) `bsts_isp15.R` | Bayesian structural time-series counterfactual for ISP15 on Big-4 ΔQ. See `explore/_identification_target.md` Phase B for econometric motivation. |

### `attic/` — superseded scripts (1 file)

| Script | Reason |
|---|---|
| `marginal_imbalance_cost.py` | Earlier raw version of `pigouvian_clean_regression.py` (no seasonal controls). Status: DEAD-KEPT-AS-RECORD. The clean version is the canonical S7 producer. |
