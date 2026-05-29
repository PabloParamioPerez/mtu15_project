# `data/derived/`

Analysis-ready panels and derived datasets built from `data/processed/`.

Not source data: every parquet here is reproducible from a script in
`scripts/analysis/` or `scripts/pipelines/`. If you delete one, the
corresponding builder regenerates it.

## `panels/`

Current set of panels feeding the bidding / pricing analysis. Each line lists
the panel, its builder, and the analysis it feeds.

### Daily / hourly base panels

| Panel | Builder | Feeds |
|---|---|---|
| `bsts_daily_panel.parquet` | `scripts/analysis/bid/build_bsts_daily_panel.py` | §3.A and §4.A Spec A BSTS on daily prices + per-tech cleared GWh |
| `bsts_quantities_panel.parquet` | `scripts/analysis/bid/build_bsts_quantities_panel.py` | per-tech daily cleared GWh on both DA and IDA, used by `bsts_cross_market_quantities.R` for finding (iii) and the rational-anticipation spillover in finding (ii) |
| `bsts_hourly_panel.parquet` | `scripts/analysis/bid/build_bsts_hourly_panel.py` | hourly variant of Spec A (kept as a robustness panel; not used in the current memo) |
| `continuous_daily_panel.parquet` | `scripts/analysis/bid/build_continuous_daily.py` | §6 daily ES-leg continuous-market BSTS (trade count + GWh + VW-mean price) |

### Per-curve panels (Spec C)

| Panel | Builder | Feeds |
|---|---|---|
| `per_curve_metrics_da.parquet` | `scripts/analysis/bid/build_per_curve_metrics.py` | DA per-curve $\sigma_p$ / $N_{\text{eff}}$ at the legacy `h{=}140` bandwidth (kept for the bandwidth-sensitivity sweep in §5 robustness) |
| `per_curve_metrics_ida.parquet` | as above | IDA equivalent |
| `per_curve_metrics_da_full.parquet` | as above | DA full (incl. parked tier) for bandwidth-robustness checks |
| `per_curve_windowed/per_curve_*_h{50,58,62}.parquet` | `scripts/analysis/bid/build_per_curve_windowed.py` | window-and-market-specific p90 panels (h ∈ {50, 58, 62}); feeds Spec C at p90 in §4.C and the per-tech DiD table |

### Spec B per-hour-class panels

| Panel | Builder | Feeds |
|---|---|---|
| `bsts_hour_class_panel.parquet` | `scripts/analysis/bid/build_bsts_hour_class_panel.py` | pre-p90 Spec B BSTS |
| `bsts_hour_class_p90/bsts_hour_class_{ID15,DA15}_{real,placebo}_hDA*_hIDA*.parquet` | `scripts/analysis/bid/build_bsts_hour_class_p90.py` | window-specific p90 Spec B; feeds `bsts_hour_class_p90.R` and Figure ref{fig:bsts-hour-class} |

### Topic-specific panels (older / supporting)

| Panel | Notes |
|---|---|
| `bid_function_shape_panel.parquet` | early-2026 per-curve shape descriptive; superseded by `per_curve_metrics_*` |
| `bidshape_sa_daily.parquet` | seasonality-adjusted bid-shape daily series used by `bidshape_seasonality_adjusted.py` |
| `firm_revenue_panel.parquet` | per-firm revenue panel used by retired F-series analyses; kept for reproducibility |
| `passthrough_panel.parquet` | gas-passthrough analysis panel |
| `post_da_gap_sa_{daily,hourly}.parquet` | DA-IDA gap deseasonalised series |
| `reform_panel.{parquet,dta}` | early reform-regime panel from the Acts framing |
| `supply_slope_hourly.parquet` | hourly supply-slope estimates feeding retired efficiency analyses |
| `synthetic_plant_match.parquet` | counterfactual-unit matching for the retired synthetic-plant framing |
| `welfare_proxy_panel.parquet` | welfare-proxy panel for retired analyses |
| `xbid_liquidity_hourly.parquet` | hourly XBID liquidity used by retired continuous-market analyses (the current §6 continuous-market BSTS uses `continuous_daily_panel.parquet` instead) |

### `bid_shape_critical_flat/`

Subdirectory of per-curve / per-unit-hour panels supporting the critical/flat
partition analyses. The `_unit_map.parquet` here is the canonical unit map
(unit_code → tech_group + firm_class + firm_dom) used by every analysis that
needs a per-firm or per-tech grouping.

## `attic/`

Retired derived panels from earlier framings (lerner / synthetic / Allaz-Vila /
B1 critical-hours / bootstrap-Lerner / bsts_sigma variants). Kept for
reproducibility of retired analyses; not used by the current memo or paper.

## Conventions

- Parquet is the default; `*.dta` and `*.csv` are present only when an
  analysis explicitly required them (mostly Stata-side and retired).
- Date column is `d`, format `YYYY-MM-DD`.
- Tech and firm names follow the unit map; CCGT firm classes are GN, IB, GE,
  HC, Fringe.
- Cleared-quantity columns are energy in GWh per day, computed as
  `quantity_mw * mtu_minutes / 60`, never raw MW sums.
