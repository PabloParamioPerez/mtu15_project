# Critical-hours calibration

**Created:** 2026-05-07

**Purpose.** Document the empirical anchor for the multiple critical-hours
definitions in `src/mtu/classification/critical_hours.py`. Workshop
feedback (May 5) flagged that the original h{7,8,16,17,18} set is supply-
ramp-driven and misses the empirically richest bidding window (h19-21
evening peak). This memo nails down what each definition is calibrated to.

## Calibration window

Spanish DA market, **Oct 1 2025 – Dec 31 2025** (the post-MTU15-DA
quarter, 92 days). All hours in Madrid local time.

## Hourly profiles (Oct-Dec 2025)

### DA clearing price — Spain side (mean €/MWh)

| Hour | Price | | Hour | Price |
|---:|---:|---|---:|---:|
| 0 | 78.5 | | 12 | 39.1 |
| 1 | 70.6 | | 13 | 36.5 |
| 2 | 65.4 | | 14 | 36.6 |
| 3 | 62.4 | | 15 | 41.9 |
| 4 | 60.4 | | 16 | 54.8 |
| 5 | 63.8 | | 17 | 75.6 |
| 6 | 73.3 | | 18 | 92.8 |
| 7 | 86.4 | | **19** | **106.0** |
| 8 | 91.5 | | **20** | **113.3** |
| 9 | 73.8 | | **21** | **105.4** |
| 10 | 51.6 | | **22** | **93.3** |
| 11 | 42.8 | | 23 | 83.4 |

(Hourly means averaged across the 4 quarters of each clock hour.)

**Top-5 by price level** → h{20, 19, 21, 18, 22} → set **{18, 19, 20, 21, 22}**.

### Spanish actual load — ENTSO-E A65 (mean MW)

| Hour | Load (MW) | | Hour | Load (MW) |
|---:|---:|---|---:|---:|
| 0 | 22,525 | | 12 | 28,597 |
| 1 | 21,592 | | 13 | 28,479 |
| 2 | 21,110 | | 14 | 28,340 |
| 3 | 21,060 | | 15 | 28,554 |
| 4 | 21,756 | | **16** | **29,307** |
| 5 | 24,013 | | **17** | **30,654** |
| 6 | 26,811 | | **18** | **31,681** |
| 7 | 28,316 | | **19** | **32,016** |
| 8 | 28,880 | | **20** | **30,853** |
| 9 | 28,938 | | 21 | 28,269 |
| 10 | 28,630 | | 22 | 25,821 |
| 11 | 28,507 | | 23 | 23,949 |

**Top-5 by demand level** → h{19, 18, 20, 17, 16} → set **{16, 17, 18, 19, 20}**.

### Why price-peak ≠ demand-peak

The two windows differ by ~2 hours. h16-17 are in the demand-peak set
(load 29-31 GW) but not the price-peak set (price 55-76 €/MWh) because
solar still partially clears the late afternoon. By h19-21 solar is
gone and net-load (load - VRE) jumps, so price spikes even though raw
load is starting to decline.

For strategic-conduct identification, **price peak is the more
economically meaningful trigger** (that's where the rent-extraction
incentive is largest). Raw demand-peak is provided for completeness
and as a robustness check.

## Definitions exposed in the API

| Token in API | Hours | Empirical basis |
|---|---|---|
| `'supply_ramp'` (default) | {7, 8, 16, 17, 18} | Top 5 by σ²_within(net-load); pre-pivot headline |
| `'price_peak'` | {18, 19, 20, 21, 22} | Top 5 by hourly DA clearing price |
| `'demand_peak'` | {16, 17, 18, 19, 20} | Top 5 by hourly Spanish actual load |
| `'joint'` | {7, 8, 16, 17, 18, 19, 20, 21, 22} | Union of supply_ramp ∪ price_peak |

Flat-hours control (shared across all definitions): **{3, 4, 5}**.
Overnight, low demand (~21 GW), low price (~62 €/MWh), low ramp.

## Why h22 is included in `'price_peak'` but not in `'demand_peak'`

h22 has very different load (25.8 GW, well below average) but
elevated price (93.3 €/MWh, top-5). The reason: by h22, the residual
Spanish thermal+hydro fleet is meeting most of the load (solar gone,
wind variable), so even moderate demand levels translate into high
clearing prices because the supply curve is steep near the upper-end.
This is exactly the kind of hour where firms with marginal CCGT/coal
have strategic price-extraction surplus, even though "raw demand" is
not high.

## Sources

- Load: `data/processed/entsoe/load/load_actual_all.parquet`
  (column: `load_mw`, ENTSO-E A65 actual load).
- Price: `data/processed/omie/mercado_diario/precios/marginalpdbc_all.parquet`
  (column: `price_es_eur_mwh`, OMIE DA marginal price for Spain side).

Both averaged across `2025-10-01 .. 2025-12-31` Madrid local hours.

## Implications for analysis

- The original h{7,8,16,17,18} ramp-based set captures the morning-evening
  *transition* hours, which is where σ² is high but the *level* of price
  is not maximal.
- The price-peak set h{18,19,20,21,22} captures the strategic-extraction
  window. Empirically (per `_per_firm_bid_shape.md`), CCGT bids are
  richest precisely in this window — IB averages 9.5-10.0 tranches/quarter
  at h19-21 vs 4-8 at h{7,8,16,17,18}.
- A claim that survives under both `'supply_ramp'` and `'price_peak'`
  (and the `'joint'` union) is far more robust than one tied to a
  specific hour partition.
