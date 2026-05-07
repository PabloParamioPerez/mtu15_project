# STATUS: ALIVE
# LAST-AUDIT: 2026-05-07
# FEEDS: B14 (per-firm CCGT decomposition), B14b (hourly-resolution bid-shape)
# CLAIM: Per-firm × hour-of-day CCGT bid-shape profile for Oct-Dec 2025 DA.
#
# Goal: extend the per-firm CCGT decomposition (per_firm_ccgt_dominant_diff.csv)
# from critical-vs-flat aggregates to the full 24-hour profile. Tests whether
# the IB-uniform-enricher / GN-granularity-exploiter dichotomy is stable across
# the day or specific to particular hours, and whether GE Endesa's "almost no
# critical-flat differentiation" reflects activity at all hours or specific
# windows.
#
# Dimension discipline: uses tranche counts, mechanical-repeat rates, and
# price quantiles (no quantity_mw sums across periods).
#
# Window: 2025-10-01 to 2025-12-31 (post-MTU15-DA, parser-fixed; per-quarter
# bid resolution).

from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
OUTDIR = REPO / "results" / "regressions" / "bid"
OUTDIR.mkdir(parents=True, exist_ok=True)

DET = REPO / "data" / "processed" / "omie" / "mercado_diario" / "ofertas" / "det_all.parquet"
CAB = REPO / "data" / "processed" / "omie" / "mercado_diario" / "ofertas" / "cab_all.parquet"
UNITS_CSV = REPO / "data" / "external" / "omie_reference" / "lista_unidades.csv"

START = "2025-10-01"
END = "2026-01-01"


def load_unit_classification() -> pd.DataFrame:
    from mtu.classification.units import classify_units

    return classify_units(csv_path=str(UNITS_CSV))


def main() -> None:
    units = load_unit_classification()
    ccgt_units = units.loc[units["tech_group"] == "CCGT", ["unit_code", "firm_class"]]
    print(f"CCGT units in register: {len(ccgt_units)}")
    print(ccgt_units["firm_class"].value_counts().to_string())

    con = duckdb.connect()
    con.execute("PRAGMA threads = 6")
    con.register("ccgt_classification", ccgt_units)

    # Per (unit, date, period): n_tranches, p_min, p_median, p_max, mean_qty_mw,
    # plus a content hash for the mech_strict computation.
    print("\nBuilding per-(unit,date,period) bid-shape table...")
    perperiod = con.execute(
        f"""
        WITH cab_sell AS (
            SELECT date, offer_code, version, unit_code
            FROM '{CAB}'
            WHERE buy_sell = 'V'
              AND date::DATE >= DATE '{START}'
              AND date::DATE <  DATE '{END}'
        ),
        det_filtered AS (
            SELECT d.date, d.offer_code, d.version, d.period, d.mtu_minutes,
                   d.segment_number, d.price_eur_mwh, d.quantity_mw,
                   c.unit_code
            FROM '{DET}' d
            JOIN cab_sell c
              ON c.date = d.date
             AND c.offer_code = d.offer_code
             AND c.version = d.version
            WHERE d.date::DATE >= DATE '{START}'
              AND d.date::DATE <  DATE '{END}'
              AND d.quantity_mw > 0
              AND d.price_eur_mwh IS NOT NULL
        ),
        joined AS (
            SELECT df.*, cls.firm_class
            FROM det_filtered df
            JOIN ccgt_classification cls USING (unit_code)
        )
        SELECT date::DATE AS d,
               unit_code,
               firm_class,
               period,
               mtu_minutes,
               COUNT(*)                                AS n_tranches,
               MIN(price_eur_mwh)                      AS p_min,
               quantile_cont(price_eur_mwh, 0.5)        AS p_med,
               MAX(price_eur_mwh)                      AS p_max,
               AVG(quantity_mw)                        AS mean_qty_mw,
               -- content fingerprint: combine sorted (price,qty) tranches
               STRING_AGG(
                 ROUND(price_eur_mwh, 4)::VARCHAR || ':' ||
                 ROUND(quantity_mw, 4)::VARCHAR,
                 '|' ORDER BY segment_number
               )                                       AS bid_hash
        FROM joined
        GROUP BY 1,2,3,4,5
        """
    ).df()
    print(f"per-period rows: {len(perperiod):,}")

    # Map period (1-96 in MTU15) → hour-of-day in Madrid local time.
    # OMIE day-ahead periods are by Madrid local convention; period 1 = 00:00-00:15 local.
    # For MTU60 historical compatibility: period 1 = hour 0 in those rows.
    perperiod["hour"] = (
        (perperiod["period"] - 1) // (60 // perperiod["mtu_minutes"].clip(lower=1))
    ).astype(int)
    perperiod = perperiod.loc[perperiod["hour"].between(0, 23)]

    # Per-(unit, date, hour) mech_strict: do ALL 4 quarters (post-MTU15) share the
    # same bid_hash? If only 1 period in this hour (legacy MTU60 row), trivially
    # mechanical (set to True).
    print("\nComputing mech_strict per (unit, date, hour)...")
    hourly = (
        perperiod.groupby(["d", "unit_code", "firm_class", "hour"])
        .agg(
            n_periods_in_hour=("period", "count"),
            n_unique_hashes=("bid_hash", "nunique"),
            n_tranches=("n_tranches", "mean"),
            p_min=("p_min", "mean"),
            p_med=("p_med", "mean"),
            p_max=("p_max", "mean"),
            mean_qty_mw=("mean_qty_mw", "mean"),
        )
        .reset_index()
    )
    hourly["mech_strict"] = (hourly["n_unique_hashes"] == 1).astype(int)
    print(f"hourly rows: {len(hourly):,}")

    # Per-firm × hour aggregate.
    perfirm_hourly = (
        hourly.groupby(["firm_class", "hour"])
        .agg(
            n_obs=("d", "count"),
            n_unique_units=("unit_code", "nunique"),
            mean_n_tranches=("n_tranches", "mean"),
            mean_p_min=("p_min", "mean"),
            mean_p_med=("p_med", "mean"),
            mean_p_max=("p_max", "mean"),
            mean_qty_mw=("mean_qty_mw", "mean"),
            mech_strict_rate=("mech_strict", "mean"),
        )
        .reset_index()
        .sort_values(["firm_class", "hour"])
    )
    out1 = OUTDIR / "perfirm_hourly_ccgt_bidshape_oct_dec_2025.csv"
    perfirm_hourly.to_csv(out1, index=False)
    print(f"\nWritten: {out1}")
    print("\n--- per-firm × hour summary ---")
    print(
        perfirm_hourly[
            ["firm_class", "hour", "mean_n_tranches", "mean_p_max", "mech_strict_rate"]
        ].to_string(index=False)
    )

    # Compact 4-firm × 24-hour pivots for inspection.
    for col, lbl in [
        ("mean_n_tranches", "n_tranches"),
        ("mech_strict_rate", "mech_strict"),
        ("mean_p_max", "p_max"),
        ("mean_p_med", "p_med"),
        ("mean_qty_mw", "mean_qty_mw"),
    ]:
        piv = perfirm_hourly.pivot(index="hour", columns="firm_class", values=col)
        out = OUTDIR / f"perfirm_hourly_ccgt_{lbl}_pivot.csv"
        piv.to_csv(out)
        print(f"Pivot: {out}")
        print(piv.round(2).to_string())
        print()

    # Per-unit × hour profile (for inspecting Endesa anomaly).
    perunit_hourly = (
        hourly.groupby(["firm_class", "unit_code", "hour"])
        .agg(
            n_obs=("d", "count"),
            mean_n_tranches=("n_tranches", "mean"),
            mean_p_max=("p_max", "mean"),
            mean_p_med=("p_med", "mean"),
            mech_strict_rate=("mech_strict", "mean"),
        )
        .reset_index()
        .sort_values(["firm_class", "unit_code", "hour"])
    )
    out2 = OUTDIR / "perunit_hourly_ccgt_bidshape_oct_dec_2025.csv"
    perunit_hourly.to_csv(out2, index=False)
    print(f"\nWritten: {out2}")
    print(f"perunit rows: {len(perunit_hourly):,}, distinct units: {perunit_hourly['unit_code'].nunique()}")


if __name__ == "__main__":
    main()
