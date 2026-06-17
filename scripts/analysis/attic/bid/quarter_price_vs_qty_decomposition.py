# STATUS: ALIVE
# LAST-AUDIT: 2026-05-08
# FEEDS: B14 refinement (granularity-exploitation mechanism)
# CLAIM: When dominant CCGTs vary their bids across the 4 quarters of an hour
# post-MTU15-DA, are they varying PRICES (ladder shape) or QUANTITIES (MW
# reallocation) or BOTH?
#
# Decomposes the existing mech_strict measure (full bid hash on price+qty) into:
#   mech_price_only:  sorted price tuple identical across the 4 quarters?
#   mech_qty_only:    sorted quantity tuple identical across the 4 quarters?
#   mech_full:        the existing measure, identical (price, qty) tuples
#
# Restricted to competitive zone (≤250 €/MWh) and post-MTU15-DA window.

from __future__ import annotations

from pathlib import Path
import sys

import duckdb
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "src"))
from mtu.classification.units import classify_units  # noqa: E402

OUTDIR = REPO / "results" / "regressions" / "bid"
OUTDIR.mkdir(parents=True, exist_ok=True)

DET = REPO / "data" / "processed" / "omie" / "mercado_diario" / "ofertas" / "det_all.parquet"
CAB = REPO / "data" / "processed" / "omie" / "mercado_diario" / "ofertas" / "cab_all.parquet"
UNITS_CSV = REPO / "data" / "external" / "omie_reference" / "lista_unidades.csv"

START = "2025-10-01"
END = "2026-01-01"
COMP_ZONE_CUTOFF = 250.0


def main() -> None:
    units = classify_units(
        csv_path=str(UNITS_CSV),
        keep_columns=["unit_code", "owner_agent", "technology", "zone",
                      "firm_class", "tech_group"],
    )
    ccgt = units.loc[units["tech_group"] == "CCGT"].copy()

    # Build a 'parent' that distinguishes EDP-Portugal from EDP-Spain
    def parent(o: str) -> str:
        if not isinstance(o, str):
            return "Other"
        u = o.upper()
        if "IBERDROLA" in u:
            return "IB"
        if "ENDESA" in u:
            return "GE"
        if "NATURGY" in u or "GAS NATURAL" in u:
            return "GN"
        if "EDP ESPAÑA" in u:
            return "HC"
        if "EDP GEM PORTUGAL" in u:
            return "EDP-PT"
        return "Other-fringe-CCGT"

    ccgt["parent"] = ccgt["owner_agent"].apply(parent)
    ccgt = ccgt[["unit_code", "parent"]]
    print("--- CCGT units by parent ---")
    print(ccgt["parent"].value_counts().to_string())

    con = duckdb.connect()
    con.execute("PRAGMA threads = 6")
    con.register("ccgt_classification", ccgt)

    print(f"\nBuilding per-(unit, date, period) hashes (price≤{COMP_ZONE_CUTOFF})...")
    perperiod = con.execute(
        f"""
        WITH cab_sell AS (
            SELECT date, offer_code, version, unit_code
            FROM '{CAB}'
            WHERE buy_sell = 'V'
              AND date::DATE >= DATE '{START}' AND date::DATE < DATE '{END}'
        ),
        det_filtered AS (
            SELECT d.date, d.offer_code, d.version, d.period, d.mtu_minutes,
                   d.segment_number, d.price_eur_mwh, d.quantity_mw,
                   c.unit_code
            FROM '{DET}' d
            JOIN cab_sell c
              ON c.date = d.date AND c.offer_code = d.offer_code AND c.version = d.version
            WHERE d.date::DATE >= DATE '{START}' AND d.date::DATE < DATE '{END}'
              AND d.quantity_mw > 0
              AND d.price_eur_mwh IS NOT NULL
              AND d.price_eur_mwh <= {COMP_ZONE_CUTOFF}
        ),
        joined AS (
            SELECT df.*, cls.parent
            FROM det_filtered df
            JOIN ccgt_classification cls USING (unit_code)
        )
        SELECT date::DATE AS d, unit_code, parent, period, mtu_minutes,
               COUNT(*) AS n_tranches,
               -- full hash: tuple of (price, qty) sorted by tranche
               STRING_AGG(
                 ROUND(price_eur_mwh, 4)::VARCHAR || ':' || ROUND(quantity_mw, 4)::VARCHAR,
                 '|' ORDER BY segment_number
               ) AS full_hash,
               -- price-only: tuple of prices sorted
               STRING_AGG(
                 ROUND(price_eur_mwh, 4)::VARCHAR,
                 '|' ORDER BY price_eur_mwh
               ) AS price_hash,
               -- quantity-only: tuple of quantities sorted (need to sort by tranche
               -- price first to preserve ladder-position relationship)
               STRING_AGG(
                 ROUND(quantity_mw, 4)::VARCHAR,
                 '|' ORDER BY price_eur_mwh
               ) AS qty_hash
        FROM joined
        GROUP BY 1,2,3,4,5
        """
    ).df()
    print(f"per-period rows: {len(perperiod):,}")

    # Hour-of-day from (period, mtu_minutes); restrict to MTU15
    perperiod = perperiod[perperiod["mtu_minutes"] == 15].copy()
    perperiod["hour"] = ((perperiod["period"] - 1) // 4).astype(int)
    perperiod = perperiod.loc[perperiod["hour"].between(0, 23)]

    # Per (unit, date, hour): are full/price/qty hashes uniform across the 4 quarters?
    hourly = (
        perperiod.groupby(["d", "unit_code", "parent", "hour"])
        .agg(
            n_periods=("period", "count"),
            n_full=("full_hash", "nunique"),
            n_price=("price_hash", "nunique"),
            n_qty=("qty_hash", "nunique"),
        )
        .reset_index()
    )
    # Mech rates (fraction of unit-days where hashes are identical across quarters)
    hourly["mech_full"] = (hourly["n_full"] == 1).astype(int)
    hourly["mech_price"] = (hourly["n_price"] == 1).astype(int)
    hourly["mech_qty"] = (hourly["n_qty"] == 1).astype(int)

    # Restrict to hours where we have all 4 periods (otherwise mech is trivially 1)
    hourly = hourly[hourly["n_periods"] == 4].copy()

    # Aggregate per parent × hour-of-day
    parent_hourly = (
        hourly.groupby(["parent", "hour"])
        .agg(
            n_obs=("d", "count"),
            mech_full=("mech_full", "mean"),
            mech_price=("mech_price", "mean"),
            mech_qty=("mech_qty", "mean"),
        )
        .reset_index()
    )
    print("\n--- Mech rates by parent × hour-of-day ---")
    for parent_name in ["IB", "GE", "GN", "HC", "EDP-PT", "Other-fringe-CCGT"]:
        sub = parent_hourly[parent_hourly["parent"] == parent_name].sort_values("hour")
        if len(sub) == 0:
            continue
        print(f"\n{parent_name}:")
        print(sub[["hour", "mech_full", "mech_price", "mech_qty", "n_obs"]].to_string(index=False))

    parent_hourly.to_csv(OUTDIR / "quarter_decomposition_per_parent_hourly.csv", index=False)

    # Critical-hours summary (h{18-22} vs h{3-5})
    hourly["hour_class"] = hourly["hour"].apply(
        lambda h: "critical_h18_22" if h in (18,19,20,21,22) else
                  ("flat_h3_5" if h in (3,4,5) else "other")
    )
    summary = (
        hourly.groupby(["parent", "hour_class"])
        .agg(
            n_obs=("d", "count"),
            mech_full=("mech_full", "mean"),
            mech_price=("mech_price", "mean"),
            mech_qty=("mech_qty", "mean"),
        )
        .reset_index()
    )
    print("\n--- Mech rates by parent × hour_class (Oct-Dec 2025, comp zone) ---")
    print(summary.to_string(index=False))
    summary.to_csv(OUTDIR / "quarter_decomposition_per_parent_hour_class.csv", index=False)

    # Decomposition: when mech_full=0 (bids differ), is it because price differs,
    # quantity differs, or both?
    print("\n--- When bids differ across quarters (mech_full=0), what differs? ---")
    diff = hourly[hourly["mech_full"] == 0].copy()
    diff["price_only_differs"] = ((diff["n_price"] > 1) & (diff["n_qty"] == 1)).astype(int)
    diff["qty_only_differs"] = ((diff["n_price"] == 1) & (diff["n_qty"] > 1)).astype(int)
    diff["both_differ"] = ((diff["n_price"] > 1) & (diff["n_qty"] > 1)).astype(int)
    decomp = (
        diff.groupby(["parent", "hour_class"])
        .agg(
            n_diff=("d", "count"),
            pct_price_only=("price_only_differs", "mean"),
            pct_qty_only=("qty_only_differs", "mean"),
            pct_both=("both_differ", "mean"),
        )
        .reset_index()
    )
    print(decomp.to_string(index=False))
    decomp.to_csv(OUTDIR / "quarter_decomposition_what_differs.csv", index=False)


if __name__ == "__main__":
    main()
