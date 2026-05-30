# STATUS: ALIVE
# LAST-AUDIT: 2026-05-30
# FEEDS: scripts/analysis/bid/ffr_supply_curves.R (functional factor
#        regression on per-(date, hour, market) supply curves).
#
# Builds per-(date, market, session, clock-hour) sell-side cumulative
# supply curves on a fixed price grid (default [0, 200] EUR/MWh, 1-EUR
# steps). Sources:
#   curva_pbc  (DA)   -- one curve per (date, hour); 4 quarter-curves
#                       averaged into the hourly curve post-MTU15-DA.
#   curva_pibc (IDA)  -- one curve per (date, hour, session) for sessions
#                       1, 2, 3. Quarter-curves averaged within the hour
#                       post-MTU15-IDA.
#
# Implementation uses numpy.searchsorted per curve (fast O(N log G) per
# curve), avoiding the slow Cartesian-product SQL join.
#
# Output (wide, one row per (date, market, session, clock_hour)):
#   data/derived/panels/supply_curves_panel.parquet
#   columns: d, market, session, clock_hour, Q_{p}_mw for p in grid

from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
PBC = REPO / "data/processed/omie/mercado_diario/curvas/curva_pbc_all.parquet"
PIBC = REPO / "data/processed/omie/mercado_intradiario_subastas/curvas/curva_pibc_all.parquet"
OUT = REPO / "data/derived/panels/supply_curves_panel.parquet"

PRICE_GRID = np.arange(0, 201, 1, dtype=float)  # [0, 1, ..., 200]
N_GRID = len(PRICE_GRID)
DATE_MIN = "2023-06-01"


def load_raw(parquet_path, has_session):
    """Pull aggregated (date, session, clock_hour, period_raw, price) rows."""
    con = duckdb.connect()
    session_select = "session_number" if has_session else "NULL::BIGINT AS session_number"
    period_clock_hour = """
        CASE
          WHEN regexp_matches(period_raw, '^[0-9]+$')
            THEN CAST(period_raw AS INT) - 1
          WHEN regexp_matches(period_raw, '^H[0-9]+Q[1-4]$')
            THEN CAST(regexp_extract(period_raw, '^H([0-9]+)Q[1-4]$', 1) AS INT) - 1
          ELSE NULL
        END
    """
    q = f"""
    WITH sell AS (
      SELECT CAST(date AS DATE) AS d,
             {session_select},
             {period_clock_hour} AS clock_hour,
             period_raw,
             price_eur_mwh AS p,
             power_mw AS mw
      FROM read_parquet('{parquet_path}')
      WHERE offer_type = 'V' AND curve_type = 'O'
        AND price_eur_mwh IS NOT NULL AND power_mw IS NOT NULL
        AND power_mw > 0
        AND price_eur_mwh BETWEEN 0 AND {PRICE_GRID[-1]}
        AND date >= '{DATE_MIN}'
    )
    SELECT d, session_number, clock_hour, period_raw, p, SUM(mw) AS mw
    FROM sell
    WHERE clock_hour IS NOT NULL
    GROUP BY 1, 2, 3, 4, 5
    """
    df = con.execute(q).fetchdf()
    return df


def curves_per_period(raw):
    """For each (d, session, clock_hour, period_raw), evaluate cumulative
    MW at each grid price. Returns DF: (d, session, clock_hour, period_raw)
    + 201 grid columns."""
    # Sort once
    raw = raw.sort_values(
        ["d", "session_number", "clock_hour", "period_raw", "p"], kind="stable"
    ).reset_index(drop=True)
    grouper = ["d", "session_number", "clock_hour", "period_raw"]
    # Per-group cumsum
    raw["cum_mw"] = raw.groupby(grouper, dropna=False, sort=False)["mw"].cumsum()

    # For each curve, evaluate at grid points
    keys = []
    grid_mw = []
    for (d, ses, hr, prd), g in raw.groupby(grouper, dropna=False, sort=False):
        prices = g["p"].to_numpy()
        cum = g["cum_mw"].to_numpy()
        # For each p_grid, find idx of largest p <= p_grid using searchsorted
        idx = np.searchsorted(prices, PRICE_GRID, side="right") - 1
        # If idx == -1, no bids at or below this grid price → 0 MW
        vals = np.where(idx >= 0, cum[np.clip(idx, 0, len(cum) - 1)], 0.0)
        keys.append((d, ses, hr, prd))
        grid_mw.append(vals)

    grid_mw = np.vstack(grid_mw) if grid_mw else np.empty((0, N_GRID))
    out = pd.DataFrame(keys, columns=grouper)
    grid_cols = [f"Q_{int(p)}_mw" for p in PRICE_GRID]
    out[grid_cols] = pd.DataFrame(grid_mw, columns=grid_cols)
    return out


def average_across_periods(per_period):
    """Average the quarter-period curves into one hourly curve per
    (d, session, clock_hour)."""
    grid_cols = [c for c in per_period.columns if c.startswith("Q_")]
    out = (per_period
           .groupby(["d", "session_number", "clock_hour"], dropna=False, sort=False)[grid_cols]
           .mean()
           .reset_index())
    return out


def build_market(parquet_path, market_tag, has_session):
    print(f"\n{market_tag}: loading raw bid data...")
    raw = load_raw(parquet_path, has_session)
    print(f"  raw rows: {len(raw):,}")
    print(f"  unique curves: {raw.groupby(['d','session_number','clock_hour','period_raw'], dropna=False).ngroups:,}")
    print(f"{market_tag}: discretising per period...")
    per_period = curves_per_period(raw)
    print(f"  per-period rows: {len(per_period):,}")
    print(f"{market_tag}: averaging across periods within hour...")
    out = average_across_periods(per_period)
    out.insert(1, "market", market_tag)
    print(f"  hourly rows: {len(out):,}")
    return out


def main():
    print(f"Building supply curve panel on grid [0, {PRICE_GRID[-1]}] step 1 ({N_GRID} grid points)")
    print(f"From {DATE_MIN}")

    da = build_market(PBC, "DA", has_session=False)
    ida = build_market(PIBC, "IDA", has_session=True)

    panel = pd.concat([da, ida], ignore_index=True)
    panel = panel.rename(columns={"session_number": "session"})
    panel["d"] = pd.to_datetime(panel["d"])
    panel = panel.sort_values(["market", "session", "d", "clock_hour"]).reset_index(drop=True)

    OUT.parent.mkdir(parents=True, exist_ok=True)
    panel.to_parquet(OUT, index=False)
    print(f"\nWrote {OUT}")
    print(f"  shape: {panel.shape}")
    print(f"  date range: {panel['d'].min().date()} -> {panel['d'].max().date()}")
    print("  rows per (market, session):")
    print(panel.groupby(["market", "session"], dropna=False).size().rename("n_rows").to_string())


if __name__ == "__main__":
    main()
