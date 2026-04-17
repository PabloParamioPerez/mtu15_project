"""
Build curva_pbc_all.parquet from all individually parsed curva_pbc parquet files.

Deduplication strategy
----------------------
Unlike point-estimate families (marginalpdbc, pdbc), curva_pbc rows have no natural
row-level primary key: each file is a complete snapshot of the aggregated curve for
that date, and multiple steps at the same (period, side, price) are valid.

Deduplication is therefore file-level: if multiple version files exist for the same
date (e.g. curva_pbc_20180101.1 and curva_pbc_20180101.2), ALL rows from the highest
version are kept and ALL rows from lower versions are dropped.
"""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parents[4]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

INPUT_DIR = PROJECT_ROOT / "data/processed/omie/mercado_diario/curvas/curva_pbc"
OUTPUT_FILE = PROJECT_ROOT / "data/processed/omie/mercado_diario/curvas/curva_pbc_all.parquet"


def main() -> None:
    glob = str(INPUT_DIR / "*.parquet")

    con = duckdb.connect()

    n_files = con.execute(
        f"SELECT COUNT(DISTINCT filename) FROM read_parquet('{glob}', filename=true, union_by_name=true)"
    ).fetchone()[0]

    if n_files == 0:
        raise SystemExit(f"No parquet files found in {INPUT_DIR}")

    # --- Detect superseded versions ---
    # For each date, keep only rows from files whose version_suffix equals the max for that date.
    superseded = con.execute(f"""
        WITH file_meta AS (
            SELECT DISTINCT source_file, date, TRY_CAST(version_suffix AS INTEGER) as ver
            FROM read_parquet('{glob}', union_by_name=true)
        ),
        max_ver AS (
            SELECT date, MAX(ver) as max_ver FROM file_meta GROUP BY date
        )
        SELECT fm.source_file, fm.date, fm.ver as version_suffix
        FROM file_meta fm
        JOIN max_ver mv ON fm.date = mv.date
        WHERE fm.ver < mv.max_ver
        ORDER BY fm.date, fm.source_file
    """).df()

    if not superseded.empty:
        print(
            f"WARNING: {len(superseded)} source file(s) superseded by a higher version "
            f"for the same date — dropping their rows:"
        )
        print(superseded.to_string(index=False))

    # --- Write deduplicated, sorted output ---
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    con.execute(f"""
        COPY (
            WITH max_ver AS (
                SELECT date, MAX(TRY_CAST(version_suffix AS INTEGER)) as max_ver
                FROM (SELECT DISTINCT date, source_file, version_suffix FROM read_parquet('{glob}', union_by_name=true)) file_meta
                GROUP BY date
            )
            SELECT t.*
            FROM read_parquet('{glob}', union_by_name=true) t
            JOIN max_ver mv ON t.date = mv.date
                AND TRY_CAST(t.version_suffix AS INTEGER) = mv.max_ver
            ORDER BY t.date, t.source_file, t.row_order
        )
        TO '{OUTPUT_FILE}' (FORMAT PARQUET)
    """)

    # --- Summary stats ---
    stats = con.execute(f"""
        SELECT COUNT(*) as total_rows, MIN(date) as date_min, MAX(date) as date_max
        FROM read_parquet('{OUTPUT_FILE}')
    """).fetchone()

    days_by_mtu = con.execute(f"""
        SELECT mtu_minutes, COUNT(DISTINCT date) as n_days
        FROM read_parquet('{OUTPUT_FILE}')
        GROUP BY mtu_minutes ORDER BY mtu_minutes
    """).df()

    rows_per_day = con.execute(f"""
        SELECT MIN(cnt) as min_rows, MAX(cnt) as max_rows, AVG(cnt) as avg_rows
        FROM (SELECT date, COUNT(*) as cnt FROM read_parquet('{OUTPUT_FILE}') GROUP BY date) t
    """).fetchone()

    mtu_dist = {int(r[0]): int(r[1]) for r in days_by_mtu.itertuples(index=False)}

    print(f"Input files:             {n_files}")
    print(f"Output rows (latest):    {stats[0]:,}")
    print(f"Date range:              {stats[1]} -> {stats[2]}")
    print(f"Output file:             {OUTPUT_FILE}")
    print(f"Days by inferred MTU:    {mtu_dist}")
    print(
        f"Rows/day summary:        "
        f"min={int(rows_per_day[0])}, "
        f"max={int(rows_per_day[1])}, "
        f"mean={rows_per_day[2]:.0f}"
    )


if __name__ == "__main__":
    main()
