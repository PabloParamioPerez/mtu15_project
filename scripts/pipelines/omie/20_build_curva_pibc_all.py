"""
Build curva_pibc_all.parquet from all individually parsed curva_pibc parquet files.

Deduplication strategy
----------------------
Each file is a complete snapshot of the aggregated curve for a given (date, session).
Multiple version files may exist for the same (date, session) — e.g.
curva_pibc_202604092.1 and curva_pibc_202604092.2.

Deduplication is file-level per (date, session_number): ALL rows from the highest
version are kept and ALL rows from lower versions are dropped.
"""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parents[3]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

INPUT_DIR = PROJECT_ROOT / "data/processed/omie/mercado_intradiario_subastas/curvas/curva_pibc"
OUTPUT_FILE = PROJECT_ROOT / "data/processed/omie/mercado_intradiario_subastas/curvas/curva_pibc_all.parquet"


def main() -> None:
    glob = str(INPUT_DIR / "*.parquet")

    con = duckdb.connect()

    n_files = con.execute(
        f"SELECT COUNT(DISTINCT filename) FROM read_parquet('{glob}', filename=true, union_by_name=true)"
    ).fetchone()[0]

    if n_files == 0:
        raise SystemExit(f"No parquet files found in {INPUT_DIR}")

    # --- Detect superseded versions ---
    # For each (date, session_number), keep only rows from the highest version_suffix.
    superseded = con.execute(f"""
        WITH file_meta AS (
            SELECT DISTINCT source_file, date, session_number,
                   TRY_CAST(version_suffix AS INTEGER) as ver
            FROM read_parquet('{glob}', union_by_name=true)
        ),
        max_ver AS (
            SELECT date, session_number, MAX(ver) as max_ver
            FROM file_meta GROUP BY date, session_number
        )
        SELECT fm.source_file, fm.date, fm.session_number, fm.ver as version_suffix
        FROM file_meta fm
        JOIN max_ver mv ON fm.date = mv.date AND fm.session_number = mv.session_number
        WHERE fm.ver < mv.max_ver
        ORDER BY fm.date, fm.session_number, fm.source_file
    """).df()

    if not superseded.empty:
        print(
            f"WARNING: {len(superseded)} source file(s) superseded by a higher version "
            f"for the same (date, session) — dropping their rows:"
        )
        print(superseded.to_string(index=False))

    # --- Write deduplicated, sorted output ---
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    con.execute(f"""
        COPY (
            WITH max_ver AS (
                SELECT date, session_number, MAX(TRY_CAST(version_suffix AS INTEGER)) as max_ver
                FROM (
                    SELECT DISTINCT date, session_number, source_file, version_suffix
                    FROM read_parquet('{glob}', union_by_name=true)
                ) file_meta
                GROUP BY date, session_number
            )
            SELECT t.*
            FROM read_parquet('{glob}', union_by_name=true) t
            JOIN max_ver mv
                ON t.date = mv.date
                AND t.session_number = mv.session_number
                AND TRY_CAST(t.version_suffix AS INTEGER) = mv.max_ver
            ORDER BY t.date, t.session_number, t.source_file, t.row_order
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

    sessions_dist = con.execute(f"""
        SELECT session_number, COUNT(DISTINCT date) as n_days
        FROM read_parquet('{OUTPUT_FILE}')
        GROUP BY session_number ORDER BY session_number
    """).df()

    rows_per_file = con.execute(f"""
        SELECT MIN(cnt) as min_rows, MAX(cnt) as max_rows, AVG(cnt) as avg_rows
        FROM (SELECT source_file, COUNT(*) as cnt FROM read_parquet('{OUTPUT_FILE}') GROUP BY source_file) t
    """).fetchone()

    mtu_dist = {int(r[0]): int(r[1]) for r in days_by_mtu.itertuples(index=False)}

    print(f"Input files:             {n_files}")
    print(f"Output rows (latest):    {stats[0]:,}")
    print(f"Date range:              {stats[1]} -> {stats[2]}")
    print(f"Output file:             {OUTPUT_FILE}")
    print(f"Days by inferred MTU:    {mtu_dist}")
    print(f"Days per session:\n{sessions_dist.to_string(index=False)}")
    print(
        f"Rows/file summary:       "
        f"min={int(rows_per_file[0])}, "
        f"max={int(rows_per_file[1])}, "
        f"mean={rows_per_file[2]:.0f}"
    )


if __name__ == "__main__":
    main()
