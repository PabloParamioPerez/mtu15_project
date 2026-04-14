from __future__ import annotations

from pathlib import Path
import sys

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parents[3]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

# (source_file, row_number_in_file) is the natural unique key:
# each file is a daily snapshot, row_number is per-file sequential.
IDENTITY_KEY = ["source_file", "row_number_in_file"]


def main() -> None:
    processed_dir = PROJECT_ROOT / "data/processed/omie/mercado_intradiario_continuo/transacciones/trades"
    output_path = processed_dir.parent / "trades_all.parquet"

    glob = str(processed_dir / "*.parquet")

    con = duckdb.connect()

    n_files = con.execute(
        f"SELECT COUNT(DISTINCT filename) FROM read_parquet('{glob}', filename=true, union_by_name=true)"
    ).fetchone()[0]

    if n_files == 0:
        raise FileNotFoundError(f"No parquet files found in {processed_dir}")

    print(f"Input files:             {n_files}")

    # --- Uniqueness check ---
    key_expr = ", ".join(IDENTITY_KEY)
    n_dups = con.execute(f"""
        SELECT COUNT(*) FROM (
            SELECT {key_expr}
            FROM read_parquet('{glob}', union_by_name=true)
            GROUP BY ALL
            HAVING COUNT(*) > 1
        ) t
    """).fetchone()[0]

    if n_dups > 0:
        sample = con.execute(f"""
            SELECT {key_expr}, COUNT(*) as cnt
            FROM read_parquet('{glob}', union_by_name=true)
            GROUP BY ALL
            HAVING COUNT(*) > 1
            LIMIT 20
        """).df()
        raise ValueError(
            f"Trades raw table: {n_dups} duplicate key(s) on {IDENTITY_KEY}.\n"
            f"Sample:\n{sample.to_string(index=False)}"
        )

    # --- Write sorted output ---
    output_path.parent.mkdir(parents=True, exist_ok=True)
    sort_cols = "source_file, row_number_in_file"

    con.execute(f"""
        COPY (
            SELECT *
            FROM read_parquet('{glob}', union_by_name=true)
            ORDER BY {sort_cols}
        )
        TO '{output_path}' (FORMAT PARQUET)
    """)

    # --- Summary stats ---
    stats = con.execute(f"""
        SELECT
            COUNT(*) as total_rows,
            MIN(trade_date) as date_min,
            MAX(trade_date) as date_max
        FROM read_parquet('{output_path}')
    """).fetchone()

    days_by_mtu = con.execute(f"""
        SELECT mtu_minutes, COUNT(DISTINCT trade_date) as n_days
        FROM read_parquet('{output_path}')
        GROUP BY mtu_minutes
        ORDER BY mtu_minutes
    """).df()

    rows_per_file = con.execute(f"""
        SELECT MIN(cnt) as min_rows, MAX(cnt) as max_rows, AVG(cnt) as avg_rows
        FROM (
            SELECT source_file, COUNT(*) as cnt
            FROM read_parquet('{output_path}')
            GROUP BY source_file
        ) t
    """).fetchone()

    mtu_dist = {int(r[0]): int(r[1]) for r in days_by_mtu.itertuples(index=False)}

    print(f"Output rows:             {stats[0]:,}")
    print(f"Date range:              {stats[1]} -> {stats[2]}")
    print(f"Output file:             {output_path}")
    print(f"Days by inferred MTU:    {mtu_dist}")
    print(
        f"Rows/file summary:       min={int(rows_per_file[0])}, "
        f"max={int(rows_per_file[1])}, mean={rows_per_file[2]:.2f}"
    )


if __name__ == "__main__":
    main()
