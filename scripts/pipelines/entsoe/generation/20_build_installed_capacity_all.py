from __future__ import annotations

import sys
from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parents[4]


def main() -> None:
    processed_dir = PROJECT_ROOT / "data/processed/entsoe/generation/installed_capacity"
    output_path = processed_dir.parent / "installed_capacity_all.parquet"

    inputs = sorted(processed_dir.glob("*.parquet"))
    if not inputs:
        raise FileNotFoundError(f"No parquet files found in {processed_dir}")

    if output_path.exists():
        newest_input = max(f.stat().st_mtime for f in inputs)
        if output_path.stat().st_mtime >= newest_input:
            print("Up to date, skipping build.")
            return

    glob_pat = str(processed_dir / "*.parquet")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    con.execute("SET memory_limit='2GB'")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET threads=4")

    con.execute(f"""
        COPY (
            SELECT * FROM read_parquet('{glob_pat}', union_by_name=true)
            ORDER BY year, psr_type
        )
        TO '{output_path}' (FORMAT PARQUET)
    """)

    stats = con.execute(f"""
        SELECT COUNT(*), MIN(year), MAX(year), COUNT(DISTINCT psr_type)
        FROM read_parquet('{output_path}')
    """).fetchone()

    pivot = con.execute(f"""
        SELECT psr_type,
               ROUND(SUM(CASE WHEN year = 2023 THEN capacity_mw END)) AS mw_2023,
               ROUND(SUM(CASE WHEN year = 2024 THEN capacity_mw END)) AS mw_2024,
               ROUND(SUM(CASE WHEN year = 2025 THEN capacity_mw END)) AS mw_2025
        FROM read_parquet('{output_path}')
        GROUP BY 1 ORDER BY 1
    """).df()

    print(f"Input files:     {len(inputs)}")
    print(f"Output rows:     {stats[0]:,}")
    print(f"Year range:      {stats[1]}  ->  {stats[2]}")
    print(f"psr_type count:  {stats[3]}")
    print(f"Output file:     {output_path}")
    print("\nCapacity by psrType (MW), 2023-2025:")
    print(pivot.to_string(index=False))


if __name__ == "__main__":
    sys.exit(main())
