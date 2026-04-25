"""Consolidate per-month liquicomun Parquet into liquicomun_all.parquet."""
from __future__ import annotations

from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parents[4]


def main() -> None:
    processed_dir = PROJECT_ROOT / "data/processed/esios/liquidaciones"
    output_path = processed_dir.parent / "liquicomun_all.parquet"

    inputs = sorted(processed_dir.glob("liquicomun_*.parquet"))
    if not inputs:
        raise FileNotFoundError(f"No parquet files found in {processed_dir}")

    if output_path.exists():
        newest_input = max(f.stat().st_mtime for f in inputs)
        if output_path.stat().st_mtime >= newest_input:
            print("Up to date, skipping build.")
            return

    glob_pat = str(processed_dir / "liquicomun_*.parquet")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET threads=4")

    con.execute(f"""
        COPY (
            SELECT * FROM read_parquet('{glob_pat}', union_by_name=true)
            ORDER BY family, date, hour, quarter
        )
        TO '{output_path}' (FORMAT PARQUET)
    """)

    stats = con.execute(f"""
        SELECT COUNT(*), COUNT(DISTINCT family),
               MIN(date), MAX(date),
               COUNT(DISTINCT DATE_TRUNC('month', date))
        FROM read_parquet('{output_path}')
    """).fetchone()

    fam_dist = con.execute(f"""
        SELECT family, COUNT(*) AS n
        FROM read_parquet('{output_path}')
        GROUP BY family ORDER BY n DESC
    """).df()

    print(f"Input files:      {len(inputs)}")
    print(f"Output rows:      {stats[0]:,}")
    print(f"Families:         {stats[1]}")
    print(f"Date range:       {stats[2]} -> {stats[3]}")
    print(f"Distinct months:  {stats[4]}")
    print(f"Output file:      {output_path}")
    print("Rows by family:")
    print(fam_dist.to_string(index=False))


if __name__ == "__main__":
    main()
