from __future__ import annotations

import sys
from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parents[4]


def main() -> None:
    processed_dir = PROJECT_ROOT / "data/processed/entsoe/balancing/activations"
    output_path = processed_dir.parent / "activated_prices_all.parquet"

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
    con.execute("SET memory_limit='4GB'")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET threads=4")

    con.execute(f"""
        COPY (
            SELECT * FROM read_parquet('{glob_pat}', union_by_name=true)
            ORDER BY isp_start_utc, business_type, flow_direction, position
        )
        TO '{output_path}' (FORMAT PARQUET)
    """)

    stats = con.execute(f"""
        SELECT COUNT(*), MIN(isp_start_utc), MAX(isp_start_utc),
               COUNT(DISTINCT DATE_TRUNC('day', isp_start_utc))
        FROM read_parquet('{output_path}')
    """).fetchone()

    bt_dist = con.execute(f"""
        SELECT business_type, flow_direction, COUNT(*) AS n
        FROM read_parquet('{output_path}')
        GROUP BY 1, 2 ORDER BY 1, 2
    """).df()

    print(f"Input files:     {len(inputs)}")
    print(f"Output rows:     {stats[0]:,}")
    print(f"Time range:      {stats[1]}  ->  {stats[2]}")
    print(f"Distinct days:   {stats[3]:,}")
    print(f"Output file:     {output_path}")
    print("Rows by businessType x flowDirection:")
    print(bt_dist.to_string(index=False))


if __name__ == "__main__":
    sys.exit(main())
