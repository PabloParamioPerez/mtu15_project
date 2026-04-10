from __future__ import annotations

import argparse
from pathlib import Path

import duckdb


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Compact OMIE processed coverage summary by family")
    p.add_argument(
        "--root-dir",
        default="data/processed/omie",
        help="Root processed OMIE folder to scan (default: data/processed/omie)",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    root_dir = Path(args.root_dir)

    if not root_dir.exists():
        raise FileNotFoundError(f"Root dir does not exist: {root_dir}")

    all_parquets = sorted(
        p for p in root_dir.rglob("*_all.parquet")
        if "_STALE_" not in p.name
    )

    if not all_parquets:
        print("No *_all.parquet files found.")
        return

    con = duckdb.connect()
    col_w = max(len(p.stem.removesuffix("_all")) for p in all_parquets)

    for path in all_parquets:
        family = path.stem.removesuffix("_all")
        try:
            segments = con.execute(
                """
                WITH dates AS (
                    SELECT DISTINCT date::DATE AS d FROM read_parquet(?)
                ),
                numbered AS (
                    SELECT d, ROW_NUMBER() OVER (ORDER BY d) AS rn FROM dates
                ),
                islands AS (
                    SELECT (d - INTERVAL (rn) DAY) AS grp, d FROM numbered
                )
                SELECT
                    MIN(d)::VARCHAR AS date_from,
                    MAX(d)::VARCHAR AS date_to,
                    COUNT(*)        AS days
                FROM islands
                GROUP BY grp
                ORDER BY date_from
                """,
                [str(path)],
            ).fetchall()
        except Exception as e:
            print(f"{family:<{col_w}}  ERROR: {e}")
            continue

        if not segments:
            print(f"{family:<{col_w}}  (empty)")
            continue

        first = True
        for date_from, date_to, days in segments:
            prefix = f"{family:<{col_w}}" if first else " " * col_w
            print(f"{prefix}  {date_from} .. {date_to}  ({days} days)")
            first = False


if __name__ == "__main__":
    main()
