from __future__ import annotations

from collections import defaultdict
from pathlib import Path
import re
import sys
import tempfile

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parents[4]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

FILE_RE = re.compile(r"pdbf_(\d{8})\.(\d+)\.parquet$")


def main() -> None:
    processed_dir = PROJECT_ROOT / "data/processed/omie/mercado_diario/programas/pdbf"
    output_path = processed_dir.parent / "pdbf_all.parquet"

    if output_path.exists():
        newest_input = max((f.stat().st_mtime for f in processed_dir.glob("*.parquet")), default=0)
        if output_path.stat().st_mtime >= newest_input:
            print("Up to date, skipping build.")
            return

    all_files = sorted(processed_dir.glob("*.parquet"))
    if not all_files:
        raise FileNotFoundError(f"No parquet files found in {processed_dir}")

    # Pick the highest-version file per date (filename encodes version: pdbf_YYYYMMDD.V.parquet).
    # Cross-file dedup at the row level becomes unnecessary because OMIE's later versions
    # supersede earlier ones in full for that date.
    by_date: dict[str, list[tuple[int, Path]]] = defaultdict(list)
    for f in all_files:
        m = FILE_RE.match(f.name)
        if not m:
            raise ValueError(f"Unexpected filename: {f.name}")
        by_date[m.group(1)].append((int(m.group(2)), f))

    kept: list[Path] = [max(lst, key=lambda t: t[0])[1] for lst in by_date.values()]
    kept.sort()
    n_superseded = len(all_files) - len(kept)

    print(f"Input files (all):       {len(all_files)}")
    print(f"Dates covered:           {len(kept)}")
    print(f"Superseded versions:     {n_superseded} (older .V.parquet files ignored)")

    # Stage kept files into a temp dir so read_parquet() globs to exactly them.
    with tempfile.TemporaryDirectory(prefix="pdbf_stage_") as stage_dir:
        stage = Path(stage_dir)
        for f in kept:
            (stage / f.name).symlink_to(f.resolve())
        stage_glob = str(stage / "pdbf_*.parquet")

        con = duckdb.connect()
        con.execute("SET memory_limit='6GB'")
        con.execute("SET preserve_insertion_order=false")
        con.execute("SET threads=4")
        con.execute(f"SET temp_directory='{stage}'")

        output_path.parent.mkdir(parents=True, exist_ok=True)
        con.execute(f"""
            COPY (
                SELECT * FROM read_parquet('{stage_glob}', union_by_name=true)
            )
            TO '{output_path}' (FORMAT PARQUET)
        """)

    # --- Summary stats on final output ---
    con2 = duckdb.connect()
    stats = con2.execute(f"""
        SELECT COUNT(*), MIN(date), MAX(date)
        FROM read_parquet('{output_path}')
    """).fetchone()

    days_by_mtu = con2.execute(f"""
        SELECT mtu_minutes, COUNT(DISTINCT date) AS n_days
        FROM read_parquet('{output_path}')
        GROUP BY mtu_minutes
        ORDER BY mtu_minutes
    """).df()

    rows_per_day = con2.execute(f"""
        SELECT MIN(cnt), MAX(cnt), AVG(cnt) FROM (
            SELECT date, COUNT(*) AS cnt
            FROM read_parquet('{output_path}')
            GROUP BY date
        ) t
    """).fetchone()

    mtu_dist = {int(r[0]): int(r[1]) for r in days_by_mtu.itertuples(index=False)}

    print(f"Output rows (latest):    {stats[0]:,}")
    print(f"Date range:              {stats[1]} -> {stats[2]}")
    print(f"Output file:             {output_path}")
    print(f"Days by inferred MTU:    {mtu_dist}")
    print(
        f"Rows/day summary:        min={int(rows_per_day[0])}, "
        f"max={int(rows_per_day[1])}, mean={rows_per_day[2]:.2f}"
    )


if __name__ == "__main__":
    main()
