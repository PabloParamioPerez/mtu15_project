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

FILE_RE = re.compile(r"pdbc_(\d{8})\.(\d+)\.parquet$")


def _print_stats(con: duckdb.DuckDBPyConnection, output_path: Path) -> None:
    stats = con.execute(f"""
        SELECT COUNT(*), MIN(date), MAX(date) FROM read_parquet('{output_path}')
    """).fetchone()
    days_by_mtu = con.execute(f"""
        SELECT mtu_minutes, COUNT(DISTINCT date) AS n_days
        FROM read_parquet('{output_path}')
        GROUP BY mtu_minutes ORDER BY mtu_minutes
    """).df()
    rows_per_day = con.execute(f"""
        SELECT MIN(cnt), MAX(cnt), AVG(cnt) FROM (
            SELECT date, COUNT(*) cnt FROM read_parquet('{output_path}') GROUP BY date
        ) t
    """).fetchone()
    mtu_dist = {int(r[0]): int(r[1]) for r in days_by_mtu.itertuples(index=False)}
    print(f"Output rows:             {stats[0]:,}")
    print(f"Date range:              {stats[1]} -> {stats[2]}")
    print(f"Output file:             {output_path}")
    print(f"Days by inferred MTU:    {mtu_dist}")
    print(
        f"Rows/day summary:        min={int(rows_per_day[0])}, "
        f"max={int(rows_per_day[1])}, mean={rows_per_day[2]:.2f}"
    )


def _incremental_rebuild(
    processed_dir: Path, output_path: Path, new_files: list[Path]
) -> None:
    """Append new daily files to the existing all.parquet, evicting any
    rows with overlapping dates (so a new version of an existing date
    cleanly supersedes the old one)."""
    print(f"Incremental: {len(new_files)} new/changed daily file(s)")

    by_new_date: dict[str, list[tuple[int, Path]]] = defaultdict(list)
    for f in new_files:
        m = FILE_RE.match(f.name)
        if not m:
            raise ValueError(f"Unexpected filename: {f.name}")
        by_new_date[m.group(1)].append((int(m.group(2)), f))
    new_kept = [max(lst, key=lambda t: t[0])[1] for lst in by_new_date.values()]

    with tempfile.TemporaryDirectory(prefix="pdbc_inc_") as stage_dir:
        stage = Path(stage_dir)
        for f in new_kept:
            (stage / f.name).symlink_to(f.resolve())
        new_glob = str(stage / "pdbc_*.parquet")

        con = duckdb.connect()
        con.execute("SET memory_limit='6GB'")
        con.execute("SET threads=4")
        con.execute(f"SET temp_directory='{stage}'")

        tmp_output = str(output_path) + ".inc"
        con.execute(f"""
            COPY (
                SELECT * FROM read_parquet('{output_path}')
                WHERE date NOT IN (
                    SELECT DISTINCT date FROM read_parquet('{new_glob}', union_by_name=true)
                )
                UNION ALL BY NAME
                SELECT * FROM read_parquet('{new_glob}', union_by_name=true)
            )
            TO '{tmp_output}' (FORMAT PARQUET)
        """)

    output_path.unlink()
    Path(tmp_output).rename(output_path)
    print(f"Replaced {len(by_new_date)} date(s) in place.")
    _print_stats(duckdb.connect(), output_path)


def main() -> None:
    processed_dir = PROJECT_ROOT / "data/processed/omie/mercado_diario/programas/pdbc"
    output_path = processed_dir.parent / "pdbc_all.parquet"

    all_files_iter = sorted(processed_dir.glob("*.parquet"))
    if not all_files_iter:
        raise FileNotFoundError(f"No parquet files found in {processed_dir}")

    # ---------------------------------------------------------------
    # Incremental path: if output exists, only re-process files newer
    # than the output and replace those dates in the existing parquet.
    # Falls back to full rebuild if output is missing or stale.
    # ---------------------------------------------------------------
    if output_path.exists():
        output_mtime = output_path.stat().st_mtime
        new_files = [f for f in all_files_iter if f.stat().st_mtime > output_mtime]
        if not new_files:
            print("Up to date, skipping build.")
            return

        if len(new_files) < 0.5 * len(all_files_iter):
            _incremental_rebuild(processed_dir, output_path, new_files)
            return
        # else fall through to full rebuild (changed too many files)
        print(f"{len(new_files)}/{len(all_files_iter)} files changed; full rebuild.")

    all_files = all_files_iter

    # Pick the highest-version file per date (filename encodes version: pdbc_YYYYMMDD.V.parquet).
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
    with tempfile.TemporaryDirectory(prefix="pdbc_stage_") as stage_dir:
        stage = Path(stage_dir)
        for f in kept:
            (stage / f.name).symlink_to(f.resolve())
        stage_glob = str(stage / "pdbc_*.parquet")

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

    _print_stats(duckdb.connect(), output_path)


if __name__ == "__main__":
    main()
