from __future__ import annotations

from pathlib import Path
import sys

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parents[4]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

# row_number_in_file is the per-file identity key (guaranteed unique by parser).
IDENTITY_KEY = ["source_file", "row_number_in_file"]


def _print_stats(con: duckdb.DuckDBPyConnection, output_path: Path) -> None:
    stats = con.execute(f"""
        SELECT COUNT(*), MIN(date), MAX(date) FROM read_parquet('{output_path}')
    """).fetchone()
    days_by_mtu = con.execute(f"""
        SELECT mtu_minutes, COUNT(DISTINCT date) AS n_days
        FROM read_parquet('{output_path}') GROUP BY mtu_minutes ORDER BY mtu_minutes
    """).df()
    rows_per_file = con.execute(f"""
        SELECT MIN(cnt), MAX(cnt), AVG(cnt) FROM (
            SELECT source_file, COUNT(*) cnt FROM read_parquet('{output_path}') GROUP BY source_file
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


def _incremental_rebuild(processed_dir: Path, output_path: Path, new_files: list[Path]) -> None:
    """Append new per-file parquets, evicting any rows whose source_file
    matches the incoming files (so a re-extracted file cleanly supersedes
    the old rows)."""
    import tempfile
    print(f"Incremental: {len(new_files)} new/changed file(s)")
    with tempfile.TemporaryDirectory(prefix="pibci_inc_") as stage_dir:
        stage = Path(stage_dir)
        for f in new_files:
            (stage / f.name).symlink_to(f.resolve())
        new_glob = str(stage / "*.parquet")
        con = duckdb.connect()
        con.execute("SET memory_limit='6GB'")
        con.execute("SET threads=4")
        con.execute(f"SET temp_directory='{stage}'")
        tmp_output = str(output_path) + ".inc"
        con.execute(f"""
            COPY (
                SELECT * FROM read_parquet('{output_path}')
                WHERE source_file NOT IN (
                    SELECT DISTINCT source_file FROM read_parquet('{new_glob}', union_by_name=true)
                )
                UNION ALL BY NAME
                SELECT * FROM read_parquet('{new_glob}', union_by_name=true)
                ORDER BY source_file, row_number_in_file
            )
            TO '{tmp_output}' (FORMAT PARQUET)
        """)
    output_path.unlink()
    Path(tmp_output).rename(output_path)
    print("Incremental rebuild complete.")
    _print_stats(duckdb.connect(), output_path)


def main() -> None:
    processed_dir = PROJECT_ROOT / "data/processed/omie/mercado_intradiario_subastas/programas/pibci"
    output_path = processed_dir.parent / "pibci_all.parquet"

    all_files_iter = sorted(processed_dir.glob("*.parquet"))
    if not all_files_iter:
        raise FileNotFoundError(f"No parquet files found in {processed_dir}")

    # ---------------------------------------------------------------
    # Incremental path: replace only the source_files that changed.
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
        print(f"{len(new_files)}/{len(all_files_iter)} files changed; full rebuild.")

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
            f"PIBCI raw table: {n_dups} duplicate key(s) on {IDENTITY_KEY}.\n"
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

    _print_stats(con, output_path)


if __name__ == "__main__":
    main()
