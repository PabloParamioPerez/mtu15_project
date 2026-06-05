"""Idempotent incremental upsert helper for {family}_all.parquet builders.

The four OMIE bid-stack {family}_all builders (DET, CAB, IDET, ICAB) used to
re-scan every daily parquet (~3,000 files) on every rebuild because the
duplicate-key check was written over the full glob. For a refresh that adds
six new daily files, this re-reads ~8 years of data unnecessarily.

`build_or_append` centralises the upsert:
  * full build when the output parquet does not yet exist
  * incremental append when the output exists and one or more new daily
    parquets have appeared since (matched by source_file basename)
  * skip when nothing is new

Returns the DuckDB connection so each script can run its own summary-stats
queries against the freshly written output.
"""

from __future__ import annotations

from pathlib import Path

import duckdb


def build_or_append(
    processed_dir: Path,
    output_path: Path,
    family: str,
    identity_key: tuple[str, ...] = ("source_file", "row_number_in_file"),
    sort_cols: str = "source_file, row_number_in_file",
    temp_dir: Path | None = None,
) -> duckdb.DuckDBPyConnection:
    """Build or incrementally append `{output_path}` from per-day parquets in
    `processed_dir`. See module docstring for behaviour."""
    daily_files = sorted(processed_dir.glob("*.parquet"))
    if not daily_files:
        raise FileNotFoundError(f"No parquet files found in {processed_dir}")

    con = duckdb.connect()
    if temp_dir is not None:
        temp_dir.mkdir(parents=True, exist_ok=True)
        con.execute(f"SET temp_directory='{temp_dir}'")
    con.execute("SET preserve_insertion_order=false")

    key_expr = ", ".join(identity_key)
    glob = str(processed_dir / "*.parquet")

    if output_path.exists():
        try:
            existing_sources = set(
                con.execute(
                    f"SELECT DISTINCT source_file FROM read_parquet('{output_path}')"
                ).fetchdf()["source_file"].tolist()
            )
        except Exception as e:
            print(f"{family}: existing output unreadable ({e}); falling back to full build.")
            existing_sources = None

        if existing_sources is not None:
            new_files = [f for f in daily_files if f.stem not in existing_sources]
            if not new_files:
                print(
                    f"{family}: up to date ({len(existing_sources):,} source files; "
                    f"no new daily parquets)."
                )
                return con
            print(
                f"{family}: incremental rebuild --- existing {len(existing_sources):,} "
                f"files, appending {len(new_files)} new file(s)."
            )
            new_glob = ", ".join(f"'{f}'" for f in new_files)

            n_dups_internal = con.execute(f"""
                SELECT COUNT(*) FROM (
                    SELECT {key_expr}
                    FROM read_parquet([{new_glob}], union_by_name=true)
                    GROUP BY ALL HAVING COUNT(*) > 1
                ) t
            """).fetchone()[0]
            if n_dups_internal > 0:
                sample = con.execute(f"""
                    SELECT {key_expr}, COUNT(*) AS cnt
                    FROM read_parquet([{new_glob}], union_by_name=true)
                    GROUP BY ALL HAVING COUNT(*) > 1 LIMIT 20
                """).df()
                raise ValueError(
                    f"{family} incremental: {n_dups_internal} internal duplicate "
                    f"key(s) in NEW files on {list(identity_key)}.\n"
                    f"Sample:\n{sample.to_string(index=False)}"
                )

            n_collisions = con.execute(f"""
                SELECT COUNT(*) FROM (
                    SELECT {key_expr}
                    FROM read_parquet([{new_glob}], union_by_name=true)
                    INTERSECT
                    SELECT {key_expr}
                    FROM read_parquet('{output_path}')
                ) t
            """).fetchone()[0]
            if n_collisions > 0:
                raise ValueError(
                    f"{family} incremental: {n_collisions} identity-key collision(s) "
                    f"between new files and existing output on {list(identity_key)}."
                )

            output_path.parent.mkdir(parents=True, exist_ok=True)
            tmp_output = output_path.with_suffix(output_path.suffix + ".tmp")
            con.execute(f"""
                COPY (
                    SELECT * FROM (
                        SELECT * FROM read_parquet('{output_path}')
                        UNION ALL BY NAME
                        SELECT * FROM read_parquet([{new_glob}], union_by_name=true)
                    )
                    ORDER BY {sort_cols}
                )
                TO '{tmp_output}' (FORMAT PARQUET)
            """)
            tmp_output.replace(output_path)
            return con

    n_files = len(daily_files)
    print(f"{family}: full build of {n_files:,} files.")

    n_dups = con.execute(f"""
        SELECT COUNT(*) FROM (
            SELECT {key_expr}
            FROM read_parquet('{glob}', union_by_name=true)
            GROUP BY ALL HAVING COUNT(*) > 1
        ) t
    """).fetchone()[0]
    if n_dups > 0:
        sample = con.execute(f"""
            SELECT {key_expr}, COUNT(*) AS cnt
            FROM read_parquet('{glob}', union_by_name=true)
            GROUP BY ALL HAVING COUNT(*) > 1 LIMIT 20
        """).df()
        raise ValueError(
            f"{family} raw table: {n_dups} duplicate key(s) on {list(identity_key)}.\n"
            f"Sample:\n{sample.to_string(index=False)}"
        )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"""
        COPY (
            SELECT * FROM read_parquet('{glob}', union_by_name=true)
            ORDER BY {sort_cols}
        )
        TO '{output_path}' (FORMAT PARQUET)
    """)
    return con
