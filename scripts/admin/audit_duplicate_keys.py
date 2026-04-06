from __future__ import annotations

import argparse
import sqlite3
import tempfile
from pathlib import Path
import re

import pandas as pd


DEFAULT_METADATA_COLS = {
    "source_file",
    "source_path",
    "version_suffix",
    "version_suffix_num",
    "raw_row_number_in_file",
    "row_number_in_file",
    "exact_duplicate_rows_dropped",
    "market",
    "category",
    "file_family",
    "mtu_minutes",
    "value_signature",
    "file_month",
}

DEFAULT_KEY_COLS = ["date", "session_number", "period", "unit_code"]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Audit whether duplicated business keys are harmless overlaps or true conflicting duplicates."
    )
    p.add_argument(
        "--family-dir",
        required=True,
        help="Directory with per-file parquet outputs, e.g. data/processed/.../pibca",
    )
    p.add_argument(
        "--month",
        action="append",
        default=[],
        help="Optional YYYYMM filter. Can be passed multiple times.",
    )
    p.add_argument(
        "--start-month",
        default=None,
        help="Optional YYYYMM lower bound.",
    )
    p.add_argument(
        "--end-month",
        default=None,
        help="Optional YYYYMM upper bound.",
    )
    p.add_argument(
        "--key-cols",
        nargs="+",
        default=DEFAULT_KEY_COLS,
        help="Business key columns to test for duplicates.",
    )
    p.add_argument(
        "--value-cols",
        nargs="+",
        default=None,
        help="Economic columns to compare inside duplicated keys. If omitted, they are auto-detected.",
    )
    p.add_argument(
        "--sample-conflicts",
        type=int,
        default=5,
        help="How many conflicting keys to print in detail.",
    )
    return p.parse_args()


def infer_month_from_name(path: Path) -> str | None:
    m = re.search(r"_(\d{6})\d+", path.stem)
    return m.group(1) if m else None


def month_in_scope(month: str | None, wanted: set[str], start: str | None, end: str | None) -> bool:
    if month is None:
        return False
    if wanted and month not in wanted:
        return False
    if start is not None and month < start:
        return False
    if end is not None and month > end:
        return False
    return True


def stable_scalar_str(x) -> str:
    if pd.isna(x):
        return "<NA>"
    if isinstance(x, float):
        return f"{x:.12g}"
    return str(x)


def make_signature(df: pd.DataFrame, value_cols: list[str]) -> pd.Series:
    return df[value_cols].apply(
        lambda row: " || ".join(stable_scalar_str(row[c]) for c in value_cols),
        axis=1,
    )


def main() -> None:
    args = parse_args()

    family_dir = Path(args.family_dir)
    if not family_dir.exists():
        raise FileNotFoundError(f"Family dir not found: {family_dir}")

    files = sorted(family_dir.glob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"No parquet files found in {family_dir}")

    wanted_months = set(args.month)

    selected_files = []
    for p in files:
        month = infer_month_from_name(p)
        if wanted_months or args.start_month or args.end_month:
            if not month_in_scope(month, wanted_months, args.start_month, args.end_month):
                continue
        selected_files.append((p, month))

    if not selected_files:
        raise ValueError("No files selected after applying month filters.")

    first_df = pd.read_parquet(selected_files[0][0])
    missing_keys = [c for c in args.key_cols if c not in first_df.columns]
    if missing_keys:
        raise ValueError(f"Missing key columns in parquet schema: {missing_keys}")

    if args.value_cols is None:
        value_cols = [
            c for c in first_df.columns
            if c not in set(args.key_cols) and c not in DEFAULT_METADATA_COLS
        ]
    else:
        value_cols = args.value_cols

    if not value_cols:
        raise ValueError(
            "No value columns selected. Pass --value-cols explicitly."
        )

    needed_cols = list(dict.fromkeys(args.key_cols + value_cols + ["version_suffix", "source_file"]))

    tmp_db = tempfile.NamedTemporaryFile(prefix="dup_audit_", suffix=".sqlite", delete=False)
    tmp_db_path = Path(tmp_db.name)
    tmp_db.close()

    conn = sqlite3.connect(tmp_db_path)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=OFF;")
    conn.execute("PRAGMA temp_store=MEMORY;")

    total_rows_loaded = 0

    try:
        for i, (path, month) in enumerate(selected_files, start=1):
            df = pd.read_parquet(path, columns=[c for c in needed_cols if c in first_df.columns]).copy()

            if "source_file" not in df.columns:
                df["source_file"] = path.stem
            if "version_suffix" not in df.columns:
                df["version_suffix"] = None

            df["version_suffix_num"] = pd.to_numeric(df["version_suffix"], errors="coerce")
            df["file_month"] = month
            df["value_signature"] = make_signature(df, value_cols)

            keep_cols = args.key_cols + value_cols + [
                "value_signature",
                "version_suffix",
                "version_suffix_num",
                "source_file",
                "file_month",
            ]
            df = df[keep_cols]

            df.to_sql("rows", conn, if_exists="append", index=False)
            total_rows_loaded += len(df)

            if i % 250 == 0 or i == len(selected_files):
                print(f"loaded_files={i}/{len(selected_files)} rows_loaded={total_rows_loaded}")

        key_sql = ", ".join(args.key_cols)

        summary_sql = f"""
        WITH grouped AS (
            SELECT
                {key_sql},
                COUNT(*) AS n_rows,
                COUNT(DISTINCT value_signature) AS n_signatures
            FROM rows
            GROUP BY {key_sql}
            HAVING COUNT(*) > 1
        )
        SELECT
            COUNT(*) AS duplicated_keys,
            COALESCE(SUM(n_rows), 0) AS duplicated_rows,
            COALESCE(SUM(CASE WHEN n_signatures = 1 THEN 1 ELSE 0 END), 0) AS safe_duplicate_keys,
            COALESCE(SUM(CASE WHEN n_signatures > 1 THEN 1 ELSE 0 END), 0) AS conflicting_duplicate_keys
        FROM grouped;
        """
        duplicated_keys, duplicated_rows, safe_keys, conflicting_keys = conn.execute(summary_sql).fetchone()

        print()
        print(f"family_dir:                 {family_dir}")
        print(f"files_scanned:              {len(selected_files)}")
        print(f"months_scanned:             {sorted({m for _, m in selected_files if m is not None})}")
        print(f"rows_loaded:                {total_rows_loaded}")
        print(f"key_cols:                   {args.key_cols}")
        print(f"value_cols:                 {value_cols}")
        print(f"duplicated_keys:            {duplicated_keys}")
        print(f"duplicated_rows:            {duplicated_rows}")
        print(f"safe_duplicate_keys:        {safe_keys}")
        print(f"conflicting_duplicate_keys: {conflicting_keys}")

        if duplicated_keys > 0:
            month_sql = f"""
            WITH grouped AS (
                SELECT
                    MIN(file_month) AS file_month,
                    {key_sql},
                    COUNT(*) AS n_rows,
                    COUNT(DISTINCT value_signature) AS n_signatures
                FROM rows
                GROUP BY {key_sql}
                HAVING COUNT(*) > 1
            )
            SELECT
                file_month,
                COUNT(*) AS duplicated_keys,
                SUM(CASE WHEN n_signatures = 1 THEN 1 ELSE 0 END) AS safe_duplicate_keys,
                SUM(CASE WHEN n_signatures > 1 THEN 1 ELSE 0 END) AS conflicting_duplicate_keys
            FROM grouped
            GROUP BY file_month
            ORDER BY file_month;
            """
            month_df = pd.read_sql_query(month_sql, conn)
            print()
            print("duplicate_summary_by_month:")
            print(month_df.to_string(index=False))

        if conflicting_keys > 0:
            print()
            print("sample_conflicting_keys:")
            conflict_keys_sql = f"""
            WITH grouped AS (
                SELECT
                    {key_sql},
                    COUNT(*) AS n_rows,
                    COUNT(DISTINCT value_signature) AS n_signatures
                FROM rows
                GROUP BY {key_sql}
                HAVING COUNT(*) > 1 AND COUNT(DISTINCT value_signature) > 1
            )
            SELECT {key_sql}
            FROM grouped
            ORDER BY {key_sql}
            LIMIT ?;
            """
            conflict_keys = conn.execute(conflict_keys_sql, (args.sample_conflicts,)).fetchall()

            for key_vals in conflict_keys:
                where_clause = " AND ".join(f"{c} = ?" for c in args.key_cols)
                detail_sql = f"""
                SELECT
                    {", ".join(args.key_cols + value_cols + ['version_suffix', 'version_suffix_num', 'source_file'])}
                FROM rows
                WHERE {where_clause}
                ORDER BY version_suffix_num, source_file;
                """
                detail_df = pd.read_sql_query(detail_sql, conn, params=key_vals)
                print()
                print(f"KEY = {dict(zip(args.key_cols, key_vals))}")
                print(detail_df.to_string(index=False))
        else:
            print()
            print("No conflicting duplicates found in the selected scope.")
            print("That means duplicated coarse keys share the same economic values, so dropping by key looks safe in this scope.")

    finally:
        conn.close()
        try:
            tmp_db_path.unlink(missing_ok=True)
        except Exception:
            pass


if __name__ == "__main__":
    main()
