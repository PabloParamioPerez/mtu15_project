"""Parse extracted A2_liquicomun inner files into per-month Parquet.

Output:
    data/processed/esios/liquidaciones/liquicomun_<yyyymm>.parquet

Each per-month parquet contains all parsed-family rows for the month,
in long format with `family` discriminator (impdsvqh, endrozrqh, etc.).
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[4]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from mtu.parsing.esios.liquicomun import parse_extracted_dir  # noqa: E402
from mtu.parsing.omie_common import (  # noqa: E402
    append_csv_row,
    ensure_dir,
    utc_now_iso,
)

MARKET = "esios_liquidaciones"
CATEGORY = "liquicomun"
FILE_FAMILY = "liquicomun"
PARSER_NAME = "esios.liquicomun.parse_extracted_dir"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Parse extracted A2_liquicomun inner files into per-month Parquet."
    )
    p.add_argument(
        "--overwrite", action="store_true",
        help="Re-parse months even if output already exists.",
    )
    p.add_argument(
        "--start-month", default=None, help="YYYY-MM (optional filter).",
    )
    p.add_argument(
        "--end-month", default=None, help="YYYY-MM (optional filter).",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()

    raw_root = PROJECT_ROOT / "data/raw/esios/liquidaciones"
    processed_dir = PROJECT_ROOT / "data/processed/esios/liquidaciones"
    ingestion_log = PROJECT_ROOT / "data/metadata/ingestion_log.csv"
    ensure_dir(processed_dir)

    if not raw_root.exists():
        print(f"No raw root: {raw_root}")
        return

    counts = {"success": 0, "skipped": 0, "failed": 0, "empty": 0}

    for month_dir in sorted(raw_root.iterdir()):
        if not month_dir.is_dir():
            continue
        yyyymm = month_dir.name
        if not yyyymm.isdigit() or len(yyyymm) != 6:
            continue
        if args.start_month and yyyymm < args.start_month.replace("-", ""):
            continue
        if args.end_month and yyyymm > args.end_month.replace("-", ""):
            continue

        extracted = month_dir / "extracted"
        if not extracted.exists():
            print(f"[SKIP]    {yyyymm}: no extracted/ subdir")
            counts["skipped"] += 1
            continue

        out_path = processed_dir / f"liquicomun_{yyyymm}.parquet"
        if out_path.exists() and not args.overwrite:
            counts["skipped"] += 1
            continue

        status = "success"
        error_message = ""
        rows_output = 0

        try:
            df = parse_extracted_dir(extracted)
            rows_output = len(df)
            if df.empty:
                counts["empty"] += 1
                if out_path.exists():
                    out_path.unlink()
            else:
                df.to_parquet(out_path, index=False)
                counts["success"] += 1
                print(
                    f"[OK]      {yyyymm}: {len(df):,} rows, "
                    f"{df['family'].nunique()} families"
                )
        except Exception as e:
            status = "failed"
            error_message = f"{type(e).__name__}: {e}"
            counts["failed"] += 1
            print(f"[FAIL] {yyyymm} -> {error_message}")

        append_csv_row(
            ingestion_log,
            {
                "ingested_at": utc_now_iso(),
                "market": MARKET,
                "category": CATEGORY,
                "file_family": FILE_FAMILY,
                "filename": f"A2_liquicomun_{yyyymm}",
                "parser_name": PARSER_NAME,
                "raw_file_kind": "zip-extracted",
                "rows_read": rows_output,
                "rows_output": rows_output,
                "status": status,
                "output_path": str(out_path.relative_to(PROJECT_ROOT)),
                "error_message": error_message,
            },
        )

    print(
        f"\nDone. success={counts['success']}, empty={counts['empty']}, "
        f"skipped={counts['skipped']}, failed={counts['failed']}"
    )


if __name__ == "__main__":
    main()
