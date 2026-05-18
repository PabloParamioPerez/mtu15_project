"""Parse extracted liquicierre / liquicierresrs XML files into per-month Parquet.

Walks both archive trees:
    data/raw/esios/reservas/liquicierre/<yyyymm>/extracted/
    data/raw/esios/reservas/liquicierresrs/<yyyymm>/extracted/

Output:
    data/processed/esios/reservas/liquicierre_<yyyymm>.parquet
    data/processed/esios/reservas/liquicierresrs_<yyyymm>.parquet

Each per-month parquet has the long-format schema described in
`src/mtu/parsing/esios/liquicierre.py` docstring.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[4]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from mtu.parsing.esios.liquicierre import parse_liquicierre_dir  # noqa: E402
from mtu.parsing.omie_common import (  # noqa: E402
    append_csv_row,
    ensure_dir,
    utc_now_iso,
)

MARKET = "esios_reservas"

ARCHIVES = [
    # (raw_subdir, archive_tag, processed_prefix)
    ("liquicierre",     "liquicierre",     "liquicierre"),
    ("liquicierresrs",  "liquicierresrs",  "liquicierresrs"),
]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Parse liquicierre / liquicierresrs XMLs into per-month parquet."
    )
    p.add_argument("--overwrite", action="store_true")
    p.add_argument("--start-month", default=None, help="YYYY-MM (optional filter)")
    p.add_argument("--end-month", default=None, help="YYYY-MM (optional filter)")
    p.add_argument(
        "--only", choices=["liquicierre", "liquicierresrs", "both"], default="both",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()

    processed_root = PROJECT_ROOT / "data/processed/esios/reservas"
    ingestion_log = PROJECT_ROOT / "data/metadata/ingestion_log.csv"

    grand = {"success": 0, "skipped": 0, "failed": 0, "empty": 0}

    for raw_sub, archive_tag, prefix in ARCHIVES:
        if args.only != "both" and args.only != archive_tag:
            continue

        raw_root = PROJECT_ROOT / "data/raw/esios/reservas" / raw_sub
        if not raw_root.exists():
            print(f"[SKIP archive] {archive_tag}: no raw root {raw_root}")
            continue

        processed_dir = processed_root / archive_tag
        ensure_dir(processed_dir)

        print(f"\n=== {archive_tag} ===")
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
                # Some months come back as a single XML payload, no zip
                # The sync script saved it as <prefix>_<yyyymm>.xml in month_dir
                xmls = list(month_dir.glob("*.xml"))
                if xmls:
                    extracted = month_dir
                else:
                    grand["skipped"] += 1
                    continue

            out_path = processed_dir / f"{prefix}_{yyyymm}.parquet"
            if out_path.exists() and not args.overwrite:
                grand["skipped"] += 1
                continue

            status = "success"
            error_message = ""
            rows_output = 0

            try:
                df = parse_liquicierre_dir(extracted, archive=archive_tag)
                rows_output = len(df)
                if df.empty:
                    grand["empty"] += 1
                    if out_path.exists():
                        out_path.unlink()
                else:
                    df.to_parquet(out_path, index=False)
                    grand["success"] += 1
                    print(
                        f"[OK]      {prefix}_{yyyymm}: {len(df):,} rows, "
                        f"{df['bsp'].nunique()} BSPs, "
                        f"{df['info'].nunique()} info codes"
                    )
            except Exception as e:
                status = "failed"
                error_message = f"{type(e).__name__}: {e}"
                grand["failed"] += 1
                print(f"[FAIL] {yyyymm} -> {error_message}")

            append_csv_row(
                ingestion_log,
                {
                    "ingested_at": utc_now_iso(),
                    "market": MARKET,
                    "category": archive_tag,
                    "file_family": archive_tag,
                    "filename": f"{prefix}_{yyyymm}",
                    "parser_name": "esios.liquicierre.parse_liquicierre_dir",
                    "raw_file_kind": "xml-extracted",
                    "status": status,
                    "error_message": error_message,
                    "rows_output": rows_output,
                },
            )

    print(
        f"\nDone. success={grand['success']}, skipped={grand['skipped']}, "
        f"empty={grand['empty']}, failed={grand['failed']}"
    )


if __name__ == "__main__":
    main()
