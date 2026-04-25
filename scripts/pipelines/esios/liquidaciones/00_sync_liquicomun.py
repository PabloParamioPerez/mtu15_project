"""Sync ESIOS A2_liquicomun monthly settlement archives.

The ESIOS public archive endpoint serves a ZIP per calendar month.
Each ZIP contains 500+ inner files (settlement concepts).

Output:
    data/raw/esios/liquidaciones/<yyyymm>/A2_liquicomun_<yyyymm>.zip
    data/raw/esios/liquidaciones/<yyyymm>/extracted/<inner-files>

Usage:
    uv run python scripts/pipelines/esios/liquidaciones/00_sync_liquicomun.py \
        --start-month 2024-01 --end-month 2026-04
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[4]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from mtu.ingestion.esios_common import (  # noqa: E402
    ARCHIVES,
    USER_AGENT,
    extract_zip,
    fetch_archive,
    month_chunks,
)
from mtu.parsing.omie_common import (  # noqa: E402
    append_csv_row,
    ensure_dir,
    sha256_file,
    utc_now_iso,
)

MARKET = "esios_liquidaciones"
CATEGORY = "liquicomun"
FILE_FAMILY = "liquicomun"
ARCHIVE_ID = ARCHIVES["liquicomun"]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Download ESIOS A2_liquicomun monthly settlement ZIP archives. "
            "Public endpoint, no authentication required."
        )
    )
    p.add_argument("--start-month", required=True, help="YYYY-MM")
    p.add_argument("--end-month", required=True, help="YYYY-MM")
    p.add_argument(
        "--timeout", type=int, default=600,
        help="Per-request timeout in seconds (default 600).",
    )
    p.add_argument(
        "--max-retries", type=int, default=4,
        help="Retries on 5xx/network errors. Default 4.",
    )
    p.add_argument(
        "--overwrite", action="store_true",
        help="Redownload ZIP even if it already exists.",
    )
    p.add_argument(
        "--refresh-recent-months", type=int, default=2,
        help=(
            "Always redownload the most recent N requested months, "
            "because ESIOS publishes preliminary settlement that gets "
            "restated. Default 2."
        ),
    )
    p.add_argument(
        "--no-extract", action="store_true",
        help="Skip ZIP extraction (raw-ZIP-only mode).",
    )
    return p.parse_args()


def should_refresh(period_idx: int, total: int, refresh_n: int) -> bool:
    if refresh_n <= 0:
        return False
    return period_idx >= total - refresh_n


def main() -> None:
    args = parse_args()

    raw_root = PROJECT_ROOT / "data/raw/esios/liquidaciones"
    manifest_csv = PROJECT_ROOT / "data/metadata/download_manifest.csv"
    ensure_dir(raw_root)

    chunks = list(month_chunks(args.start_month, args.end_month))
    total = len(chunks)

    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    print(
        f"Syncing ESIOS A2_liquicomun from {args.start_month} to "
        f"{args.end_month}\n"
        f"Archive ID:    {ARCHIVE_ID}\n"
        f"Raw root:      {raw_root}\n"
        f"Manifest:      {manifest_csv}\n"
        f"Refresh recent months: {args.refresh_recent_months}\n"
    )

    totals = {"downloaded": 0, "skipped": 0, "empty": 0}

    for i, (yyyymm, ps_iso, pe_iso) in enumerate(chunks):
        month_dir = raw_root / yyyymm
        zip_name = f"A2_liquicomun_{yyyymm}.zip"
        zip_path = month_dir / zip_name
        extracted_dir = month_dir / "extracted"
        ensure_dir(month_dir)

        is_refresh = should_refresh(i, total, args.refresh_recent_months)
        if zip_path.exists() and not args.overwrite and not is_refresh:
            print(f"[SKIP]    {zip_name}")
            totals["skipped"] += 1
            continue
        if zip_path.exists() and is_refresh and not args.overwrite:
            print(f"[REFRESH] {zip_name}")

        body, status = fetch_archive(
            session=session,
            archive_id=ARCHIVE_ID,
            start_iso=ps_iso,
            end_iso=pe_iso,
            timeout=args.timeout,
            max_retries=args.max_retries,
        )

        tmp = zip_path.with_suffix(zip_path.suffix + ".part")
        tmp.write_bytes(body)
        tmp.replace(zip_path)

        if not args.no_extract and status == "ok":
            ensure_dir(extracted_dir)
            extracted_paths = extract_zip(body, extracted_dir)
            n_inner = len(extracted_paths)
        else:
            n_inner = 0

        append_csv_row(
            manifest_csv,
            {
                "downloaded_at": utc_now_iso(),
                "source_url": (
                    f"esios:archive_{ARCHIVE_ID}"
                    f"?start_date={ps_iso}&end_date={pe_iso}"
                ),
                "market": MARKET,
                "category": CATEGORY,
                "file_family": FILE_FAMILY,
                "filename": zip_name,
                "size_bytes": zip_path.stat().st_size,
                "sha256": sha256_file(zip_path),
                "is_zip": True,
                "file_date": f"{yyyymm[:4]}-{yyyymm[4:]}-01",
                "version_suffix": "",
                "notes": (
                    f"esios_public_archive;status={status};"
                    f"n_inner={n_inner}"
                ),
            },
        )

        if status == "empty":
            totals["empty"] += 1
            print(f"[EMPTY]   {zip_name} (no payload)")
        else:
            totals["downloaded"] += 1
            print(
                f"[OK]      {zip_name} "
                f"({zip_path.stat().st_size/1e6:.2f} MB, {n_inner} inner files)"
            )

    print("\nDone.")
    print(
        f"Downloaded={totals['downloaded']}, "
        f"skipped={totals['skipped']}, empty={totals['empty']}"
    )


if __name__ == "__main__":
    main()
