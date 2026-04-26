"""Sync ESIOS Curvas_Ofertas_aFRR — bid-level aFRR (secondary regulation) curves.

Archive id=234: per-day Excel (.xls) workbook of aFRR offer curves for
the Spanish secondary-regulation market. Coverage 2024-11-20 → present.

The payload is an OLE2 compound-document Excel file (~1.7 MB/day),
NOT a ZIP. The sync helper detects the magic bytes and writes .xls
directly without extraction. The 10_parse step downstream uses
pandas + xlrd (or openpyxl for newer files) to read the workbook.

Output:
    data/raw/esios/reservas/curvas_ofertas_afrr/<yyyymmdd>/curvas_ofertas_afrr_<yyyymmdd>.xls

Usage:
    uv run python scripts/pipelines/esios/reservas/00_sync_curvas_ofertas_afrr.py \
        --start-date 2024-11-20 --end-date 2026-04-26
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
    day_chunks,
    sync_archive_loop,
)

MARKET = "esios_reservas"
CATEGORY = "regulacion_secundaria"
FILE_FAMILY = "curvas_ofertas_afrr"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Download ESIOS Curvas_Ofertas_aFRR per-day .xls aFRR offer "
            "curves. Daily chunks. Public endpoint."
        )
    )
    p.add_argument(
        "--start-date", default="2024-11-20",
        help="YYYY-MM-DD (default: archive start)",
    )
    p.add_argument(
        "--end-date", required=True,
        help="YYYY-MM-DD (e.g. today's date)",
    )
    p.add_argument("--timeout", type=int, default=120)
    p.add_argument("--max-retries", type=int, default=4)
    p.add_argument("--overwrite", action="store_true")
    return p.parse_args()


def main() -> None:
    args = parse_args()

    raw_root = PROJECT_ROOT / "data/raw/esios/reservas/curvas_ofertas_afrr"
    manifest_csv = PROJECT_ROOT / "data/metadata/download_manifest.csv"

    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    chunks = day_chunks(args.start_date, args.end_date)

    print(
        f"Syncing {FILE_FAMILY} (id={ARCHIVES['curvas_ofertas_afrr']}) "
        f"{args.start_date} → {args.end_date} (daily, .xls payload)"
    )
    totals = sync_archive_loop(
        archive_id=ARCHIVES["curvas_ofertas_afrr"],
        chunks=chunks,
        raw_root=raw_root,
        file_stem=FILE_FAMILY,
        market=MARKET,
        category=CATEGORY,
        file_family=FILE_FAMILY,
        manifest_csv=manifest_csv,
        session=session,
        timeout=args.timeout,
        max_retries=args.max_retries,
        overwrite=args.overwrite,
        extract=False,                # payload is .xls, not ZIP
        is_zip_hint=False,
        payload_suffix=".xls",
        refresh_recent=0,
    )

    print(
        f"\nDone. downloaded={totals['downloaded']}, "
        f"skipped={totals['skipped']}, empty={totals['empty']}"
    )


if __name__ == "__main__":
    main()
