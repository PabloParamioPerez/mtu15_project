"""Sync ESIOS technical-restriction (RZ) closure prices.

Archive id=28 (totalrp48preccierre): the closure-version per-MTU price of
RZ (technical restriction) redispatch in the Spanish day-ahead programme.
This is the price at which RZ-zone units are re-instructed by REE for
network-security reasons.

Coverage 2015-01 → present, monthly ZIP packaging.

Note: id=27 (totalrp48prec, the live version) returns an empty ZIP for
all date ranges as of 2026-04-27 — deprecated. The closure version (id=28)
is what we want for stable historical analysis anyway.

Output:
    data/raw/esios/restricciones/<yyyymm>/totalrp48preccierre_<yyyymm>.zip

Usage:
    uv run python scripts/pipelines/esios/restricciones/00_sync_totalrp48preccierre.py \
        --start-month 2015-01 --end-month 2026-04
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
    month_chunks,
    sync_archive_loop,
)

MARKET = "esios_restricciones"
CATEGORY = "restricciones_tecnicas"
FILE_FAMILY = "totalrp48preccierre"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Download ESIOS totalrp48preccierre RZ technical-restriction "
            "closure prices. Public endpoint, no authentication."
        )
    )
    p.add_argument("--start-month", required=True, help="YYYY-MM")
    p.add_argument("--end-month", required=True, help="YYYY-MM")
    p.add_argument("--timeout", type=int, default=600)
    p.add_argument("--max-retries", type=int, default=4)
    p.add_argument("--overwrite", action="store_true")
    p.add_argument("--refresh-recent-months", type=int, default=2)
    return p.parse_args()


def main() -> None:
    args = parse_args()

    raw_root = PROJECT_ROOT / "data/raw/esios/restricciones"
    manifest_csv = PROJECT_ROOT / "data/metadata/download_manifest.csv"

    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    chunks = month_chunks(args.start_month, args.end_month)

    print(
        f"Syncing {FILE_FAMILY} (id={ARCHIVES['totalrp48preccierre']}) "
        f"{args.start_month} → {args.end_month}"
    )
    totals = sync_archive_loop(
        archive_id=ARCHIVES["totalrp48preccierre"],
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
        extract=True,
        refresh_recent=args.refresh_recent_months,
    )

    print(
        f"\nDone. downloaded={totals['downloaded']}, "
        f"skipped={totals['skipped']}, empty={totals['empty']}"
    )


if __name__ == "__main__":
    main()
