"""Sync ESIOS REE_BalancingEnerBids — bid-level mFRR (tertiary regulation) data.

Archive id=181: per-bid offers submitted to the Spanish mFRR market.
Coverage 2022-05-24 → 2024-12-10. Note: monthly chunks exceed the CDN
timeout (504 Gateway), so this script uses DAILY chunks.

Per-day payloads are ~150 KB ZIPs containing CSV bid records. The full
panel (~940 days) totals ~150 MB.

This is the cleanest bid-level data for tertiary regulation in the
Spanish system; key for testing whether the IB-canonical conduct
finding (F7/F8 in DA) extends to mFRR — a market that is NOT directly
affected by the MTU15-IDA reform, providing a cross-market robustness
check on structural conduct.

Output:
    data/raw/esios/reservas/balancing_bids/<yyyymmdd>/balancing_bids_<yyyymmdd>.zip

Usage:
    uv run python scripts/pipelines/esios/reservas/00_sync_balancing_bids.py \
        --start-date 2022-05-24 --end-date 2024-12-10
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
CATEGORY = "regulacion_terciaria"
FILE_FAMILY = "ree_balancing_bids"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Download ESIOS REE_BalancingEnerBids per-bid mFRR offers. "
            "Daily chunks (monthly exceeds CDN timeout). Public endpoint."
        )
    )
    p.add_argument(
        "--start-date", default="2022-05-24",
        help="YYYY-MM-DD (default: archive start)",
    )
    p.add_argument(
        "--end-date", default="2024-12-10",
        help="YYYY-MM-DD (default: archive end)",
    )
    p.add_argument("--timeout", type=int, default=120)
    p.add_argument("--max-retries", type=int, default=4)
    p.add_argument("--overwrite", action="store_true")
    return p.parse_args()


def main() -> None:
    args = parse_args()

    raw_root = PROJECT_ROOT / "data/raw/esios/reservas/balancing_bids"
    manifest_csv = PROJECT_ROOT / "data/metadata/download_manifest.csv"

    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    chunks = day_chunks(args.start_date, args.end_date)

    print(
        f"Syncing {FILE_FAMILY} (id={ARCHIVES['ree_balancing_bids']}) "
        f"{args.start_date} → {args.end_date} (daily)"
    )
    totals = sync_archive_loop(
        archive_id=ARCHIVES["ree_balancing_bids"],
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
        refresh_recent=0,
    )

    print(
        f"\nDone. downloaded={totals['downloaded']}, "
        f"skipped={totals['skipped']}, empty={totals['empty']}"
    )


if __name__ == "__main__":
    main()
