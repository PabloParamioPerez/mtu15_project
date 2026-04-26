"""Sync ESIOS aFRR settlement detail (liquicierre + liquicierresrs).

ESIOS publishes aFRR (secondary regulation) settlement detail in two
archives:

  liquicierre     id=17    2015-01-01 → 2024-12-03   (legacy format)
  liquicierresrs  id=203   2024-11-22 → present      (post-ISP15 format)

These are paired; together they form a continuous 2015–now panel of the
"closure" settlement of secondary-reserve activations. We fetch both and
let the parser handle format differences downstream.

Output:
    data/raw/esios/reservas/liquicierre/<yyyymm>/liquicierre_<yyyymm>.zip
    data/raw/esios/reservas/liquicierresrs/<yyyymm>/liquicierresrs_<yyyymm>.zip

Usage:
    uv run python scripts/pipelines/esios/liquidaciones/00_sync_liquicierre.py \
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

MARKET = "esios_reservas"
CATEGORY = "regulacion_secundaria"

ARCHIVE_CONFIGS = [
    # (archive_key, file_family, sub_dir, start_default, end_default)
    ("liquicierre",     "liquicierre",     "liquicierre",     "2015-01", "2024-12"),
    ("liquicierresrs",  "liquicierresrs",  "liquicierresrs",  "2024-11", None),
]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Download ESIOS liquicierre + liquicierresrs aFRR settlement "
            "detail. Public archive endpoint, no authentication. Both "
            "archives are fetched per month."
        )
    )
    p.add_argument("--start-month", required=True, help="YYYY-MM (oldest)")
    p.add_argument("--end-month", required=True, help="YYYY-MM (newest)")
    p.add_argument("--timeout", type=int, default=600)
    p.add_argument("--max-retries", type=int, default=4)
    p.add_argument("--overwrite", action="store_true")
    p.add_argument("--refresh-recent-months", type=int, default=2)
    p.add_argument(
        "--only", choices=["liquicierre", "liquicierresrs", "both"], default="both",
        help="Restrict to one archive (default: both).",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()

    raw_root = PROJECT_ROOT / "data/raw/esios/reservas"
    manifest_csv = PROJECT_ROOT / "data/metadata/download_manifest.csv"

    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    grand_totals = {"downloaded": 0, "skipped": 0, "empty": 0}

    for archive_key, file_family, sub_dir, def_start, def_end in ARCHIVE_CONFIGS:
        if args.only != "both" and args.only != archive_key:
            continue

        # Clamp the requested window to the archive's known coverage.
        s_user = args.start_month
        e_user = args.end_month
        s_eff = max(s_user, def_start)
        e_eff = min(e_user, def_end) if def_end else e_user
        if s_eff > e_eff:
            print(f"[SKIP archive] {archive_key}: requested window outside coverage")
            continue

        archive_id = ARCHIVES[archive_key]
        sub_root = raw_root / sub_dir
        chunks = month_chunks(s_eff, e_eff)

        print(f"\n=== Sync {archive_key} (id={archive_id}) {s_eff} → {e_eff} ===")
        totals = sync_archive_loop(
            archive_id=archive_id,
            chunks=chunks,
            raw_root=sub_root,
            file_stem=file_family,
            market=MARKET,
            category=CATEGORY,
            file_family=file_family,
            manifest_csv=manifest_csv,
            session=session,
            timeout=args.timeout,
            max_retries=args.max_retries,
            overwrite=args.overwrite,
            extract=True,
            refresh_recent=args.refresh_recent_months,
        )
        for k, v in totals.items():
            grand_totals[k] += v

    print(
        f"\nGrand totals: downloaded={grand_totals['downloaded']}, "
        f"skipped={grand_totals['skipped']}, empty={grand_totals['empty']}"
    )


if __name__ == "__main__":
    main()
