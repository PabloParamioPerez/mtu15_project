"""Sync ESIOS Indisponibilidades — generation/consumption unit outages.

Archive id=105: per-snapshot Excel (.xls, MS-CFB) workbook listing all
known outages as of the request date. Forward-looking (a 2025-06-15
snapshot may list outages running into 2028).

Snapshot cadence is monthly by default — the snapshot changes slowly,
so monthly resolution is enough for thesis-level analysis. Pass a daily
or weekly cadence for finer history.

Output:
    data/raw/esios/indisponibilidades/<yyyymmdd>/indisponibilidades_<yyyymmdd>.xls

Usage:
    uv run python scripts/pipelines/esios/indisponibilidades/00_sync_indisponibilidades.py \
        --start-date 2018-01-15 --end-date 2026-05-14 --cadence month
"""
from __future__ import annotations

import argparse
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[4]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from mtu.ingestion.esios_common import (  # noqa: E402
    ARCHIVES,
    USER_AGENT,
    fetch_archive,
)

RAW_ROOT = PROJECT_ROOT / "data" / "raw" / "esios" / "indisponibilidades"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--start-date", required=True, help="YYYY-MM-DD (inclusive)")
    p.add_argument("--end-date", required=True, help="YYYY-MM-DD (inclusive)")
    p.add_argument("--cadence", choices=("day", "week", "month"), default="month",
                   help="snapshot interval (default month)")
    p.add_argument("--sleep", type=float, default=1.0,
                   help="seconds between API calls (default 1.0)")
    p.add_argument("--timeout", type=int, default=120)
    p.add_argument("--max-retries", type=int, default=3)
    p.add_argument("--overwrite", action="store_true")
    return p.parse_args()


def get_token() -> str:
    import os
    tok = os.environ.get("ESIOS_TOKEN", "").strip()
    if tok:
        return tok
    env_path = PROJECT_ROOT / ".env"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            if line.startswith("ESIOS_TOKEN="):
                return line.split("=", 1)[1].strip()
    raise RuntimeError("ESIOS_TOKEN not set (env or .env)")


def date_range(start: str, end: str, cadence: str) -> list[date]:
    d0 = datetime.fromisoformat(start).date()
    d1 = datetime.fromisoformat(end).date()
    step = {"day": 1, "week": 7, "month": 30}[cadence]
    out: list[date] = []
    d = d0
    while d <= d1:
        out.append(d)
        d = d + timedelta(days=step)
    return out


def main() -> None:
    args = parse_args()
    RAW_ROOT.mkdir(parents=True, exist_ok=True)
    archive_id = ARCHIVES["indisponibilidades"]
    token = get_token()
    session = requests.Session()
    session.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": "application/vnd.ms-excel, application/octet-stream",
        "x-api-key": token,
    })

    snapshots = date_range(args.start_date, args.end_date, args.cadence)
    print(f"Syncing indisponibilidades (id={archive_id}) "
          f"{args.start_date} → {args.end_date} (cadence={args.cadence}, {len(snapshots)} snapshots)")

    downloaded = skipped = empty = failed = 0
    for i, d in enumerate(snapshots):
        yyyymmdd = d.strftime("%Y%m%d")
        out_dir = RAW_ROOT / yyyymmdd
        out_path = out_dir / f"indisponibilidades_{yyyymmdd}.xls"
        if out_path.exists() and not args.overwrite:
            skipped += 1
            continue
        out_dir.mkdir(parents=True, exist_ok=True)
        start_iso = d.strftime("%Y-%m-%dT00:00:00Z")
        end_iso   = d.strftime("%Y-%m-%dT23:59:59Z")
        try:
            body, status = fetch_archive(
                session=session, archive_id=archive_id,
                start_iso=start_iso, end_iso=end_iso,
                timeout=args.timeout, max_retries=args.max_retries,
            )
        except Exception as e:  # noqa: BLE001
            print(f"  [FAIL]   {yyyymmdd}: {e}")
            failed += 1
            continue
        if status == "empty":
            empty += 1
            print(f"  [EMPTY]  {yyyymmdd}")
        else:
            out_path.write_bytes(body)
            downloaded += 1
            if downloaded <= 5 or downloaded % 20 == 0:
                print(f"  [OK]     {yyyymmdd}  ({len(body)/1024:.1f} KB)")
        if i < len(snapshots) - 1 and args.sleep > 0:
            time.sleep(args.sleep)

    print(f"\nDone. downloaded={downloaded}, skipped={skipped}, empty={empty}, failed={failed}")


if __name__ == "__main__":
    main()
