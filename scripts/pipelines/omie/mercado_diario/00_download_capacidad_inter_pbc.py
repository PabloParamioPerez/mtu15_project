from __future__ import annotations

import argparse
import csv
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[4]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from mtu.parsing.omie_common import append_csv_row, ensure_dir, sha256_file, utc_now_iso  # noqa: E402

BASE_URL = "https://www.omie.es/es/file-download"
PARENTS_VALUE = "capacidad_inter_pbc"
FILE_FAMILY = "capacidad_inter_pbc"
MARKET = "mercado_diario"
CATEGORY = "capacidades"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Download OMIE CAPACIDAD_INTER_PBC files (idempotent). "
            "capacidad_inter_pbc_YYYYMMDD.v files contain interconnection capacity "
            "and occupation after day-ahead clearing, per period and border (§5.1.6.1). "
            "No confidentiality window."
        )
    )
    p.add_argument("--start-date", type=str, help="YYYY-MM-DD")
    p.add_argument("--end-date", type=str, help="YYYY-MM-DD")
    p.add_argument("--recent-days", type=int, help="Download/recheck last N days (inclusive of today)")
    p.add_argument("--max-version", type=int, default=3, help="Try version suffixes 1..N")
    p.add_argument("--timeout", type=int, default=30)
    return p.parse_args()


def daterange(start: date, end: date):
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


def resolve_date_range(args: argparse.Namespace) -> tuple[date, date]:
    if args.recent_days is not None:
        if args.recent_days <= 0:
            raise ValueError("--recent-days must be positive")
        end = date.today()
        start = end - timedelta(days=args.recent_days - 1)
        return start, end
    if not args.start_date or not args.end_date:
        raise ValueError("Provide either --recent-days OR both --start-date and --end-date")
    start = datetime.strptime(args.start_date, "%Y-%m-%d").date()
    end = datetime.strptime(args.end_date, "%Y-%m-%d").date()
    if end < start:
        raise ValueError("--end-date cannot be before --start-date")
    return start, end


def filename_for(d: date, version: int) -> str:
    return f"{FILE_FAMILY}_{d.strftime('%Y%m%d')}.{version}"


def build_url(filename: str) -> str:
    return f"{BASE_URL}?parents={PARENTS_VALUE}&filename={filename}"


def load_manifest_filenames(manifest_csv: Path) -> set[str]:
    if not manifest_csv.exists():
        return set()
    with manifest_csv.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return {row["filename"] for row in reader if row.get("filename")}


def looks_like_html(content: bytes) -> bool:
    head = content[:300].lower()
    return b"<html" in head or b"<!doctype html" in head


def append_manifest_row(manifest_csv: Path, local_path: Path, url: str, d: date, version: int) -> None:
    row = {
        "downloaded_at": utc_now_iso(),
        "source_url": url,
        "market": MARKET,
        "category": CATEGORY,
        "file_family": FILE_FAMILY,
        "filename": local_path.name,
        "size_bytes": local_path.stat().st_size,
        "sha256": sha256_file(local_path),
        "is_zip": False,
        "file_date": d.isoformat(),
        "version_suffix": str(version),
        "notes": "auto_download",
    }
    append_csv_row(manifest_csv, row)


def main() -> None:
    args = parse_args()
    start, end = resolve_date_range(args)

    raw_dir = PROJECT_ROOT / "data/raw/omie/mercado_diario/capacidades/capacidad_inter_pbc"
    manifest_csv = PROJECT_ROOT / "data/metadata/download_manifest.csv"
    ensure_dir(raw_dir)

    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0 (compatible; mtu15-thesis-downloader/1.0)"})

    downloaded = 0
    already_present = 0
    not_found = 0
    errors = 0

    print(f"Downloading {FILE_FAMILY} from {start} to {end} (max-version={args.max_version})")
    print(f"Raw dir: {raw_dir}")
    print()

    for d in daterange(start, end):
        any_found_for_day = False

        for version in range(1, args.max_version + 1):
            fname = filename_for(d, version)
            out_path = raw_dir / fname
            url = build_url(fname)

            if out_path.exists():
                print(f"[SKIP exists] {fname}")
                already_present += 1
                any_found_for_day = True
                continue

            try:
                r = session.get(url, timeout=args.timeout)
            except requests.RequestException as e:
                print(f"[ERROR] {fname} -> request error: {e}")
                errors += 1
                continue

            if r.status_code == 404:
                continue

            if r.status_code != 200:
                print(f"[WARN]  {fname} -> HTTP {r.status_code}")
                continue

            content = r.content
            if not content or looks_like_html(content):
                continue

            out_path.write_bytes(content)
            append_manifest_row(manifest_csv, out_path, url, d, version)
            print(f"[DOWNLOADED] {fname} ({out_path.stat().st_size} bytes)")
            downloaded += 1
            any_found_for_day = True

        if not any_found_for_day:
            print(f"[NO FILES] {d.isoformat()} (no versions 1..{args.max_version} found)")
            not_found += 1

    print()
    print(
        f"Done. downloaded={downloaded}, already_present={already_present}, "
        f"days_without_files={not_found}, errors={errors}"
    )


if __name__ == "__main__":
    main()
