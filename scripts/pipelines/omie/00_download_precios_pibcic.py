from __future__ import annotations

import argparse
from datetime import date, timedelta
from pathlib import Path
import sys

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from mtu.parsing.omie_common import append_csv_row, ensure_dir, sha256_file, utc_now_iso  # noqa: E402

BASE_URL = "https://www.omie.es/es/file-download"
PARENTS_VALUE = "precios_pibcic"

MARKET = "mercado_intradiario_continuo"
CATEGORY = "precios"
FILE_FAMILY = "precios_pibcic"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Download OMIE PRECIOS_PIBCIC daily files")
    p.add_argument("--start-date", required=True, help="YYYY-MM-DD")
    p.add_argument("--end-date", required=True, help="YYYY-MM-DD")
    p.add_argument("--max-version", type=int, default=3)
    p.add_argument("--timeout", type=int, default=30)
    return p.parse_args()


def dates_range(start: date, end: date):
    if end < start:
        raise ValueError("--end-date cannot be before --start-date")
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=1)


def filename_for(d: date, version: int) -> str:
    return f"{FILE_FAMILY}_{d.strftime('%Y%m%d')}.{version}"


def build_url(filename: str) -> str:
    return f"{BASE_URL}?parents={PARENTS_VALUE}&filename={filename}"


def looks_like_html(content: bytes) -> bool:
    head = content[:400].lower()
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
        "notes": "",
    }
    append_csv_row(manifest_csv, row)


def main() -> None:
    args = parse_args()

    start_date = date.fromisoformat(args.start_date)
    end_date = date.fromisoformat(args.end_date)

    raw_dir = PROJECT_ROOT / "data/raw/omie/mercado_intradiario_continuo/precios/precios_pibcic"
    manifest_csv = PROJECT_ROOT / "data/metadata/download_manifest.csv"
    ensure_dir(raw_dir)

    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0 (compatible; mtu15-thesis/1.0)"})

    downloaded = 0
    already_present = 0
    date_pairs_without_files = 0
    errors = 0

    print(
        f"Downloading PRECIOS_PIBCIC from {start_date} to {end_date} "
        f"(max-version={args.max_version})\n"
        f"Raw dir: {raw_dir}\n"
    )

    for d in dates_range(start_date, end_date):
        found_for_date = False

        for version in range(1, args.max_version + 1):
            fname = filename_for(d, version)
            out_path = raw_dir / fname
            url = build_url(fname)

            if out_path.exists():
                print(f"[SKIP exists] {fname}")
                already_present += 1
                found_for_date = True
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
                print(f"[WARN] {fname} -> HTTP {r.status_code}")
                errors += 1
                continue

            content = r.content
            if not content:
                print(f"[WARN] {fname} -> empty response")
                errors += 1
                continue

            if looks_like_html(content):
                print(f"[WARN] {fname} -> HTML response, skipping")
                errors += 1
                continue

            out_path.write_bytes(content)
            append_manifest_row(manifest_csv, out_path, url, d, version)

            print(f"[DOWNLOADED] {fname} ({out_path.stat().st_size} bytes)")
            downloaded += 1
            found_for_date = True

        if not found_for_date:
            date_pairs_without_files += 1

    print(
        f"\nDone. downloaded={downloaded}, already_present={already_present}, "
        f"date_pairs_without_files={date_pairs_without_files}, errors={errors}"
    )


if __name__ == "__main__":
    main()
