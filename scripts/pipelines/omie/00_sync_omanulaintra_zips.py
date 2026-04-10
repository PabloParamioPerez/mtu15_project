"""
Download and extract OMIE OMANULAINTRA ZIP archives (idempotent).

Strategy per year:
  1. Try annual ZIP  omanulaintra_{year}.zip — present for 2018-2022.
  2. If not found (HTTP 404), fall back to monthly ZIPs omanulaintra_{year}{MM:02d}.zip.
"""
from __future__ import annotations

import argparse
import shutil
import sys
import zipfile
from pathlib import Path

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from mtu.parsing.omie_common import append_csv_row, ensure_dir, sha256_file, utc_now_iso  # noqa: E402

BASE_URL = "https://www.omie.es/es/file-download"
PARENTS_VALUE = "omanulaintra"

MARKET = "mercado_intradiario_subastas"
CATEGORY = "anulaciones"
FILE_FAMILY = "omanulaintra"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Download and extract OMIE OMANULAINTRA ZIP archives (idempotent)"
    )
    p.add_argument("--start-year", type=int, required=True, help="e.g. 2018")
    p.add_argument("--end-year", type=int, required=True, help="e.g. 2026")
    p.add_argument("--timeout", type=int, default=120)
    p.add_argument("--overwrite-zip", action="store_true", help="Redownload ZIP even if it already exists")
    p.add_argument(
        "--overwrite-extracted",
        action="store_true",
        help="Overwrite extracted raw files if they already exist",
    )
    return p.parse_args()


def years_range(start_year: int, end_year: int):
    if end_year < start_year:
        raise ValueError("--end-year cannot be before --start-year")
    for y in range(start_year, end_year + 1):
        yield y


def build_url(filename: str) -> str:
    return f"{BASE_URL}?parents={PARENTS_VALUE}&filename={filename}"


def looks_like_html(content: bytes) -> bool:
    head = content[:400].lower()
    return b"<html" in head or b"<!doctype html" in head


def append_manifest_row_for_zip(
    manifest_csv: Path, local_path: Path, url: str, notes: str
) -> None:
    row = {
        "downloaded_at": utc_now_iso(),
        "source_url": url,
        "market": MARKET,
        "category": CATEGORY,
        "file_family": FILE_FAMILY,
        "filename": local_path.name,
        "size_bytes": local_path.stat().st_size,
        "sha256": sha256_file(local_path),
        "is_zip": True,
        "file_date": "",
        "version_suffix": "",
        "notes": notes,
    }
    append_csv_row(manifest_csv, row)


def _download_zip(
    fname: str,
    archives_dir: Path,
    manifest_csv: Path,
    session: requests.Session,
    timeout: int,
    overwrite_zip: bool,
    notes: str,
) -> tuple[str, Path | None]:
    url = build_url(fname)
    out_path = archives_dir / fname

    if out_path.exists() and not overwrite_zip:
        print(f"[SKIP ZIP exists] {fname}")
        return "skip_exists", out_path

    try:
        r = session.get(url, timeout=timeout)
    except requests.RequestException as e:
        print(f"[ERROR ZIP] {fname} -> request error: {e}")
        return "error", None

    if r.status_code == 404:
        return "not_found", None

    if r.status_code != 200:
        print(f"[WARN ZIP] {fname} -> HTTP {r.status_code}")
        return "error", None

    content = r.content
    if not content or looks_like_html(content):
        return "not_found", None

    out_path.write_bytes(content)

    try:
        with zipfile.ZipFile(out_path, "r") as zf:
            bad = zf.testzip()
            if bad is not None:
                raise zipfile.BadZipFile(f"Corrupt member: {bad}")
    except Exception as e:
        out_path.unlink(missing_ok=True)
        print(f"[ERROR ZIP] {fname} -> invalid zip: {e}")
        return "error", None

    append_manifest_row_for_zip(manifest_csv, out_path, url, notes)
    print(f"[DOWNLOADED ZIP] {fname} ({out_path.stat().st_size:,} bytes)")
    return "downloaded", out_path


def extract_zip_idempotent(
    zip_path: Path,
    raw_dir: Path,
    overwrite_extracted: bool,
) -> dict[str, int]:
    counts = {
        "members_total": 0,
        "members_data": 0,
        "dirs_skipped": 0,
        "nonfamily_skipped": 0,
        "hidden_skipped": 0,
        "extracted": 0,
        "exists_skipped": 0,
        "overwritten": 0,
        "errors": 0,
    }

    with zipfile.ZipFile(zip_path, "r") as zf:
        for info in zf.infolist():
            counts["members_total"] += 1

            if info.is_dir():
                counts["dirs_skipped"] += 1
                continue

            name = Path(info.filename).name
            if not name:
                counts["dirs_skipped"] += 1
                continue
            if name.startswith("."):
                counts["hidden_skipped"] += 1
                continue
            if not name.startswith(f"{FILE_FAMILY}_"):
                counts["nonfamily_skipped"] += 1
                continue

            counts["members_data"] += 1
            out_path = raw_dir / name

            if out_path.exists() and not overwrite_extracted:
                counts["exists_skipped"] += 1
                continue

            existed_before = out_path.exists()
            try:
                ensure_dir(out_path.parent)
                with zf.open(info, "r") as src, out_path.open("wb") as dst:
                    shutil.copyfileobj(src, dst)

                if existed_before and overwrite_extracted:
                    counts["overwritten"] += 1
                else:
                    counts["extracted"] += 1
            except Exception as e:
                counts["errors"] += 1
                print(f"[ERROR EXTRACT] {zip_path.name} :: {info.filename} -> {e}")

    return counts


def main() -> None:
    args = parse_args()

    raw_dir = PROJECT_ROOT / "data/raw/omie/mercado_intradiario_subastas/anulaciones/omanulaintra"
    archives_dir = raw_dir / "archives"
    manifest_csv = PROJECT_ROOT / "data/metadata/download_manifest.csv"

    ensure_dir(raw_dir)
    ensure_dir(archives_dir)

    http = requests.Session()
    http.headers.update({"User-Agent": "Mozilla/5.0 (compatible; mtu15-thesis-zip-sync/1.0)"})

    print(
        f"Syncing {FILE_FAMILY} ZIP archives from {args.start_year} to {args.end_year}\n"
        f"Raw dir:      {raw_dir}\n"
        f"Archives dir: {archives_dir}\n"
    )

    zips_downloaded = 0
    zips_present = 0
    zips_missing = 0
    zip_errors = 0
    extracted_total = 0
    skipped_existing_total = 0

    for year in years_range(args.start_year, args.end_year):
        # --- Try annual ZIP first ---
        fname_annual = f"{FILE_FAMILY}_{year}.zip"
        status, zip_path = _download_zip(
            fname_annual, archives_dir, manifest_csv, http, args.timeout,
            args.overwrite_zip, notes=f"auto_download_zip_archive;year={year}",
        )

        if status in ("downloaded", "skip_exists"):
            if status == "downloaded":
                zips_downloaded += 1
            else:
                zips_present += 1
            counts = extract_zip_idempotent(zip_path, raw_dir, args.overwrite_extracted)
            extracted_total += counts["extracted"] + counts["overwritten"]
            skipped_existing_total += counts["exists_skipped"]
            print(
                f"[EXTRACTED {zip_path.name}] "
                f"new={counts['extracted']}, overwritten={counts['overwritten']}, "
                f"skipped={counts['exists_skipped']}, errors={counts['errors']}"
            )
            continue

        # --- Fall back to monthly ZIPs ---
        if status == "not_found":
            print(f"[NO ANNUAL ZIP] {fname_annual} — trying monthly ZIPs for {year}")
            found_any_month = False
            for month in range(1, 13):
                fname_monthly = f"{FILE_FAMILY}_{year}{month:02d}.zip"
                m_status, m_zip_path = _download_zip(
                    fname_monthly, archives_dir, manifest_csv, http, args.timeout,
                    args.overwrite_zip,
                    notes=f"auto_download_zip_archive;year={year};month={month:02d}",
                )
                if m_status == "downloaded":
                    zips_downloaded += 1
                    found_any_month = True
                elif m_status == "skip_exists":
                    zips_present += 1
                    found_any_month = True
                elif m_status == "error":
                    zip_errors += 1
                    continue
                else:  # not_found
                    continue

                counts = extract_zip_idempotent(m_zip_path, raw_dir, args.overwrite_extracted)
                extracted_total += counts["extracted"] + counts["overwritten"]
                skipped_existing_total += counts["exists_skipped"]
                print(
                    f"[EXTRACTED {m_zip_path.name}] "
                    f"new={counts['extracted']}, overwritten={counts['overwritten']}, "
                    f"skipped={counts['exists_skipped']}, errors={counts['errors']}"
                )

            if not found_any_month:
                print(f"[NO ZIPS] No annual or monthly ZIPs found for {year}")
                zips_missing += 1
        else:
            zip_errors += 1

    print(
        "\nDone. "
        f"zips_downloaded={zips_downloaded}, "
        f"zips_present={zips_present}, "
        f"zips_missing={zips_missing}, "
        f"zip_errors={zip_errors}, "
        f"files_extracted_or_overwritten={extracted_total}, "
        f"files_skipped_existing={skipped_existing_total}"
    )


if __name__ == "__main__":
    main()
