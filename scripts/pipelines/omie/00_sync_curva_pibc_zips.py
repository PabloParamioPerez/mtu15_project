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
PARENTS_VALUE = "curva_pibc"

MARKET = "mercado_intradiario_subastas"
CATEGORY = "curvas"
FILE_FAMILY = "curva_pibc"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Download and extract OMIE CURVA_PIBC annual ZIP archives (idempotent)"
    )
    p.add_argument("--start-year", type=int, required=True, help="e.g. 2018")
    p.add_argument("--end-year", type=int, required=True, help="e.g. 2022")
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


def zip_filename_for_year(year: int) -> str:
    return f"{FILE_FAMILY}_{year}.zip"


def build_url(filename: str) -> str:
    return f"{BASE_URL}?parents={PARENTS_VALUE}&filename={filename}"


def looks_like_html(content: bytes) -> bool:
    head = content[:400].lower()
    return b"<html" in head or b"<!doctype html" in head


def append_manifest_row_for_zip(manifest_csv: Path, local_path: Path, url: str, year: int) -> None:
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
        "notes": f"auto_download_zip_archive;year={year}",
    }
    append_csv_row(manifest_csv, row)


def download_zip(
    *,
    year: int,
    archives_dir: Path,
    manifest_csv: Path,
    session: requests.Session,
    timeout: int,
    overwrite_zip: bool,
) -> tuple[str, Path | None]:
    fname = zip_filename_for_year(year)
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
        print(f"[NO ZIP] {fname} (HTTP 404)")
        return "not_found", None

    if r.status_code != 200:
        print(f"[WARN ZIP] {fname} -> HTTP {r.status_code}")
        return "error", None

    content = r.content
    if not content:
        print(f"[WARN ZIP] {fname} -> empty response")
        return "error", None

    if looks_like_html(content):
        print(f"[WARN ZIP] {fname} -> HTML response, skipping")
        return "error", None

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

    append_manifest_row_for_zip(manifest_csv, out_path, url, year)
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

    raw_dir = PROJECT_ROOT / "data/raw/omie/mercado_intradiario_subastas/curvas/curva_pibc"
    archives_dir = raw_dir / "archives"
    manifest_csv = PROJECT_ROOT / "data/metadata/download_manifest.csv"

    ensure_dir(raw_dir)
    ensure_dir(archives_dir)

    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0 (compatible; mtu15-thesis-zip-sync/1.0)"})

    print(
        f"Syncing {FILE_FAMILY} ZIP archives from {args.start_year} to {args.end_year}\n"
        f"Raw dir:      {raw_dir}\n"
        f"Archives dir: {archives_dir}\n"
        f"Manifest:     {manifest_csv}\n"
    )

    zips_downloaded = 0
    zips_present = 0
    zips_missing = 0
    zip_errors = 0
    extracted_total = 0
    skipped_existing_total = 0

    for year in years_range(args.start_year, args.end_year):
        status, zip_path = download_zip(
            year=year,
            archives_dir=archives_dir,
            manifest_csv=manifest_csv,
            session=session,
            timeout=args.timeout,
            overwrite_zip=args.overwrite_zip,
        )

        if status == "downloaded":
            zips_downloaded += 1
        elif status == "skip_exists":
            zips_present += 1
        elif status == "not_found":
            zips_missing += 1
            continue
        else:
            zip_errors += 1
            continue

        assert zip_path is not None
        counts = extract_zip_idempotent(
            zip_path=zip_path,
            raw_dir=raw_dir,
            overwrite_extracted=args.overwrite_extracted,
        )

        extracted_total += counts["extracted"] + counts["overwritten"]
        skipped_existing_total += counts["exists_skipped"]

        print(
            f"[EXTRACTED {zip_path.name}] "
            f"members_data={counts['members_data']}, "
            f"new={counts['extracted']}, overwritten={counts['overwritten']}, "
            f"skipped_existing={counts['exists_skipped']}, errors={counts['errors']}"
        )

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
