from __future__ import annotations

import argparse
import shutil
import sys
import zipfile
from pathlib import Path

import pandas as pd
import requests

PROJECT_ROOT = Path(__file__).resolve().parents[4]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from mtu.parsing.omie_common import append_csv_row, ensure_dir, sha256_file, utc_now_iso  # noqa: E402

BASE_URL = "https://www.omie.es/es/file-download"
PARENTS_VALUE = "idet"

MARKET = "mercado_intradiario_subastas"
CATEGORY = "ofertas"
FILE_FAMILY = "idet"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Download and extract OMIE IDET monthly ZIP archives (idempotent with recent-month refresh)"
    )
    p.add_argument("--start-month", required=True, help="YYYY-MM")
    p.add_argument("--end-month", required=True, help="YYYY-MM")
    p.add_argument("--timeout", type=int, default=60)
    p.add_argument(
        "--overwrite-zip",
        action="store_true",
        help="Redownload ZIP even if it already exists",
    )
    p.add_argument(
        "--overwrite-extracted",
        action="store_true",
        help="Overwrite extracted raw files if they already exist",
    )
    p.add_argument(
        "--refresh-recent-months",
        type=int,
        default=2,
        help=(
            "Always redownload the most recent N requested months, because OMIE monthly ZIPs "
            "may gain new daily files over time. Default: 2"
        ),
    )
    return p.parse_args()


def months_range(start_ym: str, end_ym: str):
    start = pd.Period(start_ym, freq="M")
    end = pd.Period(end_ym, freq="M")
    if end < start:
        raise ValueError("--end-month cannot be before --start-month")
    cur = start
    while cur <= end:
        yield cur
        cur += 1


def zip_filename_for_month(period: pd.Period) -> str:
    return f"{FILE_FAMILY}_{period.strftime('%Y%m')}.zip"


def build_url(filename: str) -> str:
    return f"{BASE_URL}?parents={PARENTS_VALUE}&filename={filename}"


def looks_like_html(content: bytes) -> bool:
    head = content[:400].lower()
    return b"<html" in head or b"<!doctype html" in head


def append_manifest_row_for_zip(manifest_csv: Path, local_path: Path, url: str, ym: str) -> None:
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
        "notes": f"auto_download_zip_archive;month={ym}",
    }
    append_csv_row(manifest_csv, row)


def should_refresh_month(
    period: pd.Period,
    requested_months: list[pd.Period],
    refresh_recent_months: int,
) -> bool:
    if refresh_recent_months <= 0:
        return False

    requested_months_sorted = sorted(requested_months)
    recent = requested_months_sorted[-refresh_recent_months:]
    return period in set(recent)


def download_zip(
    *,
    period: pd.Period,
    requested_months: list[pd.Period],
    archives_dir: Path,
    manifest_csv: Path,
    session: requests.Session,
    timeout: int,
    overwrite_zip: bool,
    refresh_recent_months: int,
) -> tuple[str, Path | None]:
    fname = zip_filename_for_month(period)
    url = build_url(fname)
    out_path = archives_dir / fname

    refresh_this_month = should_refresh_month(
        period=period,
        requested_months=requested_months,
        refresh_recent_months=refresh_recent_months,
    )

    if out_path.exists() and not overwrite_zip and not refresh_this_month:
        print(f"[SKIP ZIP exists] {fname}")
        return "skip_exists", out_path

    if out_path.exists() and refresh_this_month and not overwrite_zip:
        print(f"[REFRESH ZIP recent month] {fname}")

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

    append_manifest_row_for_zip(manifest_csv, out_path, url, period.strftime("%Y-%m"))
    print(f"[DOWNLOADED ZIP] {fname} ({out_path.stat().st_size} bytes)")
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
            # Files inside the zip are uppercase (IDET_YYYYMMDD.S)
            if not name.upper().startswith(f"{FILE_FAMILY.upper()}_"):
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

    raw_dir = PROJECT_ROOT / "data/raw/omie/mercado_intradiario_subastas/ofertas/idet"
    archives_dir = raw_dir / "archives"
    manifest_csv = PROJECT_ROOT / "data/metadata/download_manifest.csv"

    ensure_dir(raw_dir)
    ensure_dir(archives_dir)

    requested_months = list(months_range(args.start_month, args.end_month))

    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0 (compatible; mtu15-thesis-zip-sync/1.0)"})

    print(
        f"Syncing {FILE_FAMILY} ZIP archives from {args.start_month} to {args.end_month}\n"
        f"Raw dir:      {raw_dir}\n"
        f"Archives dir: {archives_dir}\n"
        f"Manifest:     {manifest_csv}\n"
        f"Refresh recent months: {args.refresh_recent_months}\n"
    )

    totals = {
        "downloaded": 0,
        "skip_exists": 0,
        "not_found": 0,
        "error": 0,
        "members_total": 0,
        "members_data": 0,
        "dirs_skipped": 0,
        "nonfamily_skipped": 0,
        "hidden_skipped": 0,
        "extracted": 0,
        "exists_skipped": 0,
        "overwritten": 0,
        "extract_errors": 0,
    }

    for period in requested_months:
        status, zip_path = download_zip(
            period=period,
            requested_months=requested_months,
            archives_dir=archives_dir,
            manifest_csv=manifest_csv,
            session=session,
            timeout=args.timeout,
            overwrite_zip=args.overwrite_zip,
            refresh_recent_months=args.refresh_recent_months,
        )
        totals[status] += 1

        if zip_path is None:
            continue

        counts = extract_zip_idempotent(
            zip_path=zip_path,
            raw_dir=raw_dir,
            overwrite_extracted=args.overwrite_extracted,
        )

        totals["members_total"] += counts["members_total"]
        totals["members_data"] += counts["members_data"]
        totals["dirs_skipped"] += counts["dirs_skipped"]
        totals["nonfamily_skipped"] += counts["nonfamily_skipped"]
        totals["hidden_skipped"] += counts["hidden_skipped"]
        totals["extracted"] += counts["extracted"]
        totals["exists_skipped"] += counts["exists_skipped"]
        totals["overwritten"] += counts["overwritten"]
        totals["extract_errors"] += counts["errors"]

        print(
            f"[EXTRACTED] {zip_path.name}: "
            f"members_data={counts['members_data']}, "
            f"extracted={counts['extracted']}, "
            f"exists_skipped={counts['exists_skipped']}, "
            f"overwritten={counts['overwritten']}, "
            f"errors={counts['errors']}"
        )

    print("\nDone.")
    print(
        f"ZIPs: downloaded={totals['downloaded']}, "
        f"skipped={totals['skip_exists']}, "
        f"not_found={totals['not_found']}, "
        f"errors={totals['error']}"
    )
    print(
        f"Members: total={totals['members_total']}, "
        f"data={totals['members_data']}, "
        f"extracted={totals['extracted']}, "
        f"exists_skipped={totals['exists_skipped']}, "
        f"overwritten={totals['overwritten']}, "
        f"errors={totals['extract_errors']}"
    )


if __name__ == "__main__":
    main()
