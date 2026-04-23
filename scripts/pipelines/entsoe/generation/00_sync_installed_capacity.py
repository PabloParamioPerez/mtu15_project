from __future__ import annotations

import argparse
import sys
from pathlib import Path

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[4]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from mtu.ingestion.entsoe_common import (  # noqa: E402
    DOC_TYPE,
    PROCESS_TYPE_YEAR_AHEAD,
    SPAIN_EIC,
    USER_AGENT,
    fetch_document,
    load_token,
    year_chunks,
)
from mtu.parsing.omie_common import (  # noqa: E402
    append_csv_row,
    ensure_dir,
    sha256_file,
    utc_now_iso,
)

MARKET = "entsoe_generation"
CATEGORY = "installed_capacity"
FILE_FAMILY = "installed_capacity"
DOCUMENT_TYPE = DOC_TYPE["installed_capacity"]  # A68


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Download ENTSO-E installed generation capacity aggregated "
            "(A68, processType A33) for Spain, one XML per year."
        )
    )
    p.add_argument("--start-year", type=int, required=True)
    p.add_argument("--end-year", type=int, required=True)
    p.add_argument("--in-domain", default=SPAIN_EIC)
    p.add_argument("--timeout", type=int, default=300)
    p.add_argument("--max-retries", type=int, default=4)
    p.add_argument("--overwrite", action="store_true")
    p.add_argument("--refresh-recent-years", type=int, default=1)
    return p.parse_args()


def should_refresh(idx: int, total: int, n: int) -> bool:
    return n > 0 and idx >= total - n


def main() -> None:
    args = parse_args()
    token = load_token()

    raw_dir = PROJECT_ROOT / "data/raw/entsoe/generation/installed_capacity"
    manifest_csv = PROJECT_ROOT / "data/metadata/download_manifest.csv"
    ensure_dir(raw_dir)

    chunks = list(year_chunks(args.start_year, args.end_year))
    total = len(chunks)

    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    print(
        f"Syncing ENTSO-E {FILE_FAMILY} from {args.start_year} to "
        f"{args.end_year}\n"
        f"In-domain:     {args.in_domain}\n"
        f"documentType:  {DOCUMENT_TYPE}\n"
        f"processType:   {PROCESS_TYPE_YEAR_AHEAD}\n"
        f"Raw dir:       {raw_dir}\n"
    )

    totals = {"downloaded": 0, "skipped": 0, "empty": 0}

    for i, (yyyy, ps, pe) in enumerate(chunks):
        out_name = f"{FILE_FAMILY}_{yyyy}.xml"
        out_path = raw_dir / out_name

        is_refresh = should_refresh(i, total, args.refresh_recent_years)
        if out_path.exists() and not args.overwrite and not is_refresh:
            print(f"[SKIP]    {out_name}")
            totals["skipped"] += 1
            continue
        if out_path.exists() and is_refresh and not args.overwrite:
            print(f"[REFRESH] {out_name}")

        params = {
            "documentType": DOCUMENT_TYPE,
            "processType": PROCESS_TYPE_YEAR_AHEAD,
            "in_Domain": args.in_domain,
            "periodStart": ps.replace("-", "") + "0000",
            "periodEnd": pe.replace("-", "") + "0000",
        }

        body, status = fetch_document(
            session=session,
            token=token,
            params=params,
            timeout=args.timeout,
            max_retries=args.max_retries,
        )

        tmp = out_path.with_suffix(out_path.suffix + ".part")
        tmp.write_bytes(body)
        tmp.replace(out_path)

        append_csv_row(
            manifest_csv,
            {
                "downloaded_at": utc_now_iso(),
                "source_url": f"entsoe:{DOCUMENT_TYPE}"
                              f"?in_Domain={args.in_domain}"
                              f"&periodStart={ps}&periodEnd={pe}",
                "market": MARKET,
                "category": CATEGORY,
                "file_family": FILE_FAMILY,
                "filename": out_name,
                "size_bytes": out_path.stat().st_size,
                "sha256": sha256_file(out_path),
                "is_zip": False,
                "file_date": f"{yyyy}-01-01",
                "version_suffix": "",
                "notes": f"auto_download_entsoe;status={status}",
            },
        )

        if status == "empty":
            totals["empty"] += 1
            print(f"[EMPTY]   {out_name} (no matching data)")
        else:
            totals["downloaded"] += 1
            print(f"[OK]      {out_name} ({out_path.stat().st_size} bytes)")

    print("\nDone.")
    print(
        f"Downloaded={totals['downloaded']}, "
        f"skipped={totals['skipped']}, empty={totals['empty']}"
    )


if __name__ == "__main__":
    main()
