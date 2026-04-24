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
    SPAIN_EIC,
    USER_AGENT,
    fetch_document,
    load_token,
    month_chunks,
)
from mtu.parsing.omie_common import (  # noqa: E402
    append_csv_row,
    ensure_dir,
    sha256_file,
    utc_now_iso,
)

MARKET = "entsoe_balancing"
CATEGORY = "financial_balance"
FILE_FAMILY = "financial_balance"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Download ENTSO-E monthly financial-balance (A87) XMLs for "
            "Spain, one XML per month (idempotent with recent-month refresh)."
        )
    )
    p.add_argument("--start-month", required=True, help="YYYY-MM")
    p.add_argument("--end-month", required=True, help="YYYY-MM")
    p.add_argument("--control-area", default=SPAIN_EIC)
    p.add_argument(
        "--timeout",
        type=int,
        default=300,
        help="Per-request timeout in seconds (default 300).",
    )
    p.add_argument(
        "--max-retries",
        type=int,
        default=4,
        help="Retries on 5xx / network errors. Default 4.",
    )
    p.add_argument(
        "--overwrite",
        action="store_true",
        help="Redownload XML even if it already exists.",
    )
    p.add_argument(
        "--refresh-recent-months",
        type=int,
        default=3,
        help=(
            "Always redownload the most recent N requested months, because "
            "A87 uses preliminary figures that are restated after final "
            "settlement (M+3 publication deadline). Default: 3."
        ),
    )
    return p.parse_args()


def should_refresh(period_idx: int, total: int, refresh_n: int) -> bool:
    if refresh_n <= 0:
        return False
    return period_idx >= total - refresh_n


def main() -> None:
    args = parse_args()
    token = load_token()

    raw_dir = PROJECT_ROOT / "data/raw/entsoe/balancing/financial_balance"
    manifest_csv = PROJECT_ROOT / "data/metadata/download_manifest.csv"
    ensure_dir(raw_dir)

    chunks = list(month_chunks(args.start_month, args.end_month))
    total = len(chunks)

    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    print(
        f"Syncing ENTSO-E {FILE_FAMILY} from {args.start_month} to "
        f"{args.end_month}\n"
        f"Control area:  {args.control_area}\n"
        f"documentType:  {DOC_TYPE['financial_balance']}\n"
        f"Raw dir:       {raw_dir}\n"
        f"Manifest:      {manifest_csv}\n"
        f"Refresh recent months: {args.refresh_recent_months}\n"
    )

    totals = {"downloaded": 0, "skipped": 0, "empty": 0}

    for i, (yyyymm, ps, pe) in enumerate(chunks):
        out_name = f"{FILE_FAMILY}_{yyyymm}.xml"
        out_path = raw_dir / out_name

        is_refresh = should_refresh(i, total, args.refresh_recent_months)
        if out_path.exists() and not args.overwrite and not is_refresh:
            print(f"[SKIP]    {out_name}")
            totals["skipped"] += 1
            continue
        if out_path.exists() and is_refresh and not args.overwrite:
            print(f"[REFRESH] {out_name}")

        params = {
            "documentType": DOC_TYPE["financial_balance"],
            "controlArea_Domain": args.control_area,
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
                "source_url": f"entsoe:A{DOC_TYPE['financial_balance'][1:]}"
                              f"?controlArea={args.control_area}"
                              f"&periodStart={ps}&periodEnd={pe}",
                "market": MARKET,
                "category": CATEGORY,
                "file_family": FILE_FAMILY,
                "filename": out_name,
                "size_bytes": out_path.stat().st_size,
                "sha256": sha256_file(out_path),
                "is_zip": False,
                "file_date": f"{yyyymm[:4]}-{yyyymm[4:]}-01",
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
