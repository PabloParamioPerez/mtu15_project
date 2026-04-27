"""Sync ENTSO-E A75 actual generation for Spanish nuclear (B14) — focused query.

The original `00_sync_wind_solar_actual.py` queries A75 without psrType,
returning all PSR types in a multi-block XML. For non-VRE types (nuclear,
hydro, gas) this returns INCOMPLETE coverage (only 30-80% of hours per
month). Re-querying with psrType=B14 returns the full Spanish-aggregate
nuclear timeseries cleanly.

Output: data/raw/entsoe/generation/actual_b14_nuclear/{YYYYMM}.xml
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

from mtu.ingestion.entsoe_common import (  # noqa: E402
    PROCESS_TYPE_REALISED,
    SPAIN_EIC,
    USER_AGENT,
    fetch_document,
    load_token,
    month_chunks,
)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="A75 actual generation Spain — psrType=B14 nuclear, monthly")
    p.add_argument("--start-month", required=True, help="YYYY-MM")
    p.add_argument("--end-month", required=True, help="YYYY-MM")
    p.add_argument("--in-domain", default=SPAIN_EIC)
    p.add_argument("--psr-type", default="B14")
    p.add_argument("--timeout", type=int, default=300)
    p.add_argument("--max-retries", type=int, default=4)
    p.add_argument("--overwrite", action="store_true")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    token = load_token()
    raw_dir = PROJECT_ROOT / f"data/raw/entsoe/generation/actual_{args.psr_type.lower()}_nuclear"
    raw_dir.mkdir(parents=True, exist_ok=True)
    chunks = list(month_chunks(args.start_month, args.end_month))
    print(f"Fetching A75 psrType={args.psr_type} for {args.in_domain}, {len(chunks)} months")

    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    n_dl = n_skip = n_fail = 0

    for i, (yyyymm, ps, pe) in enumerate(chunks, 1):
        out_name = f"{yyyymm}.xml"
        out_path = raw_dir / out_name
        if out_path.exists() and not args.overwrite:
            print(f"[SKIP]  {out_name}")
            n_skip += 1
            continue
        params = {
            "documentType": "A75",
            "processType": PROCESS_TYPE_REALISED,
            "in_Domain": args.in_domain,
            "psrType": args.psr_type,
            "periodStart": ps.replace("-", "") + "0000",
            "periodEnd": pe.replace("-", "") + "0000",
        }
        try:
            body, status = fetch_document(
                session=session, token=token, params=params,
                timeout=args.timeout, max_retries=args.max_retries,
            )
            tmp = out_path.with_suffix(out_path.suffix + ".part")
            tmp.write_bytes(body)
            tmp.replace(out_path)
            print(f"[OK]    {out_name}  ({len(body)//1024} KB)")
            n_dl += 1
        except Exception as e:
            print(f"[FAIL]  {out_name}  {e}")
            n_fail += 1

    print(f"\nDone: {n_dl} downloaded, {n_skip} skipped, {n_fail} failed")


if __name__ == "__main__":
    main()
