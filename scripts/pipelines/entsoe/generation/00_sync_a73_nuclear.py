"""Sync ENTSO-E A73 (Actual Generation per Unit) for Spanish nuclear (B14).

A73 returns per-plant generation timeseries — clean per-unit coverage
unlike A75 which had truncation issues for non-VRE PSR types.

Output: data/raw/entsoe/generation/a73_nuclear/{YYYYMM}.xml
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


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--start-month", required=True)
    p.add_argument("--end-month", required=True)
    p.add_argument("--psr-type", default="B14")
    p.add_argument("--in-domain", default=SPAIN_EIC)
    p.add_argument("--overwrite", action="store_true")
    args = p.parse_args()

    token = load_token()
    raw_dir = PROJECT_ROOT / f"data/raw/entsoe/generation/a73_{args.psr_type.lower()}"
    raw_dir.mkdir(parents=True, exist_ok=True)
    chunks = list(month_chunks(args.start_month, args.end_month))
    print(f"A73 psrType={args.psr_type} {args.in_domain}, {len(chunks)} months")

    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    n_dl = n_skip = n_fail = 0
    for yyyymm, ps, pe in chunks:
        out = raw_dir / f"{yyyymm}.xml"
        if out.exists() and not args.overwrite:
            print(f"[SKIP] {yyyymm}")
            n_skip += 1
            continue
        params = {
            "documentType": "A73",
            "processType": PROCESS_TYPE_REALISED,
            "in_Domain": args.in_domain,
            "psrType": args.psr_type,
            "periodStart": ps.replace("-", "") + "0000",
            "periodEnd": pe.replace("-", "") + "0000",
        }
        try:
            body, status = fetch_document(session=session, token=token, params=params, timeout=300, max_retries=4)
            tmp = out.with_suffix(out.suffix + ".part")
            tmp.write_bytes(body)
            tmp.replace(out)
            print(f"[OK] {yyyymm} ({len(body)//1024} KB)")
            n_dl += 1
        except Exception as e:
            print(f"[FAIL] {yyyymm}: {e}")
            n_fail += 1
    print(f"\n{n_dl} downloaded, {n_skip} skipped, {n_fail} failed")


if __name__ == "__main__":
    main()
