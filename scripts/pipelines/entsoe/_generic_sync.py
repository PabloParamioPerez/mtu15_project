"""Generic ENTSO-E sync helper — pass documentType + processType + extra params.

Usage:
  uv run python scripts/pipelines/entsoe/_generic_sync.py \\
    --doc-type A80 --process-type A16 \\
    --start-month 2018-01 --end-month 2026-04 \\
    --subdir outages/generation \\
    --extra-param businessType=A53

Saves to data/raw/entsoe/{subdir}/{YYYYMM}.xml
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from mtu.ingestion.entsoe_common import (  # noqa: E402
    SPAIN_EIC, USER_AGENT, fetch_document, load_token, month_chunks,
)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--doc-type", required=True)
    p.add_argument("--process-type", default=None)
    p.add_argument("--start-month", required=True)
    p.add_argument("--end-month", required=True)
    p.add_argument("--subdir", required=True, help="Subdir under data/raw/entsoe/")
    p.add_argument("--in-domain", default=SPAIN_EIC)
    p.add_argument("--out-domain", default=None, help="For cross-border flows, set this")
    p.add_argument("--biz-domain", default=None, help="For some queries (biddingZone)")
    p.add_argument("--out-biz-domain", default=None, help="For A65 load (outBiddingZone_Domain)")
    p.add_argument("--extra-param", action="append", default=[], help="key=value extras")
    p.add_argument("--overwrite", action="store_true")
    p.add_argument("--quiet", action="store_true")
    p.add_argument("--use-acquiring-domain", action="store_true",
                   help="Use acquiring_Domain instead of in_Domain (for unavailability docs)")
    args = p.parse_args()

    token = load_token()
    raw_dir = PROJECT_ROOT / f"data/raw/entsoe/{args.subdir}"
    raw_dir.mkdir(parents=True, exist_ok=True)
    chunks = list(month_chunks(args.start_month, args.end_month))
    if not args.quiet:
        print(f"[{args.doc_type}] {len(chunks)} months → {raw_dir}")

    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    extras = {}
    for kv in args.extra_param:
        k, v = kv.split("=", 1)
        extras[k] = v

    n_dl = n_skip = n_fail = n_empty = 0
    for yyyymm, ps, pe in chunks:
        out = raw_dir / f"{yyyymm}.xml"
        if out.exists() and out.stat().st_size > 0 and not args.overwrite:
            n_skip += 1
            continue

        params: dict = {
            "documentType": args.doc_type,
            "periodStart": ps.replace("-", "") + "0000",
            "periodEnd": pe.replace("-", "") + "0000",
        }
        if args.process_type:
            params["processType"] = args.process_type
        if args.use_acquiring_domain:
            params["acquiring_Domain"] = args.in_domain
        elif args.out_biz_domain:
            params["outBiddingZone_Domain"] = args.out_biz_domain
        elif args.out_domain:
            # Cross-border: in_Domain (export side) and out_Domain (import side)
            params["in_Domain"] = args.in_domain
            params["out_Domain"] = args.out_domain
        elif args.biz_domain:
            params["biddingZone_Domain"] = args.biz_domain
        else:
            params["in_Domain"] = args.in_domain
        params.update(extras)

        try:
            body, status = fetch_document(
                session=session, token=token, params=params,
                timeout=300, max_retries=3,
            )
            if status == "empty":
                # Save empty marker so we don't re-fetch
                out.write_bytes(b'<empty/>')
                n_empty += 1
                if not args.quiet:
                    print(f"[{args.doc_type}] {yyyymm} empty")
                continue
            tmp = out.with_suffix(out.suffix + ".part")
            tmp.write_bytes(body)
            tmp.replace(out)
            n_dl += 1
            if not args.quiet:
                print(f"[{args.doc_type}] {yyyymm} OK ({len(body)//1024} KB)")
        except Exception as e:
            n_fail += 1
            if not args.quiet:
                print(f"[{args.doc_type}] {yyyymm} FAIL: {e}")

    print(f"[{args.doc_type}] {args.subdir}: {n_dl} downloaded, {n_skip} skipped, {n_empty} empty, {n_fail} failed")


if __name__ == "__main__":
    main()
