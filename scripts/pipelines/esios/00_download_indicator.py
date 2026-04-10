from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date, timedelta
from pathlib import Path

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from mtu.parsing.omie_common import append_csv_row, ensure_dir, sha256_file, utc_now_iso  # noqa: E402

BASE_URL = "https://api.esios.ree.es"
DEMO_TOKEN = "96c56fcd69dd5c29f569ab3ea9298b37151a1ee488a1830d353babad3ec90fd7"


def get_token() -> str:
    token = os.environ.get("ESIOS_TOKEN", "").strip()
    if not token:
        print("[WARN] ESIOS_TOKEN not set; using demo key (may be rate-limited)")
        return DEMO_TOKEN
    return token


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Download ESIOS indicator time series (one JSON per day)")
    p.add_argument("--indicator-id", required=True, type=int, help="ESIOS indicator ID")
    p.add_argument("--start-date", required=True, help="YYYY-MM-DD")
    p.add_argument("--end-date", required=True, help="YYYY-MM-DD")
    p.add_argument(
        "--time-trunc",
        default=None,
        help="Optional time aggregation: hour, fifteen_minutes, ten_minutes, five_minutes, day, month",
    )
    p.add_argument("--timeout", type=int, default=60)
    return p.parse_args()


def dates_range(start: date, end: date):
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=1)


def build_url(indicator_id: int, d: date, time_trunc: str | None) -> str:
    start = f"{d.isoformat()}T00:00:00"
    end = f"{d.isoformat()}T23:59:59"
    url = f"{BASE_URL}/indicators/{indicator_id}?start_date={start}&end_date={end}"
    if time_trunc:
        url += f"&time_trunc={time_trunc}"
    return url


def append_manifest_row(
    manifest_csv: Path, local_path: Path, url: str, indicator_id: int, d: date
) -> None:
    row = {
        "downloaded_at": utc_now_iso(),
        "source_url": url,
        "market": "esios",
        "category": "indicators",
        "file_family": f"indicator_{indicator_id}",
        "filename": local_path.name,
        "size_bytes": local_path.stat().st_size,
        "sha256": sha256_file(local_path),
        "is_zip": False,
        "file_date": d.isoformat(),
        "version_suffix": "",
        "notes": "",
    }
    append_csv_row(manifest_csv, row)


def main() -> None:
    args = parse_args()
    token = get_token()

    start_date = date.fromisoformat(args.start_date)
    end_date = date.fromisoformat(args.end_date)
    if end_date < start_date:
        raise ValueError("--end-date cannot be before --start-date")

    raw_dir = PROJECT_ROOT / f"data/raw/esios/indicators/{args.indicator_id}"
    manifest_csv = PROJECT_ROOT / "data/metadata/download_manifest.csv"
    ensure_dir(raw_dir)

    session = requests.Session()
    session.headers.update(
        {
            "Accept": "application/json; application/vnd.esios-api-v1+json",
            "Content-Type": "application/json",
            "x-api-key": token,
        }
    )

    downloaded = 0
    already_present = 0
    errors = 0

    print(
        f"Downloading ESIOS indicator {args.indicator_id} from {start_date} to {end_date}"
        + (f" (time_trunc={args.time_trunc})" if args.time_trunc else "")
    )
    print(f"Raw dir: {raw_dir}\n")

    for d in dates_range(start_date, end_date):
        fname = f"indicator_{args.indicator_id}_{d.strftime('%Y%m%d')}.json"
        out_path = raw_dir / fname
        url = build_url(args.indicator_id, d, args.time_trunc)

        if out_path.exists():
            print(f"[SKIP exists] {fname}")
            already_present += 1
            continue

        try:
            r = session.get(url, timeout=args.timeout)
        except requests.RequestException as e:
            print(f"[ERROR] {fname} -> request error: {e}")
            errors += 1
            continue

        if r.status_code != 200:
            print(f"[WARN] {fname} -> HTTP {r.status_code}")
            errors += 1
            continue

        try:
            data = r.json()
        except ValueError as e:
            print(f"[ERROR] {fname} -> JSON parse error: {e}")
            errors += 1
            continue

        out_path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
        append_manifest_row(manifest_csv, out_path, url, args.indicator_id, d)

        n_values = len(data.get("indicator", {}).get("values", []))
        print(f"[DOWNLOADED] {fname} ({n_values} values)")
        downloaded += 1

    print(f"\nDone. downloaded={downloaded}, already_present={already_present}, errors={errors}")


if __name__ == "__main__":
    main()
