# STATUS: ALIVE
# LAST-AUDIT: 2026-05-15
# FEEDS: data/raw/esios/indicators/{id}/indicator_{id}_{yyyymmdd}.json
# CLAIM: Batch driver that reads data/external/esios_indicator_catalog.yaml
#        and invokes scripts/pipelines/esios/00_download_indicator.py for each
#        indicator in the requested tier(s).
"""Batch ESIOS indicator downloader (YAML-driven).

Reads data/external/esios_indicator_catalog.yaml and downloads each curated
indicator's full date range (default 2018-01-01 → today) via the underlying
00_download_indicator.py workhorse. Idempotent: skips per-day JSONs that
already exist. Anti-WAF rate-limiting baked in (--sleep arg, default 2.0s).

Usage:
    uv run scripts/pipelines/esios/indicators/00_batch_sync.py --tier A
    uv run scripts/pipelines/esios/indicators/00_batch_sync.py --tier A,B,D
    uv run scripts/pipelines/esios/indicators/00_batch_sync.py --all
    uv run scripts/pipelines/esios/indicators/00_batch_sync.py --family fuel_prices_ttf_co2_proxy
    uv run scripts/pipelines/esios/indicators/00_batch_sync.py --indicator-id 1940 --sleep 3.0
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from datetime import date
from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[4]
CATALOG = PROJECT_ROOT / "data" / "external" / "esios_indicator_catalog.yaml"
DOWNLOADER = PROJECT_ROOT / "scripts" / "pipelines" / "esios" / "00_download_indicator.py"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--tier", default=None,
                   help="Comma-separated priority tiers (A,B,B_spotcheck,D,E). Default: all.")
    p.add_argument("--all", action="store_true", help="Download all tiers.")
    p.add_argument("--family", default=None,
                   help="Restrict to a single family (group of indicators), e.g. 'afrr_prices'.")
    p.add_argument("--indicator-id", type=int, default=None,
                   help="Restrict to a single indicator ID.")
    p.add_argument("--start-date", default=None,
                   help="Override catalog default start date (YYYY-MM-DD).")
    p.add_argument("--end-date", default=None,
                   help="Override catalog default end date (YYYY-MM-DD or 'today').")
    p.add_argument("--time-trunc", default=None,
                   help="ESIOS time_trunc override (hour | fifteen_minutes | day | month).")
    p.add_argument("--sleep", type=float, default=8.0,
                   help="Seconds between API requests within an indicator (default 8.0).")
    p.add_argument("--inter-indicator-sleep", type=float, default=10.0,
                   help="Seconds between consecutive indicators (default 10.0).")
    p.add_argument("--chunk", choices=("day", "month"), default="month",
                   help="API-call granularity (default 'month' — 8 yrs of hourly data = 96 calls/indicator).")
    p.add_argument("--dry-run", action="store_true",
                   help="Print what would be downloaded, do not call API.")
    return p.parse_args()


def resolve_end_date(end: str) -> str:
    if end is None or end.lower() == "today":
        return date.today().isoformat()
    return end


def select_indicators(catalog: dict, tiers: list[str] | None,
                       family: str | None, indicator_id: int | None) -> list[dict]:
    """Flatten the YAML catalog into one indicator-level list with parent metadata.

    Carries per-indicator `time_trunc` (set by the catalog from the probed
    native granularity) so the driver requests each series at its native
    resolution: 15-min for "Quince/Cinco/Diez minutos" series, hourly for
    "Hora", daily for "Día", monthly for "Mes".
    """
    out = []
    for fam in catalog.get("tiers", []):
        if family and fam.get("name") != family:
            continue
        if tiers and fam.get("priority") not in tiers:
            continue
        for ind in fam.get("indicators", []):
            row = {
                "id": ind["id"],
                "slug": ind.get("slug"),
                "name": ind.get("name"),
                "family": fam.get("name"),
                "priority": fam.get("priority"),
                "time_trunc": ind.get("time_trunc"),   # per-indicator override
            }
            if indicator_id is not None and row["id"] != indicator_id:
                continue
            out.append(row)
    return out


def run_one(ind: dict, *, start: str, end: str, sleep: float,
            time_trunc: str | None, chunk: str, dry_run: bool) -> int:
    # Per-indicator override beats the global default
    effective_trunc = ind.get("time_trunc") or time_trunc
    cmd = [
        "uv", "run", str(DOWNLOADER),
        "--indicator-id", str(ind["id"]),
        "--start-date", start,
        "--end-date", end,
        "--sleep", str(sleep),
        "--chunk", chunk,
    ]
    if effective_trunc:
        cmd += ["--time-trunc", effective_trunc]
    print(f"\n→ {ind['family']:35s}  id={ind['id']:>5}  trunc={effective_trunc:<16}  {ind['name']}")
    print(f"   {' '.join(cmd)}")
    if dry_run:
        return 0
    return subprocess.call(cmd, cwd=str(PROJECT_ROOT))


def main():
    args = parse_args()
    catalog = yaml.safe_load(CATALOG.read_text())
    defaults = catalog.get("defaults", {})
    start = args.start_date or defaults.get("start_date", "2018-01-01")
    end = resolve_end_date(args.end_date or defaults.get("end_date", "today"))
    time_trunc = args.time_trunc or defaults.get("time_trunc", "hour")

    tiers = None
    if args.all:
        tiers = None
    elif args.tier:
        tiers = [t.strip() for t in args.tier.split(",") if t.strip()]

    inds = select_indicators(catalog, tiers, args.family, args.indicator_id)
    if not inds:
        print("No indicators matched the filter.")
        sys.exit(1)

    print(f"Selected {len(inds)} indicator(s).  Window: {start} → {end}.  "
          f"Sleep: {args.sleep}s intra / {args.inter_indicator_sleep}s inter.")
    if args.dry_run:
        print("DRY RUN — no API calls will be made.\n")

    import time as _time
    errors = 0
    for i, ind in enumerate(inds):
        rc = run_one(ind, start=start, end=end, sleep=args.sleep,
                     time_trunc=time_trunc, chunk=args.chunk, dry_run=args.dry_run)
        if rc != 0:
            print(f"   [FAIL] exit={rc}")
            errors += 1
        if i < len(inds) - 1 and not args.dry_run and args.inter_indicator_sleep > 0:
            _time.sleep(args.inter_indicator_sleep)

    print(f"\nFinished. indicators={len(inds)} errors={errors}")
    sys.exit(0 if errors == 0 else 1)


if __name__ == "__main__":
    main()
