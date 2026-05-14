"""Parse ESIOS /indicators JSON dumps into per-indicator parquet files.

For each indicator subdir under `data/raw/esios/indicators/<id>/`,
concatenate the monthly JSON dumps, normalize to long format, and write
`data/processed/esios/indicators/{id}.parquet`.

Re-runs are idempotent: an indicator parquet is rebuilt only if any of
its raw JSONs has mtime > the parquet mtime, OR if `--force` is passed.
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[4]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from mtu.parsing.esios.indicators import parse_indicator_dir  # noqa: E402

RAW_ROOT = PROJECT_ROOT / "data" / "raw"      / "esios" / "indicators"
OUT_ROOT = PROJECT_ROOT / "data" / "processed" / "esios" / "indicators"


def needs_rebuild(raw_dir: Path, out_path: Path) -> bool:
    if not out_path.exists():
        return True
    out_mtime = out_path.stat().st_mtime
    return any(p.stat().st_mtime > out_mtime for p in raw_dir.glob("indicator_*.json"))


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Parse ESIOS indicator JSONs → parquet")
    p.add_argument("--ids", nargs="*", type=int, default=None,
                   help="Optional whitelist of indicator IDs. Default: all under raw/")
    p.add_argument("--force", action="store_true",
                   help="Rebuild every parquet regardless of mtime")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    OUT_ROOT.mkdir(parents=True, exist_ok=True)

    if not RAW_ROOT.exists():
        print(f"No raw indicators dir at {RAW_ROOT}")
        return

    available_ids = sorted(int(p.name) for p in RAW_ROOT.iterdir()
                           if p.is_dir() and p.name.isdigit())
    target_ids = (sorted(set(args.ids) & set(available_ids))
                  if args.ids else available_ids)
    if not target_ids:
        print("No matching indicator IDs found.")
        return

    print(f"Parsing {len(target_ids)} indicators (force={args.force})")
    rebuilt = skipped = empty = errors = 0
    t0 = time.time()
    for ind_id in target_ids:
        raw_dir = RAW_ROOT / str(ind_id)
        out_path = OUT_ROOT / f"{ind_id}.parquet"
        if not args.force and not needs_rebuild(raw_dir, out_path):
            skipped += 1
            continue
        try:
            df = parse_indicator_dir(raw_dir)
        except Exception as e:  # noqa: BLE001
            print(f"  [ERR] {ind_id}: {e}")
            errors += 1
            continue
        if df.empty:
            empty += 1
            print(f"  [EMPTY] {ind_id}: no values")
            continue
        df.to_parquet(out_path, index=False)
        rebuilt += 1
        n_rows = len(df)
        date_min = df["date"].min()
        date_max = df["date"].max()
        size_mb = out_path.stat().st_size / 1e6
        print(f"  [OK]    {ind_id}: {n_rows:>9,} rows  {date_min} → {date_max}  ({size_mb:.1f} MB)")

    dt = time.time() - t0
    print(f"\nDone in {dt:.1f}s. rebuilt={rebuilt}, skipped={skipped}, empty={empty}, errors={errors}")


if __name__ == "__main__":
    main()
