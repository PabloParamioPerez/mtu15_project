"""Parse ESIOS Indisponibilidades .xls snapshots → per-snapshot parquet.

Reads every .xls under data/raw/esios/indisponibilidades/<yyyymmdd>/ and
writes data/processed/esios/indisponibilidades/<yyyymmdd>.parquet.
Idempotent — skips snapshots whose parquet is newer than the .xls.
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[4]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from mtu.parsing.esios.indisponibilidades import parse_indisponibilidades_xls  # noqa: E402

RAW_ROOT  = PROJECT_ROOT / "data" / "raw" / "esios" / "indisponibilidades"
OUT_ROOT  = PROJECT_ROOT / "data" / "processed" / "esios" / "indisponibilidades"


def needs_parse(xls_path: Path, out_path: Path) -> bool:
    if not out_path.exists():
        return True
    return xls_path.stat().st_mtime > out_path.stat().st_mtime


def main() -> None:
    if not RAW_ROOT.exists():
        print(f"No raw dir at {RAW_ROOT}")
        return
    OUT_ROOT.mkdir(parents=True, exist_ok=True)

    snapshots = sorted(p for p in RAW_ROOT.iterdir() if p.is_dir() and p.name.isdigit())
    if not snapshots:
        print("No snapshots to parse.")
        return

    parsed = skipped = empty = errors = 0
    t0 = time.time()
    for snap in snapshots:
        yyyymmdd = snap.name
        snapshot_date = f"{yyyymmdd[:4]}-{yyyymmdd[4:6]}-{yyyymmdd[6:8]}"
        xls_files = list(snap.glob("*.xls"))
        if not xls_files:
            continue
        xls = xls_files[0]
        out_path = OUT_ROOT / f"{yyyymmdd}.parquet"
        if not needs_parse(xls, out_path):
            skipped += 1
            continue
        try:
            df = parse_indisponibilidades_xls(xls, snapshot_date)
        except Exception as e:  # noqa: BLE001
            print(f"  [ERR]   {yyyymmdd}: {e}")
            errors += 1
            continue
        if df.empty:
            empty += 1
            continue
        df.to_parquet(out_path, index=False)
        parsed += 1
        if parsed <= 3 or parsed % 20 == 0:
            print(f"  [OK]    {yyyymmdd}: {len(df):>4} rows  ({out_path.stat().st_size/1024:.1f} KB)")

    dt = time.time() - t0
    print(f"\nDone in {dt:.1f}s. parsed={parsed}, skipped={skipped}, empty={empty}, errors={errors}")


if __name__ == "__main__":
    main()
