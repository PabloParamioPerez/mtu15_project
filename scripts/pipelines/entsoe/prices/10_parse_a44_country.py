"""Parse and build A44 day-ahead price all-month parquet for any country.

Iterates over data/raw/entsoe/prices/da_<X>/ and emits
data/processed/entsoe/prices/<X>_da_all.parquet.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[4]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from mtu.parsing.entsoe.da_price import parse_file  # noqa: E402

# Map raw subdir -> output parquet stem
SUBDIRS = {
    "prices_da_de": "de_da",
    "prices_da_pt": "pt_da",
}


def build_one(raw_subdir: str, out_stem: str) -> None:
    raw = PROJECT_ROOT / "data/raw/entsoe" / raw_subdir
    if not raw.exists():
        print(f"skip {raw_subdir} (not found)")
        return
    out_dir = PROJECT_ROOT / "data/processed/entsoe/prices" / out_stem
    out_dir.mkdir(parents=True, exist_ok=True)
    dfs = []
    for f in sorted(raw.glob("*.xml")):
        if f.stat().st_size < 50 or f.read_bytes()[:8] == b"<empty/>":
            continue
        try:
            df = parse_file(f, domain_label=out_stem)
        except Exception as e:
            print(f"  FAIL {f.name}: {e}")
            continue
        if df.empty:
            continue
        df.to_parquet(out_dir / f"{f.stem}.parquet", index=False)
        dfs.append(df)
    if not dfs:
        print(f"{raw_subdir}: no data")
        return
    big = pd.concat(dfs, ignore_index=True).drop_duplicates(["isp_start_utc"])
    out = PROJECT_ROOT / "data/processed/entsoe/prices" / f"{out_stem}_all.parquet"
    big.to_parquet(out, index=False)
    print(f"{raw_subdir}: {len(big):,} rows → {out}")


def main() -> None:
    for sub, stem in SUBDIRS.items():
        build_one(sub, stem)


if __name__ == "__main__":
    main()
