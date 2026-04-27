"""Concatenate per-month A73 per-unit parquets into a single panel.

Output: data/processed/entsoe/generation/a73_per_unit_all.parquet
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[4]
SRC_DIR = PROJECT_ROOT / "data/processed/entsoe/generation/a73"
OUT = PROJECT_ROOT / "data/processed/entsoe/generation/a73_per_unit_all.parquet"


def main() -> None:
    files = sorted(SRC_DIR.rglob("*.parquet"))
    if not files:
        print(f"no files in {SRC_DIR}")
        return
    dfs = [pd.read_parquet(f) for f in files]
    df = pd.concat(dfs, ignore_index=True).drop_duplicates(
        subset=["isp_start_utc", "unit_eic", "psr_type"]
    )
    df.to_parquet(OUT, index=False)
    print(f"{len(df):,} rows; {df['psr_type'].value_counts().to_dict()}")
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
