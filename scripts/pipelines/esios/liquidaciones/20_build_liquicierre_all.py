"""Concatenate per-month liquicierre + liquicierresrs parquet into a single panel.

Output:
    data/processed/esios/reservas/liquicierre_all.parquet

Combines both archive vintages into one continuous 2015-now panel.
The `archive` column distinguishes them ("liquicierre" pre-ISP15
legacy vs "liquicierresrs" post-ISP15). The `bsp` column is the
unified per-firm identifier across both formats (the legacy `B1`
field is parsed under the `bsp` column name for consistency).
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[4]
PROCESSED = PROJECT_ROOT / "data/processed/esios/reservas"
OUT = PROCESSED / "liquicierre_all.parquet"


def main() -> None:
    parts = []
    for p in sorted(PROCESSED.glob("liquicierre_*.parquet")):
        if p.name == "liquicierre_all.parquet":
            continue
        parts.append(pd.read_parquet(p))
    for p in sorted(PROCESSED.glob("liquicierresrs_*.parquet")):
        parts.append(pd.read_parquet(p))

    if not parts:
        print("No per-month parquet found — run 10_parse_liquicierre.py first.")
        return

    df = pd.concat(parts, ignore_index=True)
    # Coerce mixed-type date column (some month files have date as
    # datetime.date, others as NaN float) to consistent pandas datetime.
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    print(f"Concatenated: {len(df):,} rows from {len(parts)} per-month files.")
    valid_dates = df["date"].dropna()
    if len(valid_dates):
        print(f"  date range:  {valid_dates.min().date()} → {valid_dates.max().date()}")
    n_missing = df["date"].isna().sum()
    if n_missing:
        print(f"  missing-date rows: {n_missing:,} (filename did not parse to YYYYMMDD)")
    print(f"  BSPs ({df['bsp'].nunique()}): {sorted(df['bsp'].dropna().unique())}")
    print(f"  Info codes ({df['info'].nunique()}): {sorted(df['info'].dropna().unique())}")
    print(f"  Archives: {df['archive'].value_counts().to_dict()}")

    df.to_parquet(OUT, index=False)
    print(f"\nWrote {OUT} ({OUT.stat().st_size/1e6:.1f} MB)")


if __name__ == "__main__":
    main()
