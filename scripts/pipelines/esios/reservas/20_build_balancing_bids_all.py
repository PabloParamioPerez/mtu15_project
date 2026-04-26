"""Concatenate per-month balancing_bids parquet into a single panel.

Output:
    data/processed/esios/reservas/balancing_bids_all.parquet

System-aggregate mFRR offer-curve panel (2022-05-24 → 2024-12-10),
PT15M resolution. NOT per-firm.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[4]
PROCESSED = PROJECT_ROOT / "data/processed/esios/reservas"
OUT = PROCESSED / "balancing_bids_all.parquet"


def main() -> None:
    parts = []
    for p in sorted(PROCESSED.glob("balancing_bids_*.parquet")):
        if p.name == "balancing_bids_all.parquet":
            continue
        parts.append(pd.read_parquet(p))

    if not parts:
        print("No per-month parquet — run 10_parse_balancing_bids.py first.")
        return

    df = pd.concat(parts, ignore_index=True)
    print(f"Concatenated: {len(df):,} rows from {len(parts)} per-month files.")
    print(f"  date range: {df['date'].min()} → {df['date'].max()}")
    print(f"  bid types: {df.groupby('bid_type_id')['bid_type_name'].first().to_dict()}")
    df.to_parquet(OUT, index=False)
    print(f"\nWrote {OUT} ({OUT.stat().st_size/1e6:.1f} MB)")


if __name__ == "__main__":
    main()
