"""Concatenate per-month curvas_ofertas_afrr parquet into a single panel.

Output:
    data/processed/esios/reservas/curvas_ofertas_afrr_all.parquet

System-aggregate aFRR offer-curve panel (2024-11-20 → present),
PT15M resolution. NOT per-firm.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[4]
RESERVAS = PROJECT_ROOT / "data/processed/esios/reservas"
SRC = RESERVAS / "curvas_ofertas_afrr"
OUT = RESERVAS / "curvas_ofertas_afrr_all.parquet"


def main() -> None:
    parts = []
    for p in sorted(SRC.glob("curvas_ofertas_afrr_*.parquet")):
        parts.append(pd.read_parquet(p))

    if not parts:
        print("No per-month parquet — run 10_parse_curvas_ofertas_afrr.py first.")
        return

    df = pd.concat(parts, ignore_index=True)
    print(f"Concatenated: {len(df):,} rows from {len(parts)} per-month files.")
    print(f"  date range: {df['date'].min()} → {df['date'].max()}")
    print(f"  directions: {df['direction'].value_counts().to_dict()}")
    df.to_parquet(OUT, index=False)
    print(f"\nWrote {OUT} ({OUT.stat().st_size/1e6:.1f} MB)")


if __name__ == "__main__":
    main()
