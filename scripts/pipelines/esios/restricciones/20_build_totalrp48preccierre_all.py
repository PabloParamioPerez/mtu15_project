"""Concatenate per-month totalrp48preccierre parquet into a single panel.

Output:
    data/processed/esios/restricciones/totalrp48preccierre_all.parquet

Long-format panel of RZ technical-restriction closure prices and
quantities. Each row is one Intervalo (15-min ISP) of one
TipoRedespacho code. Used for the S7 Pigouvian per-segment marginal
cost validation (anchors the €210–300/MWh figure for conv-RZ).
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[4]
PROCESSED = PROJECT_ROOT / "data/processed/esios/restricciones"
OUT = PROCESSED / "totalrp48preccierre_all.parquet"


def main() -> None:
    parts = []
    for p in sorted(PROCESSED.glob("totalrp48preccierre_*.parquet")):
        if p.name == "totalrp48preccierre_all.parquet":
            continue
        parts.append(pd.read_parquet(p))

    if not parts:
        print("No per-month parquet — run 10_parse_totalrp48preccierre.py first.")
        return

    df = pd.concat(parts, ignore_index=True)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    print(f"Concatenated: {len(df):,} rows from {len(parts)} per-month files.")
    valid = df["date"].dropna()
    if len(valid):
        print(f"  date range: {valid.min().date()} → {valid.max().date()}")
    n_missing = df["date"].isna().sum()
    if n_missing:
        print(f"  missing-date rows: {n_missing:,}")
    print(f"  TipoRedespacho codes ({df['tipo_redespacho'].nunique()}): "
          f"{sorted(df['tipo_redespacho'].dropna().unique())}")

    df.to_parquet(OUT, index=False)
    print(f"\nWrote {OUT} ({OUT.stat().st_size/1e6:.1f} MB)")


if __name__ == "__main__":
    main()
