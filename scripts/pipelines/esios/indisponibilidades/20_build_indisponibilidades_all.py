"""Concatenate per-snapshot Indisponibilidades parquets into one panel.

Output: data/processed/esios/indisponibilidades/indisponibilidades_all.parquet
Schema: snapshot_date + the outage-event columns from indisponibilidades.py.

Snapshots are FORWARD-LOOKING (each lists outages already running plus
all scheduled ones), so the panel naturally contains the same outage
event in multiple snapshots until it expires. Use the latest snapshot
per (unit_name, start_local, end_local, reason_code) for the
"final-known" outage view.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[4]
PROC_ROOT = PROJECT_ROOT / "data" / "processed" / "esios" / "indisponibilidades"
OUT = PROC_ROOT / "indisponibilidades_all.parquet"


def main() -> None:
    if not PROC_ROOT.exists():
        print(f"No processed dir at {PROC_ROOT}")
        return
    parts = []
    for p in sorted(PROC_ROOT.glob("*.parquet")):
        if p.name == OUT.name:
            continue
        parts.append(pd.read_parquet(p))
    if not parts:
        print("No per-snapshot parquet — run 10_parse_indisponibilidades.py first.")
        return
    df = pd.concat(parts, ignore_index=True)
    print(f"Concatenated: {len(df):,} rows from {len(parts)} snapshots")
    print(f"  snapshot range: {df['snapshot_date'].min()} → {df['snapshot_date'].max()}")
    print(f"  unit types: {df['unit_type'].value_counts().to_dict()}")
    print(f"  reason codes (top): {df['reason_code'].value_counts().head(10).to_dict()}")
    df.to_parquet(OUT, index=False)
    print(f"\nWrote {OUT} ({OUT.stat().st_size/1e6:.1f} MB)")


if __name__ == "__main__":
    main()
