"""Concatenate per-indicator parquet files into a single long panel.

Output: data/processed/esios/indicators/indicators_all.parquet

Schema (long format, one row per (indicator_id, ts_utc, geo_id)):
    indicator_id, slug, ts_utc, ts_local, date, hour, period_15min,
    geo_id, geo_name, value, source_file

`slug` is joined from the catalog YAML so downstream queries can
identify indicators by readable name without re-reading metadata.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[4]
PROC_ROOT = PROJECT_ROOT / "data" / "processed" / "esios" / "indicators"
CATALOG = PROJECT_ROOT / "data" / "external" / "esios_indicator_catalog.yaml"
OUT = PROC_ROOT / "indicators_all.parquet"


def load_slugs() -> dict[int, str]:
    """Read indicator_id → slug from the catalog YAML."""
    if not CATALOG.exists():
        return {}
    with open(CATALOG, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}
    slugs: dict[int, str] = {}
    for tier in cfg.get("tiers", []):
        for ind in tier.get("indicators", []):
            slugs[int(ind["id"])] = ind.get("slug", "")
    return slugs


def main() -> None:
    if not PROC_ROOT.exists():
        print(f"No processed indicators dir at {PROC_ROOT}")
        return
    parts = []
    paths = sorted(p for p in PROC_ROOT.glob("*.parquet") if p.name != OUT.name)
    if not paths:
        print("No per-indicator parquet files — run 10_parse_indicators.py first.")
        return
    slugs = load_slugs()
    for p in paths:
        df = pd.read_parquet(p)
        parts.append(df)
    big = pd.concat(parts, ignore_index=True)
    big["slug"] = big["indicator_id"].map(slugs).fillna("")
    # Move slug next to indicator_id for readability.
    cols = ["indicator_id", "slug"] + [c for c in big.columns if c not in ("indicator_id", "slug")]
    big = big[cols]

    print(f"Concatenated: {len(big):,} rows from {len(paths)} indicators")
    print(f"  date range: {big['date'].min()} → {big['date'].max()}")
    print(f"  unique indicators: {big['indicator_id'].nunique()}")
    print(f"  unique geos: {big['geo_id'].nunique()}")
    big.to_parquet(OUT, index=False)
    print(f"\nWrote {OUT} ({OUT.stat().st_size/1e6:.1f} MB)")


if __name__ == "__main__":
    main()
