"""Concatenate per-month load parquets into a single all.parquet."""
from pathlib import Path
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[4]
SRC = PROJECT_ROOT / "data/processed/entsoe/load/load_actual"
OUT = PROJECT_ROOT / "data/processed/entsoe/load/load_actual_all.parquet"


def main() -> None:
    parts = sorted(SRC.glob("load_actual_*.parquet"))
    parts = [p for p in parts if p.name != OUT.name]
    if not parts:
        print("No partfiles")
        return
    dfs = [pd.read_parquet(p) for p in parts]
    df = pd.concat(dfs, ignore_index=True).drop_duplicates(["isp_start_utc"]).sort_values("isp_start_utc")
    df.to_parquet(OUT, index=False)
    print(f"built {OUT}: {len(df):,} rows, range {df.isp_start_utc.min()} → {df.isp_start_utc.max()}")


if __name__ == "__main__":
    main()
