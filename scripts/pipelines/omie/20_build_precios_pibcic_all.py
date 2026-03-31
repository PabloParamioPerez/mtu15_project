from __future__ import annotations

from pathlib import Path
import sys

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[3]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))


def main() -> None:
    processed_dir = PROJECT_ROOT / "data/processed/omie/mercado_intradiario_continuo/precios/precios_pibcic"
    output_path = processed_dir.parent / "precios_pibcic_all.parquet"

    files = sorted(processed_dir.glob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"No parquet files found in {processed_dir}")

    dfs = [pd.read_parquet(p) for p in files]
    df = pd.concat(dfs, ignore_index=True)

    input_files = len(files)

    df["version_suffix_num"] = pd.to_numeric(df["version_suffix"], errors="coerce")
    dup_mask = df.duplicated(subset=["date", "period"], keep=False)
    dup_rows = int(dup_mask.sum())
    dup_keys = int(df.loc[dup_mask, ["date", "period"]].drop_duplicates().shape[0])

    if dup_rows > 0:
        print(
            f"WARNING: Found {dup_rows} duplicated rows across {dup_keys} (date, period) keys "
            f"(likely multiple versions)."
        )
        print("Keeping latest version by version_suffix for each (date, period).")

    df = (
        df.sort_values(["date", "period", "version_suffix_num"])
          .drop_duplicates(subset=["date", "period"], keep="last")
          .drop(columns=["version_suffix_num"])
          .sort_values(["date", "period"])
          .reset_index(drop=True)
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, index=False)

    rows_per_day = df.groupby("date").size()
    partial_flag_by_day = (
        df.groupby("date")["is_partial_day_file"]
        .max()
        .fillna(False)
        .astype(bool)
    )

    def full_day_rowcount_ok(n: int) -> bool:
        return n in {23, 24, 25, 92, 96, 100}

    non_partial_mask = ~partial_flag_by_day
    full_days_ok = rows_per_day[non_partial_mask].map(full_day_rowcount_ok).all() if non_partial_mask.any() else True
    partial_days = int(partial_flag_by_day.sum())

    days_by_mtu = (
        df.groupby("date")["mtu_minutes"]
        .first()
        .value_counts()
        .sort_index()
        .to_dict()
    )

    print(f"Input files:             {input_files}")
    print(f"Output rows (latest):    {len(df)}")
    print(f"Date range:              {df['date'].min()} -> {df['date'].max()}")
    print(f"Output file:             {output_path}")
    print(f"Days by inferred MTU:    {days_by_mtu}")
    print(f"Partial-day files:       {partial_days}")
    if full_days_ok:
        print("Rows/day check:          OK (full days match allowed counts; partial days explicitly flagged)")
    else:
        print("Rows/day check:          FAILED")

if __name__ == "__main__":
    main()
