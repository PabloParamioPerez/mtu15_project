from __future__ import annotations

from pathlib import Path
import re
import sys

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[4]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

SNAP_RE = re.compile(r"^precios_pibcic_ronda_(\d{8})\.(\d+)$")


def parse_snapshot_parts(source_file: str) -> tuple[str, int, str]:
    m = SNAP_RE.match(source_file)
    if not m:
        raise ValueError(f"Unexpected source_file format: {source_file}")
    snapshot_file_date, version_suffix = m.groups()
    snapshot_token = f"{snapshot_file_date}.{version_suffix}"
    return snapshot_file_date, int(version_suffix), snapshot_token


def validate_unique(df: pd.DataFrame, subset: list[str], label: str) -> None:
    dup_mask = df.duplicated(subset=subset, keep=False)
    if dup_mask.any():
        preview = df.loc[dup_mask, subset + [c for c in ["price_mean_es_eur_mwh", "price_mean_pt_eur_mwh", "price_mean_mo_eur_mwh"] if c in df.columns]].head(20)
        raise ValueError(
            f"{label}: found duplicated rows on key {subset}. "
            f"Sample={preview.to_dict(orient='records')}"
        )


def main() -> None:
    processed_dir = PROJECT_ROOT / "data/processed/omie/mercado_intradiario_continuo/precios/precios_pibcic_ronda"
    output_path = processed_dir.parent / "precios_pibcic_ronda_all.parquet"

    files = sorted(processed_dir.glob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"No parquet files found in {processed_dir}")

    dfs = [pd.read_parquet(p) for p in files]
    df = pd.concat(dfs, ignore_index=True)

    input_files = len(files)

    if "source_file" not in df.columns:
        raise ValueError("Combined dataframe is missing source_file")

    parsed = df["source_file"].map(parse_snapshot_parts)
    df["snapshot_file_date"] = parsed.map(lambda x: x[0])
    df["version_suffix_num"] = parsed.map(lambda x: x[1])
    df["snapshot_token"] = parsed.map(lambda x: x[2])

    snapshot_key = ["source_file", "date", "round_number", "period"]
    coarse_key = ["date", "round_number", "period"]

    validate_unique(df, snapshot_key, "precios_pibcic_ronda snapshot panel")

    overlap = (
        df.groupby(coarse_key, dropna=False)
        .agg(
            n_rows=("source_file", "size"),
            n_source_files=("source_file", "nunique"),
            es_nunique=("price_mean_es_eur_mwh", "nunique"),
            pt_nunique=("price_mean_pt_eur_mwh", "nunique"),
            mo_nunique=("price_mean_mo_eur_mwh", "nunique"),
        )
        .reset_index()
    )

    overlapping_keys = int((overlap["n_source_files"] > 1).sum())
    conflicting_keys = int(
        (
            (overlap["n_source_files"] > 1)
            & (
                (overlap["es_nunique"] > 1)
                | (overlap["pt_nunique"] > 1)
                | (overlap["mo_nunique"] > 1)
            )
        ).sum()
    )

    df = (
        df.sort_values(
            ["snapshot_file_date", "version_suffix_num", "date", "round_number", "period", "source_file"]
        )
        .reset_index(drop=True)
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, index=False)

    days_by_mtu = (
        df.groupby("date")["mtu_minutes"]
        .first()
        .value_counts()
        .sort_index()
        .to_dict()
    )

    rounds_per_day = df.groupby("date")["round_number"].nunique()
    partial_days = int(
        df.groupby("date")["is_partial_day_file"].max().fillna(False).astype(bool).sum()
    )

    print(f"Input files:                  {input_files}")
    print(f"Output rows (snapshot panel): {len(df)}")
    print(f"Date range:                   {df['date'].min()} -> {df['date'].max()}")
    print(f"Output file:                  {output_path}")
    print(f"Days by inferred MTU:         {days_by_mtu}")
    print(f"Partial-day files:            {partial_days}")
    print(f"Rounds/day summary:           min={int(rounds_per_day.min())}, max={int(rounds_per_day.max())}, mean={rounds_per_day.mean():.2f}")
    print(f"Overlapping coarse keys:      {overlapping_keys}")
    print(f"Conflicting coarse keys:      {conflicting_keys}")


if __name__ == "__main__":
    main()
