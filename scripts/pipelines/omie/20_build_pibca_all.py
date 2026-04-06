from __future__ import annotations

from pathlib import Path
import sys

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[3]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))


SNAPSHOT_KEY = [
    "source_file",
    "date",
    "session_number",
    "period",
    "unit_code",
]


def validate_unique(df: pd.DataFrame, subset: list[str], label: str) -> None:
    dup_mask = df.duplicated(subset=subset, keep=False)
    if dup_mask.any():
        preview_cols = subset + [c for c in ["assigned_power_mw"] if c in df.columns and c not in subset]
        preview = df.loc[dup_mask, preview_cols].head(20)
        raise ValueError(
            f"{label}: found duplicated rows on key {subset}. "
            f"Sample={preview.to_dict(orient='records')}"
        )


def sort_output(df: pd.DataFrame) -> pd.DataFrame:
    sort_cols = [c for c in ["source_file", "date", "session_number", "period", "unit_code", "raw_row_number_in_file"] if c in df.columns]
    return df.sort_values(sort_cols).reset_index(drop=True)


def build_one_month(month: str, files: list[Path], monthly_dir: Path) -> Path:
    dfs = [pd.read_parquet(p) for p in files]
    df = pd.concat(dfs, ignore_index=True)

    validate_unique(df, SNAPSHOT_KEY, f"[{month}] monthly PIBCA snapshot table")

    df = sort_output(df)

    monthly_dir.mkdir(parents=True, exist_ok=True)
    out_path = monthly_dir / f"pibca_{month}_all.parquet"
    df.to_parquet(out_path, index=False)

    print(f"[{month}] files={len(files)} rows={len(df)} -> {out_path}")
    return out_path


def main() -> None:
    processed_dir = PROJECT_ROOT / "data/processed/omie/mercado_intradiario_subastas/programas/pibca"
    monthly_dir = processed_dir.parent / "pibca_monthly"
    output_path = processed_dir.parent / "pibca_all.parquet"

    files = sorted(processed_dir.glob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"No parquet files found in {processed_dir}")

    files_by_month: dict[str, list[Path]] = {}
    for p in files:
        stem = p.name.removesuffix(".parquet")
        month = stem.split("_", 1)[1][:6]
        files_by_month.setdefault(month, []).append(p)

    print(f"Input files:             {len(files)}")
    print(f"Months found:            {len(files_by_month)}")

    monthly_paths = []
    for month in sorted(files_by_month):
        monthly_paths.append(build_one_month(month, files_by_month[month], monthly_dir))

    print("\nCombining monthly outputs...")

    dfs = [pd.read_parquet(p) for p in monthly_paths]
    df = pd.concat(dfs, ignore_index=True)

    validate_unique(df, SNAPSHOT_KEY, "FINAL PIBCA snapshot table")

    df = sort_output(df)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, index=False)

    days_by_mtu = (
        df.groupby("date")["mtu_minutes"]
        .first()
        .value_counts()
        .sort_index()
        .to_dict()
    )

    rows_per_file = df.groupby("source_file").size()

    print(f"\nOutput rows:             {len(df)}")
    print(f"Date range:              {df['date'].min()} -> {df['date'].max()}")
    print(f"Output file:             {output_path}")
    print(f"Days by inferred MTU:    {days_by_mtu}")
    print(
        f"Rows/file summary:       min={int(rows_per_file.min())}, "
        f"max={int(rows_per_file.max())}, mean={rows_per_file.mean():.2f}"
    )


if __name__ == "__main__":
    main()
