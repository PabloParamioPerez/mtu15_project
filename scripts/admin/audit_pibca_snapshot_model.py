from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--month", required=True)
    parser.add_argument(
        "--input-dir",
        default="data/processed/omie/mercado_intradiario_subastas/programas/pibca",
    )
    parser.add_argument("--sample-keys", type=int, default=10)
    return parser.parse_args()


def discover_files(base_dir: Path, family: str, month: str) -> list[Path]:
    candidates = sorted(base_dir.glob("*.parquet"))
    out = []
    family_prefix = f"{family}_{month}"
    monthly_stem = f"{family}_{month}"
    all_stem = f"{family}_all"
    for path in candidates:
        stem = path.stem
        if stem == all_stem:
            continue
        if stem == monthly_stem:
            continue
        if stem.startswith(family_prefix):
            out.append(path)
    return out


def infer_source_file(path: Path) -> str:
    return path.stem


def load_family(files: list[Path]) -> pd.DataFrame:
    parts: list[pd.DataFrame] = []
    for path in files:
        df = pd.read_parquet(path)
        if "source_file" not in df.columns:
            df = df.copy()
            df["source_file"] = infer_source_file(path)
        if "raw_row_number_in_file" not in df.columns:
            df = df.reset_index(drop=True).copy()
            df["raw_row_number_in_file"] = df.index.astype("int64") + 1
        df["__parquet_file_name"] = path.name
        parts.append(df)
    if not parts:
        return pd.DataFrame()
    return pd.concat(parts, ignore_index=True)


def sample_conflicts(df: pd.DataFrame, coarse_key: list[str], sample_keys: int) -> list[dict]:
    g = (
        df.groupby(coarse_key, dropna=False)
        .agg(
            n_rows=("assigned_power_mw", "size"),
            n_source_files=("source_file", "nunique"),
            n_assigned_power=("assigned_power_mw", "nunique"),
        )
        .reset_index()
    )
    bad = g[(g["n_source_files"] > 1) & (g["n_assigned_power"] > 1)].head(sample_keys)
    out: list[dict] = []
    for _, row in bad.iterrows():
        mask = pd.Series(True, index=df.index)
        for k in coarse_key:
            mask &= df[k].eq(row[k])
        rows = (
            df.loc[
                mask,
                coarse_key + ["source_file", "__parquet_file_name", "raw_row_number_in_file", "assigned_power_mw"]
            ]
            .sort_values(["source_file", "raw_row_number_in_file"])
            .to_dict(orient="records")
        )
        key_dict = {k: row[k] for k in coarse_key}
        out.append({"key": key_dict, "rows": rows})
    return out


def main() -> None:
    args = parse_args()
    base_dir = Path(args.input_dir)
    family = "pibca"
    files = discover_files(base_dir, family=family, month=args.month)

    if not files:
        raise SystemExit(f"No parquet files found for {family} month={args.month} in {base_dir}")

    df = load_family(files)

    required = ["date", "session_number", "period", "unit_code", "assigned_power_mw", "source_file"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise SystemExit(f"Missing required columns: {missing}")

    coarse_key = ["date", "session_number", "period", "unit_code"]
    snapshot_key = ["source_file"] + coarse_key

    global_dups = int(df.duplicated(subset=coarse_key, keep=False).sum())
    snapshot_dups = int(df.duplicated(subset=snapshot_key, keep=False).sum())

    coarse_groups = (
        df.groupby(coarse_key, dropna=False)
        .agg(
            n_rows=("assigned_power_mw", "size"),
            n_source_files=("source_file", "nunique"),
            n_assigned_power=("assigned_power_mw", "nunique"),
        )
        .reset_index()
    )

    repeated_coarse_groups = int((coarse_groups["n_rows"] > 1).sum())
    cross_snapshot_revisions = int((coarse_groups["n_source_files"] > 1).sum())
    conflicting_cross_snapshot = int(
        ((coarse_groups["n_source_files"] > 1) & (coarse_groups["n_assigned_power"] > 1)).sum()
    )

    snapshot_groups = (
        df.groupby(snapshot_key, dropna=False)
        .agg(
            n_rows=("assigned_power_mw", "size"),
            n_assigned_power=("assigned_power_mw", "nunique"),
        )
        .reset_index()
    )

    repeated_within_snapshot = int((snapshot_groups["n_rows"] > 1).sum())
    conflicting_within_snapshot = int(
        ((snapshot_groups["n_rows"] > 1) & (snapshot_groups["n_assigned_power"] > 1)).sum()
    )

    output = {
        "family": family,
        "month": args.month,
        "input_dir": str(base_dir),
        "files_scanned": len(files),
        "rows_loaded": int(len(df)),
        "coarse_key": coarse_key,
        "snapshot_key": snapshot_key,
        "global_duplicate_rows_on_coarse_key": global_dups,
        "global_duplicate_rows_on_snapshot_key": snapshot_dups,
        "repeated_coarse_groups": repeated_coarse_groups,
        "cross_snapshot_revisions": cross_snapshot_revisions,
        "conflicting_cross_snapshot_groups": conflicting_cross_snapshot,
        "repeated_within_snapshot_groups": repeated_within_snapshot,
        "conflicting_within_snapshot_groups": conflicting_within_snapshot,
        "sample_conflicting_cross_snapshot_groups": sample_conflicts(
            df=df,
            coarse_key=coarse_key,
            sample_keys=args.sample_keys,
        ),
    }

    print(json.dumps(output, indent=2, default=str))


if __name__ == "__main__":
    main()
