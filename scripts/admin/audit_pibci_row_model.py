from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--month", required=True)
    parser.add_argument(
        "--input-dir",
        default="data/processed/omie/mercado_intradiario_subastas/programas/pibci",
    )
    parser.add_argument("--sample-keys", type=int, default=10)
    parser.add_argument("--top-varying-columns", type=int, default=25)
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


def varying_columns_summary(
    df: pd.DataFrame,
    key_cols: list[str],
    top_n: int,
) -> list[dict]:
    excluded = set(key_cols) | {"__parquet_file_name"}
    candidate_cols = [c for c in df.columns if c not in excluded]

    repeated = df.groupby(key_cols, dropna=False).size().rename("n_rows").reset_index()
    repeated = repeated[repeated["n_rows"] > 1]
    if repeated.empty:
        return []

    keys = repeated[key_cols]
    flagged = df.merge(keys, on=key_cols, how="inner")

    out = []
    for col in candidate_cols:
        nunique = (
            flagged.groupby(key_cols, dropna=False)[col]
            .nunique(dropna=False)
            .rename("nunique")
            .reset_index()
        )
        varying_groups = int((nunique["nunique"] > 1).sum())
        if varying_groups > 0:
            out.append({"column": col, "varying_groups": varying_groups})
    out.sort(key=lambda x: (-x["varying_groups"], x["column"]))
    return out[:top_n]


def sample_conflicts(df: pd.DataFrame, key_cols: list[str], sample_keys: int) -> list[dict]:
    g = (
        df.groupby(key_cols, dropna=False)
        .agg(
            n_rows=("assigned_power_mw", "size"),
            n_assigned_power=("assigned_power_mw", "nunique"),
        )
        .reset_index()
    )
    bad = g[(g["n_rows"] > 1) & (g["n_assigned_power"] > 1)].head(sample_keys)
    out: list[dict] = []
    for _, row in bad.iterrows():
        mask = pd.Series(True, index=df.index)
        for k in key_cols:
            mask &= df[k].eq(row[k])
        cols = key_cols + ["raw_row_number_in_file", "assigned_power_mw"]
        if "offer_type" in df.columns:
            cols.append("offer_type")
        if "__parquet_file_name" in df.columns:
            cols.append("__parquet_file_name")
        rows = df.loc[mask, cols].sort_values("raw_row_number_in_file").to_dict(orient="records")
        key_dict = {k: row[k] for k in key_cols}
        out.append({"key": key_dict, "rows": rows})
    return out


def main() -> None:
    args = parse_args()
    base_dir = Path(args.input_dir)
    family = "pibci"
    files = discover_files(base_dir, family=family, month=args.month)

    if not files:
        raise SystemExit(f"No parquet files found for {family} month={args.month} in {base_dir}")

    df = load_family(files)

    required = ["date", "session_number", "period", "unit_code", "assigned_power_mw", "source_file"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise SystemExit(f"Missing required columns: {missing}")

    coarse_key = ["date", "session_number", "period", "unit_code"]
    source_coarse_key = ["source_file"] + coarse_key

    global_dups = int(df.duplicated(subset=coarse_key, keep=False).sum())
    within_source_dups = int(df.duplicated(subset=source_coarse_key, keep=False).sum())

    groups = (
        df.groupby(source_coarse_key, dropna=False)
        .agg(
            n_rows=("assigned_power_mw", "size"),
            n_assigned_power=("assigned_power_mw", "nunique"),
        )
        .reset_index()
    )

    repeated_groups = int((groups["n_rows"] > 1).sum())
    conflicting_groups = int(((groups["n_rows"] > 1) & (groups["n_assigned_power"] > 1)).sum())

    varying_cols = varying_columns_summary(
        df=df,
        key_cols=source_coarse_key,
        top_n=args.top_varying_columns,
    )

    output = {
        "family": family,
        "month": args.month,
        "input_dir": str(base_dir),
        "files_scanned": len(files),
        "rows_loaded": int(len(df)),
        "coarse_key": coarse_key,
        "source_coarse_key": source_coarse_key,
        "global_duplicate_rows_on_coarse_key": global_dups,
        "within_source_duplicate_rows_on_source_coarse_key": within_source_dups,
        "repeated_within_source_groups": repeated_groups,
        "conflicting_within_source_groups": conflicting_groups,
        "top_varying_columns_inside_repeated_source_coarse_groups": varying_cols,
        "sample_conflicting_within_source_groups": sample_conflicts(
            df=df,
            key_cols=source_coarse_key,
            sample_keys=args.sample_keys,
        ),
    }

    print(json.dumps(output, indent=2, default=str))


if __name__ == "__main__":
    main()
