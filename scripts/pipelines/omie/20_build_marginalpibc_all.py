from __future__ import annotations

from pathlib import Path
import sys

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[3]


def main() -> None:
    processed_dir = PROJECT_ROOT / "data/processed/omie/mercado_intradiario_subastas/precios/marginalpibc"
    out_path = PROJECT_ROOT / "data/processed/omie/mercado_intradiario_subastas/precios/marginalpibc_all.parquet"

    files = sorted(processed_dir.glob("*.parquet"))
    if not files:
        print(f"No parquet files found in: {processed_dir}")
        sys.exit(0)

    dfs = []
    for p in files:
        try:
            df = pd.read_parquet(p)
            df["_processed_file"] = p.name  # diagnostic only
            dfs.append(df)
        except Exception as e:
            print(f"[WARN] Failed reading {p.name}: {e}")

    if not dfs:
        print("No readable parquet files found.")
        sys.exit(1)

    all_df = pd.concat(dfs, ignore_index=True)

    required_cols = ["date", "session_number", "period", "version_suffix"]
    missing = [c for c in required_cols if c not in all_df.columns]
    if missing:
        raise ValueError(f"Missing required columns in combined data: {missing}")

    # Normalize version suffix for proper ordering (latest version wins)
    all_df["_version_num"] = pd.to_numeric(all_df["version_suffix"], errors="coerce").fillna(-1).astype(int)

    key_cols = ["date", "session_number", "period"]

    # Diagnostics before dedupe
    dup_mask = all_df.duplicated(subset=key_cols, keep=False)
    n_dup_rows = int(dup_mask.sum())
    n_dup_keys = int(all_df.loc[dup_mask, key_cols].drop_duplicates().shape[0])

    if n_dup_rows > 0:
        print(
            f"WARNING: Found {n_dup_rows} duplicated rows across {n_dup_keys} "
            f"(date, session_number, period) keys (likely multiple versions)."
        )
        print("Keeping latest version by version_suffix for each (date, session_number, period).")

    # Keep latest version per key
    all_df = (
        all_df.sort_values(key_cols + ["_version_num", "source_file"], ascending=[True, True, True, True, True])
              .drop_duplicates(subset=key_cols, keep="last")
              .reset_index(drop=True)
    )

    # Final integrity check
    if all_df.duplicated(subset=key_cols).any():
        raise ValueError("Duplicate keys remain after dedupe on (date, session_number, period).")

    # Keep stable column order if present
    preferred_order = [
        "date",
        "session_number",
        "period",
        "price_es_eur_mwh",
        "price_pt_eur_mwh",
        "mtu_minutes",
        "n_periods_in_file",
        "market",
        "category",
        "file_family",
        "version_suffix",
        "source_file",
        "source_path",
    ]
    cols_present = [c for c in preferred_order if c in all_df.columns]
    other_cols = [c for c in all_df.columns if c not in cols_present and not c.startswith("_")]
    all_df = all_df[cols_present + other_cols]

    out_path.parent.mkdir(parents=True, exist_ok=True)
    all_df.to_parquet(out_path, index=False)

    # Summary
    date_series = pd.to_datetime(all_df["date"], errors="coerce")
    print(f"Input files:              {len(files)}")
    print(f"Output rows (latest):     {len(all_df)}")
    if date_series.notna().any():
        print(f"Date range:               {date_series.dt.date.min()} -> {date_series.dt.date.max()}")
    else:
        print("Date range:               (could not parse date column)")
    print(f"Output file:              {out_path}")

    # Useful distributions
    if "session_number" in all_df.columns:
        print("Rows by session_number:   ", all_df["session_number"].value_counts().sort_index().to_dict())

    per_date_session = all_df.groupby(["date", "session_number"]).size()
    print("Rows/(date,session) dist: ", per_date_session.value_counts().sort_index().to_dict())

    if "mtu_minutes" in all_df.columns:
        print("Rows by MTU:              ", all_df["mtu_minutes"].value_counts().sort_index().to_dict())


if __name__ == "__main__":
    main()
