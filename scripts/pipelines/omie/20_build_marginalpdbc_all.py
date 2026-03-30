from __future__ import annotations

from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[3]

INPUT_DIR = PROJECT_ROOT / "data/processed/omie/mercado_diario/precios/marginalpdbc"
OUTPUT_FILE = PROJECT_ROOT / "data/processed/omie/mercado_diario/precios/marginalpdbc_all.parquet"

MTU60_ALLOWED_COUNTS = {23, 24, 25}
MTU15_ALLOWED_COUNTS = {92, 96, 100}


def infer_mtu_from_row_count(n_rows: int) -> int | None:
    if n_rows in MTU60_ALLOWED_COUNTS:
        return 60
    if n_rows in MTU15_ALLOWED_COUNTS:
        return 15
    return None


def main() -> None:
    files = sorted(INPUT_DIR.glob("*.parquet"))
    files = [f for f in files if f.name != OUTPUT_FILE.name]

    if not files:
        raise SystemExit(f"No parquet files found in {INPUT_DIR}")

    dfs = [pd.read_parquet(f) for f in files]
    df = pd.concat(dfs, ignore_index=True)

    # Basic normalization
    df["date"] = pd.to_datetime(df["date"]).dt.date.astype(str)
    df["period"] = pd.to_numeric(df["period"], errors="raise").astype(int)

    # Backward compatibility with older parsed files (before mtu columns existed)
    if "mtu_minutes" not in df.columns:
        df["mtu_minutes"] = pd.NA

    if "n_periods_in_file" not in df.columns:
        df["n_periods_in_file"] = pd.NA

    # Ensure version suffix is sortable numerically
    if "version_suffix" in df.columns:
        df["version_suffix_num"] = pd.to_numeric(df["version_suffix"], errors="coerce")
    else:
        df["version_suffix"] = ""
        df["version_suffix_num"] = -1

    # Sort so "keep last" keeps the highest version suffix for same (date, period)
    df = df.sort_values(
        ["date", "period", "version_suffix_num", "source_file"],
        na_position="last",
    ).reset_index(drop=True)

    # Detect duplicated (date, period), which usually means multiple versions were parsed
    dup_mask = df.duplicated(subset=["date", "period"], keep=False)
    n_dup_rows = int(dup_mask.sum())

    if n_dup_rows > 0:
        dup_pairs = (
            df.loc[dup_mask, ["date", "period"]]
            .drop_duplicates()
            .sort_values(["date", "period"])
        )
        print(
            f"WARNING: Found {n_dup_rows} duplicated rows across "
            f"{len(dup_pairs)} (date, period) keys (likely multiple versions)."
        )
        print("Keeping latest version by version_suffix for each (date, period).")

    # Keep latest version per (date, period)
    df_latest = df.drop_duplicates(subset=["date", "period"], keep="last").copy()

    # Date-level rows/day checks (MTU-aware, DST-aware)
    rows_per_day = (
        df_latest.groupby("date", as_index=False)
        .size()
        .rename(columns={"size": "n_rows"})
        .sort_values("date")
    )
    rows_per_day["mtu_minutes_inferred"] = rows_per_day["n_rows"].map(infer_mtu_from_row_count)
    bad_days = rows_per_day[rows_per_day["mtu_minutes_inferred"].isna()].copy()

    # If row-level mtu_minutes is present, compare date-level consistency
    if df_latest["mtu_minutes"].notna().any():
        mtu_by_date = (
            df_latest.groupby("date", as_index=False)["mtu_minutes"]
            .agg(lambda s: sorted(set(int(x) for x in s.dropna())))
            .rename(columns={"mtu_minutes": "mtu_values_in_rows"})
        )
        rows_per_day = rows_per_day.merge(mtu_by_date, on="date", how="left")
    else:
        rows_per_day["mtu_values_in_rows"] = None

    # Final sort for analysis
    df_latest = df_latest.sort_values(["date", "period"]).reset_index(drop=True)

    # Drop helper column(s) before saving (keep output clean)
    if "version_suffix_num" in df_latest.columns:
        df_latest = df_latest.drop(columns=["version_suffix_num"])

    # Write output
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    df_latest.to_parquet(OUTPUT_FILE, index=False)

    # Summary
    print(f"Input files:             {len(files)}")
    print(f"Output rows (latest):    {len(df_latest)}")
    print(f"Date range:              {df_latest['date'].min()} -> {df_latest['date'].max()}")
    print(f"Output file:             {OUTPUT_FILE}")

    mtu_counts = rows_per_day["mtu_minutes_inferred"].value_counts(dropna=False).to_dict()
    print(f"Days by inferred MTU:    {mtu_counts}")

    if bad_days.empty:
        print("Rows/day check:          OK (all days match MTU60/MTU15 allowed counts incl. DST)")
    else:
        print("Rows/day check:          WARNING (unexpected row counts found)")
        print(bad_days.to_string(index=False))


if __name__ == "__main__":
    main()