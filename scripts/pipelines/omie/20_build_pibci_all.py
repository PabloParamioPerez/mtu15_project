from __future__ import annotations

from pathlib import Path
import sys

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[3]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))


def main() -> None:
    processed_dir = PROJECT_ROOT / "data/processed/omie/mercado_intradiario_subastas/programas/pibci"
    output_path = processed_dir.parent / "pibci_all.parquet"

    files = sorted(processed_dir.glob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"No parquet files found in {processed_dir}")

    dfs = [pd.read_parquet(p) for p in files]
    df = pd.concat(dfs, ignore_index=True)

    input_files = len(files)

    df["version_suffix_num"] = pd.to_numeric(df["version_suffix"], errors="coerce")

    dedup_keys = [
        "source_file",
        "date",
        "session_number",
        "period",
        "unit_code",
        "assigned_power_mw",
        "unused_zero",
        "offer_type",
        "row_number_in_file",
    ]

    dup_mask = df.duplicated(subset=dedup_keys, keep=False)
    dup_rows = int(dup_mask.sum())
    dup_keys = int(df.loc[dup_mask, dedup_keys].drop_duplicates().shape[0])

    if dup_rows > 0:
        print(
            f"WARNING: Found {dup_rows} duplicated rows across {dup_keys} PIBCI row identities "
            f"(likely multiple versions)."
        )
        print("Keeping latest version by version_suffix for each parsed row identity.")

    df = (
        df.sort_values(dedup_keys + ["version_suffix_num"])
          .drop_duplicates(subset=dedup_keys, keep="last")
          .drop(columns=["version_suffix_num"])
          .sort_values(["date", "session_number", "period", "unit_code", "row_number_in_file"])
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

    rows_per_file = df.groupby("source_file").size()

    print(f"Input files:             {input_files}")
    print(f"Output rows (latest):    {len(df)}")
    print(f"Date range:              {df['date'].min()} -> {df['date'].max()}")
    print(f"Output file:             {output_path}")
    print(f"Days by inferred MTU:    {days_by_mtu}")
    print(
        f"Rows/file summary:       min={int(rows_per_file.min())}, "
        f"max={int(rows_per_file.max())}, mean={rows_per_file.mean():.2f}"
    )


if __name__ == "__main__":
    main()
